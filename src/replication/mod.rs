//! La réplication : chaque follower copie le log de son leader en lui
//! envoyant des `Fetch` en boucle, comme un consumer (mais avec son
//! identifiant de broker dans `replica_id`).
//!
//! Le leader, lui, suit la position de chaque follower (voir
//! [`crate::replica::Replica::record_follower_fetch`]) pour calculer le
//! high watermark et maintenir l'ISR.

pub mod client;

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{BrokerId, FetchRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use tokio::task::AbortHandle;

use crate::Broker;
use crate::record_batch;
use crate::replica::Replica;
use client::BrokerClient;

/// Version de Fetch utilisée entre brokers.
const FETCH_VERSION: i16 = 12;

/// Un fetcher en cours : la partition suivie, son leader et l'epoch.
#[derive(Debug)]
pub struct Fetcher {
    leader: i32,
    leader_epoch: i32,
    task: AbortHandle,
}

/// Les fetchers en cours, par (topic, partition).
pub type Fetchers = std::sync::Mutex<HashMap<(String, i32), Fetcher>>;

impl Broker {
    /// L'adresse d'un broker vivant.
    pub fn broker_address(&self, node_id: i32) -> Option<SocketAddr> {
        let broker = self.live_brokers().into_iter().find(|b| b.node_id == node_id)?;
        (broker.host.as_str(), broker.port).to_socket_addrs().ok()?.next()
    }

    /// Les partitions que ce broker doit suivre, avec leur leader et l'epoch.
    fn wanted_fetchers(&self) -> HashMap<(String, i32), (i32, i32)> {
        let me = self.config.node_id;
        let mut wanted = HashMap::new();
        for topic in self.topics.list() {
            for p in &topic.partitions {
                if p.leader >= 0 && p.leader != me && p.replicas.contains(&me) {
                    wanted.insert((topic.name.clone(), p.index), (p.leader, p.leader_epoch));
                }
            }
        }
        wanted
    }

    /// Arrête les fetchers qui ne correspondent plus aux métadonnées (la
    /// partition a changé de leader ou d'epoch, ou ce broker en est leader).
    pub(crate) fn stop_stale_fetchers(&self) {
        let wanted = self.wanted_fetchers();
        self.fetchers.lock().unwrap().retain(|key, fetcher| {
            let keep = wanted.get(key) == Some(&(fetcher.leader, fetcher.leader_epoch));
            if !keep {
                fetcher.task.abort();
            }
            keep
        });
    }

    /// Démarre un fetcher pour chaque partition suivie qui n'en a pas.
    pub(crate) fn start_missing_fetchers(self: &Arc<Self>) {
        let mut fetchers = self.fetchers.lock().unwrap();
        for (key, (leader, leader_epoch)) in self.wanted_fetchers() {
            if fetchers.contains_key(&key) {
                continue;
            }
            let Some(replica) = self.replicas.get(&key.0, key.1) else { continue };
            tracing::info!(topic = %key.0, partition = key.1, leader, "réplication depuis le leader");
            let task = self.spawn_abortable(fetch_loop(self.clone(), replica, leader));
            fetchers.insert(key, Fetcher { leader, leader_epoch, task });
        }
    }

    /// Côté leader : retire de l'ISR les followers en retard.
    pub(crate) async fn shrink_isrs(&self) {
        for replica in self.replicas.all() {
            if let Some(isr) = replica.isr_to_shrink(self.config.replica_lag_time_max) {
                tracing::info!(topic = %replica.topic, partition = replica.partition, ?isr, "follower(s) en retard retiré(s) de l'ISR");
                self.alter_isr(&replica.topic, replica.partition, isr).await;
            }
        }
    }
}

/// La boucle d'un follower : copier le log du leader, indéfiniment.
async fn fetch_loop(broker: Arc<Broker>, replica: Arc<Replica>, leader: i32) {
    let client_id = format!("broker-{}-fetcher", broker.config.node_id);
    loop {
        if let Some(addr) = broker.broker_address(leader) {
            match BrokerClient::connect(addr, client_id.clone()).await {
                Ok(mut client) => {
                    if let Err(e) = fetch_from(&broker, &replica, &mut client).await {
                        tracing::debug!(topic = %replica.topic, partition = replica.partition, leader, "réplication interrompue : {e:#}");
                    }
                }
                Err(e) => tracing::debug!(leader, "leader injoignable : {e:#}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Envoie des Fetch au leader sur une connexion, jusqu'à une erreur.
async fn fetch_from(broker: &Broker, replica: &Replica, client: &mut BrokerClient) -> anyhow::Result<()> {
    loop {
        let offset = replica.log_end_offset();
        let request = FetchRequest::default()
            .with_replica_id(BrokerId(broker.config.node_id))
            .with_max_wait_ms(500)
            .with_min_bytes(1)
            .with_max_bytes(1024 * 1024)
            .with_session_id(0)
            .with_session_epoch(-1)
            .with_topics(vec![FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_string(replica.topic.clone())))
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(replica.partition)
                    .with_current_leader_epoch(replica.leader_epoch())
                    .with_fetch_offset(offset)
                    .with_partition_max_bytes(1024 * 1024)])]);
        let response = client.send(&request, FETCH_VERSION).await?;
        let data = response
            .responses
            .first()
            .and_then(|t| t.partitions.first())
            .ok_or_else(|| anyhow::anyhow!("réponse Fetch vide"))?;

        match ResponseError::try_from_code(data.error_code) {
            None => {
                let records = data.records.clone().unwrap_or_default();
                let batches = record_batch::parse_batches(records)?;
                replica.append_as_follower(batches, data.high_watermark)?;
            }
            Some(ResponseError::OffsetOutOfRange) if offset < data.log_start_offset => {
                // Le leader a supprimé ces données (rétention) : on repart de
                // son début de log.
                tracing::info!(topic = %replica.topic, partition = replica.partition, start = data.log_start_offset, "log réinitialisé");
                replica.reset_to(data.log_start_offset)?;
            }
            Some(ResponseError::OffsetOutOfRange) => {
                // Le follower a des données que le leader n'a pas : on les
                // supprime, jusqu'au high watermark du leader.
                replica.truncate_to(data.high_watermark)?;
            }
            Some(error) => anyhow::bail!("le leader répond {error:?}"),
        }
    }
}
