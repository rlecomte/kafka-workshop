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

    /// Démarre ou arrête les fetchers pour qu'il y en ait exactement un par
    /// partition dont ce broker est follower, vers le leader actuel.
    pub(crate) fn sync_fetchers(self: &Arc<Self>) {
        todo!("ch.7 : sync_fetchers")
    }

    /// Côté leader : retire de l'ISR les followers en retard.
    pub(crate) async fn shrink_isrs(&self) {
        todo!("ch.7 : shrink_isrs")
    }
}

/// La boucle d'un follower : copier le log du leader, indéfiniment.
async fn fetch_loop(broker: Arc<Broker>, replica: Arc<Replica>, leader: i32) {
    todo!("ch.7 : fetch_loop")
}

/// Envoie des Fetch au leader sur une connexion, jusqu'à une erreur.
async fn fetch_from(broker: &Broker, replica: &Replica, client: &mut BrokerClient) -> anyhow::Result<()> {
    todo!("ch.7 : fetch_from")
}
