//! Un broker Kafka pédagogique.
//!
//! Organisation du code :
//! - [`server`] : la couche réseau (accept TCP, découpage des trames) ;
//! - [`handlers`] : le décodage des requêtes, le dispatch et un module par API ;
//! - [`apis`] : la liste des API (et versions) que le broker annonce ;
//! - [`topics`] : le registre des topics et de leurs partitions ;
//! - [`record_batch`] : le format binaire des batches de messages ;
//! - [`log`] : le stockage sur disque d'une partition ;
//! - [`replica`] : les partitions hébergées par ce broker et leurs logs ;
//! - [`groups`] : le coordinateur des consumer groups et leurs offsets ;
//! - [`cluster`] : le mode cluster, coordonné par etcd.

// Squelette du workshop : tant que les `todo!()` ne sont pas remplacés,
// certains imports, paramètres et fonctions ne servent pas encore.
#![allow(dead_code, unused_imports, unused_variables, unused_mut)]

pub mod apis;
pub mod cluster;
pub mod groups;
pub mod handlers;
pub mod log;
pub mod record_batch;
pub mod replica;
pub mod replication;
pub mod server;
pub mod topics;

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use topics::{NewTopic, Topic, TopicError};

/// Configuration statique d'un broker.
#[derive(Debug, Clone)]
pub struct Config {
    /// Identifiant du broker dans le cluster (`node.id` dans Kafka).
    pub node_id: i32,
    /// Hôte annoncé aux clients dans la réponse `Metadata`.
    pub host: String,
    /// Port d'écoute, également annoncé aux clients.
    pub port: u16,
    /// Identifiant du cluster, renvoyé dans `Metadata`.
    pub cluster_id: String,
    /// Nombre de partitions d'un topic créé sans préciser ce nombre
    /// (`num.partitions` dans Kafka).
    pub default_num_partitions: i32,
    /// Créer automatiquement un topic inconnu demandé dans `Metadata`
    /// (`auto.create.topics.enable` dans Kafka).
    pub auto_create_topics: bool,
    /// Répertoire où sont stockés les logs (`log.dirs` dans Kafka).
    pub data_dir: PathBuf,
    /// Taille maximale d'un batch (`message.max.bytes` dans Kafka).
    pub max_message_bytes: usize,
    /// Paramètres des logs : segments, index, rétention.
    pub log: log::LogConfig,
    /// Fréquence de vérification de la rétention
    /// (`log.retention.check.interval.ms` dans Kafka).
    pub retention_check_interval: Duration,
    /// Paramètres des consumer groups.
    pub group: groups::group::GroupConfig,
    /// Nombre de partitions de `__consumer_offsets`
    /// (`offsets.topic.num.partitions`, 50 dans Kafka).
    pub offsets_topic_partitions: i32,
    /// Mode cluster (coordonné par etcd), ou `None` pour un broker autonome.
    pub cluster: Option<cluster::ClusterConfig>,
    /// Nombre minimal de répliques dans l'ISR pour accepter un Produce avec
    /// acks=all (`min.insync.replicas`).
    pub min_insync_replicas: usize,
    /// Un follower qui n'a pas rattrapé le leader depuis ce délai sort de
    /// l'ISR (`replica.lag.time.max.ms`, 30 s dans Kafka).
    pub replica_lag_time_max: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: 0,
            host: "localhost".into(),
            port: 9092,
            cluster_id: "workshop-cluster".into(),
            default_num_partitions: 1,
            auto_create_topics: true,
            data_dir: PathBuf::from("data"),
            max_message_bytes: 1024 * 1024,
            log: log::LogConfig::default(),
            retention_check_interval: Duration::from_secs(300),
            group: groups::group::GroupConfig::default(),
            offsets_topic_partitions: 10,
            cluster: None,
            min_insync_replicas: 1,
            replica_lag_time_max: Duration::from_secs(10),
        }
    }
}

/// L'état partagé du broker. Il est enveloppé dans un `Arc` et partagé
/// entre toutes les connexions.
#[derive(Debug)]
pub struct Broker {
    pub config: Config,
    pub topics: topics::Topics,
    pub replicas: replica::ReplicaManager,
    /// Prochain identifiant de producer attribué par `InitProducerId`.
    pub next_producer_id: AtomicI64,
    /// Annulé à l'arrêt du broker : toutes les tâches de fond s'arrêtent.
    pub shutdown: CancellationToken,
    pub groups: groups::GroupCoordinator,
    /// L'état du cluster, en mode cluster.
    pub cluster: Option<cluster::node::ClusterNode>,
    /// Les tâches de réplication (une par partition dont ce broker est follower).
    pub fetchers: replication::Fetchers,
}

impl Broker {
    /// Crée le broker. En mode autonome, les topics et les logs présents dans
    /// le répertoire de données sont rechargés. En mode cluster, les topics
    /// viendront d'etcd (voir [`Broker::start`]).
    pub fn new(config: Config) -> std::io::Result<Self> {
        let replicas = replica::ReplicaManager::new(config.node_id, config.data_dir.clone(), config.log.clone());
        let topics = topics::Topics::new(config.node_id);
        if config.cluster.is_none() {
            for topic in replicas.load_existing(config.node_id)? {
                tracing::info!(topic = %topic.name, partitions = topic.partitions.len(), "topic rechargé");
                topics.restore(topic);
            }
        }
        let broker = Self {
            groups: groups::GroupCoordinator::new(config.group.clone()),
            cluster: config.cluster.clone().map(cluster::node::ClusterNode::new),
            config,
            topics,
            replicas,
            next_producer_id: AtomicI64::new(0),
            shutdown: CancellationToken::new(),
            fetchers: Default::default(),
        };
        if broker.cluster.is_none()
            && let Some(topic) = broker.topics.get(groups::offsets::OFFSETS_TOPIC)
        {
            for partition in &topic.partitions {
                broker.load_offsets_partition(partition.index)?;
            }
        }
        Ok(broker)
    }

    /// Relit une partition de `__consumer_offsets` pour retrouver les offsets
    /// commités qu'elle contient.
    pub(crate) fn load_offsets_partition(&self, partition: i32) -> std::io::Result<()> {
        if let Some(replica) = self.replicas.get(groups::offsets::OFFSETS_TOPIC, partition) {
            let commits = groups::offsets::decode_commits(replica.read_all()?);
            tracing::info!(partition, commits = commits.len(), "offsets rechargés");
            self.groups.apply_commits(commits);
        }
        Ok(())
    }

    /// Lance une tâche de fond, arrêtée automatiquement à l'arrêt du broker.
    pub fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = task => {}
            }
        });
    }

    /// Lance une tâche de fond qu'on peut aussi annuler individuellement.
    pub fn spawn_abortable(&self, task: impl Future<Output = ()> + Send + 'static) -> tokio::task::AbortHandle {
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = task => {}
            }
        })
        .abort_handle()
    }

    /// Démarre le broker : tâches de fond et, en mode cluster, connexion à etcd.
    pub async fn start(self: &Arc<Self>) -> anyhow::Result<()> {
        let broker = self.clone();
        self.spawn(async move {
            let mut interval = tokio::time::interval(broker.config.retention_check_interval);
            loop {
                interval.tick().await;
                broker.replicas.apply_retention();
            }
        });

        let broker = self.clone();
        self.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                broker.groups.tick();
            }
        });

        if self.cluster.is_some() {
            self.join_cluster().await?;

            let broker = self.clone();
            self.spawn(async move {
                let mut interval = tokio::time::interval(broker.config.replica_lag_time_max / 2);
                loop {
                    interval.tick().await;
                    broker.shrink_isrs().await;
                }
            });
        }
        Ok(())
    }

    /// Attend que le high watermark de `replica` atteigne `offset`, au plus
    /// `timeout`. Renvoie `false` en cas de délai dépassé.
    pub async fn wait_for_high_watermark(&self, replica: &replica::Replica, offset: i64, timeout: Duration) -> bool {
        todo!("ch.7 : wait_for_high_watermark")
    }

    // -----------------------------------------------------------------------
    // Vue du cluster, identique en mode autonome et en mode cluster
    // -----------------------------------------------------------------------

    /// Les brokers vivants, triés par identifiant.
    pub fn live_brokers(&self) -> Vec<cluster::BrokerRecord> {
        match self.cluster() {
            Some(node) => node.state().brokers.into_values().collect(),
            None => vec![cluster::BrokerRecord {
                node_id: self.config.node_id,
                host: self.config.host.clone(),
                port: self.config.port,
            }],
        }
    }

    /// Le controller, ou -1 s'il n'y en a pas.
    pub fn controller_id(&self) -> i32 {
        match self.cluster() {
            Some(node) => node.state().controller.unwrap_or(-1),
            None => self.config.node_id,
        }
    }

    /// La réplique locale d'une partition dont ce broker est le leader.
    pub fn leader_replica(&self, topic: &str, partition: i32) -> Result<Arc<replica::Replica>, kafka_protocol::ResponseError> {
        use kafka_protocol::ResponseError;
        let metadata = self.topics.get(topic).and_then(|t| t.partitions.iter().find(|p| p.index == partition).cloned());
        let Some(metadata) = metadata else { return Err(ResponseError::UnknownTopicOrPartition) };
        if metadata.leader != self.config.node_id {
            return Err(ResponseError::NotLeaderOrFollower);
        }
        self.replicas.get(topic, partition).ok_or(ResponseError::NotLeaderOrFollower)
    }

    /// Le coordinateur d'un groupe : le leader de sa partition de
    /// `__consumer_offsets`. Renvoie la partition et le leader (-1 si aucun).
    pub fn coordinator_for(&self, group_id: &str) -> Option<(i32, i32)> {
        let topic = self.topics.get(groups::offsets::OFFSETS_TOPIC)?;
        let partition = groups::offsets::partition_for(group_id, topic.partitions.len() as i32);
        let leader = topic.partitions.iter().find(|p| p.index == partition).map_or(-1, |p| p.leader);
        Some((partition, leader))
    }

    /// Ce broker est-il le coordinateur de ce groupe (et ses offsets sont-ils
    /// chargés) ? Un broker autonome coordonne tous les groupes.
    pub fn is_coordinator(&self, group_id: &str) -> bool {
        if self.cluster.is_none() {
            return true;
        }
        match self.coordinator_for(group_id) {
            Some((partition, leader)) => leader == self.config.node_id && self.offsets_loaded(partition),
            None => false,
        }
    }

    // -----------------------------------------------------------------------
    // Création des topics
    // -----------------------------------------------------------------------

    /// Crée un topic. En mode autonome : l'enregistre dans le registre, puis
    /// crée les logs de ses partitions. En mode cluster : l'écrit dans etcd.
    pub async fn create_topic(&self, new: NewTopic, validate_only: bool) -> Result<Arc<Topic>, TopicError> {
        if self.cluster.is_some() {
            return self.create_topic_in_cluster(new, validate_only).await;
        }
        let topic = self.topics.create(new, validate_only)?;
        if !validate_only {
            self.replicas.create_replicas(&topic).map_err(|e| {
                tracing::error!(topic = %topic.name, "impossible de créer les logs : {e}");
                TopicError::Storage(e.to_string())
            })?;
        }
        Ok(topic)
    }

    /// Crée `__consumer_offsets` s'il n'existe pas encore, avec 3 répliques
    /// au plus (`offsets.topic.replication.factor`).
    pub async fn ensure_offsets_topic(&self) -> Result<Arc<Topic>, TopicError> {
        if let Some(topic) = self.topics.get(groups::offsets::OFFSETS_TOPIC) {
            return Ok(topic);
        }
        let new = NewTopic {
            name: groups::offsets::OFFSETS_TOPIC.to_string(),
            num_partitions: self.config.offsets_topic_partitions,
            replication_factor: self.live_brokers().len().clamp(1, 3) as i16,
        };
        match self.create_topic(new, false).await {
            Err(TopicError::AlreadyExists(_)) => {
                Ok(self.topics.get(groups::offsets::OFFSETS_TOPIC).expect("créé entre-temps"))
            }
            other => other,
        }
    }
}
