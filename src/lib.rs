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
//! - [`groups`] : le coordinateur des consumer groups et leurs offsets.

// Squelette du workshop : tant que les `todo!()` ne sont pas remplacés,
// certains imports, paramètres et fonctions ne servent pas encore.
#![allow(dead_code, unused_imports, unused_variables, unused_mut)]

pub mod apis;
pub mod groups;
pub mod handlers;
pub mod log;
pub mod record_batch;
pub mod replica;
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
        }
    }
}

/// L'état partagé du broker. Il est enveloppé dans un `Arc` et partagé
/// entre toutes les connexions. Il s'enrichira au fil des chapitres
/// (logs, groupes de consommateurs, client etcd...).
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
}

impl Broker {
    /// Crée le broker, en rechargeant les topics et les logs présents dans
    /// le répertoire de données.
    pub fn new(config: Config) -> std::io::Result<Self> {
        let replicas = replica::ReplicaManager::new(config.data_dir.clone(), config.log.clone());
        let topics = topics::Topics::default();
        for topic in replicas.load_existing()? {
            tracing::info!(topic = %topic.name, partitions = topic.partitions.len(), "topic rechargé");
            topics.restore(topic);
        }
        let broker = Self {
            groups: groups::GroupCoordinator::new(config.group.clone()),
            config,
            topics,
            replicas,
            next_producer_id: AtomicI64::new(0),
            shutdown: CancellationToken::new(),
        };
        broker.load_offsets()?;
        Ok(broker)
    }

    /// Relit `__consumer_offsets` pour retrouver les offsets commités.
    fn load_offsets(&self) -> std::io::Result<()> {
        todo!("ch.5 : load_offsets")
    }

    /// Crée `__consumer_offsets` s'il n'existe pas encore.
    pub fn ensure_offsets_topic(&self) -> Result<Arc<Topic>, TopicError> {
        todo!("ch.5 : ensure_offsets_topic")
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

    /// Lance les tâches de fond du broker.
    pub fn start_background_tasks(self: &Arc<Self>) {
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
    }

    /// Crée un topic : l'enregistre dans le registre, puis crée les logs de
    /// ses partitions sur disque.
    pub fn create_topic(&self, new: NewTopic, validate_only: bool) -> Result<Arc<Topic>, TopicError> {
        let topic = self.topics.create(new, validate_only)?;
        if !validate_only {
            self.replicas.create_replicas(&topic).map_err(|e| {
                tracing::error!(topic = %topic.name, "impossible de créer les logs : {e}");
                TopicError::Storage(e.to_string())
            })?;
        }
        Ok(topic)
    }
}
