//! Un broker Kafka pédagogique.
//!
//! Organisation du code :
//! - [`server`] : la couche réseau (accept TCP, découpage des trames) ;
//! - [`handlers`] : le décodage des requêtes, le dispatch et un module par API ;
//! - [`apis`] : la liste des API (et versions) que le broker annonce ;
//! - [`topics`] : le registre des topics et de leurs partitions ;
//! - [`record_batch`] : le format binaire des batches de messages ;
//! - [`log`] : le stockage sur disque d'une partition ;
//! - [`replica`] : les partitions hébergées par ce broker et leurs logs.

// Squelette du workshop : tant que les `todo!()` ne sont pas remplacés,
// certains imports, paramètres et fonctions ne servent pas encore.
#![allow(dead_code, unused_imports, unused_variables, unused_mut)]

pub mod apis;
pub mod handlers;
pub mod log;
pub mod record_batch;
pub mod replica;
pub mod server;
pub mod topics;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

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
}

impl Broker {
    pub fn new(config: Config) -> Self {
        let replicas = replica::ReplicaManager::new(config.data_dir.clone(), log::LogConfig::default());
        Self { config, topics: topics::Topics::default(), replicas, next_producer_id: AtomicI64::new(0) }
    }

    /// Crée un topic : l'enregistre dans le registre, puis crée les logs de
    /// ses partitions sur disque.
    pub fn create_topic(&self, new: NewTopic, validate_only: bool) -> Result<Arc<Topic>, TopicError> {
        todo!("ch.2 : create_topic")
    }
}
