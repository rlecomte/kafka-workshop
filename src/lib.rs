//! Un broker Kafka pédagogique.
//!
//! Organisation du code :
//! - [`server`] : la couche réseau (accept TCP, découpage des trames) ;
//! - [`handlers`] : le décodage des requêtes, le dispatch et un module par API ;
//! - [`apis`] : la liste des API (et versions) que le broker annonce ;
//! - [`topics`] : le registre des topics et de leurs partitions.

pub mod apis;
pub mod handlers;
pub mod server;
pub mod topics;

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
}

impl Broker {
    pub fn new(config: Config) -> Self {
        Self { config, topics: topics::Topics::default() }
    }
}
