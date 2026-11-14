//! Un broker Kafka pédagogique.
//!
//! Organisation du code :
//! - [`server`] : la couche réseau (accept TCP, découpage des trames) ;
//! - [`handlers`] : le décodage des requêtes, le dispatch et un module par API ;
//! - [`apis`] : la liste des API (et versions) que le broker annonce.

pub mod apis;
pub mod handlers;
pub mod server;

/// Configuration statique d'un broker.
#[derive(Debug, Clone)]
pub struct Config {
    /// Identifiant du broker dans le cluster (`node.id` dans Kafka).
    pub node_id: i32,
    /// Hôte annoncé aux clients (dans la réponse `Metadata`, à partir du ch.1).
    pub host: String,
    /// Port d'écoute, également annoncé aux clients.
    pub port: u16,
}

/// L'état partagé du broker. Il est enveloppé dans un `Arc` et partagé
/// entre toutes les connexions. Il s'enrichira au fil des chapitres
/// (topics, logs, groupes de consommateurs, client etcd...).
#[derive(Debug)]
pub struct Broker {
    pub config: Config,
}

impl Broker {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}
