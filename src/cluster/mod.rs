//! Le mode cluster : plusieurs brokers coordonnés par etcd.
//!
//! etcd joue le rôle qu'avait ZooKeeper dans Kafka avant KRaft. Il stocke,
//! sous un préfixe commun (`/workshop` par défaut) :
//!
//! ```text
//! /workshop/brokers/<id>              {"node_id":0,"host":"localhost","port":9092}   (lease)
//! /workshop/controller                {"node_id":0}                                   (lease)
//! /workshop/topics/<topic>            {"id":"...","partitions":[[0,1,2],[1,2,0]]}
//! /workshop/partitions/<topic>/<p>    {"leader":0,"leader_epoch":0,"isr":[0,1,2]}
//! ```
//!
//! - Les clés des brokers et du controller sont attachées au **lease** de leur
//!   broker : si le broker s'arrête, elles disparaissent quand le lease expire.
//! - Chaque broker garde une copie de tout ce préfixe en mémoire
//!   ([`ClusterState`]), tenue à jour par un **watch**.

pub mod etcd;
pub mod node;

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use etcd::{EventKind, KeyValue, WatchEvent};

/// Paramètres du mode cluster.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// L'adresse d'etcd, par exemple `http://localhost:2379`.
    pub etcd_url: String,
    /// Le préfixe des clés du cluster dans etcd.
    pub prefix: String,
    /// La durée de vie du lease d'un broker : un broker arrêté est considéré
    /// comme mort après ce délai.
    pub lease_ttl: Duration,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self { etcd_url: "http://localhost:2379".into(), prefix: "/workshop".into(), lease_ttl: Duration::from_secs(6) }
    }
}

/// Les clés etcd du cluster.
#[derive(Debug, Clone)]
pub struct Keys {
    pub prefix: String,
}

impl Keys {
    pub fn broker(&self, id: i32) -> String {
        format!("{}/brokers/{id}", self.prefix)
    }
    pub fn controller(&self) -> String {
        format!("{}/controller", self.prefix)
    }
    pub fn topic(&self, name: &str) -> String {
        format!("{}/topics/{name}", self.prefix)
    }
    pub fn partition(&self, topic: &str, partition: i32) -> String {
        format!("{}/partitions/{topic}/{partition}", self.prefix)
    }
    /// Le préfixe de toutes les clés du cluster (avec le `/` final).
    pub fn all(&self) -> String {
        format!("{}/", self.prefix)
    }
}

/// `/workshop/brokers/<id>` : un broker vivant et son adresse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRecord {
    pub node_id: i32,
    pub host: String,
    pub port: u16,
}

/// `/workshop/controller` : le broker controller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerRecord {
    pub node_id: i32,
}

/// `/workshop/topics/<topic>` : l'identifiant d'un topic et l'assignation de
/// ses partitions (la liste des répliques de chaque partition). Ne change pas
/// après la création du topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRecord {
    pub id: Uuid,
    pub partitions: Vec<Vec<i32>>,
}

/// `/workshop/partitions/<topic>/<p>` : l'état d'une partition, qui change à
/// chaque élection de leader ou modification de l'ISR.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionRecord {
    /// Le leader, ou -1 s'il n'y en a pas (partition hors ligne).
    pub leader: i32,
    /// Incrémenté à chaque changement de leader.
    pub leader_epoch: i32,
    /// Les répliques à jour (in-sync replicas).
    pub isr: Vec<i32>,
}

/// La copie locale de l'état du cluster stocké dans etcd.
#[derive(Debug, Clone, Default)]
pub struct ClusterState {
    pub brokers: BTreeMap<i32, BrokerRecord>,
    pub controller: Option<i32>,
    pub topics: BTreeMap<String, TopicRecord>,
    /// L'état de chaque partition et la révision etcd de sa dernière
    /// modification (pour les mises à jour conditionnelles).
    pub partitions: HashMap<(String, i32), (PartitionRecord, i64)>,
}

impl ClusterState {
    /// Construit l'état à partir d'une lecture complète du préfixe.
    pub fn from_kvs(keys: &Keys, kvs: Vec<KeyValue>) -> ClusterState {
        let mut state = ClusterState::default();
        for kv in kvs {
            state.apply(keys, &WatchEvent { kind: EventKind::Put, kv });
        }
        state
    }

    /// Applique un changement observé dans etcd.
    pub fn apply(&mut self, keys: &Keys, event: &WatchEvent) {
        todo!("ch.6 : apply")
    }

    pub fn is_alive(&self, node_id: i32) -> bool {
        self.brokers.contains_key(&node_id)
    }
}

/// Répartit les répliques de `partitions` partitions sur les brokers `brokers`
/// (triés), `replication_factor` répliques par partition. Les répliques d'une
/// partition sont sur des brokers différents ; les leaders (première réplique)
/// sont répartis en tourniquet à partir de `start`.
pub fn assign_replicas(brokers: &[i32], partitions: i32, replication_factor: usize, start: usize) -> Vec<Vec<i32>> {
    todo!("ch.6 : assign_replicas")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicas_are_spread() {
        let assignment = assign_replicas(&[0, 1, 2], 4, 2, 0);
        assert_eq!(assignment, vec![vec![0, 1], vec![1, 2], vec![2, 0], vec![0, 1]]);
    }
}
