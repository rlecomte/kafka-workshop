//! La participation d'un broker au cluster : enregistrement, suivi de l'état
//! du cluster, élection du controller, création des topics.

use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::Notify;
use uuid::Uuid;

use super::etcd::{Compare, Etcd, Op};
use super::{BrokerRecord, ClusterConfig, ClusterState, ControllerRecord, Keys, PartitionRecord, TopicRecord, assign_replicas};
use crate::Broker;
use crate::groups::offsets::OFFSETS_TOPIC;
use crate::topics::{self, NewTopic, Partition, Topic, TopicError};

/// L'état d'un broker en mode cluster.
#[derive(Debug)]
pub struct ClusterNode {
    pub config: ClusterConfig,
    pub etcd: Etcd,
    pub keys: Keys,
    /// La copie locale de l'état du cluster.
    pub state: RwLock<ClusterState>,
    /// Notifié à chaque changement de l'état du cluster.
    pub changed: Notify,
    /// Le lease auquel sont attachées les clés de ce broker.
    lease: AtomicI64,
    /// Les partitions de `__consumer_offsets` dont les offsets sont chargés
    /// (celles dont ce broker est leader).
    loaded_offsets: Mutex<HashSet<i32>>,
}

impl ClusterNode {
    pub fn new(config: ClusterConfig) -> ClusterNode {
        ClusterNode {
            etcd: Etcd::new(&config.etcd_url),
            keys: Keys { prefix: config.prefix.trim_end_matches('/').to_string() },
            config,
            state: RwLock::default(),
            changed: Notify::new(),
            lease: AtomicI64::new(0),
            loaded_offsets: Mutex::default(),
        }
    }

    pub fn state(&self) -> ClusterState {
        self.state.read().unwrap().clone()
    }

    fn lease_ttl_secs(&self) -> i64 {
        self.config.lease_ttl.as_secs().max(1) as i64
    }

    /// Attend un changement de l'état du cluster, au plus `timeout`.
    pub async fn wait_for_change(&self, timeout: Duration) {
        let _ = tokio::time::timeout(timeout, self.changed.notified()).await;
    }
}

impl Broker {
    pub fn cluster(&self) -> Option<&ClusterNode> {
        self.cluster.as_ref()
    }

    /// Rejoint le cluster : enregistre ce broker (avec un lease), charge
    /// l'état du cluster, puis lance le watch, le keepalive et l'élection du
    /// controller.
    pub(crate) async fn join_cluster(self: &Arc<Self>) -> Result<()> {
        todo!("ch.6 : join_cluster")
    }

    /// Crée un lease et y attache la clé `/brokers/<id>`.
    async fn register(&self) -> Result<()> {
        todo!("ch.6 : register")
    }

    /// Prolonge régulièrement le lease. S'il a expiré (broker figé trop
    /// longtemps, etcd redémarré...), on se réenregistre.
    async fn keep_alive_loop(self: Arc<Self>) {
        todo!("ch.6 : keep_alive_loop")
    }

    /// Suit les changements du préfixe du cluster dans etcd. En cas
    /// d'interruption, relit tout l'état et reprend.
    async fn watch_loop(self: Arc<Self>, mut revision: i64) {
        todo!("ch.6 : watch_loop")
    }

    /// Se présente à l'élection du controller dès que le poste est libre.
    ///
    /// L'élection est une transaction etcd : « si la clé `/controller`
    /// n'existe pas (révision de création 0), crée-la avec mon identifiant,
    /// attachée à mon lease ». Un seul broker peut réussir ; si le controller
    /// meurt, son lease expire, la clé disparaît, et l'élection recommence.
    async fn controller_loop(self: Arc<Self>) {
        todo!("ch.6 : controller_loop")
    }

    /// Ce broker est-il le controller ?
    pub fn is_controller(&self) -> bool {
        todo!("ch.6 : is_controller")
    }

    /// Met à jour le broker après un changement de l'état du cluster :
    /// registre des topics, répliques locales, offsets des groupes.
    pub(crate) fn on_cluster_change(&self) {
        todo!("ch.6 : on_cluster_change")
    }

    /// Charge les offsets des partitions de `__consumer_offsets` dont ce
    /// broker devient leader, et oublie les groupes de celles qu'il ne mène
    /// plus.
    fn sync_offsets_partitions(&self) {
        todo!("ch.6 : sync_offsets_partitions")
    }

    /// Les offsets de cette partition de `__consumer_offsets` sont-ils chargés ?
    pub fn offsets_loaded(&self, partition: i32) -> bool {
        match self.cluster() {
            Some(node) => node.loaded_offsets.lock().unwrap().contains(&partition),
            None => true,
        }
    }

    /// Crée un topic dans le cluster : calcule l'assignation des répliques sur
    /// les brokers vivants et l'écrit dans etcd, dans une transaction qui
    /// échoue si le topic existe déjà.
    pub(crate) async fn create_topic_in_cluster(&self, new: NewTopic, validate_only: bool) -> Result<Arc<Topic>, TopicError> {
        todo!("ch.6 : create_topic_in_cluster")
    }
}
