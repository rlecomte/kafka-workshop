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
        let node = self.cluster().expect("mode cluster");
        self.register().await.context("enregistrement dans etcd impossible")?;

        let (kvs, revision) = node.etcd.get_prefix(&node.keys.all()).await?;
        *node.state.write().unwrap() = ClusterState::from_kvs(&node.keys, kvs);
        self.on_cluster_change();
        tracing::info!(prefix = %node.keys.prefix, "cluster rejoint");

        let broker = self.clone();
        self.spawn(async move { broker.watch_loop(revision + 1).await });
        let broker = self.clone();
        self.spawn(async move { broker.keep_alive_loop().await });
        let broker = self.clone();
        self.spawn(async move { broker.controller_loop().await });
        Ok(())
    }

    /// Crée un lease et y attache la clé `/brokers/<id>`.
    async fn register(&self) -> Result<()> {
        let node = self.cluster().unwrap();
        let lease = node.etcd.grant_lease(node.lease_ttl_secs()).await?;
        node.lease.store(lease, Ordering::SeqCst);
        let record = BrokerRecord { node_id: self.config.node_id, host: self.config.host.clone(), port: self.config.port };
        node.etcd.put(&node.keys.broker(self.config.node_id), &serde_json::to_vec(&record)?, Some(lease)).await?;
        Ok(())
    }

    /// Prolonge régulièrement le lease. S'il a expiré (broker figé trop
    /// longtemps, etcd redémarré...), on se réenregistre.
    async fn keep_alive_loop(self: Arc<Self>) {
        let node = self.cluster().unwrap();
        let period = (node.config.lease_ttl / 3).max(Duration::from_millis(200));
        loop {
            tokio::time::sleep(period).await;
            match node.etcd.keep_alive(node.lease.load(Ordering::SeqCst)).await {
                Ok(true) => {}
                Ok(false) => {
                    tracing::warn!("lease expiré, nouvel enregistrement");
                    if let Err(e) = self.register().await {
                        tracing::error!("réenregistrement impossible : {e:#}");
                    }
                }
                Err(e) => tracing::warn!("keepalive impossible : {e:#}"),
            }
        }
    }

    /// Suit les changements du préfixe du cluster dans etcd. En cas
    /// d'interruption, relit tout l'état et reprend.
    async fn watch_loop(self: Arc<Self>, mut revision: i64) {
        let node = self.cluster().unwrap();
        loop {
            match node.etcd.watch_prefix(&node.keys.all(), revision).await {
                Ok(mut events) => {
                    while let Some(batch) = events.recv().await {
                        let batch = match batch {
                            Ok(batch) => batch,
                            Err(e) => {
                                tracing::warn!("watch interrompu : {e:#}");
                                break;
                            }
                        };
                        {
                            let mut state = node.state.write().unwrap();
                            for event in &batch {
                                state.apply(&node.keys, event);
                                revision = revision.max(event.kv.mod_revision + 1);
                            }
                        }
                        self.on_cluster_change();
                    }
                }
                Err(e) => tracing::warn!("watch impossible : {e:#}"),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            // On repart d'une lecture complète : des changements ont pu être manqués.
            if let Ok((kvs, current)) = node.etcd.get_prefix(&node.keys.all()).await {
                *node.state.write().unwrap() = ClusterState::from_kvs(&node.keys, kvs);
                revision = current + 1;
                self.on_cluster_change();
            }
        }
    }

    /// Se présente à l'élection du controller dès que le poste est libre.
    ///
    /// L'élection est une transaction etcd : « si la clé `/controller`
    /// n'existe pas (révision de création 0), crée-la avec mon identifiant,
    /// attachée à mon lease ». Un seul broker peut réussir ; si le controller
    /// meurt, son lease expire, la clé disparaît, et l'élection recommence.
    async fn controller_loop(self: Arc<Self>) {
        let node = self.cluster().unwrap();
        loop {
            let changed = node.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();

            if node.state().controller.is_none() {
                let key = node.keys.controller();
                let record = serde_json::to_vec(&ControllerRecord { node_id: self.config.node_id }).unwrap();
                let lease = Some(node.lease.load(Ordering::SeqCst));
                match node.etcd.txn(&[Compare::CreateRevision(key.clone(), 0)], &[Op::Put { key, value: record, lease }], &[]).await {
                    Ok(true) => tracing::info!("élu controller"),
                    Ok(false) => {}
                    Err(e) => tracing::warn!("élection impossible : {e:#}"),
                }
            }
            let _ = tokio::time::timeout(Duration::from_secs(1), changed).await;
        }
    }

    /// Ce broker est-il le controller ?
    pub fn is_controller(&self) -> bool {
        match self.cluster() {
            Some(node) => node.state().controller == Some(self.config.node_id),
            None => true,
        }
    }

    /// Met à jour le broker après un changement de l'état du cluster :
    /// registre des topics, répliques locales et leur rôle, réplication,
    /// offsets des groupes.
    pub(crate) fn on_cluster_change(self: &Arc<Self>) {
        let node = self.cluster().unwrap();
        let state = node.state();
        let me = self.config.node_id;

        let topics: Vec<Topic> = state
            .topics
            .iter()
            .map(|(name, record)| Topic {
                name: name.clone(),
                id: record.id,
                partitions: record
                    .partitions
                    .iter()
                    .enumerate()
                    .map(|(index, replicas)| {
                        let partition = state.partitions.get(&(name.clone(), index as i32)).map(|(p, _)| p.clone());
                        let partition = partition.unwrap_or(PartitionRecord { leader: -1, leader_epoch: 0, isr: vec![] });
                        Partition {
                            index: index as i32,
                            replicas: replicas.clone(),
                            leader: partition.leader,
                            leader_epoch: partition.leader_epoch,
                            isr: partition.isr,
                        }
                    })
                    .collect(),
            })
            .collect();

        // Ouvre les répliques locales des partitions assignées à ce broker.
        for topic in &topics {
            for partition in topic.partitions.iter().filter(|p| p.replicas.contains(&me)) {
                if self.replicas.get(&topic.name, partition.index).is_none()
                    && let Err(e) = self.replicas.open_replica(&topic.name, topic.id, partition.index)
                {
                    tracing::error!(topic = %topic.name, partition = partition.index, "ouverture de la réplique impossible : {e}");
                }
            }
        }
        // Rôle de chaque réplique locale (leader ou follower, epoch, ISR).
        for topic in &topics {
            for partition in &topic.partitions {
                if let Some(replica) = self.replicas.get(&topic.name, partition.index) {
                    replica.update_partition_state(partition);
                }
            }
        }
        self.topics.replace_all(topics);
        self.sync_fetchers();
        self.sync_offsets_partitions();
        // Le high watermark a pu changer (ISR modifiée).
        self.replicas.new_data.notify_waiters();
        node.changed.notify_waiters();
    }

    /// Côté leader : modifie l'ISR d'une partition dans etcd, à condition que
    /// personne ne l'ait modifiée depuis notre dernière lecture (le controller
    /// a pu élire un autre leader entre-temps).
    pub(crate) async fn alter_isr(&self, topic: &str, partition: i32, isr: Vec<i32>) {
        todo!("ch.7 : alter_isr")
    }

    /// Charge les offsets des partitions de `__consumer_offsets` dont ce
    /// broker devient leader, et oublie les groupes de celles qu'il ne mène
    /// plus.
    fn sync_offsets_partitions(&self) {
        let node = self.cluster().unwrap();
        let Some(topic) = self.topics.get(OFFSETS_TOPIC) else { return };
        let mut loaded = node.loaded_offsets.lock().unwrap();
        for partition in &topic.partitions {
            let leader = partition.leader == self.config.node_id;
            if leader && !loaded.contains(&partition.index) {
                match self.load_offsets_partition(partition.index) {
                    Ok(()) => {
                        loaded.insert(partition.index);
                    }
                    Err(e) => tracing::error!(partition = partition.index, "chargement des offsets impossible : {e}"),
                }
            } else if !leader && loaded.remove(&partition.index) {
                let partitions = topic.partitions.len() as i32;
                self.groups.unload(|group| crate::groups::offsets::partition_for(group, partitions) == partition.index);
                tracing::info!(partition = partition.index, "groupes de la partition déchargés");
            }
        }
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
        let node = self.cluster().unwrap();
        topics::validate_name(&new.name)?;
        if new.num_partitions < 1 {
            return Err(TopicError::InvalidPartitions(new.num_partitions));
        }
        let state = node.state();
        if state.topics.contains_key(&new.name) {
            return Err(TopicError::AlreadyExists(new.name));
        }
        let brokers: Vec<i32> = state.brokers.keys().copied().collect();
        if new.replication_factor < 1 || new.replication_factor as usize > brokers.len() {
            return Err(TopicError::InvalidReplicationFactor(new.replication_factor));
        }

        let id = Uuid::new_v4();
        let start = (id.as_u128() % brokers.len() as u128) as usize;
        let assignment = assign_replicas(&brokers, new.num_partitions, new.replication_factor as usize, start);
        let topic = Topic {
            name: new.name.clone(),
            id,
            partitions: assignment
                .iter()
                .enumerate()
                .map(|(index, replicas)| Partition {
                    index: index as i32,
                    replicas: replicas.clone(),
                    leader: replicas[0],
                    leader_epoch: 0,
                    isr: replicas.clone(),
                })
                .collect(),
        };
        if validate_only {
            return Ok(Arc::new(topic));
        }

        let topic_key = node.keys.topic(&new.name);
        let mut ops = vec![Op::Put {
            key: topic_key.clone(),
            value: serde_json::to_vec(&TopicRecord { id, partitions: assignment.clone() }).unwrap(),
            lease: None,
        }];
        for partition in &topic.partitions {
            let record = PartitionRecord { leader: partition.leader, leader_epoch: 0, isr: partition.isr.clone() };
            ops.push(Op::Put {
                key: node.keys.partition(&new.name, partition.index),
                value: serde_json::to_vec(&record).unwrap(),
                lease: None,
            });
        }
        let created = node
            .etcd
            .txn(&[Compare::CreateRevision(topic_key, 0)], &ops, &[])
            .await
            .map_err(|e| TopicError::Unavailable(format!("{e:#}")))?;
        if !created {
            return Err(TopicError::AlreadyExists(new.name));
        }

        // Attend que le watch nous ait apporté le topic.
        for _ in 0..50 {
            if let Some(topic) = self.topics.get(&new.name).filter(|t| t.id == id) {
                return Ok(topic);
            }
            node.wait_for_change(Duration::from_millis(100)).await;
        }
        Ok(Arc::new(topic))
    }
}
