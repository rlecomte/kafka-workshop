//! Les répliques locales : pour chaque partition hébergée par ce broker, son
//! log sur disque.
//!
//! `topics` décrit les topics du cluster (les métadonnées) ; ce module gère
//! les données que ce broker stocke effectivement.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::log::{Log, LogConfig, ReadError};
use crate::record_batch::RecordBatch;
use crate::topics::{Partition, Topic};

/// Nom du fichier qui mémorise l'identifiant du topic dans le répertoire
/// d'une partition.
pub const PARTITION_METADATA: &str = "partition.metadata";

/// La réplique locale d'une partition.
#[derive(Debug)]
pub struct Replica {
    pub topic: String,
    pub topic_id: Uuid,
    pub partition: i32,
    /// Le log est protégé par un `Mutex` : les écritures sont sérialisées.
    /// C'est un `std::sync::Mutex`, on ne le garde jamais pendant un `.await`.
    log: Mutex<Log>,
    /// Réveille les `Fetch` en attente quand de nouvelles données sont
    /// disponibles. Partagé par toutes les répliques du broker.
    new_data: Arc<Notify>,
}

/// Le résultat d'une lecture dans le log.
#[derive(Debug, Clone)]
pub struct FetchData {
    pub records: Bytes,
    pub high_watermark: i64,
    pub log_start_offset: i64,
}

/// Le résultat d'une écriture dans le log.
#[derive(Debug, Clone, Copy)]
pub struct AppendInfo {
    pub base_offset: i64,
    pub log_start_offset: i64,
}

impl Replica {
    pub fn append(&self, batches: Vec<RecordBatch>) -> io::Result<AppendInfo> {
        let mut log = self.log.lock().unwrap();
        let base_offset = log.append(batches)?;
        let info = AppendInfo { base_offset, log_start_offset: log.log_start_offset() };
        drop(log);
        self.new_data.notify_waiters();
        Ok(info)
    }

    /// Lit des batches à partir de `offset`, sans dépasser le high watermark.
    pub fn read(&self, offset: i64, max_bytes: usize) -> Result<FetchData, ReadError> {
        let log = self.log.lock().unwrap();
        let high_watermark = log.log_end_offset();
        let records = log.read(offset, max_bytes, high_watermark)?;
        Ok(FetchData { records, high_watermark, log_start_offset: log.log_start_offset() })
    }

    /// Le high watermark : le dernier offset (exclu) visible par les
    /// consumers. Avec une seule réplique, c'est le log end offset.
    pub fn high_watermark(&self) -> i64 {
        self.log.lock().unwrap().log_end_offset()
    }

    pub fn offset_for_timestamp(&self, timestamp: i64) -> std::io::Result<Option<i64>> {
        self.log.lock().unwrap().offset_for_timestamp(timestamp)
    }

    pub fn apply_retention(&self, now_ms: i64) -> io::Result<usize> {
        todo!("ch.4 : apply_retention")
    }

    pub fn log_end_offset(&self) -> i64 {
        self.log.lock().unwrap().log_end_offset()
    }

    pub fn log_start_offset(&self) -> i64 {
        self.log.lock().unwrap().log_start_offset()
    }
}

/// L'ensemble des répliques locales, indexées par (topic, partition).
#[derive(Debug)]
pub struct ReplicaManager {
    data_dir: PathBuf,
    log_config: LogConfig,
    replicas: RwLock<HashMap<(String, i32), Arc<Replica>>>,
    /// Notifié à chaque écriture dans une réplique.
    pub new_data: Arc<Notify>,
}

impl ReplicaManager {
    pub fn new(data_dir: PathBuf, log_config: LogConfig) -> Self {
        Self { data_dir, log_config, replicas: RwLock::default(), new_data: Arc::new(Notify::new()) }
    }

    /// Crée (ou rouvre) les répliques locales de toutes les partitions d'un topic.
    pub fn create_replicas(&self, topic: &Topic) -> io::Result<()> {
        for partition in &topic.partitions {
            self.open_replica(&topic.name, topic.id, partition.index)?;
        }
        Ok(())
    }

    /// Ouvre (ou crée) la réplique locale d'une partition. Le fichier
    /// `partition.metadata` du répertoire mémorise l'identifiant du topic.
    pub fn open_replica(&self, topic: &str, topic_id: Uuid, partition: i32) -> io::Result<Arc<Replica>> {
        todo!("ch.4 : open_replica")
    }

    /// Recharge les répliques présentes dans le répertoire de données, et
    /// renvoie les topics correspondants.
    ///
    /// Chaque sous-répertoire `<topic>-<partition>` contenant un fichier
    /// `partition.metadata` est une réplique. Le nom du topic peut lui-même
    /// contenir des `-` : on coupe au dernier.
    pub fn load_existing(&self) -> io::Result<Vec<Topic>> {
        todo!("ch.4 : load_existing")
    }

    pub fn all(&self) -> Vec<Arc<Replica>> {
        self.replicas.read().unwrap().values().cloned().collect()
    }

    /// Applique la rétention à toutes les répliques.
    pub fn apply_retention(&self) {
        todo!("ch.4 : apply_retention")
    }

    pub fn get(&self, topic: &str, partition: i32) -> Option<Arc<Replica>> {
        self.replicas.read().unwrap().get(&(topic.to_string(), partition)).cloned()
    }
}
