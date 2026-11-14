//! Les répliques locales : pour chaque partition hébergée par ce broker, son
//! log sur disque.
//!
//! `topics` décrit les topics du cluster (les métadonnées) ; ce module gère
//! les données que ce broker stocke effectivement.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::log::{Log, LogConfig, ReadError};
use crate::record_batch::RecordBatch;
use crate::topics::Topic;

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
            let dir = self.data_dir.join(format!("{}-{}", topic.name, partition.index));
            let log = Log::open(&dir, self.log_config.clone())?;
            let replica = Arc::new(Replica {
                topic: topic.name.clone(),
                topic_id: topic.id,
                partition: partition.index,
                log: Mutex::new(log),
                new_data: self.new_data.clone(),
            });
            self.replicas.write().unwrap().insert((topic.name.clone(), partition.index), replica);
        }
        Ok(())
    }

    pub fn get(&self, topic: &str, partition: i32) -> Option<Arc<Replica>> {
        self.replicas.read().unwrap().get(&(topic.to_string(), partition)).cloned()
    }
}
