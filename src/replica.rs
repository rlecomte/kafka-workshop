//! Les répliques locales : pour chaque partition hébergée par ce broker, son
//! log sur disque.
//!
//! `topics` décrit les topics du cluster (les métadonnées) ; ce module gère
//! les données que ce broker stocke effectivement.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use uuid::Uuid;

use crate::log::{Log, LogConfig};
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
        Ok(AppendInfo { base_offset, log_start_offset: log.log_start_offset() })
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
}

impl ReplicaManager {
    pub fn new(data_dir: PathBuf, log_config: LogConfig) -> Self {
        Self { data_dir, log_config, replicas: RwLock::default() }
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
            });
            self.replicas.write().unwrap().insert((topic.name.clone(), partition.index), replica);
        }
        Ok(())
    }

    pub fn get(&self, topic: &str, partition: i32) -> Option<Arc<Replica>> {
        self.replicas.read().unwrap().get(&(topic.to_string(), partition)).cloned()
    }
}
