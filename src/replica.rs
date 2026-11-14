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

    /// Lit tout le log (jusqu'au high watermark).
    pub fn read_all(&self) -> io::Result<Bytes> {
        todo!("ch.5 : read_all")
    }

    pub fn apply_retention(&self, now_ms: i64) -> io::Result<usize> {
        self.log.lock().unwrap().apply_retention(now_ms)
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
        let dir = self.data_dir.join(format!("{topic}-{partition}"));
        let mut config = self.log_config.clone();
        if topic.starts_with("__") {
            // Les topics internes (__consumer_offsets) ne doivent pas perdre
            // de données avec le temps. Kafka les compacte au lieu de les purger.
            config.retention_ms = None;
            config.retention_bytes = None;
        }
        let log = Log::open(&dir, config)?;
        std::fs::write(dir.join(PARTITION_METADATA), format!("version: 0\ntopic_id: {topic_id}\n"))?;
        let replica = Arc::new(Replica {
            topic: topic.to_string(),
            topic_id,
            partition,
            log: Mutex::new(log),
            new_data: self.new_data.clone(),
        });
        self.replicas.write().unwrap().insert((topic.to_string(), partition), replica.clone());
        Ok(replica)
    }

    /// Recharge les répliques présentes dans le répertoire de données, et
    /// renvoie les topics correspondants.
    ///
    /// Chaque sous-répertoire `<topic>-<partition>` contenant un fichier
    /// `partition.metadata` est une réplique. Le nom du topic peut lui-même
    /// contenir des `-` : on coupe au dernier.
    pub fn load_existing(&self) -> io::Result<Vec<Topic>> {
        let mut found: BTreeMap<String, (Uuid, Vec<i32>)> = BTreeMap::new();
        if !self.data_dir.exists() {
            return Ok(Vec::new());
        }
        for entry in std::fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some((topic, partition)) = name.rsplit_once('-') else { continue };
            let Ok(partition) = partition.parse::<i32>() else { continue };
            let Ok(metadata) = std::fs::read_to_string(entry.path().join(PARTITION_METADATA)) else { continue };
            let Some(topic_id) = metadata
                .lines()
                .find_map(|l| l.strip_prefix("topic_id: "))
                .and_then(|id| Uuid::parse_str(id.trim()).ok())
            else {
                tracing::warn!(dir = %entry.path().display(), "partition.metadata invalide, répertoire ignoré");
                continue;
            };
            found.entry(topic.to_string()).or_insert((topic_id, Vec::new())).1.push(partition);
        }

        let mut topics = Vec::new();
        for (name, (id, mut partitions)) in found {
            partitions.sort();
            for &p in &partitions {
                self.open_replica(&name, id, p)?;
            }
            let partitions = partitions.into_iter().map(|index| Partition { index }).collect();
            topics.push(Topic { name, id, partitions });
        }
        Ok(topics)
    }

    pub fn all(&self) -> Vec<Arc<Replica>> {
        self.replicas.read().unwrap().values().cloned().collect()
    }

    /// Applique la rétention à toutes les répliques.
    pub fn apply_retention(&self) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        for replica in self.all() {
            if let Err(e) = replica.apply_retention(now_ms) {
                tracing::error!(topic = %replica.topic, partition = replica.partition, "rétention impossible : {e}");
            }
        }
    }

    pub fn get(&self, topic: &str, partition: i32) -> Option<Arc<Replica>> {
        self.replicas.read().unwrap().get(&(topic.to_string(), partition)).cloned()
    }
}
