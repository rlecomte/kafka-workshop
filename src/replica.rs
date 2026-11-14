//! Les répliques locales : pour chaque partition hébergée par ce broker, son
//! log sur disque.
//!
//! `topics` décrit les topics du cluster (les métadonnées) ; ce module gère
//! les données que ce broker stocke effectivement.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::Notify;
use tokio::time::Instant;
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
    /// L'identifiant de ce broker.
    node_id: i32,
    /// Le log est protégé par un `Mutex` : les écritures sont sérialisées.
    /// C'est un `std::sync::Mutex`, on ne le garde jamais pendant un `.await`.
    log: Mutex<Log>,
    /// Le rôle de la réplique et l'état de la réplication.
    state: Mutex<ReplicationState>,
    /// Réveille les requêtes en attente (Fetch, Produce avec acks=all) quand
    /// des données arrivent ou que le high watermark avance. Partagé par
    /// toutes les répliques du broker.
    new_data: Arc<Notify>,
}

/// L'état de la réplication d'une partition, vu par cette réplique.
#[derive(Debug, Clone)]
struct ReplicationState {
    is_leader: bool,
    leader_epoch: i32,
    isr: Vec<i32>,
    /// L'offset jusqu'où les données sont répliquées sur toute l'ISR, donc
    /// visibles par les consumers.
    high_watermark: i64,
    /// Côté leader : la position de chaque follower.
    followers: HashMap<i32, FollowerState>,
}

/// Ce que le leader sait d'un follower.
#[derive(Debug, Clone, Copy)]
pub struct FollowerState {
    /// Le log end offset du follower (l'offset de son dernier Fetch).
    pub log_end_offset: i64,
    /// La dernière fois que le follower était à jour.
    pub last_caught_up: Instant,
}

/// Le résultat d'une écriture dans le log.
#[derive(Debug, Clone, Copy)]
pub struct AppendInfo {
    pub base_offset: i64,
    /// L'offset qui suit le dernier record écrit.
    pub next_offset: i64,
    pub log_start_offset: i64,
}

/// Le résultat d'une lecture dans le log.
#[derive(Debug, Clone)]
pub struct FetchData {
    pub records: Bytes,
    pub high_watermark: i64,
    pub log_start_offset: i64,
}

impl Replica {
    fn new(topic: &str, topic_id: Uuid, partition: i32, node_id: i32, log: Log, new_data: Arc<Notify>) -> Replica {
        let high_watermark = log.log_end_offset();
        Replica {
            topic: topic.to_string(),
            topic_id,
            partition,
            node_id,
            log: Mutex::new(log),
            // Par défaut (mode autonome) : seule réplique, donc leader.
            state: Mutex::new(ReplicationState {
                is_leader: true,
                leader_epoch: 0,
                isr: vec![node_id],
                high_watermark,
                followers: HashMap::new(),
            }),
            new_data,
        }
    }

    /// Écrit des batches en tant que leader : attribue les offsets, puis met
    /// à jour le high watermark.
    pub fn append(&self, batches: Vec<RecordBatch>) -> io::Result<AppendInfo> {
        let epoch = self.state.lock().unwrap().leader_epoch;
        let info = {
            let mut log = self.log.lock().unwrap();
            let base_offset = log.append_with_epoch(batches, epoch)?;
            AppendInfo { base_offset, next_offset: log.log_end_offset(), log_start_offset: log.log_start_offset() }
        };
        self.update_high_watermark();
        self.new_data.notify_waiters();
        Ok(info)
    }

    /// Écrit des batches copiés depuis le leader (follower), puis adopte le
    /// high watermark du leader (sans dépasser son propre log end offset).
    pub fn append_as_follower(&self, batches: Vec<RecordBatch>, leader_high_watermark: i64) -> io::Result<()> {
        let log_end_offset = {
            let mut log = self.log.lock().unwrap();
            log.append_replicated(batches)?;
            log.log_end_offset()
        };
        let mut state = self.state.lock().unwrap();
        state.high_watermark = state.high_watermark.max(leader_high_watermark.min(log_end_offset));
        Ok(())
    }

    /// Lecture pour un consumer : jusqu'au high watermark.
    pub fn read(&self, offset: i64, max_bytes: usize) -> Result<FetchData, ReadError> {
        let high_watermark = self.high_watermark();
        let log = self.log.lock().unwrap();
        let records = log.read(offset, max_bytes, high_watermark)?;
        Ok(FetchData { records, high_watermark, log_start_offset: log.log_start_offset() })
    }

    /// Lecture pour un follower : jusqu'au log end offset.
    pub fn read_for_follower(&self, offset: i64, max_bytes: usize) -> Result<FetchData, ReadError> {
        let log = self.log.lock().unwrap();
        let records = log.read(offset, max_bytes, i64::MAX)?;
        Ok(FetchData { records, high_watermark: self.high_watermark(), log_start_offset: log.log_start_offset() })
    }

    /// Le high watermark : l'offset (exclu) jusqu'où les données sont visibles
    /// par les consumers.
    pub fn high_watermark(&self) -> i64 {
        self.state.lock().unwrap().high_watermark
    }

    pub fn log_end_offset(&self) -> i64 {
        self.log.lock().unwrap().log_end_offset()
    }

    pub fn log_start_offset(&self) -> i64 {
        self.log.lock().unwrap().log_start_offset()
    }

    pub fn is_leader(&self) -> bool {
        self.state.lock().unwrap().is_leader
    }

    pub fn leader_epoch(&self) -> i32 {
        self.state.lock().unwrap().leader_epoch
    }

    pub fn isr(&self) -> Vec<i32> {
        self.state.lock().unwrap().isr.clone()
    }

    /// Met à jour le rôle de la réplique d'après les métadonnées du cluster.
    /// Renvoie `true` si la réplique vient de devenir leader ou follower.
    pub fn update_partition_state(&self, partition: &Partition) -> bool {
        let is_leader = partition.leader == self.node_id;
        let changed = {
            let mut state = self.state.lock().unwrap();
            let changed = state.is_leader != is_leader || state.leader_epoch != partition.leader_epoch;
            if changed && is_leader {
                // Nouveau leader : on ne sait encore rien des followers. On leur
                // laisse le délai habituel pour se manifester.
                let now = Instant::now();
                state.followers = partition
                    .replicas
                    .iter()
                    .filter(|&&r| r != self.node_id)
                    .map(|&r| (r, FollowerState { log_end_offset: 0, last_caught_up: now }))
                    .collect();
            }
            state.is_leader = is_leader;
            state.leader_epoch = partition.leader_epoch;
            state.isr = partition.isr.clone();
            changed
        };
        self.update_high_watermark();
        changed
    }

    /// Côté leader : enregistre le Fetch d'un follower. Son `fetch_offset`
    /// indique qu'il a tout ce qui précède. Renvoie la nouvelle ISR si le
    /// follower doit y entrer (il a rattrapé le high watermark).
    pub fn record_follower_fetch(&self, follower: i32, fetch_offset: i64) -> Option<Vec<i32>> {
        let log_end_offset = self.log_end_offset();
        let expand = {
            let mut state = self.state.lock().unwrap();
            let now = Instant::now();
            let entry = state.followers.entry(follower).or_insert(FollowerState { log_end_offset: 0, last_caught_up: now });
            entry.log_end_offset = fetch_offset;
            if fetch_offset >= log_end_offset {
                entry.last_caught_up = now;
            }
            let caught_up = fetch_offset >= state.high_watermark;
            (state.is_leader && caught_up && !state.isr.contains(&follower)).then(|| {
                let mut isr = state.isr.clone();
                isr.push(follower);
                isr
            })
        };
        self.update_high_watermark();
        expand
    }

    /// Côté leader : la nouvelle ISR si des followers sont en retard depuis
    /// plus de `max_lag`.
    pub fn isr_to_shrink(&self, max_lag: Duration) -> Option<Vec<i32>> {
        let state = self.state.lock().unwrap();
        if !state.is_leader {
            return None;
        }
        let now = Instant::now();
        let isr: Vec<i32> = state
            .isr
            .iter()
            .copied()
            .filter(|r| *r == self.node_id || state.followers.get(r).is_some_and(|f| now.duration_since(f.last_caught_up) <= max_lag))
            .collect();
        (isr.len() < state.isr.len()).then_some(isr)
    }

    /// Côté leader : le high watermark est le plus petit log end offset des
    /// répliques de l'ISR. Il ne recule jamais.
    fn update_high_watermark(&self) {
        let log_end_offset = self.log_end_offset();
        let advanced = {
            let mut state = self.state.lock().unwrap();
            if !state.is_leader {
                return;
            }
            let computed = state
                .isr
                .iter()
                .map(|r| if *r == self.node_id { log_end_offset } else { state.followers.get(r).map_or(0, |f| f.log_end_offset) })
                .min()
                .unwrap_or(log_end_offset);
            let previous = state.high_watermark;
            state.high_watermark = previous.max(computed.min(log_end_offset));
            state.high_watermark > previous
        };
        if advanced {
            self.new_data.notify_waiters();
        }
    }

    /// Supprime tout ce qui suit `offset` (follower qui a divergé du leader).
    pub fn truncate_to(&self, offset: i64) -> io::Result<()> {
        let log_end_offset = {
            let mut log = self.log.lock().unwrap();
            log.truncate_to(offset)?;
            log.log_end_offset()
        };
        let mut state = self.state.lock().unwrap();
        state.high_watermark = state.high_watermark.min(log_end_offset);
        Ok(())
    }

    /// Vide le log et le fait recommencer à `offset`.
    pub fn reset_to(&self, offset: i64) -> io::Result<()> {
        self.log.lock().unwrap().reset_to(offset)?;
        self.state.lock().unwrap().high_watermark = offset;
        Ok(())
    }

    pub fn offset_for_timestamp(&self, timestamp: i64) -> std::io::Result<Option<i64>> {
        self.log.lock().unwrap().offset_for_timestamp(timestamp)
    }

    /// Lit tout le log (jusqu'au high watermark).
    pub fn read_all(&self) -> io::Result<Bytes> {
        let mut all = bytes::BytesMut::new();
        let mut offset = self.log_start_offset();
        loop {
            let data = match self.read(offset, 1024 * 1024) {
                Ok(data) => data,
                Err(ReadError::Io(e)) => return Err(e),
                Err(ReadError::OffsetOutOfRange { .. }) => break,
            };
            if data.records.is_empty() {
                break;
            }
            let mut next = offset;
            let mut rest = &data.records[..];
            while let Ok(header) = crate::record_batch::RecordBatchHeader::parse(rest) {
                next = header.next_offset();
                rest = &rest[header.total_size()..];
            }
            all.extend_from_slice(&data.records);
            offset = next;
        }
        Ok(all.freeze())
    }

    pub fn apply_retention(&self, now_ms: i64) -> io::Result<usize> {
        self.log.lock().unwrap().apply_retention(now_ms)
    }
}

/// L'ensemble des répliques locales, indexées par (topic, partition).
#[derive(Debug)]
pub struct ReplicaManager {
    node_id: i32,
    data_dir: PathBuf,
    log_config: LogConfig,
    replicas: RwLock<HashMap<(String, i32), Arc<Replica>>>,
    /// Notifié à chaque écriture dans une réplique.
    pub new_data: Arc<Notify>,
}

impl ReplicaManager {
    pub fn new(node_id: i32, data_dir: PathBuf, log_config: LogConfig) -> Self {
        Self { node_id, data_dir, log_config, replicas: RwLock::default(), new_data: Arc::new(Notify::new()) }
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
        let replica = Arc::new(Replica::new(topic, topic_id, partition, self.node_id, log, self.new_data.clone()));
        self.replicas.write().unwrap().insert((topic.to_string(), partition), replica.clone());
        Ok(replica)
    }

    /// Recharge les répliques présentes dans le répertoire de données, et
    /// renvoie les topics correspondants.
    ///
    /// Chaque sous-répertoire `<topic>-<partition>` contenant un fichier
    /// `partition.metadata` est une réplique. Le nom du topic peut lui-même
    /// contenir des `-` : on coupe au dernier.
    pub fn load_existing(&self, node_id: i32) -> io::Result<Vec<Topic>> {
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
            let partitions = partitions.into_iter().map(|index| Partition::local(index, node_id)).collect();
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
