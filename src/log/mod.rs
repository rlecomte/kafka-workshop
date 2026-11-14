//! Le log d'une partition : une suite de batches, stockés sur disque dans un
//! répertoire `<data_dir>/<topic>-<partition>/`.
//!
//! Le log est découpé en segments (voir [`segment`]). Chaque segment est un
//! fichier nommé d'après l'offset de son premier batch, sur 20 chiffres :
//! `00000000000000000000.log`, `00000000000000001234.log`... Chacun a son
//! index d'offsets (voir [`index`]).
//!
//! On n'écrit que dans le dernier segment, le segment **actif**. Quand il
//! dépasse `segment_bytes`, on en crée un nouveau. Les plus anciens sont
//! supprimés par la rétention.

pub mod index;
pub mod segment;

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::record_batch::RecordBatch;
use segment::Segment;

/// Les erreurs de lecture du log.
#[derive(Debug)]
pub enum ReadError {
    /// L'offset demandé est avant le début ou après la fin du log.
    OffsetOutOfRange { offset: i64, log_start_offset: i64, log_end_offset: i64 },
    Io(io::Error),
}

impl From<io::Error> for ReadError {
    fn from(e: io::Error) -> Self {
        ReadError::Io(e)
    }
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::OffsetOutOfRange { offset, log_start_offset, log_end_offset } => write!(
                f,
                "offset {offset} hors du log [{log_start_offset}, {log_end_offset}]"
            ),
            ReadError::Io(e) => write!(f, "erreur de lecture : {e}"),
        }
    }
}

/// Paramètres d'un log.
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Taille maximale d'un segment (`log.segment.bytes`, 1 Gio par défaut).
    pub segment_bytes: u64,
    /// Écart entre deux entrées de l'index (`log.index.interval.bytes`).
    pub index_interval_bytes: u64,
    /// Durée de conservation des messages (`log.retention.ms`), `None` : illimitée.
    pub retention_ms: Option<i64>,
    /// Taille maximale du log (`log.retention.bytes`), `None` : illimitée.
    pub retention_bytes: Option<u64>,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 1024 * 1024 * 1024,
            index_interval_bytes: 4096,
            retention_ms: Some(7 * 24 * 3600 * 1000),
            retention_bytes: None,
        }
    }
}

#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
    config: LogConfig,
    /// Les segments, indexés par leur offset de base. Le dernier est le
    /// segment actif, le seul dans lequel on écrit.
    segments: BTreeMap<i64, Segment>,
    /// L'offset que recevra le prochain record (le « log end offset »).
    next_offset: i64,
    /// Le premier offset encore présent dans le log.
    log_start_offset: i64,
}

impl Log {
    /// Ouvre le log situé dans `dir`, en créant le répertoire si besoin.
    ///
    /// Si le répertoire contient déjà des segments, ils sont rechargés. Le
    /// dernier segment est vérifié et tronqué au premier batch invalide
    /// (voir [`Segment::recover`]).
    pub fn open(dir: &Path, config: LogConfig) -> io::Result<Log> {
        std::fs::create_dir_all(dir)?;

        let mut base_offsets = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let name = entry?.file_name();
            let name = name.to_string_lossy();
            if let Some(base) = name.strip_suffix(".log").and_then(|b| b.parse::<i64>().ok()) {
                base_offsets.push(base);
            }
        }
        base_offsets.sort();

        let mut segments = BTreeMap::new();
        for base in &base_offsets {
            segments.insert(*base, Segment::open(dir, *base, config.index_interval_bytes)?);
        }
        if let Some(last) = segments.values_mut().next_back() {
            let truncated = last.recover()?;
            if truncated > 0 {
                tracing::warn!(dir = %dir.display(), truncated, "fin de log invalide supprimée");
            }
        } else {
            segments.insert(0, Segment::create(dir, 0, config.index_interval_bytes)?);
        }

        let next_offset = segments.values().next_back().unwrap().next_offset();
        let log_start_offset = *segments.keys().next().unwrap();
        Ok(Log { dir: dir.to_path_buf(), config, segments, next_offset, log_start_offset })
    }

    /// Ajoute des batches à la fin du log, en leur attribuant leurs offsets.
    /// Renvoie l'offset de base du premier batch.
    pub fn append(&mut self, batches: Vec<RecordBatch>) -> io::Result<i64> {
        self.append_with_epoch(batches, -1)
    }

    /// Comme [`Log::append`], en inscrivant l'epoch du leader dans chaque batch.
    pub fn append_with_epoch(&mut self, batches: Vec<RecordBatch>, leader_epoch: i32) -> io::Result<i64> {
        todo!("ch.7 : append_with_epoch")
    }

    /// Ajoute des batches qui ont déjà leurs offsets (copiés depuis le leader
    /// par un follower). Le premier doit commencer au log end offset.
    pub fn append_replicated(&mut self, batches: Vec<RecordBatch>) -> io::Result<()> {
        todo!("ch.7 : append_replicated")
    }

    /// Supprime tout ce qui suit `offset` (exclu) : le log end offset devient
    /// `offset` (ou le début du batch qui le contient).
    pub fn truncate_to(&mut self, offset: i64) -> io::Result<()> {
        todo!("ch.7 : truncate_to")
    }

    /// Vide le log et le fait recommencer à `offset` (un follower dont le
    /// leader a supprimé les données par rétention).
    pub fn reset_to(&mut self, offset: i64) -> io::Result<()> {
        todo!("ch.7 : reset_to")
    }

    /// Ajoute un batch dont l'offset est déjà attribué, en créant un nouveau
    /// segment si le segment actif est plein.
    fn append_batch(&mut self, batch: &RecordBatch) -> io::Result<()> {
        let active = self.active_segment();
        if active.size() > 0 && active.size() + batch.bytes.len() as u64 > self.config.segment_bytes {
            self.roll()?;
        }
        self.active_segment().append(batch)?;
        self.next_offset = batch.header.next_offset();
        Ok(())
    }

    /// Crée un nouveau segment actif, qui commence au log end offset.
    fn roll(&mut self) -> io::Result<()> {
        let segment = Segment::create(&self.dir, self.next_offset, self.config.index_interval_bytes)?;
        tracing::debug!(dir = %self.dir.display(), base_offset = self.next_offset, "nouveau segment");
        self.segments.insert(self.next_offset, segment);
        Ok(())
    }

    /// Supprime les segments trop anciens ou en trop, selon `retention_ms` et
    /// `retention_bytes`. Le segment actif n'est jamais supprimé. Renvoie le
    /// nombre de segments supprimés.
    pub fn apply_retention(&mut self, now_ms: i64) -> io::Result<usize> {
        let mut deleted = 0;
        let mut total_size: u64 = self.segments.values().map(|s| s.size()).sum();
        while self.segments.len() > 1 {
            let oldest = self.segments.values().next().unwrap();
            let expired = self.config.retention_ms.is_some_and(|retention| {
                oldest.max_timestamp() >= 0 && oldest.max_timestamp() < now_ms - retention
            });
            let too_big = self.config.retention_bytes.is_some_and(|max| total_size - oldest.size() >= max);
            if !expired && !too_big {
                break;
            }
            let (_, segment) = self.segments.pop_first().unwrap();
            total_size -= segment.size();
            tracing::info!(path = %segment.path().display(), expired, too_big, "segment supprimé par la rétention");
            segment.delete()?;
            deleted += 1;
        }
        self.log_start_offset = self.log_start_offset.max(*self.segments.keys().next().unwrap());
        Ok(deleted)
    }

    /// Les segments, dans l'ordre.
    pub fn segments(&self) -> impl Iterator<Item = &Segment> {
        self.segments.values()
    }

    /// Lit des batches à partir de `offset`, sur `max_bytes` octets au plus
    /// (le premier batch est toujours renvoyé en entier, même s'il est plus
    /// gros), sans renvoyer de batch commençant à `max_offset` ou au-delà.
    ///
    /// Un offset égal au log end offset est valide : la lecture est vide.
    pub fn read(&self, offset: i64, max_bytes: usize, max_offset: i64) -> Result<Bytes, ReadError> {
        if offset < self.log_start_offset || offset > self.next_offset {
            return Err(ReadError::OffsetOutOfRange {
                offset,
                log_start_offset: self.log_start_offset,
                log_end_offset: self.next_offset,
            });
        }
        // Le segment qui contient l'offset est le dernier dont l'offset de
        // base est inférieur ou égal. Comme Kafka, on ne lit que dans un seul
        // segment par requête. Si l'offset est tout à la fin d'un segment, la
        // lecture est vide et on passe au suivant.
        for (_, segment) in self.segments.range(..=offset).next_back().into_iter().chain(self.segments.range(offset + 1..)) {
            let bytes = segment.read(offset, max_bytes, max_offset)?;
            if !bytes.is_empty() {
                return Ok(bytes);
            }
        }
        Ok(Bytes::new())
    }

    /// Le premier offset dont le batch a un timestamp supérieur ou égal à
    /// `timestamp` (à la granularité du batch), ou `None`.
    pub fn offset_for_timestamp(&self, timestamp: i64) -> io::Result<Option<i64>> {
        for segment in self.segments.values() {
            for entry in segment.headers(0) {
                let (_, header) = entry?;
                if header.max_timestamp >= timestamp {
                    return Ok(Some(header.base_offset.max(self.log_start_offset)));
                }
            }
        }
        Ok(None)
    }

    pub fn log_end_offset(&self) -> i64 {
        self.next_offset
    }

    pub fn log_start_offset(&self) -> i64 {
        self.log_start_offset
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn active_segment(&mut self) -> &mut Segment {
        // Le dernier segment de la BTreeMap.
        self.segments.values_mut().next_back().expect("un log a toujours au moins un segment")
    }
}
