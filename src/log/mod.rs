//! Le log d'une partition : une suite de batches, stockés sur disque dans un
//! répertoire `<data_dir>/<topic>-<partition>/`.
//!
//! Le log est découpé en segments (voir [`segment`]). Chaque segment est un
//! fichier nommé d'après l'offset de son premier batch, sur 20 chiffres :
//! `00000000000000000000.log`, `00000000000000001234.log`...

pub mod segment;

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use crate::record_batch::RecordBatch;
use segment::Segment;

/// Paramètres d'un log.
#[derive(Debug, Clone)]
pub struct LogConfig {}

impl Default for LogConfig {
    fn default() -> Self {
        Self {}
    }
}

#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
    #[allow(dead_code)]
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
    /// Pour l'instant, on repart d'un log vide : un éventuel contenu existant
    /// est écrasé. Le rechargement d'un log existant viendra au chapitre 4.
    pub fn open(dir: &Path, config: LogConfig) -> io::Result<Log> {
        std::fs::create_dir_all(dir)?;
        let segment = Segment::create(dir, 0)?;
        Ok(Log {
            dir: dir.to_path_buf(),
            config,
            segments: BTreeMap::from([(0, segment)]),
            next_offset: 0,
            log_start_offset: 0,
        })
    }

    /// Ajoute des batches à la fin du log, en leur attribuant leurs offsets.
    /// Renvoie l'offset de base du premier batch.
    pub fn append(&mut self, batches: Vec<RecordBatch>) -> io::Result<i64> {
        let first_offset = self.next_offset;
        for mut batch in batches {
            batch.set_base_offset(self.next_offset);
            self.active_segment().append(&batch)?;
            self.next_offset = batch.header.next_offset();
        }
        Ok(first_offset)
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
        self.segments.values_mut().next_back().expect("un log a toujours au moins un segment")
    }
}
