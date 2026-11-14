//! L'index d'offsets d'un segment : un fichier `.index` qui associe des
//! offsets à des positions dans le fichier `.log`.
//!
//! L'index est **clairsemé** : il ne contient pas une entrée par batch, mais
//! une entrée tous les `index_interval_bytes` octets environ. Pour trouver un
//! offset, on cherche la dernière entrée inférieure ou égale, puis on
//! parcourt le segment à partir de sa position.
//!
//! Chaque entrée fait 8 octets, en big-endian :
//!
//! ```text
//! +------------------------------+----------------------+
//! | offset relatif : u32         | position : u32       |
//! | (offset - base_offset)       | (octets dans le .log)|
//! +------------------------------+----------------------+
//! ```
//!
//! Le fichier est relu entièrement en mémoire à l'ouverture (Kafka utilise un
//! `mmap`).

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub const ENTRY_SIZE: usize = 8;

#[derive(Debug)]
pub struct OffsetIndex {
    base_offset: i64,
    path: PathBuf,
    file: File,
    /// Les entrées (offset relatif, position), triées.
    entries: Vec<(u32, u32)>,
}

impl OffsetIndex {
    /// Crée un index vide (un fichier existant est écrasé).
    pub fn create(path: &Path, base_offset: i64) -> io::Result<OffsetIndex> {
        File::create(path)?;
        // En mode « append », chaque écriture va à la fin du fichier, même
        // après un `set_len` qui l'a raccourci.
        let file = OpenOptions::new().append(true).read(true).open(path)?;
        Ok(OffsetIndex { base_offset, path: path.to_path_buf(), file, entries: Vec::new() })
    }

    /// Ouvre un index existant. Renvoie `None` s'il est absent ou invalide
    /// (taille non multiple de 8, entrées non croissantes, positions au-delà
    /// de `log_size`) : il faudra alors le reconstruire.
    pub fn open(path: &Path, base_offset: i64, log_size: u64) -> io::Result<Option<OffsetIndex>> {
        let content = match std::fs::read(path) {
            Ok(content) => content,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if content.len() % ENTRY_SIZE != 0 {
            return Ok(None);
        }
        let entries: Vec<(u32, u32)> = content
            .chunks_exact(ENTRY_SIZE)
            .map(|e| (u32::from_be_bytes(e[0..4].try_into().unwrap()), u32::from_be_bytes(e[4..8].try_into().unwrap())))
            .collect();
        let sorted = entries.windows(2).all(|w| w[0].0 < w[1].0 && w[0].1 < w[1].1);
        let in_bounds = entries.iter().all(|&(_, position)| (position as u64) < log_size);
        if !sorted || !in_bounds {
            return Ok(None);
        }
        let file = OpenOptions::new().append(true).read(true).open(path)?;
        Ok(Some(OffsetIndex { base_offset, path: path.to_path_buf(), file, entries }))
    }

    /// Ajoute une entrée : le batch d'offset `offset` commence à `position`.
    pub fn append(&mut self, offset: i64, position: u64) -> io::Result<()> {
        let relative = (offset - self.base_offset) as u32;
        let position = position as u32;
        let mut entry = [0u8; ENTRY_SIZE];
        entry[0..4].copy_from_slice(&relative.to_be_bytes());
        entry[4..8].copy_from_slice(&position.to_be_bytes());
        self.file.write_all(&entry)?;
        self.entries.push((relative, position));
        Ok(())
    }

    /// La position à partir de laquelle chercher `offset` : celle de la
    /// dernière entrée dont l'offset est inférieur ou égal, ou 0.
    pub fn lookup(&self, offset: i64) -> u64 {
        if offset < self.base_offset {
            return 0;
        }
        let relative = (offset - self.base_offset).min(u32::MAX as i64) as u32;
        // partition_point : nombre d'entrées dont l'offset est <= relative.
        let count = self.entries.partition_point(|&(o, _)| o <= relative);
        if count == 0 { 0 } else { self.entries[count - 1].1 as u64 }
    }

    /// Supprime les entrées qui pointent à `position` ou au-delà.
    pub fn truncate_to_position(&mut self, position: u64) -> io::Result<()> {
        let keep = self.entries.partition_point(|&(_, p)| (p as u64) < position);
        self.entries.truncate(keep);
        self.file.set_len((keep * ENTRY_SIZE) as u64)?;
        Ok(())
    }

    pub fn entries(&self) -> &[(u32, u32)] {
        &self.entries
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}
