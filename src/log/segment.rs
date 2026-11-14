//! Un segment : un fichier `.log` contenant des batches mis bout à bout,
//! exactement dans le format où ils sont arrivés (avec leur `base_offset`
//! réécrit).

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::record_batch::RecordBatch;

#[derive(Debug)]
pub struct Segment {
    /// Offset du premier batch du segment (et nom du fichier).
    pub base_offset: i64,
    path: PathBuf,
    file: File,
    /// Taille actuelle du fichier, en octets.
    size: u64,
}

impl Segment {
    /// Crée un nouveau segment vide (un fichier existant est écrasé).
    pub fn create(dir: &Path, base_offset: i64) -> io::Result<Segment> {
        let path = log_file_path(dir, base_offset);
        let file = OpenOptions::new().create(true).read(true).write(true).truncate(true).open(&path)?;
        Ok(Segment { base_offset, path, file, size: 0 })
    }

    /// Ajoute un batch à la fin du fichier.
    pub fn append(&mut self, batch: &RecordBatch) -> io::Result<()> {
        self.file.write_all(&batch.bytes)?;
        self.size += batch.bytes.len() as u64;
        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Le chemin du fichier `.log` d'un segment : l'offset de base sur 20 chiffres.
pub fn log_file_path(dir: &Path, base_offset: i64) -> PathBuf {
    dir.join(format!("{base_offset:020}.log"))
}
