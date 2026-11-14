//! Un segment : un fichier `.log` contenant des batches mis bout à bout,
//! exactement dans le format où ils sont arrivés (avec leur `base_offset`
//! réécrit), accompagné de son index d'offsets (fichier `.index`).

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::index::OffsetIndex;
use crate::record_batch::{self, HEADER_SIZE, RecordBatch, RecordBatchHeader};

#[derive(Debug)]
pub struct Segment {
    /// Offset du premier batch du segment (et nom du fichier).
    pub base_offset: i64,
    path: PathBuf,
    file: File,
    index: OffsetIndex,
    /// Taille actuelle du fichier, en octets.
    size: u64,
    /// L'offset qui suit le dernier batch du segment (`base_offset` s'il est vide).
    next_offset: i64,
    /// Le plus grand timestamp des batches du segment (-1 s'il est vide).
    /// Sert à la rétention par durée.
    max_timestamp: i64,
    /// Une entrée d'index est ajoutée tous les `index_interval_bytes` octets.
    index_interval_bytes: u64,
    bytes_since_last_index_entry: u64,
}

impl Segment {
    /// Crée un nouveau segment vide (des fichiers existants sont écrasés).
    pub fn create(dir: &Path, base_offset: i64, index_interval_bytes: u64) -> io::Result<Segment> {
        todo!("ch.4 : create")
    }

    /// Ouvre un segment existant. Les en-têtes des batches sont parcourus
    /// pour retrouver `next_offset` et `max_timestamp`, et l'index est
    /// reconstruit s'il est absent ou invalide.
    ///
    /// Les CRC ne sont pas vérifiés : voir [`Segment::recover`].
    pub fn open(dir: &Path, base_offset: i64, index_interval_bytes: u64) -> io::Result<Segment> {
        todo!("ch.4 : open")
    }

    /// Vérifie chaque batch du segment (taille, magic, CRC) et tronque le
    /// fichier au premier batch invalide : c'est ce qui reste d'une écriture
    /// interrompue par un arrêt brutal. L'index est reconstruit.
    ///
    /// Renvoie le nombre d'octets supprimés.
    pub fn recover(&mut self) -> io::Result<u64> {
        todo!("ch.4 : recover")
    }

    /// Lit et valide entièrement le batch à `position`. `None` s'il est
    /// incomplet ou invalide.
    fn read_valid_batch(&self, position: u64, size: u64) -> io::Result<Option<RecordBatchHeader>> {
        todo!("ch.4 : read_valid_batch")
    }

    /// Met à jour l'état du segment pour un batch situé à `position`, et
    /// ajoute une entrée d'index si besoin (et si `index` est vrai).
    fn track(&mut self, header: &RecordBatchHeader, position: u64, index: bool) -> io::Result<()> {
        todo!("ch.4 : track")
    }

    /// Ajoute un batch à la fin du fichier.
    pub fn append(&mut self, batch: &RecordBatch) -> io::Result<()> {
        todo!("ch.4 : append")
    }

    /// Lit l'en-tête du batch qui commence à la position `position` du fichier.
    pub fn read_header(&self, position: u64) -> io::Result<RecordBatchHeader> {
        let mut buf = [0u8; HEADER_SIZE];
        self.file.read_exact_at(&mut buf, position)?;
        RecordBatchHeader::parse(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Parcourt les en-têtes des batches du segment, à partir de la position
    /// `from`. Renvoie des couples (position, en-tête).
    pub fn headers(&self, from: u64) -> impl Iterator<Item = io::Result<(u64, RecordBatchHeader)>> + '_ {
        let mut position = from;
        std::iter::from_fn(move || {
            if position >= self.size {
                return None;
            }
            let result = self.read_header(position).map(|header| {
                let at = position;
                position += header.total_size() as u64;
                (at, header)
            });
            if result.is_err() {
                position = self.size; // on s'arrête à la première erreur
            }
            Some(result)
        })
    }

    /// Lit des batches entiers à partir de celui qui contient `offset`, sans
    /// dépasser `max_bytes` (sauf pour le premier batch, toujours renvoyé en
    /// entier) ni inclure de batch commençant à `max_offset` ou au-delà.
    ///
    /// L'index donne la position à partir de laquelle chercher.
    pub fn read(&self, offset: i64, max_bytes: usize, max_offset: i64) -> io::Result<Bytes> {
        todo!("ch.4 : read")
    }

    /// Supprime les fichiers du segment.
    pub fn delete(self) -> io::Result<()> {
        todo!("ch.4 : delete")
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn next_offset(&self) -> i64 {
        self.next_offset
    }

    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn index(&self) -> &OffsetIndex {
        &self.index
    }
}

/// Le chemin du fichier `.log` d'un segment : l'offset de base sur 20 chiffres.
pub fn log_file_path(dir: &Path, base_offset: i64) -> PathBuf {
    dir.join(format!("{base_offset:020}.log"))
}

/// Le chemin du fichier `.index` d'un segment.
pub fn index_file_path(dir: &Path, base_offset: i64) -> PathBuf {
    dir.join(format!("{base_offset:020}.index"))
}
