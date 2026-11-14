//! Un segment : un fichier `.log` contenant des batches mis bout à bout,
//! exactement dans le format où ils sont arrivés (avec leur `base_offset`
//! réécrit).

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::record_batch::{HEADER_SIZE, RecordBatch, RecordBatchHeader};

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
    pub fn read(&self, offset: i64, max_bytes: usize, max_offset: i64) -> io::Result<Bytes> {
        let mut start = None;
        let mut end = 0u64;
        for entry in self.headers(0) {
            let (position, header) = entry?;
            if header.base_offset >= max_offset {
                break;
            }
            if header.last_offset() < offset {
                continue; // batch entièrement avant l'offset demandé
            }
            let batch_end = position + header.total_size() as u64;
            match start {
                None => start = Some(position),
                Some(first) if (batch_end - first) as usize > max_bytes => break,
                Some(_) => {}
            }
            end = batch_end;
        }
        let Some(start) = start else {
            return Ok(Bytes::new());
        };
        let mut buf = vec![0u8; (end - start) as usize];
        self.file.read_exact_at(&mut buf, start)?;
        Ok(Bytes::from(buf))
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
