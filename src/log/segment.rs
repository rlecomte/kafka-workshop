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
        let path = log_file_path(dir, base_offset);
        File::create(&path)?;
        // En mode « append », chaque écriture va à la fin du fichier, même
        // après un `set_len` qui l'a raccourci (voir `recover`).
        let file = OpenOptions::new().read(true).append(true).open(&path)?;
        let index = OffsetIndex::create(&index_file_path(dir, base_offset), base_offset)?;
        Ok(Segment {
            base_offset,
            path,
            file,
            index,
            size: 0,
            next_offset: base_offset,
            max_timestamp: -1,
            index_interval_bytes,
            bytes_since_last_index_entry: 0,
        })
    }

    /// Ouvre un segment existant. Les en-têtes des batches sont parcourus
    /// pour retrouver `next_offset` et `max_timestamp`, et l'index est
    /// reconstruit s'il est absent ou invalide.
    ///
    /// Les CRC ne sont pas vérifiés : voir [`Segment::recover`].
    pub fn open(dir: &Path, base_offset: i64, index_interval_bytes: u64) -> io::Result<Segment> {
        let path = log_file_path(dir, base_offset);
        let file = OpenOptions::new().read(true).append(true).open(&path)?;
        let size = file.metadata()?.len();
        let index_path = index_file_path(dir, base_offset);
        let (index, rebuild) = match OffsetIndex::open(&index_path, base_offset, size)? {
            Some(index) => (index, false),
            None => {
                tracing::info!(path = %index_path.display(), "index absent ou invalide, reconstruction");
                (OffsetIndex::create(&index_path, base_offset)?, true)
            }
        };
        let mut segment = Segment {
            base_offset,
            path,
            file,
            index,
            size,
            next_offset: base_offset,
            max_timestamp: -1,
            index_interval_bytes,
            bytes_since_last_index_entry: 0,
        };
        // On s'arrête au premier en-tête illisible ou incomplet : s'il s'agit
        // du dernier segment, `recover` supprimera la fin invalide.
        let headers: Vec<_> = segment
            .headers(0)
            .map_while(Result::ok)
            .take_while(|(position, header)| position + header.total_size() as u64 <= size)
            .collect();
        for (position, header) in headers {
            segment.track(&header, position, rebuild)?;
        }
        Ok(segment)
    }

    /// Vérifie chaque batch du segment (taille, magic, CRC) et tronque le
    /// fichier au premier batch invalide : c'est ce qui reste d'une écriture
    /// interrompue par un arrêt brutal. L'index est reconstruit.
    ///
    /// Renvoie le nombre d'octets supprimés.
    pub fn recover(&mut self) -> io::Result<u64> {
        let original_size = self.size;
        self.index.truncate_to_position(0)?;
        self.next_offset = self.base_offset;
        self.max_timestamp = -1;
        self.bytes_since_last_index_entry = 0;

        let mut position = 0u64;
        while position < original_size {
            let Some(header) = self.read_valid_batch(position, original_size)? else { break };
            self.track(&header, position, true)?;
            position += header.total_size() as u64;
        }
        if position < original_size {
            self.file.set_len(position)?;
            self.size = position;
        }
        Ok(original_size - position)
    }

    /// Lit et valide entièrement le batch à `position`. `None` s'il est
    /// incomplet ou invalide.
    fn read_valid_batch(&self, position: u64, size: u64) -> io::Result<Option<RecordBatchHeader>> {
        if size - position < HEADER_SIZE as u64 {
            return Ok(None);
        }
        let Ok(header) = self.read_header(position) else { return Ok(None) };
        let total = header.total_size() as u64;
        if header.magic != 2 || header.batch_length < (HEADER_SIZE - record_batch::LOG_OVERHEAD) as i32 || position + total > size {
            return Ok(None);
        }
        let mut buf = vec![0u8; total as usize];
        self.file.read_exact_at(&mut buf, position)?;
        if record_batch::compute_crc(&buf) != header.crc {
            return Ok(None);
        }
        Ok(Some(header))
    }

    /// Met à jour l'état du segment pour un batch situé à `position`, et
    /// ajoute une entrée d'index si besoin (et si `index` est vrai).
    fn track(&mut self, header: &RecordBatchHeader, position: u64, index: bool) -> io::Result<()> {
        if index && self.bytes_since_last_index_entry >= self.index_interval_bytes {
            self.index.append(header.base_offset, position)?;
            self.bytes_since_last_index_entry = 0;
        }
        self.bytes_since_last_index_entry += header.total_size() as u64;
        self.next_offset = header.next_offset();
        self.max_timestamp = self.max_timestamp.max(header.max_timestamp);
        Ok(())
    }

    /// Ajoute un batch à la fin du fichier.
    pub fn append(&mut self, batch: &RecordBatch) -> io::Result<()> {
        let position = self.size;
        self.file.write_all(&batch.bytes)?;
        self.size += batch.bytes.len() as u64;
        self.track(&batch.header, position, true)
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
        let mut start = None;
        let mut end = 0u64;
        for entry in self.headers(self.index.lookup(offset)) {
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

    /// Supprime les batches à partir de `offset` (le batch qui contient
    /// `offset` est supprimé en entier).
    pub fn truncate_to(&mut self, offset: i64) -> io::Result<()> {
        let mut cut = self.size;
        for entry in self.headers(self.index.lookup(offset)) {
            let (position, header) = entry?;
            if header.last_offset() >= offset {
                cut = position;
                break;
            }
        }
        self.file.set_len(cut)?;
        self.size = cut;
        // Recalcule next_offset, max_timestamp et l'index.
        self.recover()?;
        Ok(())
    }

    /// Supprime les fichiers du segment.
    pub fn delete(self) -> io::Result<()> {
        let index_path = self.index.path().to_path_buf();
        drop(self.file);
        std::fs::remove_file(&self.path)?;
        match std::fs::remove_file(index_path) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
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
