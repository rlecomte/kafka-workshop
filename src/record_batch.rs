//! Le format RecordBatch v2 (« magic 2 »), utilisé par Kafka depuis la 0.11
//! pour transporter et stocker les messages.
//!
//! Le broker ne regarde que l'en-tête de chaque batch : il le valide,
//! réécrit le `base_offset`, et stocke le batch tel quel. Il ne décode
//! jamais les records eux-mêmes (qui peuvent d'ailleurs être compressés).
//!
//! ```text
//! offset  taille  champ
//!   0       8     base_offset             ┐ réécrits par le broker,
//!   8       4     batch_length            │ hors du CRC
//!  12       4     partition_leader_epoch  ┘
//!  16       1     magic (= 2)
//!  17       4     crc (CRC-32C des octets 21..fin)
//!  21       2     attributes (bits 0-2 : compression, bit 4 : transactionnel, bit 5 : contrôle)
//!  23       4     last_offset_delta
//!  27       8     base_timestamp
//!  35       8     max_timestamp
//!  43       8     producer_id
//!  51       2     producer_epoch
//!  53       4     base_sequence
//!  57       4     records_count
//!  61       ...   records
//! ```
//!
//! `batch_length` compte les octets qui suivent ce champ : la taille totale
//! du batch est donc `12 + batch_length`.

use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::ResponseError;

/// Taille de l'en-tête d'un batch v2.
pub const HEADER_SIZE: usize = 61;
/// Taille des champs qui précèdent `batch_length` inclus (base_offset + batch_length).
pub const LOG_OVERHEAD: usize = 12;
/// Début de la zone couverte par le CRC (juste après le champ `crc`).
const CRC_START: usize = 21;
const PARTITION_LEADER_EPOCH_OFFSET: usize = 12;
/// Position du champ `magic`, au même endroit dans toutes les versions du format.
const MAGIC_OFFSET: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordBatchHeader {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records_count: i32,
}

impl RecordBatchHeader {
    /// Décode l'en-tête au début de `buf`, sans rien valider d'autre que la
    /// présence de suffisamment d'octets.
    pub fn parse(mut buf: &[u8]) -> Result<Self, BatchError> {
        if buf.len() < HEADER_SIZE {
            return Err(BatchError::Truncated);
        }
        Ok(Self {
            base_offset: buf.get_i64(),
            batch_length: buf.get_i32(),
            partition_leader_epoch: buf.get_i32(),
            magic: buf.get_i8(),
            crc: buf.get_u32(),
            attributes: buf.get_i16(),
            last_offset_delta: buf.get_i32(),
            base_timestamp: buf.get_i64(),
            max_timestamp: buf.get_i64(),
            producer_id: buf.get_i64(),
            producer_epoch: buf.get_i16(),
            base_sequence: buf.get_i32(),
            records_count: buf.get_i32(),
        })
    }

    /// Taille totale du batch, en-tête compris.
    pub fn total_size(&self) -> usize {
        LOG_OVERHEAD + self.batch_length as usize
    }

    /// Offset du dernier record du batch.
    pub fn last_offset(&self) -> i64 {
        self.base_offset + self.last_offset_delta as i64
    }

    /// Offset qui suivra ce batch.
    pub fn next_offset(&self) -> i64 {
        self.last_offset() + 1
    }

    /// Codec de compression (0 : aucune, 1 : gzip, 2 : snappy, 3 : lz4, 4 : zstd).
    pub fn compression(&self) -> i16 {
        self.attributes & 0x07
    }
}

/// Un batch validé : son en-tête décodé et ses octets bruts.
#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub header: RecordBatchHeader,
    pub bytes: BytesMut,
}

impl RecordBatch {
    /// Réécrit le `base_offset`, dans l'en-tête décodé et dans les octets.
    /// Ce champ n'est pas couvert par le CRC, qui reste donc valide.
    pub fn set_base_offset(&mut self, offset: i64) {
        self.header.base_offset = offset;
        self.bytes[0..8].copy_from_slice(&offset.to_be_bytes());
    }

    /// Réécrit le `partition_leader_epoch` (également hors du CRC).
    pub fn set_partition_leader_epoch(&mut self, epoch: i32) {
        self.header.partition_leader_epoch = epoch;
        self.bytes[PARTITION_LEADER_EPOCH_OFFSET..PARTITION_LEADER_EPOCH_OFFSET + 4]
            .copy_from_slice(&epoch.to_be_bytes());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchError {
    /// Moins d'octets que ce qu'annonce l'en-tête (ou qu'un en-tête complet).
    Truncated,
    /// `batch_length` incohérent.
    InvalidLength(i32),
    /// Version du format autre que 2.
    UnsupportedMagic(i8),
    /// Le CRC ne correspond pas au contenu.
    CorruptCrc { expected: u32, actual: u32 },
    /// `records_count` et `last_offset_delta` incohérents.
    InvalidRecordCount { records_count: i32, last_offset_delta: i32 },
}

impl BatchError {
    /// Le code d'erreur Kafka renvoyé au producer.
    pub fn error(&self) -> ResponseError {
        match self {
            BatchError::UnsupportedMagic(_) => ResponseError::UnsupportedForMessageFormat,
            BatchError::InvalidRecordCount { .. } => ResponseError::InvalidRecord,
            _ => ResponseError::CorruptMessage,
        }
    }
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchError::Truncated => write!(f, "batch tronqué"),
            BatchError::InvalidLength(n) => write!(f, "batch_length invalide : {n}"),
            BatchError::UnsupportedMagic(m) => write!(f, "magic {m} non supporté (seul le format v2 l'est)"),
            BatchError::CorruptCrc { expected, actual } => {
                write!(f, "CRC invalide : annoncé {expected:#010x}, calculé {actual:#010x}")
            }
            BatchError::InvalidRecordCount { records_count, last_offset_delta } => write!(
                f,
                "records_count ({records_count}) incohérent avec last_offset_delta ({last_offset_delta})"
            ),
        }
    }
}

impl std::error::Error for BatchError {}

/// Calcule le CRC-32C d'un batch complet (sur les octets 21..fin).
pub fn compute_crc(batch: &[u8]) -> u32 {
    crc32c::crc32c(&batch[CRC_START..])
}

/// Découpe le contenu du champ `records` d'une requête Produce en batches,
/// et valide chacun d'eux.
pub fn parse_batches(mut records: Bytes) -> Result<Vec<RecordBatch>, BatchError> {
    let mut batches = Vec::new();
    while !records.is_empty() {
        // Le magic est lu en premier : les anciens formats (v0, v1) ont un
        // en-tête plus court que 61 octets.
        if let Some(&magic) = records.get(MAGIC_OFFSET)
            && magic as i8 != 2
        {
            return Err(BatchError::UnsupportedMagic(magic as i8));
        }
        let header = RecordBatchHeader::parse(&records)?;
        if (header.batch_length as usize) < HEADER_SIZE - LOG_OVERHEAD {
            return Err(BatchError::InvalidLength(header.batch_length));
        }
        if records.len() < header.total_size() {
            return Err(BatchError::Truncated);
        }
        let bytes = records.split_to(header.total_size());
        let actual = compute_crc(&bytes);
        if actual != header.crc {
            return Err(BatchError::CorruptCrc { expected: header.crc, actual });
        }
        if header.records_count < 1 || header.last_offset_delta != header.records_count - 1 {
            return Err(BatchError::InvalidRecordCount {
                records_count: header.records_count,
                last_offset_delta: header.last_offset_delta,
            });
        }
        batches.push(RecordBatch { header, bytes: BytesMut::from(&bytes[..]) });
    }
    Ok(batches)
}
