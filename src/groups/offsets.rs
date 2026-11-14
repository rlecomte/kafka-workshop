//! Le stockage des offsets commités dans le topic interne `__consumer_offsets`.
//!
//! Chaque commit est écrit comme un record dans la partition du topic qui
//! correspond au groupe. Au démarrage, on relit ces partitions pour
//! reconstruire les offsets en mémoire : le dernier record d'une clé gagne.
//!
//! Kafka utilise un format binaire versionné. Ici, clé et valeur sont en JSON,
//! pour pouvoir les lire avec `kcat -C -t __consumer_offsets -f '%k => %s\n'`.

use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use serde::{Deserialize, Serialize};

use super::group::CommittedOffset;

pub const OFFSETS_TOPIC: &str = "__consumer_offsets";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffsetKey {
    pub group: String,
    pub topic: String,
    pub partition: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffsetValue {
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: String,
    pub commit_timestamp: i64,
}

impl From<&OffsetValue> for CommittedOffset {
    fn from(v: &OffsetValue) -> Self {
        CommittedOffset { offset: v.offset, leader_epoch: v.leader_epoch, metadata: v.metadata.clone() }
    }
}

/// La partition de `__consumer_offsets` qui stocke les offsets d'un groupe :
/// comme Kafka, `abs(hashCode(group_id)) % partitions`, avec le `hashCode`
/// des chaînes Java.
pub fn partition_for(group_id: &str, partitions: i32) -> i32 {
    todo!("ch.5 : partition_for")
}

/// Encode des commits en un batch (un record par partition commitée).
pub fn encode_commits(commits: &[(OffsetKey, OffsetValue)]) -> Bytes {
    let records: Vec<Record> = commits
        .iter()
        .enumerate()
        .map(|(i, (key, value))| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            sequence: i as i32,
            timestamp: value.commit_timestamp,
            key: Some(Bytes::from(serde_json::to_vec(key).unwrap())),
            value: Some(Bytes::from(serde_json::to_vec(value).unwrap())),
            headers: IndexMap::new(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(&mut buf, &records, &RecordEncodeOptions { version: 2, compression: Compression::None })
        .expect("encodage d'un batch d'offsets");
    buf.freeze()
}

/// Décode les commits contenus dans des batches lus dans le log. Les records
/// illisibles sont ignorés.
pub fn decode_commits(mut bytes: Bytes) -> Vec<(OffsetKey, OffsetValue)> {
    let mut commits = Vec::new();
    while !bytes.is_empty() {
        let Ok(set) = RecordBatchDecoder::decode(&mut bytes) else { break };
        for record in set.records {
            let (Some(key), Some(value)) = (record.key, record.value) else { continue };
            if let (Ok(key), Ok(value)) = (serde_json::from_slice(&key), serde_json::from_slice(&value)) {
                commits.push((key, value));
            }
        }
    }
    commits
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_matches_kafka() {
        // (String.hashCode() & 0x7fffffff) % 50, comme dans Kafka.
        assert_eq!(partition_for("my-group", 50), 36);
        assert_eq!(partition_for("console-consumer-1", 50), 24);
        assert_eq!(partition_for("é-groupe", 50), 26);
    }
}
