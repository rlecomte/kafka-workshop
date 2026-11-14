//! Chapitre 2 : Produce et le format RecordBatch.
//!
//! Lancer avec : `cargo test --test ch02`

mod common;

use bytes::{Bytes, BytesMut};
use common::*;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, InitProducerIdRequest};
use kafka_protocol::records::Compression;

fn assert_error(actual: i16, expected: ResponseError, context: &str) {
    assert_eq!(
        actual,
        expected.code(),
        "{context} : code d'erreur {actual} ({:?}), attendu {expected:?}",
        ResponseError::try_from_code(actual)
    );
}

fn assert_ok(actual: i16, context: &str) {
    assert_eq!(actual, 0, "{context} : code d'erreur {actual} ({:?})", ResponseError::try_from_code(actual));
}

/// Modifie un batch puis recalcule son CRC, pour fabriquer des batches
/// invalides autrement que par leur CRC.
fn patch(batch: &Bytes, at: usize, bytes: &[u8]) -> Bytes {
    let mut patched = BytesMut::from(&batch[..]);
    patched[at..at + bytes.len()].copy_from_slice(bytes);
    let crc = crc32c::crc32c(&patched[21..]);
    patched[17..21].copy_from_slice(&crc.to_be_bytes());
    patched.freeze()
}

#[tokio::test]
async fn api_versions_announces_produce() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let response = client.send(ApiVersionsRequest::default(), 3).await.unwrap();

    for key in [ApiKey::Produce, ApiKey::InitProducerId] {
        assert!(
            response.api_keys.iter().any(|a| a.api_key == key as i16),
            "{key:?} doit être annoncée dans ApiVersions"
        );
    }
}

#[tokio::test]
async fn produce_assigns_consecutive_offsets() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "orders", 2).await;

    let first = produce(&mut client, "orders", 0, batch(&["a", "b", "c"])).await;
    assert_ok(first.error_code, "premier produce");
    assert_eq!(first.base_offset, 0, "le premier batch commence à l'offset 0");

    let second = produce(&mut client, "orders", 0, batch(&["d", "e"])).await;
    assert_eq!(second.base_offset, 3, "le second batch suit les 3 records du premier");

    let third = produce(&mut client, "orders", 0, batch(&["f"])).await;
    assert_eq!(third.base_offset, 5);

    let other = produce(&mut client, "orders", 1, batch(&["x"])).await;
    assert_eq!(other.base_offset, 0, "chaque partition a ses propres offsets");
}

/// Le champ records peut contenir plusieurs batches mis bout à bout.
#[tokio::test]
async fn produce_several_batches_at_once() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "multi", 1).await;

    let mut records = BytesMut::new();
    records.extend_from_slice(&batch(&["a", "b"]));
    records.extend_from_slice(&batch(&["c", "d", "e"]));
    let response = produce(&mut client, "multi", 0, records.freeze()).await;
    assert_ok(response.error_code, "produce de deux batches");
    assert_eq!(response.base_offset, 0, "base_offset est celui du premier batch");

    let next = produce(&mut client, "multi", 0, batch(&["f"])).await;
    assert_eq!(next.base_offset, 5, "les deux batches ont reçu leurs offsets");
}

/// Les batches sont écrits sur disque tels quels, avec leurs offsets réécrits.
#[tokio::test]
async fn batches_are_written_to_disk() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "stored", 1).await;

    produce(&mut client, "stored", 0, batch(&["one", "two"])).await;
    produce(&mut client, "stored", 0, batch(&["three"])).await;

    let path = broker.data_dir.path().join("stored-0").join("00000000000000000000.log");
    let content = std::fs::read(&path).unwrap_or_else(|e| panic!("{} illisible : {e}", path.display()));
    let records = decode_records(Bytes::from(content));
    assert_eq!(values(&records), vec!["one", "two", "three"]);
    let offsets: Vec<_> = records.iter().map(|r| r.offset).collect();
    assert_eq!(offsets, vec![0, 1, 2], "les offsets stockés sont ceux attribués par le broker");
}

/// Le broker ne décompresse pas : un batch compressé est stocké tel quel.
#[tokio::test]
async fn compressed_batches_are_stored_as_is() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "compressed", 1).await;

    let records = (0..50).map(|i| record(Some("key"), &format!("message compressé numéro {i}"))).collect();
    let compressed = encode_batch(records, Compression::Gzip);
    let response = produce(&mut client, "compressed", 0, compressed.clone()).await;
    assert_ok(response.error_code, "batch gzip");

    let next = produce(&mut client, "compressed", 0, batch(&["après"])).await;
    assert_eq!(next.base_offset, 50, "le batch compressé contient 50 records");

    let path = broker.data_dir.path().join("compressed-0").join("00000000000000000000.log");
    let content = std::fs::read(path).unwrap();
    assert_eq!(&content[8..compressed.len()], &compressed[8..], "le batch est stocké octet pour octet");
}

#[tokio::test]
async fn produce_to_unknown_topic_or_partition() {
    let broker = start_broker_with(broker::Config { auto_create_topics: false, ..Default::default() }).await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "known", 1).await;

    let response = produce(&mut client, "unknown", 0, batch(&["a"])).await;
    assert_error(response.error_code, ResponseError::UnknownTopicOrPartition, "topic inconnu");

    let response = produce(&mut client, "known", 3, batch(&["a"])).await;
    assert_error(response.error_code, ResponseError::UnknownTopicOrPartition, "partition inconnue");
}

#[tokio::test]
async fn corrupt_crc_is_rejected() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "crc", 1).await;

    let mut corrupt = BytesMut::from(&batch(&["hello"])[..]);
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0xff; // un octet modifié dans le dernier record
    let response = produce(&mut client, "crc", 0, corrupt.freeze()).await;
    assert_error(response.error_code, ResponseError::CorruptMessage, "CRC invalide");

    let next = produce(&mut client, "crc", 0, batch(&["ok"])).await;
    assert_eq!(next.base_offset, 0, "un batch refusé ne consomme pas d'offset");
}

#[tokio::test]
async fn truncated_batch_is_rejected() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "truncated", 1).await;

    let full = batch(&["hello", "world"]);
    for len in [10, 61, full.len() - 1] {
        let response = produce(&mut client, "truncated", 0, full.slice(..len)).await;
        assert_error(response.error_code, ResponseError::CorruptMessage, &format!("batch tronqué à {len} octets"));
    }
}

#[tokio::test]
async fn old_message_format_is_rejected() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "magic", 1).await;

    // Le magic (octet 16) n'est pas couvert par le CRC.
    let mut old = BytesMut::from(&batch(&["hello"])[..]);
    old[16] = 1;
    let response = produce(&mut client, "magic", 0, old.freeze()).await;
    assert_error(response.error_code, ResponseError::UnsupportedForMessageFormat, "magic 1");

    // Un message au format v0 (MessageSet), tel qu'envoyé par librdkafka quand
    // le broker n'annonce pas Fetch v4+ : son en-tête fait moins de 61 octets.
    let legacy: &[u8] = &[
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 87, 231, 73, 110, 0, 0, 0, 0, 0, 2, 107, 49, 0, 0, 0, 2, 118, 49,
    ];
    let response = produce(&mut client, "magic", 0, Bytes::from_static(legacy)).await;
    assert_error(response.error_code, ResponseError::UnsupportedForMessageFormat, "MessageSet v0");
}

#[tokio::test]
async fn inconsistent_record_count_is_rejected() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "count", 1).await;

    // records_count (octet 57) : 3 au lieu de 2, avec un CRC recalculé.
    let invalid = patch(&batch(&["a", "b"]), 57, &3i32.to_be_bytes());
    let response = produce(&mut client, "count", 0, invalid).await;
    assert_error(response.error_code, ResponseError::InvalidRecord, &format!("records_count incohérent ({:?})", response.error_message));
}

#[tokio::test]
async fn too_large_batch_is_rejected() {
    let config = broker::Config { max_message_bytes: 1000, ..Default::default() };
    let broker = start_broker_with(config).await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "large", 1).await;

    let big = "x".repeat(2000);
    let response = produce(&mut client, "large", 0, batch(&[big.as_str()])).await;
    assert_error(response.error_code, ResponseError::MessageTooLarge, "batch de plus de 1000 octets");

    let response = produce(&mut client, "large", 0, batch(&["small"])).await;
    assert_ok(response.error_code, "petit batch");
}

#[tokio::test]
async fn invalid_acks_is_rejected() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "acks", 1).await;

    let response = client.send(produce_request("acks", 0, batch(&["a"]), 2), 9).await.unwrap();
    assert_error(response.responses[0].partition_responses[0].error_code, ResponseError::InvalidRequiredAcks, "acks=2");
}

/// Avec acks = 0, le broker écrit les messages mais n'envoie pas de réponse.
#[tokio::test]
async fn acks_zero_sends_no_response() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "fire-and-forget", 1).await;

    client.send_only(&produce_request("fire-and-forget", 0, batch(&["a", "b"]), 0), 9).await.unwrap();
    // La prochaine réponse reçue doit être celle de cette requête-ci, pas
    // celle du Produce (Client::send vérifie le correlation_id).
    let versions = client.send(ApiVersionsRequest::default(), 3).await;
    assert!(versions.is_ok(), "aucune réponse ne doit être envoyée pour acks=0 : {versions:?}");

    let next = produce(&mut client, "fire-and-forget", 0, batch(&["c"])).await;
    assert_eq!(next.base_offset, 2, "les messages envoyés avec acks=0 ont bien été écrits");
}

#[tokio::test]
async fn older_produce_version_works() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "old", 1).await;

    let response = client.send(produce_request("old", 0, batch(&["a"]), 1), 3).await.unwrap();
    assert_ok(response.responses[0].partition_responses[0].error_code, "Produce v3");
}

/// Plusieurs producers en parallèle sur la même partition : chaque record
/// reçoit un offset unique, sans trou.
#[tokio::test]
async fn concurrent_producers() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "concurrent", 1).await;

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let addr = broker.addr;
        tasks.push(tokio::spawn(async move {
            let mut client = Client::connect(addr).await;
            let mut offsets = Vec::new();
            for _ in 0..20 {
                let response = produce(&mut client, "concurrent", 0, batch(&["a", "b"])).await;
                assert_ok(response.error_code, "produce concurrent");
                offsets.push(response.base_offset);
            }
            offsets
        }));
    }
    let mut offsets = Vec::new();
    for task in tasks {
        offsets.extend(task.await.unwrap());
    }
    offsets.sort();
    let expected: Vec<i64> = (0..320).step_by(2).collect();
    assert_eq!(offsets, expected);
}

#[tokio::test]
async fn init_producer_id_gives_unique_ids() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let mut ids = Vec::new();
    for _ in 0..3 {
        let request = InitProducerIdRequest::default().with_transactional_id(None).with_transaction_timeout_ms(60000);
        let response = client.send(request, 4).await.unwrap();
        assert_ok(response.error_code, "InitProducerId");
        assert!(response.producer_id.0 >= 0);
        ids.push(response.producer_id.0);
    }
    ids.dedup();
    assert_eq!(ids.len(), 3, "chaque producer reçoit un identifiant différent");
}
