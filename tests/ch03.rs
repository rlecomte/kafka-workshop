//! Chapitre 3 : Fetch et ListOffsets.
//!
//! Lancer avec : `cargo test --test ch03`

mod common;

use std::time::{Duration, Instant};

use common::*;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::fetch_request::FetchPartition;
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest};
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

fn offsets(records: &[kafka_protocol::records::Record]) -> Vec<i64> {
    records.iter().map(|r| r.offset).collect()
}

#[tokio::test]
async fn api_versions_announces_fetch() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let response = client.send(ApiVersionsRequest::default(), 3).await.unwrap();

    for (key, min) in [(ApiKey::Fetch, 4), (ApiKey::ListOffsets, 1)] {
        let api = response.api_keys.iter().find(|a| a.api_key == key as i16);
        let api = api.unwrap_or_else(|| panic!("{key:?} doit être annoncée dans ApiVersions"));
        assert!(api.min_version <= min, "{key:?} doit être supportée à partir de la v{min}");
    }
}

#[tokio::test]
async fn fetch_what_was_produced() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "orders", 1).await;
    produce(&mut client, "orders", 0, batch(&["a", "b", "c"])).await;
    produce(&mut client, "orders", 0, batch(&["d", "e"])).await;

    let data = fetch(&mut client, "orders", 0, 0).await;

    assert_ok(data.error_code, "fetch");
    assert_eq!(data.high_watermark, 5);
    assert_eq!(data.log_start_offset, 0);
    let records = fetched_records(&data);
    assert_eq!(values(&records), vec!["a", "b", "c", "d", "e"]);
    assert_eq!(offsets(&records), vec![0, 1, 2, 3, 4]);
}

/// Le broker renvoie des batches entiers : si l'offset demandé est au milieu
/// d'un batch, tout le batch est renvoyé, et le consumer ignore les records
/// qui précèdent l'offset demandé.
#[tokio::test]
async fn fetch_from_the_middle_of_a_batch() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "middle", 1).await;
    produce(&mut client, "middle", 0, batch(&["a", "b", "c"])).await;
    produce(&mut client, "middle", 0, batch(&["d", "e", "f"])).await;

    let records = fetched_records(&fetch(&mut client, "middle", 0, 4).await);
    assert_eq!(offsets(&records), vec![3, 4, 5], "le batch 3..=5 contient l'offset 4");

    let records = fetched_records(&fetch(&mut client, "middle", 0, 3).await);
    assert_eq!(offsets(&records), vec![3, 4, 5], "le premier batch (0..=2) est sauté");
}

#[tokio::test]
async fn fetch_at_log_end_is_empty() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "end", 1).await;

    let data = fetch(&mut client, "end", 0, 0).await;
    assert_ok(data.error_code, "fetch d'un log vide");
    assert!(fetched_records(&data).is_empty());
    assert_eq!(data.high_watermark, 0);

    produce(&mut client, "end", 0, batch(&["a", "b"])).await;
    let data = fetch(&mut client, "end", 0, 2).await;
    assert_ok(data.error_code, "fetch au log end offset");
    assert!(fetched_records(&data).is_empty());
    assert_eq!(data.high_watermark, 2);
}

#[tokio::test]
async fn fetch_out_of_range() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "range", 1).await;
    produce(&mut client, "range", 0, batch(&["a", "b"])).await;

    for offset in [3, 100, -1] {
        let data = fetch(&mut client, "range", 0, offset).await;
        assert_error(data.error_code, ResponseError::OffsetOutOfRange, &format!("offset {offset}"));
    }
}

#[tokio::test]
async fn fetch_unknown_partition() {
    let broker = start_broker_with(broker::Config { auto_create_topics: false, ..Default::default() }).await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "known", 1).await;

    let data = fetch(&mut client, "unknown", 0, 0).await;
    assert_error(data.error_code, ResponseError::UnknownTopicOrPartition, "topic inconnu");
    let data = fetch(&mut client, "known", 1, 0).await;
    assert_error(data.error_code, ResponseError::UnknownTopicOrPartition, "partition inconnue");
}

#[tokio::test]
async fn fetch_respects_max_bytes_with_whole_batches() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "sized", 1).await;
    let value = "x".repeat(100);
    for _ in 0..10 {
        produce(&mut client, "sized", 0, batch(&[value.as_str()])).await;
    }
    let batch_size = batch(&[value.as_str()]).len() as i32;

    let response =
        client.send(fetch_request("sized", 0, 0, batch_size * 3 + 10), FETCH_VERSION).await.unwrap();
    let records = fetched_records(&response.responses[0].partitions[0]);
    assert_eq!(offsets(&records), vec![0, 1, 2], "3 batches entiers tiennent dans la limite");

    // Le premier batch est toujours renvoyé, même s'il dépasse la limite.
    let response = client.send(fetch_request("sized", 0, 5, 10), FETCH_VERSION).await.unwrap();
    let records = fetched_records(&response.responses[0].partitions[0]);
    assert_eq!(offsets(&records), vec![5], "un batch plus gros que max_bytes est renvoyé seul");
}

#[tokio::test]
async fn fetch_several_partitions() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "multi", 3).await;
    for p in 0..3 {
        produce(&mut client, "multi", p, batch(&[format!("p{p}").as_str()])).await;
    }

    let mut request = fetch_request("multi", 0, 0, 1024 * 1024);
    request.topics[0].partitions = (0..3)
        .map(|p| FetchPartition::default().with_partition(p).with_fetch_offset(0).with_partition_max_bytes(1024))
        .collect();
    let response = client.send(request, FETCH_VERSION).await.unwrap();

    let partitions = &response.responses[0].partitions;
    assert_eq!(partitions.len(), 3);
    for data in partitions {
        let expected = format!("p{}", data.partition_index);
        assert_eq!(values(&fetched_records(data)), vec![expected]);
    }
}

/// Long polling : un Fetch sans données attend qu'un message arrive.
#[tokio::test]
async fn fetch_waits_for_new_data() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "wait", 1).await;

    let mut request = fetch_request("wait", 0, 0, 1024 * 1024);
    request.max_wait_ms = 10_000;
    request.min_bytes = 1;
    let mut consumer = Client::connect(broker.addr).await;
    let start = Instant::now();
    let pending = tokio::spawn(async move { consumer.send(request, FETCH_VERSION).await.unwrap() });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!pending.is_finished(), "le Fetch doit attendre tant qu'il n'y a pas de données");
    produce(&mut client, "wait", 0, batch(&["enfin"])).await;

    let response = tokio::time::timeout(Duration::from_secs(3), pending)
        .await
        .expect("le Fetch doit répondre dès que des données arrivent")
        .unwrap();
    assert!(start.elapsed() < Duration::from_secs(5));
    assert_eq!(values(&fetched_records(&response.responses[0].partitions[0])), vec!["enfin"]);
}

/// Sans données, un Fetch répond après max_wait_ms.
#[tokio::test]
async fn fetch_times_out_after_max_wait() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "timeout", 1).await;

    let mut request = fetch_request("timeout", 0, 0, 1024 * 1024);
    request.max_wait_ms = 500;
    request.min_bytes = 1;
    let start = Instant::now();
    let response = client.send(request, FETCH_VERSION).await.unwrap();

    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(450), "réponse trop tôt : {elapsed:?}");
    assert!(elapsed < Duration::from_millis(3000), "réponse trop tard : {elapsed:?}");
    assert!(fetched_records(&response.responses[0].partitions[0]).is_empty());
}

/// Une erreur sur une partition fait répondre immédiatement.
#[tokio::test]
async fn fetch_with_error_does_not_wait() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "err", 1).await;

    let mut request = fetch_request("err", 0, 42, 1024 * 1024);
    request.max_wait_ms = 5000;
    request.min_bytes = 1;
    let start = Instant::now();
    let response = client.send(request, FETCH_VERSION).await.unwrap();

    assert!(start.elapsed() < Duration::from_secs(2), "une erreur doit être renvoyée sans attendre");
    assert_error(response.responses[0].partitions[0].error_code, ResponseError::OffsetOutOfRange, "offset 42");
}

#[tokio::test]
async fn compressed_batches_round_trip() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "zipped", 1).await;

    let records = (0..20).map(|i| record(Some("k"), &format!("message {i}"))).collect();
    produce(&mut client, "zipped", 0, encode_batch(records, Compression::Gzip)).await;

    let records = fetched_records(&fetch(&mut client, "zipped", 0, 0).await);
    assert_eq!(records.len(), 20);
    assert_eq!(values(&records)[19], "message 19");
}

#[tokio::test]
async fn list_offsets_earliest_and_latest() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "offsets", 1).await;

    assert_eq!(list_offset(&mut client, "offsets", 0, -2).await.offset, 0, "earliest d'un log vide");
    assert_eq!(list_offset(&mut client, "offsets", 0, -1).await.offset, 0, "latest d'un log vide");

    produce(&mut client, "offsets", 0, batch(&["a", "b", "c"])).await;

    let earliest = list_offset(&mut client, "offsets", 0, -2).await;
    assert_ok(earliest.error_code, "earliest");
    assert_eq!(earliest.offset, 0);
    let latest = list_offset(&mut client, "offsets", 0, -1).await;
    assert_ok(latest.error_code, "latest");
    assert_eq!(latest.offset, 3, "latest est l'offset du prochain message");
}

#[tokio::test]
async fn list_offsets_by_timestamp() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "times", 1).await;

    let at = |ts: i64, value: &str| {
        let mut r = record(None, value);
        r.timestamp = ts;
        encode_batch(vec![r], Compression::None)
    };
    produce(&mut client, "times", 0, at(1_000, "old")).await;
    produce(&mut client, "times", 0, at(2_000, "middle")).await;
    produce(&mut client, "times", 0, at(3_000, "recent")).await;

    assert_eq!(list_offset(&mut client, "times", 0, 500).await.offset, 0);
    assert_eq!(list_offset(&mut client, "times", 0, 1_500).await.offset, 1);
    assert_eq!(list_offset(&mut client, "times", 0, 3_000).await.offset, 2);
    assert_eq!(list_offset(&mut client, "times", 0, 9_000).await.offset, -1, "aucun message assez récent");
}

#[tokio::test]
async fn list_offsets_unknown_partition() {
    let broker = start_broker_with(broker::Config { auto_create_topics: false, ..Default::default() }).await;
    let mut client = Client::connect(broker.addr).await;

    let response = list_offset(&mut client, "nope", 0, -1).await;
    assert_error(response.error_code, ResponseError::UnknownTopicOrPartition, "topic inconnu");
}

#[tokio::test]
async fn older_fetch_version_works() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "old", 1).await;
    produce(&mut client, "old", 0, batch(&["a"])).await;

    let request = fetch_request("old", 0, 0, 1024 * 1024);
    let response = client.send(request, 4).await.unwrap();
    assert_eq!(values(&fetched_records(&response.responses[0].partitions[0])), vec!["a"]);
}

