//! Chapitre 5c : persistance des offsets dans `__consumer_offsets`.
//!
//! Lancer avec : `cargo test --test ch05c`

mod common;

use broker::groups::offsets::partition_for;
use common::groups::*;
use common::*;

#[tokio::test]
async fn offsets_survive_a_restart() {
    let broker = start_broker_with(group_config()).await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "orders", 2).await;
    simple_commit(&mut client, "g1", &[("orders", 0, 10), ("orders", 1, 11)]).await;
    simple_commit(&mut client, "g2", &[("orders", 0, 20)]).await;
    simple_commit(&mut client, "g1", &[("orders", 0, 15)]).await;
    drop(client);

    let broker = broker.restart().await;
    let mut client = Client::connect(broker.addr).await;

    assert_eq!(fetch_offset_v7(&mut client, "g1", "orders", 0).await.0, 15, "le dernier commit gagne");
    assert_eq!(fetch_offset_v7(&mut client, "g1", "orders", 1).await.0, 11);
    assert_eq!(fetch_offset_v7(&mut client, "g2", "orders", 0).await.0, 20);
}

/// Les commits sont écrits dans la partition de __consumer_offsets qui
/// correspond au groupe, sous forme de records lisibles.
#[tokio::test]
async fn commits_are_records_in_the_offsets_topic() {
    let broker = start_broker_with(group_config()).await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "orders", 1).await;
    simple_commit(&mut client, "my-group", &[("orders", 0, 42)]).await;

    let partition = partition_for("my-group", 4);
    let records = fetched_records(&fetch(&mut client, "__consumer_offsets", partition, 0).await);
    assert_eq!(records.len(), 1, "un record par partition commitée");

    let key: serde_json::Value = serde_json::from_slice(records[0].key.as_ref().unwrap()).unwrap();
    let value: serde_json::Value = serde_json::from_slice(records[0].value.as_ref().unwrap()).unwrap();
    assert_eq!(key["group"], "my-group");
    assert_eq!(key["topic"], "orders");
    assert_eq!(key["partition"], 0);
    assert_eq!(value["offset"], 42);
    assert_eq!(value["metadata"], "meta-42");

    for other in (0..4).filter(|p| *p != partition) {
        let records = fetched_records(&fetch(&mut client, "__consumer_offsets", other, 0).await);
        assert!(records.is_empty(), "rien dans la partition {other}");
    }
}

/// Les offsets de __consumer_offsets ne sont pas soumis à la rétention.
#[tokio::test]
async fn offsets_topic_is_not_deleted_by_retention() {
    let config = broker::Config {
        log: broker::log::LogConfig {
            segment_bytes: 300,
            retention_bytes: Some(1),
            ..Default::default()
        },
        retention_check_interval: std::time::Duration::from_millis(50),
        ..group_config()
    };
    let broker = start_broker_with(config).await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "orders", 1).await;
    for offset in 0..20 {
        simple_commit(&mut client, "g", &[("orders", 0, offset)]).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let partition = partition_for("g", 4);
    assert_eq!(list_offset(&mut client, "__consumer_offsets", partition, -2).await.offset, 0);
}
