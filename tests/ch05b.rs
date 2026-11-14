//! Chapitre 5b : commit et lecture des offsets.
//!
//! Lancer avec : `cargo test --test ch05b`

mod common;

use common::groups::*;
use common::*;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::{GroupId, OffsetFetchRequest};

fn assert_error(actual: i16, expected: ResponseError, context: &str) {
    assert_eq!(
        actual,
        expected.code(),
        "{context} : code d'erreur {actual} ({:?}), attendu {expected:?}",
        ResponseError::try_from_code(actual)
    );
}

#[tokio::test]
async fn commit_then_fetch() {
    let broker = start_broker_with(group_config()).await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "orders", 2).await;

    let errors = simple_commit(&mut client, "g", &[("orders", 0, 42), ("orders", 1, 7)]).await;
    assert_eq!(errors, vec![0, 0]);

    assert_eq!(fetch_offset_v7(&mut client, "g", "orders", 0).await, (42, "meta-42".to_string()));
    assert_eq!(fetch_offset_v7(&mut client, "g", "orders", 1).await.0, 7);
    assert_eq!(fetch_offset_v8(&mut client, "g", "orders", 0).await, 42, "OffsetFetch v8");
}

#[tokio::test]
async fn last_commit_wins() {
    let broker = start_broker_with(group_config()).await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "orders", 1).await;

    simple_commit(&mut client, "g", &[("orders", 0, 10)]).await;
    simple_commit(&mut client, "g", &[("orders", 0, 20)]).await;

    assert_eq!(fetch_offset_v7(&mut client, "g", "orders", 0).await.0, 20);
}

#[tokio::test]
async fn no_committed_offset() {
    let broker = start_broker_with(group_config()).await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "orders", 2).await;
    simple_commit(&mut client, "g", &[("orders", 0, 5)]).await;

    assert_eq!(fetch_offset_v7(&mut client, "g", "orders", 1).await.0, -1, "partition sans commit");
    assert_eq!(fetch_offset_v7(&mut client, "unknown", "orders", 0).await.0, -1, "groupe inconnu");
    assert_eq!(fetch_offset_v8(&mut client, "unknown", "orders", 0).await, -1);
}

/// `topics = null` : tous les offsets commités du groupe.
#[tokio::test]
async fn fetch_all_offsets_of_a_group() {
    let broker = start_broker_with(group_config()).await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "a", 2).await;
    create_topic(&mut client, "b", 1).await;
    simple_commit(&mut client, "g", &[("a", 0, 1), ("a", 1, 2), ("b", 0, 3)]).await;

    let request = OffsetFetchRequest::default().with_group_id(GroupId(s("g"))).with_topics(None);
    let response = client.send(request, 7).await.unwrap();

    let mut offsets: Vec<(String, i32, i64)> = response
        .topics
        .iter()
        .flat_map(|t| t.partitions.iter().map(move |p| (t.name.to_string(), p.partition_index, p.committed_offset)))
        .collect();
    offsets.sort();
    assert_eq!(offsets, vec![("a".into(), 0, 1), ("a".into(), 1, 2), ("b".into(), 0, 3)]);
}

#[tokio::test]
async fn members_commit_with_their_generation() {
    let broker = start_broker_with(group_config()).await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "orders", 1).await;
    let mut member = Member::new(broker.addr, "g").await;
    member.join().await;
    let id = member.member_id.clone();
    member.sync(&[(id.as_str(), "orders-0")]).await;

    let errors = commit(&mut client, "g", member.generation, &id, &[("orders", 0, 100)]).await;
    assert_eq!(errors, vec![0]);
    assert_eq!(fetch_offset_v7(&mut client, "g", "orders", 0).await.0, 100);

    let errors = commit(&mut client, "g", member.generation + 1, &id, &[("orders", 0, 200)]).await;
    assert_error(errors[0], ResponseError::IllegalGeneration, "mauvaise génération");
    let errors = commit(&mut client, "g", member.generation, "intrus", &[("orders", 0, 200)]).await;
    assert_error(errors[0], ResponseError::UnknownMemberId, "membre inconnu");
    let errors = simple_commit(&mut client, "g", &[("orders", 0, 200)]).await;
    assert_error(errors[0], ResponseError::UnknownMemberId, "commit simple dans un groupe actif");
    assert_eq!(fetch_offset_v7(&mut client, "g", "orders", 0).await.0, 100, "les commits refusés sont ignorés");
}

#[tokio::test]
async fn commit_for_unknown_partition() {
    let broker = start_broker_with(broker::Config { auto_create_topics: false, ..group_config() }).await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "orders", 1).await;

    let errors = simple_commit(&mut client, "g", &[("orders", 0, 1), ("orders", 5, 1), ("nope", 0, 1)]).await;

    assert_eq!(errors[0], 0);
    assert_error(errors[1], ResponseError::UnknownTopicOrPartition, "partition inconnue");
    assert_error(errors[2], ResponseError::UnknownTopicOrPartition, "topic inconnu");
}
