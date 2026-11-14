//! Chapitre 6 : cluster multi-broker coordonné par etcd.
//!
//! Prérequis : `docker compose up -d etcd`
//! Lancer avec : `cargo test --test ch06`

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::cluster::*;
use common::groups::*;
use common::*;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::metadata_response::MetadataResponseTopic;
use kafka_protocol::messages::{CreateTopicsRequest, FindCoordinatorRequest, MetadataRequest, MetadataResponse};

fn assert_error(actual: i16, expected: ResponseError, context: &str) {
    assert_eq!(
        actual,
        expected.code(),
        "{context} : code d'erreur {actual} ({:?}), attendu {expected:?}",
        ResponseError::try_from_code(actual)
    );
}

fn topic<'a>(metadata: &'a MetadataResponse, name: &str) -> &'a MetadataResponseTopic {
    metadata
        .topics
        .iter()
        .find(|t| t.name.as_deref().map(|n| n.as_str()) == Some(name))
        .unwrap_or_else(|| panic!("topic {name} absent des métadonnées"))
}

/// Crée un topic en envoyant la requête au controller ; renvoie le code d'erreur.
async fn create(cluster: &TestCluster, name: &str, partitions: i32, replication_factor: i16) -> i16 {
    let controller = cluster.controller().await as usize;
    let mut client = cluster.client(controller).await;
    let request = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(topic_name(name))
            .with_num_partitions(partitions)
            .with_replication_factor(replication_factor),
    ]);
    client.send(request, 7).await.unwrap().topics[0].error_code
}

/// Le leader de chaque partition d'un topic.
async fn leaders(cluster: &TestCluster, name: &str) -> Vec<i32> {
    let metadata = cluster.metadata().await;
    let mut partitions: Vec<_> = topic(&metadata, name).partitions.iter().map(|p| (p.partition_index, p.leader_id.0)).collect();
    partitions.sort();
    partitions.into_iter().map(|(_, leader)| leader).collect()
}

#[tokio::test]
async fn brokers_see_each_other() {
    let cluster = TestCluster::start(3).await;

    for id in 0..3 {
        let metadata = cluster.metadata_from(id).await;
        let mut ids: Vec<i32> = metadata.brokers.iter().map(|b| b.node_id.0).collect();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2], "le broker {id} doit voir les 3 brokers");
        for b in &metadata.brokers {
            assert_eq!(b.port, cluster.addr(b.node_id.0 as usize).port() as i32, "adresse annoncée du broker {}", b.node_id.0);
        }
    }
}

#[tokio::test]
async fn a_single_controller_is_elected() {
    let cluster = TestCluster::start(3).await;

    let mut controllers = Vec::new();
    for id in 0..3 {
        controllers.push(cluster.metadata_from(id).await.controller_id.0);
    }
    controllers.dedup();
    assert_eq!(controllers.len(), 1, "tous les brokers doivent voir le même controller");
    assert!((0..3).contains(&controllers[0]));
}

#[tokio::test]
async fn controller_is_replaced_when_it_dies() {
    let mut cluster = TestCluster::start(3).await;
    let old = cluster.controller().await;

    cluster.kill(old as usize).await;

    let replaced = wait_until(Duration::from_secs(10), || async {
        let c = cluster.controller().await;
        c >= 0 && c != old
    })
    .await;
    assert!(replaced, "un nouveau controller doit être élu après l'expiration du lease");
    assert!(cluster.wait_for_brokers(2).await, "le broker arrêté disparaît des métadonnées");
}

#[tokio::test]
async fn topics_are_assigned_across_brokers() {
    let cluster = TestCluster::start(3).await;

    assert_eq!(create(&cluster, "orders", 6, 3).await, 0);

    for id in 0..3 {
        let metadata = cluster.metadata_from(id).await;
        let orders = topic(&metadata, "orders");
        assert_eq!(orders.partitions.len(), 6, "le broker {id} voit le topic");
        for p in &orders.partitions {
            let mut replicas: Vec<i32> = p.replica_nodes.iter().map(|r| r.0).collect();
            assert_eq!(replicas[0], p.leader_id.0, "le leader est la première réplique");
            replicas.sort();
            assert_eq!(replicas, vec![0, 1, 2], "3 répliques sur 3 brokers différents");
            assert_eq!(p.isr_nodes.len(), 3);
        }
    }
    let mut per_broker: HashMap<i32, usize> = HashMap::new();
    for leader in leaders(&cluster, "orders").await {
        *per_broker.entry(leader).or_default() += 1;
    }
    assert_eq!(per_broker.values().copied().collect::<Vec<_>>(), vec![2, 2, 2], "les leaders sont répartis : {per_broker:?}");
}

#[tokio::test]
async fn only_the_controller_creates_topics() {
    let cluster = TestCluster::start(3).await;
    let controller = cluster.controller().await as usize;
    let other = (controller + 1) % 3;

    let mut client = cluster.client(other).await;
    let request = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default().with_name(topic_name("t")).with_num_partitions(1).with_replication_factor(1),
    ]);
    let response = client.send(request, 7).await.unwrap();
    assert_error(response.topics[0].error_code, ResponseError::NotController, "CreateTopics hors du controller");
}

#[tokio::test]
async fn topic_creation_errors() {
    let cluster = TestCluster::start(2).await;

    assert_error(create(&cluster, "too-many", 1, 3).await, ResponseError::InvalidReplicationFactor, "3 répliques sur 2 brokers");
    assert_eq!(create(&cluster, "once", 1, 2).await, 0);
    assert_error(create(&cluster, "once", 1, 2).await, ResponseError::TopicAlreadyExists, "second CreateTopics");
    assert_error(create(&cluster, "bad/name", 1, 1).await, ResponseError::InvalidTopicException, "nom invalide");
}

/// Chaque broker n'ouvre que les répliques qui lui sont assignées.
#[tokio::test]
async fn replicas_live_on_their_brokers() {
    let cluster = TestCluster::start(3).await;
    assert_eq!(create(&cluster, "spread", 3, 1).await, 0);
    let leaders = leaders(&cluster, "spread").await;

    for (partition, leader) in leaders.iter().enumerate() {
        for id in 0..3 {
            let dir = cluster.data_dir(id).join(format!("spread-{partition}"));
            let expected = *leader as usize == id;
            let present = wait_until(Duration::from_secs(5), || async { dir.exists() == expected }).await;
            assert!(present, "spread-{partition} doit exister uniquement sur le broker {leader} (broker {id})");
        }
    }
}

/// On ne produit et ne consomme qu'auprès du leader de la partition.
#[tokio::test]
async fn produce_and_fetch_go_to_the_leader() {
    let cluster = TestCluster::start(3).await;
    assert_eq!(create(&cluster, "routed", 1, 3).await, 0);
    let leader = leaders(&cluster, "routed").await[0] as usize;
    let follower = (leader + 1) % 3;

    let mut client = cluster.client(follower).await;
    let response = produce(&mut client, "routed", 0, batch(&["perdu"])).await;
    assert_error(response.error_code, ResponseError::NotLeaderOrFollower, "Produce sur un follower");
    let data = fetch(&mut client, "routed", 0, 0).await;
    assert_error(data.error_code, ResponseError::NotLeaderOrFollower, "Fetch sur un follower");

    let mut client = cluster.client(leader).await;
    let response = produce(&mut client, "routed", 0, batch(&["a", "b"])).await;
    assert_eq!(response.error_code, 0);
    assert_eq!(values(&fetched_records(&fetch(&mut client, "routed", 0, 0).await)), vec!["a", "b"]);
}

#[tokio::test]
async fn metadata_auto_creates_topics_from_any_broker() {
    let cluster = TestCluster::start(2).await;
    let controller = cluster.controller().await as usize;
    let other = (controller + 1) % 2;

    let mut client = cluster.client(other).await;
    let request = MetadataRequest::default()
        .with_topics(Some(vec![kafka_protocol::messages::metadata_request::MetadataRequestTopic::default()
            .with_name(Some(topic_name("auto")))]))
        .with_allow_auto_topic_creation(true);
    let response = client.send(request, 12).await.unwrap();

    assert_eq!(response.topics[0].error_code, 0);
    assert!(wait_until(Duration::from_secs(5), || async { cluster.metadata_from(controller).await.topics.len() == 1 }).await);
}

/// Un broker arrêté disparaît des métadonnées ; ses répliques apparaissent
/// comme hors ligne. Relancé, il réapparaît.
#[tokio::test]
async fn dead_brokers_are_reported_offline() {
    let mut cluster = TestCluster::start(3).await;
    assert_eq!(create(&cluster, "t", 1, 3).await, 0);
    let controller = cluster.controller().await as usize;
    let victim = (controller + 1) % 3;

    cluster.kill(victim).await;
    assert!(cluster.wait_for_brokers(2).await);
    let metadata = cluster.metadata().await;
    let offline: Vec<i32> = topic(&metadata, "t").partitions[0].offline_replicas.iter().map(|r| r.0).collect();
    assert_eq!(offline, vec![victim as i32]);

    cluster.restart(victim).await;
    assert!(cluster.wait_for_brokers(3).await, "le broker relancé se réenregistre");
    let metadata = cluster.metadata().await;
    assert!(topic(&metadata, "t").partitions[0].offline_replicas.is_empty());
}

/// Le coordinateur d'un groupe est le leader de sa partition de
/// __consumer_offsets ; les autres brokers répondent NOT_COORDINATOR.
#[tokio::test]
async fn group_coordinator_is_the_offsets_partition_leader() {
    let cluster = TestCluster::start(3).await;
    assert_eq!(create(&cluster, "orders", 1, 1).await, 0);

    let mut client = Client::connect(cluster.any_addr()).await;
    let response = client.send(FindCoordinatorRequest::default().with_key(s("my-group")), 3).await.unwrap();
    assert_eq!(response.error_code, 0);
    let coordinator = response.node_id.0 as usize;
    assert_eq!(response.port, cluster.addr(coordinator).port() as i32);

    let metadata = cluster.metadata().await;
    let offsets = topic(&metadata, "__consumer_offsets");
    let partition = broker::groups::offsets::partition_for("my-group", offsets.partitions.len() as i32);
    let leader = offsets.partitions.iter().find(|p| p.partition_index == partition).unwrap().leader_id.0;
    assert_eq!(leader as usize, coordinator);

    let other = (coordinator + 1) % 3;
    let mut wrong = Member::new(cluster.addr(other), "my-group").await;
    assert_error(wrong.join_once().await.error_code, ResponseError::NotCoordinator, "JoinGroup hors du coordinateur");

    let mut client = cluster.client(coordinator).await;
    let mut ok = false;
    for _ in 0..50 {
        if simple_commit(&mut client, "my-group", &[("orders", 0, 7)]).await == vec![0] {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ok, "le coordinateur accepte les commits");
    assert_eq!(fetch_offset_v7(&mut client, "my-group", "orders", 0).await.0, 7);
}
