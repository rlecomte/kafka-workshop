//! Chapitre 8 : failover.
//!
//! Prérequis : `docker compose up -d etcd`
//! Lancer avec : `cargo test --test ch08`

mod common;

use std::cell::Cell;
use std::time::Duration;

use bytes::Bytes;
use common::cluster::*;
use common::groups::*;
use common::*;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::{CreateTopicsRequest, FindCoordinatorRequest};

async fn cluster(n: usize) -> TestCluster {
    TestCluster::start_with(n, |c| c.replica_lag_time_max = Duration::from_secs(1)).await
}

async fn create(cluster: &TestCluster, name: &str, partitions: i32, replication_factor: i16) {
    let controller = cluster.controller().await as usize;
    let mut client = cluster.client(controller).await;
    let request = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(topic_name(name))
            .with_num_partitions(partitions)
            .with_replication_factor(replication_factor),
    ]);
    assert_eq!(client.send(request, 7).await.unwrap().topics[0].error_code, 0);
}

/// (leader, epoch, réplicas, ISR) de la partition 0 d'un topic.
async fn partition_state(cluster: &TestCluster, name: &str) -> (i32, i32, Vec<i32>, Vec<i32>) {
    let metadata = cluster.metadata().await;
    let topic = metadata.topics.iter().find(|t| t.name.as_deref().map(|n| n.as_str()) == Some(name)).unwrap();
    let p = &topic.partitions[0];
    let mut isr: Vec<i32> = p.isr_nodes.iter().map(|r| r.0).collect();
    isr.sort();
    (p.leader_id.0, p.leader_epoch, p.replica_nodes.iter().map(|r| r.0).collect(), isr)
}

/// Produit un message. Comme un vrai client, réessaie pendant quelques
/// secondes si le broker ne se sait pas encore leader (juste après une
/// élection, les métadonnées d'un autre broker peuvent être en avance).
async fn produce_acks(cluster: &TestCluster, broker: usize, topic: &str, value: &str, acks: i16) -> i16 {
    let mut client = cluster.client(broker).await;
    let mut error = 0;
    for _ in 0..50 {
        let request = produce_request(topic, 0, batch(&[value]), acks);
        error = client.send(request, 9).await.unwrap().responses[0].partition_responses[0].error_code;
        if error != ResponseError::NotLeaderOrFollower.code() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    error
}

/// Toutes les valeurs de la partition 0, lues sur son leader (en réessayant
/// tant qu'il ne se sait pas leader).
async fn read_all(cluster: &TestCluster, broker: usize, topic: &str) -> Vec<String> {
    let mut client = cluster.client(broker).await;
    for _ in 0..50 {
        let data = fetch(&mut client, topic, 0, 0).await;
        if data.error_code != ResponseError::NotLeaderOrFollower.code() {
            return values(&fetched_records(&data));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("le broker {broker} ne devient pas leader de {topic}");
}

fn local_values(cluster: &TestCluster, broker: usize, topic: &str) -> Vec<String> {
    let path = cluster.data_dir(broker).join(format!("{topic}-0")).join("00000000000000000000.log");
    values(&decode_records(Bytes::from(std::fs::read(path).unwrap_or_default())))
}

async fn wait_for_leader(cluster: &TestCluster, topic: &str, check: impl Fn(i32) -> bool) -> i32 {
    let leader = Cell::new(-1);
    let seen = &leader;
    let check = &check;
    let ok = wait_until(Duration::from_secs(15), || async move {
        seen.set(partition_state(cluster, topic).await.0);
        check(seen.get())
    })
    .await;
    assert!(ok, "pas de leader attendu pour {topic} (dernier vu : {})", leader.get());
    leader.get()
}

/// Le leader meurt : un membre de l'ISR prend le relais, avec tous les
/// messages confirmés.
#[tokio::test]
async fn leader_failover_keeps_acknowledged_messages() {
    let mut cluster = cluster(3).await;
    create(&cluster, "orders", 1, 3).await;
    let (leader, epoch, _, _) = partition_state(&cluster, "orders").await;
    for i in 0..20 {
        assert_eq!(produce_acks(&cluster, leader as usize, "orders", &format!("m{i}"), -1).await, 0);
    }

    cluster.kill(leader as usize).await;
    let new_leader = wait_for_leader(&cluster, "orders", |l| l >= 0 && l != leader).await;

    let (_, new_epoch, _, isr) = partition_state(&cluster, "orders").await;
    assert_eq!(new_epoch, epoch + 1, "l'epoch est incrémentée à chaque élection");
    assert!(!isr.contains(&leader), "l'ancien leader sort de l'ISR");
    // Le high watermark du nouveau leader peut avoir un échange de retard :
    // les derniers messages deviennent visibles quand les followers refont un Fetch.
    let expected: Vec<String> = (0..20).map(|i| format!("m{i}")).collect();
    let (cluster_ref, expected_ref) = (&cluster, &expected);
    let complete = wait_until(Duration::from_secs(5), || async move {
        read_all(cluster_ref, new_leader as usize, "orders").await == *expected_ref
    })
    .await;
    assert!(complete, "aucun message confirmé n'est perdu : {:?}", read_all(&cluster, new_leader as usize, "orders").await);
    assert_eq!(produce_acks(&cluster, new_leader as usize, "orders", "après", -1).await, 0, "on peut produire sur le nouveau leader");
}

/// L'ancien leader produit un message ou deux ; à son retour, il est follower.
#[tokio::test]
async fn old_leader_comes_back_as_follower() {
    let mut cluster = cluster(3).await;
    create(&cluster, "back", 1, 3).await;
    let (leader, _, _, _) = partition_state(&cluster, "back").await;
    assert_eq!(produce_acks(&cluster, leader as usize, "back", "a", -1).await, 0);

    cluster.kill(leader as usize).await;
    let new_leader = wait_for_leader(&cluster, "back", |l| l >= 0 && l != leader).await;
    assert_eq!(produce_acks(&cluster, new_leader as usize, "back", "b", -1).await, 0);

    cluster.restart(leader as usize).await;
    let rejoined = wait_until(Duration::from_secs(15), || async { partition_state(&cluster, "back").await.3.len() == 3 }).await;
    assert!(rejoined, "l'ancien leader revient dans l'ISR");
    assert_eq!(partition_state(&cluster, "back").await.0, new_leader, "le leader ne change pas à son retour");
    assert_eq!(local_values(&cluster, leader as usize, "back"), vec!["a", "b"], "l'ancien leader a rattrapé le nouveau");
}

/// Le controller meurt alors qu'il est leader : un nouveau controller est
/// élu, puis élit un nouveau leader pour ses partitions.
#[tokio::test]
async fn controller_failover_then_leader_election() {
    let mut cluster = cluster(3).await;
    create(&cluster, "ctl", 3, 3).await;
    let controller = cluster.controller().await;

    cluster.kill(controller as usize).await;

    let ok = wait_until(Duration::from_secs(15), || async {
        let metadata = cluster.metadata().await;
        let c = metadata.controller_id.0;
        let topic = metadata.topics.iter().find(|t| t.name.as_deref().map(|n| n.as_str()) == Some("ctl")).unwrap();
        c >= 0 && c != controller && topic.partitions.iter().all(|p| p.leader_id.0 >= 0 && p.leader_id.0 != controller)
    })
    .await;
    assert!(ok, "nouveau controller, et plus aucune partition menée par l'ancien");
}

/// Sans réplique, une partition dont le leader meurt est hors ligne jusqu'à
/// son retour.
#[tokio::test]
async fn partition_without_replicas_goes_offline() {
    let mut cluster = cluster(3).await;
    create(&cluster, "fragile", 1, 1).await;
    let (leader, _, _, _) = partition_state(&cluster, "fragile").await;
    assert_eq!(produce_acks(&cluster, leader as usize, "fragile", "seul", -1).await, 0);

    cluster.kill(leader as usize).await;
    wait_for_leader(&cluster, "fragile", |l| l == -1).await;
    let metadata = cluster.metadata().await;
    let topic = metadata.topics.iter().find(|t| t.name.as_deref().map(|n| n.as_str()) == Some("fragile")).unwrap();
    assert_eq!(topic.partitions[0].error_code, ResponseError::LeaderNotAvailable.code());

    cluster.restart(leader as usize).await;
    wait_for_leader(&cluster, "fragile", |l| l == leader).await;
    let mut ok = false;
    for _ in 0..30 {
        if read_all(&cluster, leader as usize, "fragile").await == vec!["seul"] {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ok, "la partition est de retour avec ses données");
}

/// Un message écrit par le leader mais jamais répliqué (acks=1) disparaît
/// quand un autre broker devient leader : l'ancien leader tronque son log à
/// son retour.
#[tokio::test]
async fn unreplicated_messages_are_truncated_on_the_old_leader() {
    let mut cluster = cluster(3).await;
    create(&cluster, "diverge", 1, 2).await;
    let (leader, _, replicas, _) = partition_state(&cluster, "diverge").await;
    let follower = *replicas.iter().find(|r| **r != leader).unwrap();
    assert_eq!(produce_acks(&cluster, leader as usize, "diverge", "confirmé", -1).await, 0);
    tokio::time::sleep(Duration::from_millis(1500)).await; // high watermark sauvegardé

    // Le follower s'arrête ; le leader écrit un message qu'il ne pourra pas
    // répliquer, puis s'arrête à son tour, avant toute réaction du cluster.
    cluster.kill(follower as usize).await;
    assert_eq!(produce_acks(&cluster, leader as usize, "diverge", "jamais répliqué", 1).await, 0);
    cluster.kill(leader as usize).await;

    // Le follower revient le premier : il est dans l'ISR, il devient leader.
    cluster.restart(follower as usize).await;
    wait_for_leader(&cluster, "diverge", |l| l == follower).await;
    let mut written = false;
    for _ in 0..30 {
        if produce_acks(&cluster, follower as usize, "diverge", "nouveau", 1).await == 0 {
            written = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(written);

    // L'ancien leader revient : il tronque son log puis copie le nouveau leader.
    cluster.restart(leader as usize).await;
    let converged = wait_until(Duration::from_secs(15), || async {
        local_values(&cluster, leader as usize, "diverge") == vec!["confirmé", "nouveau"]
    })
    .await;
    assert!(
        converged,
        "l'ancien leader doit avoir le même log que le nouveau : {:?}",
        local_values(&cluster, leader as usize, "diverge")
    );
}

/// Le coordinateur d'un groupe meurt : le nouveau leader de la partition de
/// __consumer_offsets reprend le groupe, avec ses offsets.
#[tokio::test]
async fn group_coordinator_failover_keeps_offsets() {
    let mut cluster = cluster(3).await;
    create(&cluster, "orders", 1, 3).await;
    let find = |cluster: &TestCluster| {
        let addr = cluster.any_addr();
        async move {
            let mut client = Client::connect(addr).await;
            let response = client.send(FindCoordinatorRequest::default().with_key(s("billing")), 3).await.unwrap();
            if response.error_code == 0 { response.node_id.0 } else { -1 }
        }
    };
    let coordinator = find(&cluster).await as usize;
    let mut client = cluster.client(coordinator).await;
    let mut committed = false;
    for _ in 0..50 {
        if simple_commit(&mut client, "billing", &[("orders", 0, 1234)]).await == vec![0] {
            committed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(committed);

    cluster.kill(coordinator).await;

    let new_coordinator = Cell::new(-1);
    let (seen, cluster_ref, find_ref) = (&new_coordinator, &cluster, &find);
    let moved = wait_until(Duration::from_secs(15), || async move {
        seen.set(find_ref(cluster_ref).await);
        seen.get() >= 0 && seen.get() as usize != coordinator
    })
    .await;
    assert!(moved, "un autre broker doit devenir coordinateur");
    let offset = Cell::new(-1);
    let (seen_offset, target) = (&offset, new_coordinator.get() as usize);
    let found = wait_until(Duration::from_secs(5), || async move {
        let mut client = cluster_ref.client(target).await;
        seen_offset.set(fetch_offset_v7(&mut client, "billing", "orders", 0).await.0);
        seen_offset.get() == 1234
    })
    .await;
    assert!(found, "le nouveau coordinateur connaît l'offset commité (vu : {})", offset.get());
}
