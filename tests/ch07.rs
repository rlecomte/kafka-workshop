//! Chapitre 7 : réplication, high watermark et ISR.
//!
//! Prérequis : `docker compose up -d etcd`
//! Lancer avec : `cargo test --test ch07`

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::cluster::*;
use common::groups::*;
use common::*;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::CreateTopicsRequest;

fn assert_error(actual: i16, expected: ResponseError, context: &str) {
    assert_eq!(
        actual,
        expected.code(),
        "{context} : code d'erreur {actual} ({:?}), attendu {expected:?}",
        ResponseError::try_from_code(actual)
    );
}

/// Un cluster où les followers en retard sortent vite de l'ISR.
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

/// (leader, ISR) d'une partition, vus par un broker vivant.
async fn partition_state(cluster: &TestCluster, name: &str, partition: i32) -> (i32, Vec<i32>) {
    let metadata = cluster.metadata().await;
    let topic = metadata.topics.iter().find(|t| t.name.as_deref().map(|n| n.as_str()) == Some(name)).unwrap();
    let p = topic.partitions.iter().find(|p| p.partition_index == partition).unwrap();
    let mut isr: Vec<i32> = p.isr_nodes.iter().map(|r| r.0).collect();
    isr.sort();
    (p.leader_id.0, isr)
}

/// Le contenu du log local d'une partition sur un broker.
fn local_log(cluster: &TestCluster, broker: usize, topic: &str, partition: i32) -> Bytes {
    let path = cluster.data_dir(broker).join(format!("{topic}-{partition}")).join("00000000000000000000.log");
    Bytes::from(std::fs::read(path).unwrap_or_default())
}

async fn produce_acks(client: &mut Client, topic: &str, records: Bytes, acks: i16, timeout_ms: i32) -> i16 {
    let mut request = produce_request(topic, 0, records, acks);
    request.timeout_ms = timeout_ms;
    client.send(request, 9).await.unwrap().responses[0].partition_responses[0].error_code
}

#[tokio::test]
async fn followers_copy_the_leader_log() {
    let cluster = cluster(3).await;
    create(&cluster, "replicated", 1, 3).await;
    let (leader, _) = partition_state(&cluster, "replicated", 0).await;
    let mut client = cluster.client(leader as usize).await;

    for i in 0..5 {
        let response = produce(&mut client, "replicated", 0, batch(&[format!("m{i}").as_str()])).await;
        assert_eq!(response.error_code, 0);
    }

    let leader_log = local_log(&cluster, leader as usize, "replicated", 0);
    for follower in (0..3).filter(|b| *b != leader as usize) {
        let copied = wait_until(Duration::from_secs(5), || async {
            local_log(&cluster, follower, "replicated", 0) == leader_log
        })
        .await;
        assert!(copied, "le log du broker {follower} doit être identique à celui du leader");
    }
    assert_eq!(values(&decode_records(leader_log)), vec!["m0", "m1", "m2", "m3", "m4"]);
}

/// Avec acks=all, la réponse n'arrive qu'une fois les messages copiés par
/// toute l'ISR : ils sont alors déjà sur disque chez les followers.
#[tokio::test]
async fn acks_all_waits_for_the_isr() {
    let cluster = cluster(3).await;
    create(&cluster, "durable", 1, 3).await;
    let (leader, isr) = partition_state(&cluster, "durable", 0).await;
    assert_eq!(isr, vec![0, 1, 2]);
    let mut client = cluster.client(leader as usize).await;

    assert_eq!(produce_acks(&mut client, "durable", batch(&["sûr"]), -1, 5000).await, 0);

    for follower in (0..3).filter(|b| *b != leader as usize) {
        let records = decode_records(local_log(&cluster, follower, "durable", 0));
        assert_eq!(values(&records), vec!["sûr"], "le broker {follower} a déjà le message");
    }
}

/// Un message n'est visible par les consumers qu'une fois répliqué sur toute
/// l'ISR. Un follower arrêté bloque le high watermark jusqu'à ce qu'il
/// sorte de l'ISR.
#[tokio::test]
async fn high_watermark_waits_for_a_dead_follower_to_leave_the_isr() {
    let mut cluster = cluster(3).await;
    create(&cluster, "hw", 1, 3).await;
    let (leader, _) = partition_state(&cluster, "hw", 0).await;
    let follower = (leader as usize + 1) % 3;
    let mut client = cluster.client(leader as usize).await;
    assert_eq!(produce_acks(&mut client, "hw", batch(&["avant"]), -1, 5000).await, 0);

    cluster.kill(follower).await;
    // acks=1 : le leader répond dès qu'il a écrit.
    assert_eq!(produce_acks(&mut client, "hw", batch(&["pendant"]), 1, 5000).await, 0);

    let data = fetch(&mut client, "hw", 0, 0).await;
    assert_eq!(data.high_watermark, 1, "le follower arrêté n'a pas copié le second message");
    assert_eq!(values(&fetched_records(&data)), vec!["avant"], "un consumer ne voit pas au-delà du high watermark");

    // Après replica_lag_time_max, le follower sort de l'ISR et le high watermark avance.
    let shrunk = wait_until(Duration::from_secs(10), || async {
        partition_state(&cluster, "hw", 0).await.1.len() == 2
    })
    .await;
    assert!(shrunk, "le follower arrêté doit sortir de l'ISR");
    let visible = wait_until(Duration::from_secs(5), || async {
        let mut client = cluster.client(leader as usize).await;
        fetch(&mut client, "hw", 0, 0).await.high_watermark == 2
    })
    .await;
    assert!(visible, "le high watermark avance quand l'ISR rétrécit");
}

/// acks=all expire si l'ISR contient un follower qui ne répond plus.
#[tokio::test]
async fn acks_all_times_out_while_a_dead_follower_is_in_the_isr() {
    let mut cluster = cluster(3).await;
    create(&cluster, "slow", 1, 3).await;
    let (leader, _) = partition_state(&cluster, "slow", 0).await;
    let mut client = cluster.client(leader as usize).await;

    cluster.kill((leader as usize + 1) % 3).await;
    let error = produce_acks(&mut client, "slow", batch(&["attente"]), -1, 300).await;
    assert_error(error, ResponseError::RequestTimedOut, "acks=all avec un follower mort dans l'ISR");

    assert!(wait_until(Duration::from_secs(10), || async { partition_state(&cluster, "slow", 0).await.1.len() == 2 }).await);
    assert_eq!(produce_acks(&mut client, "slow", batch(&["ok"]), -1, 5000).await, 0, "acks=all fonctionne avec l'ISR réduite");
}

/// Un follower relancé rattrape son retard puis revient dans l'ISR.
#[tokio::test]
async fn restarted_follower_catches_up_and_rejoins_the_isr() {
    let mut cluster = cluster(3).await;
    create(&cluster, "catchup", 1, 3).await;
    let (leader, _) = partition_state(&cluster, "catchup", 0).await;
    let follower = (leader as usize + 1) % 3;
    let mut client = cluster.client(leader as usize).await;

    cluster.kill(follower).await;
    assert!(wait_until(Duration::from_secs(10), || async { partition_state(&cluster, "catchup", 0).await.1.len() == 2 }).await);
    for i in 0..10 {
        assert_eq!(produce_acks(&mut client, "catchup", batch(&[format!("m{i}").as_str()]), -1, 5000).await, 0);
    }

    cluster.restart(follower).await;
    let rejoined = wait_until(Duration::from_secs(10), || async {
        partition_state(&cluster, "catchup", 0).await.1 == vec![0, 1, 2]
    })
    .await;
    assert!(rejoined, "le follower relancé doit revenir dans l'ISR");
    let records = decode_records(local_log(&cluster, follower, "catchup", 0));
    assert_eq!(records.len(), 10, "le follower a rattrapé les messages écrits pendant son absence");
}

/// min.insync.replicas : acks=all est refusé quand l'ISR est trop petite.
#[tokio::test]
async fn min_insync_replicas_protects_acks_all() {
    let mut cluster = TestCluster::start_with(2, |c| {
        c.replica_lag_time_max = Duration::from_secs(1);
        c.min_insync_replicas = 2;
    })
    .await;
    create(&cluster, "strict", 1, 2).await;
    let (leader, _) = partition_state(&cluster, "strict", 0).await;
    let mut client = cluster.client(leader as usize).await;
    assert_eq!(produce_acks(&mut client, "strict", batch(&["a"]), -1, 5000).await, 0);

    cluster.kill(1 - leader as usize).await;
    assert!(wait_until(Duration::from_secs(10), || async { partition_state(&cluster, "strict", 0).await.1.len() == 1 }).await);

    let error = produce_acks(&mut client, "strict", batch(&["b"]), -1, 5000).await;
    assert_error(error, ResponseError::NotEnoughReplicas, "acks=all avec une ISR de 1 et min.insync.replicas=2");
    assert_eq!(produce_acks(&mut client, "strict", batch(&["c"]), 1, 5000).await, 0, "acks=1 reste possible");
}

/// Les offsets commités sont répliqués comme n'importe quelle partition.
#[tokio::test]
async fn committed_offsets_are_replicated() {
    let cluster = cluster(3).await;
    create(&cluster, "orders", 1, 1).await;
    let mut client = Client::connect(cluster.any_addr()).await;
    let response = client.send(kafka_protocol::messages::FindCoordinatorRequest::default().with_key(s("g")), 3).await.unwrap();
    let coordinator = response.node_id.0 as usize;

    let mut client = cluster.client(coordinator).await;
    let mut committed = false;
    for _ in 0..50 {
        if simple_commit(&mut client, "g", &[("orders", 0, 42)]).await == vec![0] {
            committed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(committed);

    let partition = broker::groups::offsets::partition_for("g", 4);
    for b in 0..3 {
        let records = decode_records(local_log(&cluster, b, "__consumer_offsets", partition));
        assert_eq!(records.len(), 1, "le commit est sur le broker {b}");
    }
}
