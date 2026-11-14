//! Chapitre 5a : consumer groups, appartenance et rebalance.
//!
//! Lancer avec : `cargo test --test ch05a`

mod common;

use std::time::{Duration, Instant};

use common::groups::*;
use common::*;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::{
    BrokerId, DescribeGroupsRequest, FindCoordinatorRequest, GroupId, ListGroupsRequest, MetadataRequest,
};

fn assert_error(actual: i16, expected: ResponseError, context: &str) {
    assert_eq!(
        actual,
        expected.code(),
        "{context} : code d'erreur {actual} ({:?}), attendu {expected:?}",
        ResponseError::try_from_code(actual)
    );
}

#[tokio::test]
async fn find_coordinator_returns_this_broker() {
    let broker = start_broker_with(group_config()).await;
    let mut client = Client::connect(broker.addr).await;

    // v3 : une seule clé.
    let response = client.send(FindCoordinatorRequest::default().with_key(s("my-group")), 3).await.unwrap();
    assert_eq!(response.error_code, 0);
    assert_eq!(response.node_id, BrokerId(0));
    assert_eq!(response.port, broker.addr.port() as i32);

    // v4 : plusieurs clés.
    let request = FindCoordinatorRequest::default().with_coordinator_keys(vec![s("a"), s("b")]);
    let response = client.send(request, 4).await.unwrap();
    assert_eq!(response.coordinators.len(), 2);
    for c in &response.coordinators {
        assert_eq!(c.error_code, 0);
        assert_eq!(c.node_id, BrokerId(0));
        assert_eq!(c.host.as_str(), "127.0.0.1");
    }
}

/// Le coordinateur crée __consumer_offsets au premier besoin.
#[tokio::test]
async fn find_coordinator_creates_the_offsets_topic() {
    let broker = start_broker_with(group_config()).await;
    let mut client = Client::connect(broker.addr).await;

    client.send(FindCoordinatorRequest::default().with_key(s("my-group")), 3).await.unwrap();

    let response = client.send(MetadataRequest::default().with_topics(None), 12).await.unwrap();
    let topic = response
        .topics
        .iter()
        .find(|t| t.name.as_deref().map(|n| n.as_str()) == Some("__consumer_offsets"))
        .expect("__consumer_offsets doit avoir été créé");
    assert_eq!(topic.partitions.len(), 4, "offsets_topic_partitions partitions");
    assert!(topic.is_internal, "__consumer_offsets est un topic interne");
}

/// À partir de la v4, un nouveau membre reçoit d'abord un identifiant
/// (MEMBER_ID_REQUIRED), puis rejoint avec.
#[tokio::test]
async fn new_member_must_get_an_id_first() {
    let broker = start_broker_with(group_config()).await;
    let mut member = Member::new(broker.addr, "g").await;

    let first = member.join_once().await;
    assert_error(first.error_code, ResponseError::MemberIdRequired, "premier JoinGroup");
    assert!(!first.member_id.is_empty(), "le broker doit fournir un member_id");

    let second = member.join_once().await;
    assert_eq!(second.error_code, 0);
    assert_eq!(second.member_id, first.member_id);
    assert_eq!(second.generation_id, 1);
    assert_eq!(second.leader, first.member_id, "le seul membre est le leader");
    assert_eq!(second.protocol_name.as_deref(), Some("range"));
    assert_eq!(second.members.len(), 1, "le leader reçoit la liste des membres");
    assert_eq!(&second.members[0].metadata[..], b"metadata", "avec leurs métadonnées");
}

/// Avant la v4, le membre est accepté directement.
#[tokio::test]
async fn old_join_group_version_needs_no_member_id() {
    let broker = start_broker_with(group_config()).await;
    let mut member = Member::new(broker.addr, "g").await;

    let response = member.client.send(member.join_request(), 3).await.unwrap();

    assert_eq!(response.error_code, 0);
    assert!(!response.member_id.is_empty());
    assert_eq!(response.generation_id, 1);
}

#[tokio::test]
async fn single_member_lifecycle() {
    let broker = start_broker_with(group_config()).await;
    let mut member = Member::new(broker.addr, "solo").await;

    member.join().await;
    let me = member.member_id.clone();
    let sync = member.sync(&[(me.as_str(), "orders-0,orders-1")]).await;

    assert_eq!(sync.error_code, 0);
    assert_eq!(assignment(&sync), "orders-0,orders-1");
    assert_eq!(member.heartbeat().await, 0);
}

/// Un second membre arrive : le premier doit rejoindre à nouveau, puis le
/// leader répartit les partitions entre les deux.
#[tokio::test]
async fn second_member_triggers_a_rebalance() {
    let broker = start_broker_with(group_config()).await;
    let mut a = Member::new(broker.addr, "duo").await;
    a.join().await;
    let a_id = a.member_id.clone();
    a.sync(&[(a_id.as_str(), "tout")]).await;

    // B rejoint : sa réponse attend que A ait rejoint lui aussi.
    let mut b = Member::new(broker.addr, "duo").await;
    let b_join = tokio::spawn(async move {
        let response = b.join().await;
        (b, response)
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!b_join.is_finished(), "le JoinGroup de B attend A");

    assert_error(a.heartbeat().await, ResponseError::RebalanceInProgress, "heartbeat de A pendant le rebalance");
    let a_join = a.join().await;
    let (mut b, b_response) = b_join.await.unwrap();

    assert_eq!(a_join.generation_id, 2);
    assert_eq!(b_response.generation_id, 2);
    assert_eq!(a_join.leader.as_str(), a_id, "le leader reste le même");
    assert_eq!(b_response.leader.as_str(), a_id);
    assert_eq!(a_join.members.len(), 2, "le leader reçoit les deux membres");
    assert!(b_response.members.is_empty(), "un membre qui n'est pas leader ne reçoit pas la liste");

    // B synchronise en premier : il attend l'assignation du leader.
    let b_id = b.member_id.clone();
    let b_sync = tokio::spawn(async move { b.sync(&[]).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!b_sync.is_finished(), "le SyncGroup de B attend celui du leader");

    let a_sync = a.sync(&[(a_id.as_str(), "moitié A"), (b_id.as_str(), "moitié B")]).await;
    let b_sync = b_sync.await.unwrap();
    assert_eq!(assignment(&a_sync), "moitié A");
    assert_eq!(assignment(&b_sync), "moitié B");
}

/// Un membre qui n'envoie plus de heartbeat est exclu après session_timeout.
#[tokio::test]
async fn silent_member_is_expired() {
    let broker = start_broker_with(group_config()).await;
    let mut a = Member::new(broker.addr, "expiry").await;
    let mut b = Member::new(broker.addr, "expiry").await;
    b.session_timeout_ms = 500;
    a.join().await;
    let a_id = a.member_id.clone();
    a.sync(&[(a_id.as_str(), "x")]).await;
    let b_join = tokio::spawn(async move {
        b.join().await;
        b
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    a.join().await;
    let mut b = b_join.await.unwrap();
    let b_id = b.member_id.clone();
    let b_sync = tokio::spawn(async move { b.sync(&[]).await });
    a.sync(&[(a_id.as_str(), "x"), (b_id.as_str(), "y")]).await;
    b_sync.await.unwrap();

    // B se tait ; A continue d'envoyer des heartbeats.
    let start = Instant::now();
    let mut error = 0;
    while start.elapsed() < Duration::from_secs(5) {
        error = a.heartbeat().await;
        if error != 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_error(error, ResponseError::RebalanceInProgress, "heartbeat de A après l'expiration de B");
    assert!(start.elapsed() >= Duration::from_millis(400), "B ne doit pas être exclu avant son session_timeout");

    let response = a.join().await;
    assert_eq!(response.members.len(), 1, "B a été exclu");
}

#[tokio::test]
async fn leave_group_triggers_a_rebalance() {
    let broker = start_broker_with(group_config()).await;
    let mut a = Member::new(broker.addr, "leave").await;
    let mut b = Member::new(broker.addr, "leave").await;
    a.join().await;
    let b_join = tokio::spawn(async move {
        b.join().await;
        b
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    a.join().await;
    let mut b = b_join.await.unwrap();
    let (a_id, b_id) = (a.member_id.clone(), b.member_id.clone());
    let b_sync = tokio::spawn(async move {
        b.sync(&[]).await;
        b
    });
    a.sync(&[(a_id.as_str(), "x"), (b_id.as_str(), "y")]).await;
    let mut b = b_sync.await.unwrap();

    assert_eq!(b.leave().await, 0);
    assert_error(a.heartbeat().await, ResponseError::RebalanceInProgress, "heartbeat après le départ de B");
    assert_eq!(a.join().await.members.len(), 1);
    assert_error(b.leave().await, ResponseError::UnknownMemberId, "second LeaveGroup");
}

/// Pendant un rebalance, un membre qui ne rejoint pas avant le
/// rebalance_timeout est exclu.
#[tokio::test]
async fn member_not_rejoining_in_time_is_removed() {
    let broker = start_broker_with(group_config()).await;
    let mut a = Member::new(broker.addr, "slow").await;
    a.rebalance_timeout_ms = 500;
    a.join().await;
    let a_id = a.member_id.clone();
    a.sync(&[(a_id.as_str(), "x")]).await;

    // B rejoint, A ne répond pas au rebalance.
    let mut b = Member::new(broker.addr, "slow").await;
    b.rebalance_timeout_ms = 500;
    let start = Instant::now();
    let response = b.join().await;

    assert!(start.elapsed() >= Duration::from_millis(400), "on attend A jusqu'au rebalance_timeout");
    assert_eq!(response.members.len(), 1, "A a été exclu");
    assert_eq!(response.leader.as_str(), b.member_id, "B devient leader");
    assert_error(a.heartbeat().await, ResponseError::UnknownMemberId, "heartbeat de A exclu");
}

/// Le premier rebalance d'un groupe vide attend initial_rebalance_delay pour
/// que tous les membres qui démarrent en même temps soient dans la même
/// génération.
#[tokio::test]
async fn initial_rebalance_delay_groups_members() {
    let mut config = group_config();
    config.group.initial_rebalance_delay = Duration::from_millis(600);
    let broker = start_broker_with(config).await;

    let start = Instant::now();
    let mut joins = Vec::new();
    for _ in 0..3 {
        let mut m = Member::new(broker.addr, "delayed").await;
        joins.push(tokio::spawn(async move { m.join().await }));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut generations = Vec::new();
    for j in joins {
        generations.push(j.await.unwrap().generation_id);
    }

    assert!(start.elapsed() >= Duration::from_millis(550), "le premier rebalance attend le délai initial");
    assert_eq!(generations, vec![1, 1, 1], "un seul rebalance pour les trois membres");
}

#[tokio::test]
async fn protocol_is_chosen_among_common_ones() {
    let broker = start_broker_with(group_config()).await;
    let mut a = Member::new(broker.addr, "proto").await;
    a.protocols = vec![("range".into(), "a".into()), ("roundrobin".into(), "a".into())];
    let mut b = Member::new(broker.addr, "proto").await;
    b.protocols = vec![("roundrobin".into(), "b".into())];

    a.join().await;
    let b_join = tokio::spawn(async move { b.join().await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let a_response = a.join().await;
    let b_response = b_join.await.unwrap();

    for response in [&a_response, &b_response] {
        assert_eq!(response.protocol_name.as_deref(), Some("roundrobin"));
    }
    let mut metadata: Vec<_> = a_response.members.iter().map(|m| m.metadata.clone()).collect();
    metadata.sort();
    assert_eq!(metadata, vec!["a", "b"], "le leader reçoit les métadonnées du protocole choisi");
}

#[tokio::test]
async fn incompatible_members_are_rejected() {
    let broker = start_broker_with(group_config()).await;
    let mut a = Member::new(broker.addr, "compat").await;
    a.join().await;

    let mut other_strategy = Member::new(broker.addr, "compat").await;
    other_strategy.protocols = vec![("cooperative-sticky".into(), "x".into())];
    other_strategy.member_id = String::new();
    let response = other_strategy.client.send(other_strategy.join_request(), 3).await.unwrap();
    assert_error(response.error_code, ResponseError::InconsistentGroupProtocol, "aucune stratégie commune");

    let mut other_type = Member::new(broker.addr, "compat").await;
    let request = other_type.join_request().with_protocol_type(s("connect"));
    let response = other_type.client.send(request, 3).await.unwrap();
    assert_error(response.error_code, ResponseError::InconsistentGroupProtocol, "autre protocol_type");
}

#[tokio::test]
async fn invalid_requests_are_rejected() {
    let broker = start_broker_with(group_config()).await;

    let mut m = Member::new(broker.addr, "").await;
    assert_error(m.join_once().await.error_code, ResponseError::InvalidGroupId, "group_id vide");

    let mut m = Member::new(broker.addr, "g").await;
    m.session_timeout_ms = 10;
    assert_error(m.join_once().await.error_code, ResponseError::InvalidSessionTimeout, "session trop courte");

    let mut m = Member::new(broker.addr, "g").await;
    m.member_id = "inconnu".into();
    assert_error(m.join_once().await.error_code, ResponseError::UnknownMemberId, "member_id inventé");
    assert_error(m.heartbeat().await, ResponseError::UnknownMemberId, "heartbeat d'un inconnu");
    assert_error(m.sync(&[]).await.error_code, ResponseError::UnknownMemberId, "sync d'un inconnu");

    let mut m = Member::new(broker.addr, "g").await;
    m.join().await;
    m.generation += 10;
    assert_error(m.heartbeat().await, ResponseError::IllegalGeneration, "mauvaise génération");
}

#[tokio::test]
async fn list_and_describe_groups() {
    let broker = start_broker_with(group_config()).await;
    let mut a = Member::new(broker.addr, "described").await;
    a.join().await;
    let a_id = a.member_id.clone();
    a.sync(&[(a_id.as_str(), "assignation")]).await;
    let mut client = Client::connect(broker.addr).await;

    let listed = client.send(ListGroupsRequest::default(), 5).await.unwrap();
    let group = listed.groups.iter().find(|g| g.group_id.as_str() == "described").expect("groupe listé");
    assert_eq!(group.protocol_type.as_str(), "consumer");
    assert_eq!(group.group_state.as_str(), "Stable");

    let request = DescribeGroupsRequest::default().with_groups(vec![GroupId(s("described")), GroupId(s("unknown"))]);
    let described = client.send(request, 5).await.unwrap();
    let group = &described.groups[0];
    assert_eq!(group.group_state.as_str(), "Stable");
    assert_eq!(group.protocol_data.as_str(), "range");
    assert_eq!(group.members.len(), 1);
    assert_eq!(group.members[0].member_id.as_str(), a_id);
    assert_eq!(group.members[0].client_id.as_str(), "workshop-tests");
    assert_eq!(group.members[0].client_host.as_str(), "/127.0.0.1");
    assert_eq!(&group.members[0].member_assignment[..], b"assignation");
    assert_eq!(described.groups[1].group_state.as_str(), "Dead", "un groupe inconnu est « Dead »");
}
