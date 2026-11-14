//! Chapitre 1 : métadonnées et création de topics.
//!
//! Lancer avec : `cargo test --test ch01`

mod common;

use broker::Config;
use common::{Client, start_broker, start_broker_with};
use kafka_protocol::ResponseError;
use kafka_protocol::messages::create_topics_request::{CreatableReplicaAssignment, CreatableTopic};
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::metadata_response::MetadataResponseTopic;
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, BrokerId, CreateTopicsRequest, DescribeConfigsRequest, MetadataRequest, MetadataResponse,
    TopicName,
};
use kafka_protocol::protocol::StrBytes;
use uuid::Uuid;

const METADATA_VERSION: i16 = 12;
const CREATE_TOPICS_VERSION: i16 = 7;

fn topic_name(name: &str) -> TopicName {
    TopicName(StrBytes::from_string(name.to_string()))
}

fn creatable(name: &str, partitions: i32, replication_factor: i16) -> CreatableTopic {
    CreatableTopic::default()
        .with_name(topic_name(name))
        .with_num_partitions(partitions)
        .with_replication_factor(replication_factor)
}

async fn create_topics(client: &mut Client, topics: Vec<CreatableTopic>) -> Vec<CreatableTopicResult> {
    let request = CreateTopicsRequest::default().with_topics(topics).with_timeout_ms(5000);
    client.send(request, CREATE_TOPICS_VERSION).await.unwrap().topics
}

async fn create_topic(client: &mut Client, name: &str, partitions: i32) -> CreatableTopicResult {
    let mut results = create_topics(client, vec![creatable(name, partitions, 1)]).await;
    assert_eq!(results.len(), 1, "une réponse par topic demandé");
    results.remove(0)
}

/// Metadata pour une liste de topics (`None` = tous les topics).
async fn metadata(client: &mut Client, topics: Option<&[&str]>, auto_create: bool) -> MetadataResponse {
    let request = MetadataRequest::default()
        .with_topics(topics.map(|names| {
            names.iter().map(|n| MetadataRequestTopic::default().with_name(Some(topic_name(n)))).collect()
        }))
        .with_allow_auto_topic_creation(auto_create);
    client.send(request, METADATA_VERSION).await.unwrap()
}

fn find<'a>(response: &'a MetadataResponse, name: &str) -> &'a MetadataResponseTopic {
    response
        .topics
        .iter()
        .find(|t| t.name.as_deref().map(|n| n.as_str()) == Some(name))
        .unwrap_or_else(|| panic!("le topic {name} est absent de la réponse Metadata"))
}

fn names(response: &MetadataResponse) -> Vec<String> {
    let mut names: Vec<_> = response.topics.iter().filter_map(|t| t.name.as_ref()).map(|n| n.to_string()).collect();
    names.sort();
    names
}

fn assert_ok(actual: i16, context: &str) {
    assert_eq!(actual, 0, "{context} : code d'erreur {actual} ({:?}), attendu aucune erreur", ResponseError::try_from_code(actual));
}

fn assert_error(actual: i16, expected: ResponseError, context: &str) {
    assert_eq!(
        actual,
        expected.code(),
        "{context} : code d'erreur {actual} ({:?}), attendu {expected:?}",
        ResponseError::try_from_code(actual)
    );
}

#[tokio::test]
async fn api_versions_announces_metadata_and_create_topics() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let response = client.send(ApiVersionsRequest::default(), 3).await.unwrap();

    // Versions à partir desquelles chaque API est flexible.
    for (key, flexible) in [(ApiKey::Metadata, 9), (ApiKey::CreateTopics, 5), (ApiKey::DescribeConfigs, 4)] {
        let api = response
            .api_keys
            .iter()
            .find(|a| a.api_key == key as i16)
            .unwrap_or_else(|| panic!("{key:?} doit être annoncée dans ApiVersions"));
        assert!(api.max_version >= flexible, "{key:?} : supportez au moins jusqu'à la v{flexible} (version flexible)");
    }
}

#[tokio::test]
async fn metadata_describes_the_cluster() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let response = metadata(&mut client, None, false).await;

    assert_eq!(response.brokers.len(), 1, "un seul broker dans le cluster");
    let me = &response.brokers[0];
    assert_eq!(me.node_id, BrokerId(0));
    assert_eq!(me.host.as_str(), "127.0.0.1", "le host annoncé vient de la configuration");
    assert_eq!(me.port, broker.addr.port() as i32, "le port annoncé vient de la configuration");
    assert_eq!(response.controller_id, BrokerId(0), "ce broker est aussi le controller");
    assert_eq!(response.cluster_id.as_deref(), Some("workshop-cluster"));
    assert!(response.topics.is_empty(), "aucun topic au démarrage");
}

#[tokio::test]
async fn create_topic_then_describe_it() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let created = create_topic(&mut client, "orders", 3).await;
    assert_ok(created.error_code, "création de orders");
    assert_eq!(created.num_partitions, 3);
    assert_eq!(created.replication_factor, 1);
    assert!(!created.topic_id.is_nil(), "un topic_id doit être attribué à la création");

    let response = metadata(&mut client, Some(&["orders"]), false).await;
    let topic = find(&response, "orders");
    assert_ok(topic.error_code, "metadata de orders");
    assert_eq!(topic.topic_id, created.topic_id, "le même topic_id dans CreateTopics et Metadata");
    assert!(!topic.is_internal);

    let mut indexes: Vec<_> = topic.partitions.iter().map(|p| p.partition_index).collect();
    indexes.sort();
    assert_eq!(indexes, vec![0, 1, 2]);
    for p in &topic.partitions {
        assert_ok(p.error_code, "partition");
        assert_eq!(p.leader_id, BrokerId(0), "le seul broker est leader de toutes les partitions");
        assert_eq!(p.replica_nodes, vec![BrokerId(0)]);
        assert_eq!(p.isr_nodes, vec![BrokerId(0)]);
    }
}

#[tokio::test]
async fn metadata_without_topic_list_returns_all_topics() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    for name in ["b-topic", "a-topic", "c-topic"] {
        create_topic(&mut client, name, 1).await;
    }

    let response = metadata(&mut client, None, false).await;
    assert_eq!(names(&response), vec!["a-topic", "b-topic", "c-topic"]);
}

#[tokio::test]
async fn topics_are_shared_between_connections() {
    let broker = start_broker().await;
    let mut first = Client::connect(broker.addr).await;
    let mut second = Client::connect(broker.addr).await;

    create_topic(&mut first, "shared", 2).await;

    let response = metadata(&mut second, Some(&["shared"]), false).await;
    assert_eq!(find(&response, "shared").partitions.len(), 2);
}

/// -1 signifie « valeur par défaut du broker », pour le nombre de partitions
/// comme pour le facteur de réplication.
#[tokio::test]
async fn create_topic_with_defaults() {
    let config = Config { default_num_partitions: 4, ..Config::default() };
    let broker = start_broker_with(config).await;
    let mut client = Client::connect(broker.addr).await;

    let results = create_topics(&mut client, vec![creatable("defaults", -1, -1)]).await;

    assert_ok(results[0].error_code, "création avec les valeurs par défaut");
    assert_eq!(results[0].num_partitions, 4);
    assert_eq!(results[0].replication_factor, 1);
    let response = metadata(&mut client, Some(&["defaults"]), false).await;
    assert_eq!(find(&response, "defaults").partitions.len(), 4);
}

#[tokio::test]
async fn create_existing_topic_fails() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let first = create_topic(&mut client, "twice", 1).await;
    let second = create_topic(&mut client, "twice", 5).await;

    assert_error(second.error_code, ResponseError::TopicAlreadyExists, "seconde création");
    let response = metadata(&mut client, Some(&["twice"]), false).await;
    let topic = find(&response, "twice");
    assert_eq!(topic.topic_id, first.topic_id, "le topic existant n'est pas remplacé");
    assert_eq!(topic.partitions.len(), 1);
}

#[tokio::test]
async fn invalid_topic_names_are_rejected() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let too_long = "x".repeat(250);
    for name in ["", ".", "..", "with space", "slash/topic", "accentué", too_long.as_str()] {
        let result = create_topic(&mut client, name, 1).await;
        assert_error(result.error_code, ResponseError::InvalidTopicException, &format!("nom {name:?}"));
    }

    let max_len = "y".repeat(249);
    for name in ["valid.name_with-dashes.42", max_len.as_str()] {
        let result = create_topic(&mut client, name, 1).await;
        assert_ok(result.error_code, &format!("nom {name:?}"));
    }
}

#[tokio::test]
async fn invalid_partitions_and_replication_factor_are_rejected() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let results = create_topics(
        &mut client,
        vec![creatable("zero-partitions", 0, 1), creatable("too-many-replicas", 1, 3), creatable("zero-replicas", 1, 0)],
    )
    .await;

    assert_error(results[0].error_code, ResponseError::InvalidPartitions, "0 partition");
    assert_error(results[1].error_code, ResponseError::InvalidReplicationFactor, "3 répliques sur 1 broker");
    assert_error(results[2].error_code, ResponseError::InvalidReplicationFactor, "0 réplique");
    assert!(metadata(&mut client, None, false).await.topics.is_empty(), "aucun topic ne doit avoir été créé");
}

/// Chaque topic d'une requête est traité indépendamment.
#[tokio::test]
async fn create_topics_partial_success() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let results = create_topics(&mut client, vec![creatable("good", 2, 1), creatable("bad/name", 2, 1)]).await;

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].name.as_str(), "good");
    assert_ok(results[0].error_code, "good");
    assert_eq!(results[1].name.as_str(), "bad/name");
    assert_error(results[1].error_code, ResponseError::InvalidTopicException, "bad/name");
    assert_eq!(names(&metadata(&mut client, None, false).await), vec!["good"]);
}

#[tokio::test]
async fn create_topics_validate_only() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let request = CreateTopicsRequest::default()
        .with_topics(vec![creatable("dry-run", 3, 1), creatable("..", 3, 1)])
        .with_validate_only(true);
    let response = client.send(request, CREATE_TOPICS_VERSION).await.unwrap();

    assert_ok(response.topics[0].error_code, "dry-run valide");
    assert_error(response.topics[1].error_code, ResponseError::InvalidTopicException, "dry-run invalide");
    assert!(metadata(&mut client, None, false).await.topics.is_empty(), "validate_only ne crée rien");
}

#[tokio::test]
async fn manual_partition_assignment_is_rejected() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let topic = CreatableTopic::default()
        .with_name(topic_name("manual"))
        .with_num_partitions(-1)
        .with_replication_factor(-1)
        .with_assignments(vec![
            CreatableReplicaAssignment::default().with_partition_index(0).with_broker_ids(vec![BrokerId(0)]),
        ]);
    let results = create_topics(&mut client, vec![topic]).await;

    assert_error(results[0].error_code, ResponseError::InvalidRequest, "assignation manuelle");
}

#[tokio::test]
async fn metadata_for_unknown_topic() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "known", 1).await;

    let response = metadata(&mut client, Some(&["known", "unknown"]), false).await;

    assert_eq!(response.topics.len(), 2, "une entrée par topic demandé");
    assert_ok(find(&response, "known").error_code, "known");
    assert_error(find(&response, "unknown").error_code, ResponseError::UnknownTopicOrPartition, "unknown");
    assert_eq!(names(&metadata(&mut client, None, false).await), vec!["known"], "unknown n'a pas été créé");
}

#[tokio::test]
async fn metadata_auto_creates_topics() {
    let config = Config { default_num_partitions: 2, ..Config::default() };
    let broker = start_broker_with(config).await;
    let mut client = Client::connect(broker.addr).await;

    let response = metadata(&mut client, Some(&["auto"]), true).await;

    let topic = find(&response, "auto");
    assert_ok(topic.error_code, "topic auto-créé");
    assert_eq!(topic.partitions.len(), 2, "créé avec le nombre de partitions par défaut");
    assert_eq!(names(&metadata(&mut client, None, false).await), vec!["auto"]);
}

#[tokio::test]
async fn auto_creation_can_be_disabled() {
    let config = Config { auto_create_topics: false, ..Config::default() };
    let broker = start_broker_with(config).await;
    let mut client = Client::connect(broker.addr).await;

    let response = metadata(&mut client, Some(&["auto"]), true).await;

    assert_error(find(&response, "auto").error_code, ResponseError::UnknownTopicOrPartition, "auto-création désactivée");
}

#[tokio::test]
async fn auto_creation_rejects_invalid_names() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let response = metadata(&mut client, Some(&["bad/name"]), true).await;

    assert_error(find(&response, "bad/name").error_code, ResponseError::InvalidTopicException, "nom invalide");
}

/// À partir de la v10, un topic peut être demandé par son identifiant.
#[tokio::test]
async fn metadata_by_topic_id() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    let created = create_topic(&mut client, "by-id", 1).await;

    let unknown_id = Uuid::new_v4();
    let by_id = |id| MetadataRequestTopic::default().with_name(None).with_topic_id(id);
    let request = MetadataRequest::default()
        .with_topics(Some(vec![by_id(created.topic_id), by_id(unknown_id)]))
        .with_allow_auto_topic_creation(false);
    let response = client.send(request, METADATA_VERSION).await.unwrap();

    let known = response.topics.iter().find(|t| t.topic_id == created.topic_id).expect("topic demandé par id");
    assert_eq!(known.name.as_deref().map(|n| n.as_str()), Some("by-id"));
    let unknown = response.topics.iter().find(|t| t.topic_id == unknown_id).expect("id inconnu");
    assert_error(unknown.error_code, ResponseError::UnknownTopicId, "id inconnu");
}

/// Les anciennes versions (non flexibles) doivent aussi fonctionner.
#[tokio::test]
async fn older_versions_work() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let request = CreateTopicsRequest::default().with_topics(vec![creatable("old", 2, 1)]);
    let response = client.send(request, 2).await.unwrap();
    assert_ok(response.topics[0].error_code, "CreateTopics v2");

    let request = MetadataRequest::default().with_topics(None);
    let response = client.send(request, 1).await.unwrap();
    assert_eq!(find(&response, "old").partitions.len(), 2, "Metadata v1");
}

/// DescribeConfigs : une configuration vide pour un topic existant, une
/// erreur pour un topic inconnu.
#[tokio::test]
async fn describe_configs() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "configured", 1).await;

    let resource = |kind: i8, name: &str| {
        DescribeConfigsResource::default()
            .with_resource_type(kind)
            .with_resource_name(StrBytes::from_string(name.to_string()))
            .with_configuration_keys(None)
    };
    let request = DescribeConfigsRequest::default().with_resources(vec![
        resource(2, "configured"),
        resource(2, "missing"),
        resource(4, "0"),
    ]);
    let response = client.send(request, 4).await.unwrap();

    assert_eq!(response.results.len(), 3, "un résultat par ressource demandée");
    assert_eq!(response.results[0].resource_name.as_str(), "configured");
    assert_ok(response.results[0].error_code, "topic existant");
    assert_error(response.results[1].error_code, ResponseError::UnknownTopicOrPartition, "topic inconnu");
    assert_ok(response.results[2].error_code, "broker 0");
}
