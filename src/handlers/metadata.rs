//! `Metadata` (api_key 3) : décrit le cluster (les brokers, le controller)
//! et les topics demandés (partitions, leader, répliques).
//!
//! C'est la requête qui permet à un client de savoir à quel broker envoyer
//! ses `Produce` et ses `Fetch` pour chaque partition.

use std::sync::Arc;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::Broker;
use crate::topics::{NewTopic, Topic, TopicError};

impl Broker {
    pub(super) async fn handle_metadata(&self, request: MetadataRequest) -> MetadataResponse {
        let brokers = self.live_brokers();
        let alive: Vec<i32> = brokers.iter().map(|b| b.node_id).collect();

        let topics = match request.topics {
            // `null` : tous les topics.
            None => self.topics.list().iter().map(|t| describe_topic(t, &alive)).collect(),
            Some(requested) => {
                let mut topics = Vec::new();
                for t in requested {
                    topics.push(self.lookup_topic(t, request.allow_auto_topic_creation, &alive).await);
                }
                topics
            }
        };

        let brokers = brokers
            .into_iter()
            .map(|b| {
                MetadataResponseBroker::default()
                    .with_node_id(BrokerId(b.node_id))
                    .with_host(StrBytes::from_string(b.host))
                    .with_port(b.port as i32)
            })
            .collect();
        MetadataResponse::default()
            .with_brokers(brokers)
            .with_cluster_id(Some(StrBytes::from_string(self.config.cluster_id.clone())))
            .with_controller_id(BrokerId(self.controller_id()))
            .with_topics(topics)
    }

    /// Cherche un topic demandé par nom (ou par id à partir de la v10), en le
    /// créant si besoin et si c'est autorisé.
    async fn lookup_topic(&self, requested: MetadataRequestTopic, allow_auto_create: bool, alive: &[i32]) -> MetadataResponseTopic {
        let Some(name) = requested.name else {
            return match self.topics.get_by_id(requested.topic_id) {
                Some(topic) => describe_topic(&topic, alive),
                None => MetadataResponseTopic::default()
                    .with_topic_id(requested.topic_id)
                    .with_error_code(ResponseError::UnknownTopicId.code()),
            };
        };

        if let Some(topic) = self.topics.get(name.as_str()) {
            return describe_topic(&topic, alive);
        }
        if allow_auto_create && self.config.auto_create_topics {
            let new = NewTopic {
                name: name.to_string(),
                num_partitions: self.config.default_num_partitions,
                replication_factor: 1,
            };
            match self.create_topic(new, false).await {
                Ok(topic) => {
                    tracing::info!(topic = %topic.name, "topic créé automatiquement");
                    return describe_topic(&topic, alive);
                }
                // Créé entre-temps par une autre connexion.
                Err(TopicError::AlreadyExists(_)) => {
                    if let Some(topic) = self.topics.get(name.as_str()) {
                        return describe_topic(&topic, alive);
                    }
                }
                Err(e) => {
                    return MetadataResponseTopic::default().with_name(Some(name)).with_error_code(e.error().code());
                }
            }
        }
        MetadataResponseTopic::default()
            .with_name(Some(name))
            .with_error_code(ResponseError::UnknownTopicOrPartition.code())
    }
}

/// Décrit un topic : pour chaque partition, son leader, ses répliques, son
/// ISR et ses répliques hors ligne (sur des brokers arrêtés).
fn describe_topic(topic: &Arc<Topic>, alive: &[i32]) -> MetadataResponseTopic {
    let ids = |nodes: &[i32]| nodes.iter().map(|&n| BrokerId(n)).collect::<Vec<_>>();
    let partitions = topic
        .partitions
        .iter()
        .map(|p| {
            let offline: Vec<i32> = p.replicas.iter().copied().filter(|r| !alive.contains(r)).collect();
            let error = if p.leader < 0 { ResponseError::LeaderNotAvailable.code() } else { 0 };
            MetadataResponsePartition::default()
                .with_error_code(error)
                .with_partition_index(p.index)
                .with_leader_id(BrokerId(p.leader))
                .with_leader_epoch(p.leader_epoch)
                .with_replica_nodes(ids(&p.replicas))
                .with_isr_nodes(ids(&p.isr))
                .with_offline_replicas(ids(&offline))
        })
        .collect();
    MetadataResponseTopic::default()
        .with_name(Some(TopicName(StrBytes::from_string(topic.name.clone()))))
        .with_topic_id(topic.id)
        .with_is_internal(topic.name.starts_with("__"))
        .with_partitions(partitions)
}
