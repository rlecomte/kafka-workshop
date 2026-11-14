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
use crate::topics::{NewTopic, Topic};

impl Broker {
    pub(super) fn handle_metadata(&self, request: MetadataRequest) -> MetadataResponse {
        let topics = match request.topics {
            // `null` : tous les topics.
            None => self.topics.list().iter().map(|t| self.describe_topic(t)).collect(),
            Some(requested) => requested
                .into_iter()
                .map(|t| self.lookup_topic(t, request.allow_auto_topic_creation))
                .collect(),
        };

        let me = BrokerId(self.config.node_id);
        MetadataResponse::default()
            .with_brokers(vec![
                MetadataResponseBroker::default()
                    .with_node_id(me)
                    .with_host(StrBytes::from_string(self.config.host.clone()))
                    .with_port(self.config.port as i32),
            ])
            .with_cluster_id(Some(StrBytes::from_string(self.config.cluster_id.clone())))
            .with_controller_id(me)
            .with_topics(topics)
    }

    /// Cherche un topic demandé par nom (ou par id à partir de la v10), en le
    /// créant si besoin et si c'est autorisé.
    fn lookup_topic(&self, requested: MetadataRequestTopic, allow_auto_create: bool) -> MetadataResponseTopic {
        let Some(name) = requested.name else {
            return match self.topics.get_by_id(requested.topic_id) {
                Some(topic) => self.describe_topic(&topic),
                None => MetadataResponseTopic::default()
                    .with_topic_id(requested.topic_id)
                    .with_error_code(ResponseError::UnknownTopicId.code()),
            };
        };

        if let Some(topic) = self.topics.get(name.as_str()) {
            return self.describe_topic(&topic);
        }
        if allow_auto_create && self.config.auto_create_topics {
            let new = NewTopic {
                name: name.to_string(),
                num_partitions: self.config.default_num_partitions,
                replication_factor: 1,
            };
            match self.topics.create(new, false) {
                Ok(topic) => {
                    tracing::info!(topic = %topic.name, "topic créé automatiquement");
                    return self.describe_topic(&topic);
                }
                // Créé entre-temps par une autre connexion.
                Err(crate::topics::TopicError::AlreadyExists(_)) => {
                    if let Some(topic) = self.topics.get(name.as_str()) {
                        return self.describe_topic(&topic);
                    }
                }
                Err(e) => {
                    return MetadataResponseTopic::default()
                        .with_name(Some(name))
                        .with_error_code(e.error().code());
                }
            }
        }
        MetadataResponseTopic::default()
            .with_name(Some(name))
            .with_error_code(ResponseError::UnknownTopicOrPartition.code())
    }

    fn describe_topic(&self, topic: &Arc<Topic>) -> MetadataResponseTopic {
        let me = BrokerId(self.config.node_id);
        let partitions = topic
            .partitions
            .iter()
            .map(|p| {
                MetadataResponsePartition::default()
                    .with_partition_index(p.index)
                    .with_leader_id(me)
                    .with_leader_epoch(0)
                    .with_replica_nodes(vec![me])
                    .with_isr_nodes(vec![me])
            })
            .collect();
        MetadataResponseTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.name.clone()))))
            .with_topic_id(topic.id)
            .with_is_internal(topic.name.starts_with("__"))
            .with_partitions(partitions)
    }
}
