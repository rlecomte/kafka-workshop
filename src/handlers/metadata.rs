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
        todo!("ch.1 : handle_metadata")
    }

    /// Cherche un topic demandé par nom (ou par id à partir de la v10), en le
    /// créant si besoin et si c'est autorisé.
    fn lookup_topic(&self, requested: MetadataRequestTopic, allow_auto_create: bool) -> MetadataResponseTopic {
        todo!("ch.1 : lookup_topic")
    }

    fn describe_topic(&self, topic: &Arc<Topic>) -> MetadataResponseTopic {
        todo!("ch.1 : describe_topic")
    }
}
