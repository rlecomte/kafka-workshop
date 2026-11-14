//! `CreateTopics` (api_key 19) : création d'un ou plusieurs topics.
//!
//! Chaque topic de la requête est traité indépendamment : certains peuvent
//! être créés et d'autres refusés, chacun avec son propre code d'erreur.

use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::{CreateTopicsRequest, CreateTopicsResponse};
use kafka_protocol::protocol::StrBytes;

use crate::Broker;
use crate::topics::NewTopic;

impl Broker {
    pub(super) fn handle_create_topics(&self, request: CreateTopicsRequest) -> CreateTopicsResponse {
        todo!("ch.1 : handle_create_topics")
    }

    fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> CreatableTopicResult {
        todo!("ch.1 : create_topic")
    }
}
