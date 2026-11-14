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
        let topics = request
            .topics
            .into_iter()
            .map(|topic| self.create_one_topic(topic, request.validate_only))
            .collect();
        CreateTopicsResponse::default().with_topics(topics)
    }

    fn create_one_topic(&self, topic: CreatableTopic, validate_only: bool) -> CreatableTopicResult {
        let name = topic.name.clone();
        let result = CreatableTopicResult::default().with_name(name.clone());

        if !topic.assignments.is_empty() {
            return result
                .with_error_code(kafka_protocol::ResponseError::InvalidRequest.code())
                .with_error_message(Some(StrBytes::from_static_str(
                    "l'assignation manuelle des partitions n'est pas supportée",
                )));
        }
        if !topic.configs.is_empty() {
            tracing::warn!(topic = %name.as_str(), "configuration de topic ignorée : {:?}", topic.configs);
        }

        // -1 signifie « valeur par défaut du broker ».
        let num_partitions = match topic.num_partitions {
            -1 => self.config.default_num_partitions,
            n => n,
        };
        let replication_factor = match topic.replication_factor {
            -1 => 1,
            n => n,
        };
        let new = NewTopic { name: name.to_string(), num_partitions, replication_factor };

        match self.create_topic(new, validate_only) {
            Ok(created) => {
                tracing::info!(topic = %created.name, partitions = num_partitions, validate_only, "topic créé");
                result
                    .with_topic_id(created.id)
                    .with_num_partitions(num_partitions)
                    .with_replication_factor(replication_factor)
            }
            Err(e) => {
                tracing::info!(topic = %name.as_str(), "création refusée : {e}");
                result
                    .with_error_code(e.error().code())
                    .with_error_message(Some(StrBytes::from_string(e.to_string())))
            }
        }
    }
}
