//! `DescribeConfigs` (api_key 32) : la configuration d'un topic ou d'un broker.
//!
//! Le broker n'a pas encore de configuration par topic : on renvoie une liste
//! vide, mais on vérifie que la ressource existe. C'est suffisant pour que
//! `kafka-topics.sh --describe` fonctionne.

use kafka_protocol::ResponseError;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::describe_configs_response::DescribeConfigsResult;
use kafka_protocol::messages::{DescribeConfigsRequest, DescribeConfigsResponse};
use kafka_protocol::protocol::StrBytes;

use crate::Broker;

/// Types de ressources (`ConfigResource.Type` dans Kafka).
pub const RESOURCE_TOPIC: i8 = 2;
pub const RESOURCE_BROKER: i8 = 4;

impl Broker {
    pub(super) fn handle_describe_configs(&self, request: DescribeConfigsRequest) -> DescribeConfigsResponse {
        let results = request.resources.into_iter().map(|r| self.describe_resource(r)).collect();
        DescribeConfigsResponse::default().with_results(results)
    }

    fn describe_resource(&self, resource: DescribeConfigsResource) -> DescribeConfigsResult {
        let error = match resource.resource_type {
            RESOURCE_TOPIC if self.topics.get(resource.resource_name.as_str()).is_none() => {
                Some(ResponseError::UnknownTopicOrPartition)
            }
            RESOURCE_TOPIC => None,
            RESOURCE_BROKER if resource.resource_name.as_str() == self.config.node_id.to_string() => None,
            RESOURCE_BROKER => Some(ResponseError::InvalidRequest),
            _ => Some(ResponseError::InvalidRequest),
        };
        let result = DescribeConfigsResult::default()
            .with_resource_type(resource.resource_type)
            .with_resource_name(resource.resource_name);
        match error {
            None => result,
            Some(e) => result
                .with_error_code(e.code())
                .with_error_message(Some(StrBytes::from_string(format!("{e:?}")))),
        }
    }
}
