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
        todo!("ch.1 : handle_describe_configs")
    }

    fn describe_resource(&self, resource: DescribeConfigsResource) -> DescribeConfigsResult {
        todo!("ch.1 : describe_resource")
    }
}
