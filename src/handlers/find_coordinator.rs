//! `FindCoordinator` (api_key 10) : quel broker coordonne ce groupe ?
//!
//! Le coordinateur d'un groupe est le leader de la partition de
//! `__consumer_offsets` qui stocke ses offsets. Le topic est créé au premier
//! besoin.

use kafka_protocol::ResponseError;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::{BrokerId, FindCoordinatorRequest, FindCoordinatorResponse};
use kafka_protocol::protocol::StrBytes;

use super::RequestContext;
use crate::Broker;

/// Types de clé : un groupe, ou un identifiant transactionnel.
const KEY_TYPE_GROUP: i8 = 0;

impl Broker {
    pub(super) async fn handle_find_coordinator(&self, request: FindCoordinatorRequest, ctx: &RequestContext) -> FindCoordinatorResponse {
        // Jusqu'à la v3, une seule clé ; à partir de la v4, une liste.
        let keys = if ctx.version >= 4 { request.coordinator_keys.clone() } else { vec![request.key.clone()] };
        let mut coordinators: Vec<Coordinator> = Vec::new();
        for key in keys {
            coordinators.push(self.find_coordinator(key, request.key_type).await);
        }

        if ctx.version >= 4 {
            return FindCoordinatorResponse::default().with_coordinators(coordinators);
        }
        let c = coordinators.into_iter().next().unwrap();
        FindCoordinatorResponse::default()
            .with_error_code(c.error_code)
            .with_error_message(c.error_message)
            .with_node_id(c.node_id)
            .with_host(c.host)
            .with_port(c.port)
    }

    async fn find_coordinator(&self, key: StrBytes, key_type: i8) -> Coordinator {
        todo!("ch.6 : find_coordinator")
    }
}
