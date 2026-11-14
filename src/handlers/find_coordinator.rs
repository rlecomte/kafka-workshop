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
    pub(super) fn handle_find_coordinator(&self, request: FindCoordinatorRequest, ctx: &RequestContext) -> FindCoordinatorResponse {
        // Jusqu'à la v3, une seule clé ; à partir de la v4, une liste.
        let keys = if ctx.version >= 4 { request.coordinator_keys.clone() } else { vec![request.key.clone()] };
        let coordinators: Vec<Coordinator> = keys.into_iter().map(|key| self.find_coordinator(key, request.key_type)).collect();

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

    fn find_coordinator(&self, key: StrBytes, key_type: i8) -> Coordinator {
        let coordinator = Coordinator::default().with_key(key.clone()).with_node_id(BrokerId(-1)).with_port(-1);
        if key_type != KEY_TYPE_GROUP {
            return coordinator
                .with_error_code(ResponseError::InvalidRequest.code())
                .with_error_message(Some(StrBytes::from_static_str("les transactions ne sont pas supportées")));
        }
        if let Err(e) = self.ensure_offsets_topic() {
            tracing::error!("impossible de créer __consumer_offsets : {e}");
            return coordinator.with_error_code(ResponseError::CoordinatorNotAvailable.code());
        }
        // Un seul broker : c'est toujours lui le coordinateur.
        coordinator
            .with_node_id(BrokerId(self.config.node_id))
            .with_host(StrBytes::from_string(self.config.host.clone()))
            .with_port(self.config.port as i32)
    }
}
