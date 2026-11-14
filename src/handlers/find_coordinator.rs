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
        todo!("ch.5 : handle_find_coordinator")
    }

    fn find_coordinator(&self, key: StrBytes, key_type: i8) -> Coordinator {
        todo!("ch.5 : find_coordinator")
    }
}
