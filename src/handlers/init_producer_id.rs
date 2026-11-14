//! `InitProducerId` (api_key 22) : attribue un identifiant à un producer.
//!
//! Depuis Kafka 3.0, les producers Java sont idempotents par défaut : ils
//! demandent un `producer_id` au démarrage et numérotent leurs batches. Ici
//! on se contente d'attribuer des identifiants uniques. La déduplication
//! des batches est un bonus.

use std::sync::atomic::Ordering;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::{InitProducerIdRequest, InitProducerIdResponse, ProducerId};

use crate::Broker;

impl Broker {
    pub(super) fn handle_init_producer_id(&self, request: InitProducerIdRequest) -> InitProducerIdResponse {
        if request.transactional_id.is_some() {
            // Les transactions sont hors du périmètre du workshop.
            return InitProducerIdResponse::default()
                .with_error_code(ResponseError::TransactionalIdAuthorizationFailed.code())
                .with_producer_id(ProducerId(-1))
                .with_producer_epoch(-1);
        }
        let producer_id = self.next_producer_id.fetch_add(1, Ordering::Relaxed);
        InitProducerIdResponse::default().with_producer_id(ProducerId(producer_id)).with_producer_epoch(0)
    }
}
