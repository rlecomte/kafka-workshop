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
        todo!("ch.2 : handle_init_producer_id")
    }
}
