//! `Fetch` (api_key 1) : lire des batches dans des partitions.
//!
//! Un `Fetch` peut attendre (« long polling ») : si moins de `min_bytes`
//! octets sont disponibles, le broker attend que de nouvelles données
//! arrivent, au plus `max_wait_ms` millisecondes, avant de répondre.

use std::time::Duration;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::fetch_request::FetchPartition;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::{FetchRequest, FetchResponse};
use tokio::time::Instant;

use crate::Broker;
use crate::log::ReadError;

impl Broker {
    pub(super) async fn handle_fetch(&self, request: FetchRequest) -> FetchResponse {
        todo!("ch.3 : handle_fetch")
    }

    /// Lit toutes les partitions demandées. Renvoie la réponse, le nombre
    /// d'octets lus et s'il y a eu au moins une erreur.
    fn read_fetch(&self, request: &FetchRequest) -> (FetchResponse, usize, bool) {
        todo!("ch.3 : read_fetch")
    }

    fn fetch_partition(&self, topic: &str, p: &FetchPartition, max_bytes: usize) -> PartitionData {
        todo!("ch.3 : fetch_partition")
    }
}
