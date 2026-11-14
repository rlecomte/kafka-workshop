//! `Produce` (api_key 0) : écrire des batches de messages dans des partitions.

use kafka_protocol::ResponseError;
use kafka_protocol::messages::produce_request::PartitionProduceData;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::protocol::StrBytes;

use std::sync::Arc;
use std::time::Duration;

use crate::Broker;
use crate::record_batch;
use crate::replica::Replica;

impl Broker {
    /// Renvoie `None` si aucune réponse ne doit être envoyée (`acks = 0`).
    pub(super) async fn handle_produce(&self, request: ProduceRequest) -> Option<ProduceResponse> {
        todo!("ch.7 : handle_produce")
    }

    /// Écrit les batches d'une partition. Renvoie la réponse et, en cas de
    /// succès, la réplique et l'offset qui suit les messages écrits.
    fn produce_partition(
        &self,
        topic: &str,
        data: PartitionProduceData,
        acks: i16,
    ) -> (PartitionProduceResponse, Option<(Arc<Replica>, i64)>) {
        todo!("ch.7 : produce_partition")
    }
}
