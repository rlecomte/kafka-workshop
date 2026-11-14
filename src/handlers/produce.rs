//! `Produce` (api_key 0) : écrire des batches de messages dans des partitions.

use kafka_protocol::ResponseError;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::protocol::StrBytes;

use crate::Broker;
use crate::record_batch;

impl Broker {
    /// Renvoie `None` si aucune réponse ne doit être envoyée (`acks = 0`).
    pub(super) async fn handle_produce(&self, request: ProduceRequest) -> Option<ProduceResponse> {
        todo!("ch.2 : handle_produce")
    }

    fn produce_topic(&self, topic: TopicProduceData, acks: i16) -> TopicProduceResponse {
        todo!("ch.2 : produce_topic")
    }

    fn produce_partition(&self, topic: &str, data: PartitionProduceData, acks: i16) -> PartitionProduceResponse {
        todo!("ch.2 : produce_partition")
    }
}
