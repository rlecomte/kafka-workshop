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
        let acks = request.acks;
        let responses = request
            .topic_data
            .into_iter()
            .map(|topic| self.produce_topic(topic, acks))
            .collect();

        // acks = 0 : le producer n'attend pas de réponse, et n'en lira pas.
        if acks == 0 {
            return None;
        }
        Some(ProduceResponse::default().with_responses(responses))
    }

    fn produce_topic(&self, topic: TopicProduceData, acks: i16) -> TopicProduceResponse {
        let partitions = topic
            .partition_data
            .into_iter()
            .map(|partition| self.produce_partition(topic.name.as_str(), partition, acks))
            .collect();
        TopicProduceResponse::default().with_name(topic.name).with_partition_responses(partitions)
    }

    fn produce_partition(&self, topic: &str, data: PartitionProduceData, acks: i16) -> PartitionProduceResponse {
        let response = PartitionProduceResponse::default().with_index(data.index);
        let error = |e: ResponseError, message: String| {
            tracing::debug!(topic, partition = data.index, "produce refusé : {message}");
            response
                .clone()
                .with_base_offset(-1)
                .with_error_code(e.code())
                .with_error_message(Some(StrBytes::from_string(message)))
        };

        if !matches!(acks, -1..=1) {
            return error(ResponseError::InvalidRequiredAcks, format!("acks={acks} invalide"));
        }
        let Some(replica) = self.replicas.get(topic, data.index) else {
            return error(ResponseError::UnknownTopicOrPartition, format!("{topic}-{} inconnue", data.index));
        };
        let Some(records) = data.records else {
            return error(ResponseError::CorruptMessage, "champ records absent".into());
        };
        let batches = match record_batch::parse_batches(records) {
            Ok(batches) => batches,
            Err(e) => return error(e.error(), e.to_string()),
        };
        if let Some(big) = batches.iter().find(|b| b.bytes.len() > self.config.max_message_bytes) {
            return error(
                ResponseError::MessageTooLarge,
                format!("batch de {} octets, maximum {}", big.bytes.len(), self.config.max_message_bytes),
            );
        }

        match replica.append(batches) {
            Ok(info) => {
                tracing::debug!(topic, partition = data.index, base_offset = info.base_offset, "batches écrits");
                response.with_base_offset(info.base_offset).with_log_start_offset(info.log_start_offset)
            }
            Err(e) => error(ResponseError::KafkaStorageError, format!("écriture impossible : {e}")),
        }
    }
}
