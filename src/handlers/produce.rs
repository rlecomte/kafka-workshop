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
        let acks = request.acks;
        let timeout = Duration::from_millis(request.timeout_ms.max(0) as u64);
        let mut responses = Vec::new();
        for topic in request.topic_data {
            let mut partitions = Vec::new();
            for partition in topic.partition_data {
                let (response, written) = self.produce_partition(topic.name.as_str(), partition, acks);
                partitions.push((response, written));
            }
            responses.push((topic.name, partitions));
        }

        // acks = all : on attend que toute l'ISR ait copié les messages,
        // c'est-à-dire que le high watermark les dépasse.
        if acks == -1 {
            for (_, partitions) in &mut responses {
                for (response, written) in partitions.iter_mut() {
                    if let Some((replica, next_offset)) = written.take()
                        && !self.wait_for_high_watermark(&replica, next_offset, timeout).await
                    {
                        response.error_code = ResponseError::RequestTimedOut.code();
                    }
                }
            }
        }

        // acks = 0 : le producer n'attend pas de réponse, et n'en lira pas.
        if acks == 0 {
            return None;
        }
        let responses = responses
            .into_iter()
            .map(|(name, partitions)| {
                TopicProduceResponse::default()
                    .with_name(name)
                    .with_partition_responses(partitions.into_iter().map(|(r, _)| r).collect())
            })
            .collect();
        Some(ProduceResponse::default().with_responses(responses))
    }

    /// Écrit les batches d'une partition. Renvoie la réponse et, en cas de
    /// succès, la réplique et l'offset qui suit les messages écrits.
    fn produce_partition(
        &self,
        topic: &str,
        data: PartitionProduceData,
        acks: i16,
    ) -> (PartitionProduceResponse, Option<(Arc<Replica>, i64)>) {
        let response = PartitionProduceResponse::default().with_index(data.index);
        let error = |e: ResponseError, message: String| {
            tracing::debug!(topic, partition = data.index, "produce refusé : {message}");
            let response = response
                .clone()
                .with_base_offset(-1)
                .with_error_code(e.code())
                .with_error_message(Some(StrBytes::from_string(message)));
            (response, None)
        };

        if !matches!(acks, -1..=1) {
            return error(ResponseError::InvalidRequiredAcks, format!("acks={acks} invalide"));
        }
        let replica = match self.leader_replica(topic, data.index) {
            Ok(replica) => replica,
            Err(e) => return error(e, format!("{topic}-{} : pas leader ici", data.index)),
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

        // Avec acks = all, il faut assez de répliques à jour pour que l'écriture
        // ait un sens (min.insync.replicas).
        let isr = replica.isr().len();
        if acks == -1 && isr < self.config.min_insync_replicas {
            return error(
                ResponseError::NotEnoughReplicas,
                format!("{isr} réplique(s) dans l'ISR, minimum {}", self.config.min_insync_replicas),
            );
        }

        match replica.append(batches) {
            Ok(info) => {
                tracing::debug!(topic, partition = data.index, base_offset = info.base_offset, "batches écrits");
                let response = response.with_base_offset(info.base_offset).with_log_start_offset(info.log_start_offset);
                (response, Some((replica, info.next_offset)))
            }
            Err(e) => error(ResponseError::KafkaStorageError, format!("écriture impossible : {e}")),
        }
    }
}
