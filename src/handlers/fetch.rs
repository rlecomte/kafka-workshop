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
        let deadline = Instant::now() + Duration::from_millis(request.max_wait_ms.max(0) as u64);
        loop {
            // On s'abonne aux notifications *avant* de lire : une écriture qui
            // arriverait entre la lecture et l'attente ne serait pas manquée.
            let notified = self.replicas.new_data.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let (response, bytes, has_error) = self.read_fetch(&request);
            if bytes >= request.min_bytes.max(0) as usize || has_error || Instant::now() >= deadline {
                return response;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                // Délai écoulé : une dernière lecture, puis on répond.
                return self.read_fetch(&request).0;
            }
        }
    }

    /// Lit toutes les partitions demandées. Renvoie la réponse, le nombre
    /// d'octets lus et s'il y a eu au moins une erreur.
    fn read_fetch(&self, request: &FetchRequest) -> (FetchResponse, usize, bool) {
        let mut total = 0usize;
        let mut has_error = false;
        let max_bytes = request.max_bytes.max(0) as usize;

        let topics = request
            .topics
            .iter()
            .map(|topic| {
                let partitions = topic
                    .partitions
                    .iter()
                    .map(|p| {
                        // La réponse ne doit pas dépasser max_bytes, sauf pour le
                        // premier batch renvoyé (sinon un batch plus gros que
                        // max_bytes bloquerait le consumer).
                        let budget = if total == 0 { max_bytes } else { max_bytes.saturating_sub(total) };
                        let data = self.fetch_partition(topic.topic.as_str(), p, budget.min(p.partition_max_bytes.max(0) as usize));
                        let len = data.records.as_ref().map_or(0, |r| r.len());
                        if total > 0 && len > budget {
                            // Ne rentre plus : la partition sera lue au prochain Fetch.
                            return data.with_records(Some(Default::default()));
                        }
                        total += len;
                        has_error |= data.error_code != 0;
                        data
                    })
                    .collect();
                FetchableTopicResponse::default().with_topic(topic.topic.clone()).with_partitions(partitions)
            })
            .collect();

        (FetchResponse::default().with_responses(topics), total, has_error)
    }

    fn fetch_partition(&self, topic: &str, p: &FetchPartition, max_bytes: usize) -> PartitionData {
        let data = PartitionData::default().with_partition_index(p.partition);
        let replica = match self.leader_replica(topic, p.partition) {
            Ok(replica) => replica,
            Err(e) => return data.with_error_code(e.code()).with_high_watermark(-1),
        };
        match replica.read(p.fetch_offset, max_bytes) {
            Ok(read) => data
                .with_high_watermark(read.high_watermark)
                .with_last_stable_offset(read.high_watermark)
                .with_log_start_offset(read.log_start_offset)
                .with_aborted_transactions(None)
                .with_records(Some(read.records)),
            Err(e) => {
                tracing::debug!(topic, partition = p.partition, "fetch refusé : {e}");
                let code = match e {
                    ReadError::OffsetOutOfRange { .. } => ResponseError::OffsetOutOfRange,
                    ReadError::Io(_) => ResponseError::KafkaStorageError,
                };
                data.with_error_code(code.code())
                    .with_high_watermark(replica.high_watermark())
                    .with_log_start_offset(replica.log_start_offset())
            }
        }
    }
}
