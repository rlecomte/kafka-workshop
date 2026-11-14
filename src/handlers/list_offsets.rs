//! `ListOffsets` (api_key 2) : trouver un offset dans une partition, à
//! partir d'un timestamp ou de l'une des valeurs spéciales :
//!
//! - `-2` (earliest) : le premier offset présent dans le log ;
//! - `-1` (latest) : le high watermark, l'offset du prochain message.

use kafka_protocol::ResponseError;
use kafka_protocol::messages::list_offsets_request::ListOffsetsPartition;
use kafka_protocol::messages::list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse};
use kafka_protocol::messages::{ListOffsetsRequest, ListOffsetsResponse};

use crate::Broker;

pub const LATEST_TIMESTAMP: i64 = -1;
pub const EARLIEST_TIMESTAMP: i64 = -2;

impl Broker {
    pub(super) fn handle_list_offsets(&self, request: ListOffsetsRequest) -> ListOffsetsResponse {
        let topics = request
            .topics
            .into_iter()
            .map(|topic| {
                let partitions =
                    topic.partitions.iter().map(|p| self.list_offset(topic.name.as_str(), p)).collect();
                ListOffsetsTopicResponse::default().with_name(topic.name).with_partitions(partitions)
            })
            .collect();
        ListOffsetsResponse::default().with_topics(topics)
    }

    fn list_offset(&self, topic: &str, p: &ListOffsetsPartition) -> ListOffsetsPartitionResponse {
        let response = ListOffsetsPartitionResponse::default().with_partition_index(p.partition_index);
        let replica = match self.leader_replica(topic, p.partition_index) {
            Ok(replica) => replica,
            Err(e) => return response.with_error_code(e.code()),
        };
        let offset = match p.timestamp {
            LATEST_TIMESTAMP => Some(replica.high_watermark()),
            EARLIEST_TIMESTAMP => Some(replica.log_start_offset()),
            timestamp if timestamp >= 0 => match replica.offset_for_timestamp(timestamp) {
                Ok(offset) => offset,
                Err(_) => return response.with_error_code(ResponseError::KafkaStorageError.code()),
            },
            other => {
                tracing::debug!(topic, timestamp = other, "timestamp spécial non supporté");
                return response.with_error_code(ResponseError::InvalidRequest.code());
            }
        };
        match offset {
            Some(offset) => response.with_offset(offset).with_timestamp(-1),
            // Aucun message assez récent : offset et timestamp inconnus.
            None => response.with_offset(-1).with_timestamp(-1),
        }
    }
}
