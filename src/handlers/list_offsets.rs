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
        todo!("ch.3 : handle_list_offsets")
    }

    fn list_offset(&self, topic: &str, p: &ListOffsetsPartition) -> ListOffsetsPartitionResponse {
        todo!("ch.3 : list_offset")
    }
}
