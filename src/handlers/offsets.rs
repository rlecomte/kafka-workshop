//! `OffsetCommit` (api_key 8) et `OffsetFetch` (api_key 9) : enregistrer et
//! relire la position d'un groupe dans chaque partition.
//!
//! Les offsets commités sont écrits dans `__consumer_offsets` avant d'être
//! pris en compte en mémoire : ils survivent ainsi à un redémarrage.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use kafka_protocol::ResponseError;
use kafka_protocol::messages::offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions, OffsetFetchResponseTopic,
    OffsetFetchResponseTopics,
};
use kafka_protocol::messages::{
    GroupId, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;

use super::RequestContext;
use crate::Broker;
use crate::groups::group::CommittedOffset;
use crate::groups::offsets::{self, OFFSETS_TOPIC, OffsetKey, OffsetValue};
use crate::record_batch;

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

impl Broker {
    pub(super) async fn handle_offset_commit(&self, request: OffsetCommitRequest) -> OffsetCommitResponse {
        todo!("ch.5 : handle_offset_commit")
    }

    /// Écrit des commits dans `__consumer_offsets`, puis les applique en mémoire.
    fn store_commits(&self, group_id: &str, commits: &[(OffsetKey, OffsetValue)]) -> anyhow::Result<()> {
        todo!("ch.5 : store_commits")
    }

    pub(super) fn handle_offset_fetch(&self, request: OffsetFetchRequest, ctx: &RequestContext) -> OffsetFetchResponse {
        todo!("ch.5 : handle_offset_fetch")
    }

    /// Les offsets commités d'un groupe, pour les partitions demandées (ou
    /// toutes si `requested` est `None`). `None` : pas d'offset commité.
    #[allow(clippy::type_complexity)]
    fn fetch_offsets(
        &self,
        group_id: &GroupId,
        requested: Option<Vec<(TopicName, Vec<i32>)>>,
    ) -> Vec<(TopicName, Vec<(i32, Option<CommittedOffset>)>)> {
        todo!("ch.5 : fetch_offsets")
    }
}
