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
        let group_id = request.group_id.to_string();
        let check = if !self.is_coordinator(&group_id) {
            Err(ResponseError::NotCoordinator)
        } else {
            self.groups.with_group(&group_id, |group, _, now| {
                group.check_commit(&request.member_id, request.generation_id_or_member_epoch, now)
            })
        };

        // Une erreur par partition : celle du groupe, ou celle de la partition.
        let mut commits = Vec::new();
        let mut results: Vec<(TopicName, Vec<(i32, Option<ResponseError>)>)> = Vec::new();
        for topic in &request.topics {
            let mut partitions = Vec::new();
            for p in &topic.partitions {
                let error = match check {
                    Err(e) => Some(e),
                    // La partition doit exister dans le cluster (pas forcément
                    // sur ce broker).
                    Ok(()) if !self.topics.get(&topic.name).is_some_and(|t| t.partitions.iter().any(|x| x.index == p.partition_index)) => {
                        Some(ResponseError::UnknownTopicOrPartition)
                    }
                    Ok(()) => {
                        let key = OffsetKey { group: group_id.clone(), topic: topic.name.to_string(), partition: p.partition_index };
                        let value = OffsetValue {
                            offset: p.committed_offset,
                            leader_epoch: p.committed_leader_epoch,
                            metadata: p.committed_metadata.as_deref().unwrap_or_default().to_string(),
                            commit_timestamp: now_ms(),
                        };
                        commits.push((key, value));
                        None
                    }
                };
                partitions.push((p.partition_index, error));
            }
            results.push((topic.name.clone(), partitions));
        }

        if !commits.is_empty()
            && let Err(e) = self.store_commits(&group_id, &commits).await
        {
            tracing::error!(group = %group_id, "écriture des offsets impossible : {e}");
            for (_, partitions) in &mut results {
                for (_, error) in partitions.iter_mut() {
                    error.get_or_insert(ResponseError::CoordinatorNotAvailable);
                }
            }
        }

        let topics = results
            .into_iter()
            .map(|(name, partitions)| {
                let partitions = partitions
                    .into_iter()
                    .map(|(index, error)| {
                        OffsetCommitResponsePartition::default()
                            .with_partition_index(index)
                            .with_error_code(error.map_or(0, |e| e.code()))
                    })
                    .collect();
                OffsetCommitResponseTopic::default().with_name(name).with_partitions(partitions)
            })
            .collect();
        OffsetCommitResponse::default().with_topics(topics)
    }

    /// Écrit des commits dans `__consumer_offsets`, puis les applique en mémoire.
    async fn store_commits(&self, group_id: &str, commits: &[(OffsetKey, OffsetValue)]) -> anyhow::Result<()> {
        todo!("ch.7 : store_commits")
    }

    pub(super) fn handle_offset_fetch(&self, request: OffsetFetchRequest, ctx: &RequestContext) -> OffsetFetchResponse {
        if ctx.version < 8 {
            if !self.is_coordinator(&request.group_id) {
                return OffsetFetchResponse::default().with_error_code(ResponseError::NotCoordinator.code());
            }
            // Un seul groupe.
            let requested = request
                .topics
                .map(|topics| topics.into_iter().map(|t| (t.name, t.partition_indexes)).collect());
            let topics = self
                .fetch_offsets(&request.group_id, requested)
                .into_iter()
                .map(|(name, partitions)| {
                    let partitions = partitions
                        .into_iter()
                        .map(|(index, c)| {
                            OffsetFetchResponsePartition::default()
                                .with_partition_index(index)
                                .with_committed_offset(c.as_ref().map_or(-1, |c| c.offset))
                                .with_committed_leader_epoch(c.as_ref().map_or(-1, |c| c.leader_epoch))
                                .with_metadata(Some(StrBytes::from_string(c.map(|c| c.metadata).unwrap_or_default())))
                        })
                        .collect();
                    OffsetFetchResponseTopic::default().with_name(name).with_partitions(partitions)
                })
                .collect();
            return OffsetFetchResponse::default().with_topics(topics);
        }

        // À partir de la v8 : plusieurs groupes par requête.
        let groups = request
            .groups
            .into_iter()
            .map(|g| {
                if !self.is_coordinator(&g.group_id) {
                    return OffsetFetchResponseGroup::default()
                        .with_group_id(g.group_id)
                        .with_error_code(ResponseError::NotCoordinator.code());
                }
                let requested = g.topics.map(|topics| topics.into_iter().map(|t| (t.name, t.partition_indexes)).collect());
                let topics = self
                    .fetch_offsets(&g.group_id, requested)
                    .into_iter()
                    .map(|(name, partitions)| {
                        let partitions = partitions
                            .into_iter()
                            .map(|(index, c)| {
                                OffsetFetchResponsePartitions::default()
                                    .with_partition_index(index)
                                    .with_committed_offset(c.as_ref().map_or(-1, |c| c.offset))
                                    .with_committed_leader_epoch(c.as_ref().map_or(-1, |c| c.leader_epoch))
                                    .with_metadata(Some(StrBytes::from_string(c.map(|c| c.metadata).unwrap_or_default())))
                            })
                            .collect();
                        OffsetFetchResponseTopics::default().with_name(name).with_partitions(partitions)
                    })
                    .collect();
                OffsetFetchResponseGroup::default().with_group_id(g.group_id).with_topics(topics)
            })
            .collect();
        OffsetFetchResponse::default().with_groups(groups)
    }

    /// Les offsets commités d'un groupe, pour les partitions demandées (ou
    /// toutes si `requested` est `None`). `None` : pas d'offset commité.
    #[allow(clippy::type_complexity)]
    fn fetch_offsets(
        &self,
        group_id: &GroupId,
        requested: Option<Vec<(TopicName, Vec<i32>)>>,
    ) -> Vec<(TopicName, Vec<(i32, Option<CommittedOffset>)>)> {
        let committed = self.groups.with_existing_group(group_id, |group, _, _| group.offsets.clone()).unwrap_or_default();
        match requested {
            Some(topics) => topics
                .into_iter()
                .map(|(name, partitions)| {
                    let partitions = partitions
                        .into_iter()
                        .map(|p| (p, committed.get(&(name.to_string(), p)).cloned()))
                        .collect();
                    (name, partitions)
                })
                .collect(),
            None => {
                let mut by_topic: BTreeMap<String, Vec<(i32, Option<CommittedOffset>)>> = BTreeMap::new();
                for ((topic, partition), offset) in committed {
                    by_topic.entry(topic).or_default().push((partition, Some(offset)));
                }
                by_topic
                    .into_iter()
                    .map(|(topic, mut partitions)| {
                        partitions.sort_by_key(|(p, _)| *p);
                        (TopicName(StrBytes::from_string(topic)), partitions)
                    })
                    .collect()
            }
        }
    }
}
