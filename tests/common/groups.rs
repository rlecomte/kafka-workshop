//! Un membre de consumer group simulé, pour les tests du chapitre 5.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    GroupId, HeartbeatRequest, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest, SyncGroupRequest,
    SyncGroupResponse,
};
use kafka_protocol::protocol::StrBytes;

use super::Client;

/// Une config de broker adaptée aux tests de groupes : pas de délai initial,
/// des sessions courtes acceptées.
pub fn group_config() -> broker::Config {
    broker::Config {
        group: broker::groups::group::GroupConfig {
            initial_rebalance_delay: Duration::ZERO,
            min_session_timeout: Duration::from_millis(100),
            max_session_timeout: Duration::from_secs(60),
        },
        offsets_topic_partitions: 4,
        ..Default::default()
    }
}

pub fn s(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

/// Un membre de groupe, avec sa propre connexion.
pub struct Member {
    pub client: Client,
    pub group: String,
    pub member_id: String,
    pub generation: i32,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocols: Vec<(String, Bytes)>,
}

impl Member {
    pub async fn new(addr: SocketAddr, group: &str) -> Member {
        Member {
            client: Client::connect(addr).await,
            group: group.to_string(),
            member_id: String::new(),
            generation: -1,
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 10_000,
            protocols: vec![("range".to_string(), Bytes::from_static(b"metadata"))],
        }
    }

    pub fn join_request(&self) -> JoinGroupRequest {
        JoinGroupRequest::default()
            .with_group_id(GroupId(s(&self.group)))
            .with_session_timeout_ms(self.session_timeout_ms)
            .with_rebalance_timeout_ms(self.rebalance_timeout_ms)
            .with_member_id(s(&self.member_id))
            .with_protocol_type(s("consumer"))
            .with_protocols(
                self.protocols
                    .iter()
                    .map(|(name, metadata)| JoinGroupRequestProtocol::default().with_name(s(name)).with_metadata(metadata.clone()))
                    .collect(),
            )
    }

    /// Envoie un JoinGroup v9 (un seul essai) et mémorise member_id et génération.
    pub async fn join_once(&mut self) -> JoinGroupResponse {
        let response = self.client.send(self.join_request(), 9).await.unwrap();
        if !response.member_id.is_empty() {
            self.member_id = response.member_id.to_string();
        }
        if response.error_code == 0 {
            self.generation = response.generation_id;
        }
        response
    }

    /// Rejoint le groupe, en gérant MEMBER_ID_REQUIRED, et vérifie le succès.
    pub async fn join(&mut self) -> JoinGroupResponse {
        let mut response = self.join_once().await;
        if response.error_code == ResponseError::MemberIdRequired.code() {
            response = self.join_once().await;
        }
        assert_eq!(response.error_code, 0, "JoinGroup de {} : {:?}", self.member_id, ResponseError::try_from_code(response.error_code));
        response
    }

    /// Envoie un SyncGroup (le leader fournit les assignations).
    pub async fn sync(&mut self, assignments: &[(&str, &str)]) -> SyncGroupResponse {
        let request = SyncGroupRequest::default()
            .with_group_id(GroupId(s(&self.group)))
            .with_generation_id(self.generation)
            .with_member_id(s(&self.member_id))
            .with_protocol_type(Some(s("consumer")))
            .with_protocol_name(Some(s("range")))
            .with_assignments(
                assignments
                    .iter()
                    .map(|(member, assignment)| {
                        SyncGroupRequestAssignment::default()
                            .with_member_id(s(member))
                            .with_assignment(Bytes::from(assignment.to_string()))
                    })
                    .collect(),
            );
        self.client.send(request, 5).await.unwrap()
    }

    /// Envoie un Heartbeat et renvoie le code d'erreur.
    pub async fn heartbeat(&mut self) -> i16 {
        let request = HeartbeatRequest::default()
            .with_group_id(GroupId(s(&self.group)))
            .with_generation_id(self.generation)
            .with_member_id(s(&self.member_id));
        self.client.send(request, 4).await.unwrap().error_code
    }

    /// Quitte le groupe et renvoie le code d'erreur du membre.
    pub async fn leave(&mut self) -> i16 {
        let request = LeaveGroupRequest::default()
            .with_group_id(GroupId(s(&self.group)))
            .with_members(vec![MemberIdentity::default().with_member_id(s(&self.member_id))]);
        let response = self.client.send(request, 5).await.unwrap();
        response.members[0].error_code
    }
}

pub fn assignment(response: &SyncGroupResponse) -> String {
    String::from_utf8_lossy(&response.assignment).into_owned()
}

use kafka_protocol::messages::offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic};
use kafka_protocol::messages::offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics};
use kafka_protocol::messages::{OffsetCommitRequest, OffsetFetchRequest};

/// Commit d'offsets (v9) ; renvoie les codes d'erreur par partition.
pub async fn commit(
    client: &mut Client,
    group: &str,
    generation: i32,
    member_id: &str,
    offsets: &[(&str, i32, i64)],
) -> Vec<i16> {
    let mut topics: Vec<OffsetCommitRequestTopic> = Vec::new();
    for (topic, partition, offset) in offsets {
        let p = OffsetCommitRequestPartition::default()
            .with_partition_index(*partition)
            .with_committed_offset(*offset)
            .with_committed_metadata(Some(s(&format!("meta-{offset}"))));
        match topics.iter_mut().find(|t| t.name.as_str() == *topic) {
            Some(t) => t.partitions.push(p),
            None => topics.push(OffsetCommitRequestTopic::default().with_name(super::topic_name(topic)).with_partitions(vec![p])),
        }
    }
    let request = OffsetCommitRequest::default()
        .with_group_id(GroupId(s(group)))
        .with_generation_id_or_member_epoch(generation)
        .with_member_id(s(member_id))
        .with_topics(topics);
    let response = client.send(request, 9).await.unwrap();
    response.topics.iter().flat_map(|t| t.partitions.iter().map(|p| p.error_code)).collect()
}

/// Commit « simple », hors group management (génération -1).
pub async fn simple_commit(client: &mut Client, group: &str, offsets: &[(&str, i32, i64)]) -> Vec<i16> {
    commit(client, group, -1, "", offsets).await
}

/// Lit l'offset commité d'une partition avec OffsetFetch v7 (un groupe).
pub async fn fetch_offset_v7(client: &mut Client, group: &str, topic: &str, partition: i32) -> (i64, String) {
    let request = OffsetFetchRequest::default().with_group_id(GroupId(s(group))).with_topics(Some(vec![
        OffsetFetchRequestTopic::default().with_name(super::topic_name(topic)).with_partition_indexes(vec![partition]),
    ]));
    let response = client.send(request, 7).await.unwrap();
    let p = &response.topics[0].partitions[0];
    (p.committed_offset, p.metadata.as_deref().unwrap_or_default().to_string())
}

/// Lit l'offset commité d'une partition avec OffsetFetch v8 (plusieurs groupes).
pub async fn fetch_offset_v8(client: &mut Client, group: &str, topic: &str, partition: i32) -> i64 {
    let request = OffsetFetchRequest::default().with_groups(vec![OffsetFetchRequestGroup::default()
        .with_group_id(GroupId(s(group)))
        .with_topics(Some(vec![OffsetFetchRequestTopics::default()
            .with_name(super::topic_name(topic))
            .with_partition_indexes(vec![partition])]))]);
    let response = client.send(request, 8).await.unwrap();
    response.groups[0].topics[0].partitions[0].committed_offset
}
