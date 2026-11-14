//! Les requêtes d'appartenance à un groupe : `JoinGroup` (11), `SyncGroup`
//! (14), `Heartbeat` (12) et `LeaveGroup` (13).
//!
//! La logique est dans [`crate::groups::group::Group`] ; ce module ne fait que
//! traduire les requêtes et attendre les réponses différées.

use std::time::Duration;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::messages::leave_group_response::MemberResponse;
use kafka_protocol::messages::{
    HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
    LeaveGroupResponse, SyncGroupRequest, SyncGroupResponse,
};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::oneshot;

use super::RequestContext;
use crate::Broker;
use crate::groups::group::{JoinRequest, JoinResult, Outcome, SyncResult};

fn code(error: Option<ResponseError>) -> i16 {
    error.map_or(0, |e| e.code())
}

fn str_bytes(s: Option<String>) -> Option<StrBytes> {
    s.map(StrBytes::from_string)
}

/// Attend une réponse différée, au plus `timeout`.
async fn wait<T>(receiver: oneshot::Receiver<T>, timeout: Duration, on_error: T) -> T {
    match tokio::time::timeout(timeout, receiver).await {
        Ok(Ok(result)) => result,
        _ => on_error,
    }
}

impl Broker {
    pub(super) async fn handle_join_group(&self, request: JoinGroupRequest, ctx: &RequestContext) -> JoinGroupResponse {
        todo!("ch.5 : handle_join_group")
    }

    pub(super) async fn handle_sync_group(&self, request: SyncGroupRequest) -> SyncGroupResponse {
        todo!("ch.5 : handle_sync_group")
    }

    pub(super) fn handle_heartbeat(&self, request: HeartbeatRequest) -> HeartbeatResponse {
        todo!("ch.5 : handle_heartbeat")
    }

    pub(super) fn handle_leave_group(&self, request: LeaveGroupRequest, ctx: &RequestContext) -> LeaveGroupResponse {
        todo!("ch.5 : handle_leave_group")
    }
}

fn join_response(result: JoinResult, version: i16) -> JoinGroupResponse {
    // Avant la v7, protocol_name ne peut pas être nul.
    let protocol_name = match result.protocol_name {
        None if version < 7 => Some(String::new()),
        name => name,
    };
    let members = result
        .members
        .into_iter()
        .map(|(id, metadata)| JoinGroupResponseMember::default().with_member_id(StrBytes::from_string(id)).with_metadata(metadata))
        .collect();
    JoinGroupResponse::default()
        .with_error_code(code(result.error))
        .with_generation_id(result.generation_id)
        .with_protocol_type(str_bytes(result.protocol_type))
        .with_protocol_name(str_bytes(protocol_name))
        .with_leader(StrBytes::from_string(result.leader))
        .with_member_id(StrBytes::from_string(result.member_id))
        .with_members(members)
}
