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
        let group_id = request.group_id.to_string();
        let member_id = request.member_id.to_string();
        if group_id.is_empty() {
            return join_response(JoinResult::error(ResponseError::InvalidGroupId, &member_id), ctx.version);
        }
        if !self.is_coordinator(&group_id) {
            return join_response(JoinResult::error(ResponseError::NotCoordinator, &member_id), ctx.version);
        }
        // La v0 n'a pas de rebalance_timeout (-1) : c'est alors le session_timeout.
        let rebalance_timeout_ms =
            if request.rebalance_timeout_ms < 0 { request.session_timeout_ms } else { request.rebalance_timeout_ms };
        let rebalance_timeout = Duration::from_millis(rebalance_timeout_ms.max(0) as u64);
        let join = JoinRequest {
            member_id: member_id.clone(),
            client_id: ctx.client_id.clone(),
            client_host: ctx.client_host.clone(),
            session_timeout: Duration::from_millis(request.session_timeout_ms.max(0) as u64),
            rebalance_timeout,
            protocol_type: request.protocol_type.to_string(),
            protocols: request.protocols.iter().map(|p| (p.name.to_string(), p.metadata.clone())).collect(),
            require_known_member_id: ctx.version >= 4,
        };

        let outcome = self.groups.with_group(&group_id, |group, config, now| group.join(join, config, now));
        let result = match outcome {
            Outcome::Now(result) => result,
            Outcome::Later(receiver) => {
                // Le délai initial peut dépasser le rebalance timeout du membre.
                let timeout = rebalance_timeout.max(self.groups.config.initial_rebalance_delay) + Duration::from_secs(5);
                wait(receiver, timeout, JoinResult::error(ResponseError::RebalanceInProgress, &member_id)).await
            }
        };
        join_response(result, ctx.version)
    }

    pub(super) async fn handle_sync_group(&self, request: SyncGroupRequest) -> SyncGroupResponse {
        let group_id = request.group_id.to_string();
        let member_id = request.member_id.to_string();
        if !self.is_coordinator(&group_id) {
            return SyncGroupResponse::default().with_error_code(ResponseError::NotCoordinator.code());
        }
        let assignments = request.assignments.iter().map(|a| (a.member_id.to_string(), a.assignment.clone())).collect();

        let outcome = self.groups.with_existing_group(&group_id, |group, _, now| {
            group.sync(&member_id, request.generation_id, assignments, now)
        });
        let result = match outcome {
            None => SyncResult::error(ResponseError::UnknownMemberId),
            Some(Outcome::Now(result)) => result,
            Some(Outcome::Later(receiver)) => {
                // Le SyncGroup d'un membre attend celui du leader.
                wait(receiver, Duration::from_secs(300), SyncResult::error(ResponseError::RebalanceInProgress)).await
            }
        };
        SyncGroupResponse::default()
            .with_error_code(code(result.error))
            .with_protocol_type(str_bytes(result.protocol_type))
            .with_protocol_name(str_bytes(result.protocol_name))
            .with_assignment(result.assignment)
    }

    pub(super) fn handle_heartbeat(&self, request: HeartbeatRequest) -> HeartbeatResponse {
        if !self.is_coordinator(&request.group_id) {
            return HeartbeatResponse::default().with_error_code(ResponseError::NotCoordinator.code());
        }
        let result = self
            .groups
            .with_existing_group(&request.group_id, |group, _, now| {
                group.heartbeat(&request.member_id, request.generation_id, now)
            })
            .unwrap_or(Err(ResponseError::UnknownMemberId));
        HeartbeatResponse::default().with_error_code(code(result.err()))
    }

    pub(super) fn handle_leave_group(&self, request: LeaveGroupRequest, ctx: &RequestContext) -> LeaveGroupResponse {
        if !self.is_coordinator(&request.group_id) {
            return LeaveGroupResponse::default().with_error_code(ResponseError::NotCoordinator.code());
        }
        // Jusqu'à la v2, un seul membre ; à partir de la v3, une liste.
        let member_ids: Vec<StrBytes> = if ctx.version >= 3 {
            request.members.iter().map(|m| m.member_id.clone()).collect()
        } else {
            vec![request.member_id.clone()]
        };
        let results: Vec<(StrBytes, Result<(), ResponseError>)> = member_ids
            .into_iter()
            .map(|id| {
                let result = self
                    .groups
                    .with_existing_group(&request.group_id, |group, config, now| group.leave(&id, config, now))
                    .unwrap_or(Err(ResponseError::UnknownMemberId));
                (id, result)
            })
            .collect();

        if ctx.version >= 3 {
            let members = results
                .into_iter()
                .map(|(id, r)| MemberResponse::default().with_member_id(id).with_error_code(code(r.err())))
                .collect();
            LeaveGroupResponse::default().with_members(members)
        } else {
            let error = results.into_iter().next().and_then(|(_, r)| r.err());
            LeaveGroupResponse::default().with_error_code(code(error))
        }
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
