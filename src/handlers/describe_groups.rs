//! `ListGroups` (api_key 16) et `DescribeGroups` (api_key 15) : pour les
//! outils d'administration (`kafka-consumer-groups.sh`).

use kafka_protocol::messages::describe_groups_response::{DescribedGroup, DescribedGroupMember};
use kafka_protocol::messages::list_groups_response::ListedGroup;
use kafka_protocol::messages::{
    DescribeGroupsRequest, DescribeGroupsResponse, GroupId, ListGroupsRequest, ListGroupsResponse,
};
use kafka_protocol::protocol::StrBytes;

use crate::Broker;

fn s(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

impl Broker {
    pub(super) fn handle_list_groups(&self, request: ListGroupsRequest) -> ListGroupsResponse {
        let states: Vec<String> = request.states_filter.iter().map(|s| s.to_lowercase()).collect();
        let types: Vec<String> = request.types_filter.iter().map(|s| s.to_lowercase()).collect();
        let mut groups = Vec::new();
        self.groups.for_each_group(|group| {
            let state = group.state.name();
            if (!states.is_empty() && !states.contains(&state.to_lowercase()))
                || (!types.is_empty() && !types.iter().any(|t| t == "classic"))
            {
                return;
            }
            groups.push(
                ListedGroup::default()
                    .with_group_id(GroupId(s(&group.id)))
                    .with_protocol_type(s(group.protocol_type.as_deref().unwrap_or("consumer")))
                    .with_group_state(s(state))
                    .with_group_type(s("classic")),
            );
        });
        ListGroupsResponse::default().with_groups(groups)
    }

    pub(super) fn handle_describe_groups(&self, request: DescribeGroupsRequest) -> DescribeGroupsResponse {
        let groups = request
            .groups
            .into_iter()
            .map(|group_id| {
                if !self.is_coordinator(&group_id) {
                    return DescribedGroup::default()
                        .with_group_id(group_id)
                        .with_error_code(kafka_protocol::ResponseError::NotCoordinator.code());
                }
                self.groups
                    .with_existing_group(&group_id, |group, _, _| {
                        let protocol = group.protocol_name.clone().unwrap_or_default();
                        let members = group
                            .members
                            .values()
                            .map(|m| {
                                DescribedGroupMember::default()
                                    .with_member_id(s(&m.id))
                                    .with_client_id(s(&m.client_id))
                                    .with_client_host(s(&m.client_host))
                                    .with_member_metadata(m.metadata(&protocol))
                                    .with_member_assignment(m.assignment.clone())
                            })
                            .collect();
                        DescribedGroup::default()
                            .with_group_id(group_id.clone())
                            .with_group_state(s(group.state.name()))
                            .with_protocol_type(s(group.protocol_type.as_deref().unwrap_or_default()))
                            .with_protocol_data(s(&protocol))
                            .with_members(members)
                    })
                    // Un groupe inconnu est décrit comme « Dead », sans erreur.
                    .unwrap_or_else(|| DescribedGroup::default().with_group_id(group_id).with_group_state(s("Dead")))
            })
            .collect();
        DescribeGroupsResponse::default().with_groups(groups)
    }
}
