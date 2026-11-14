//! `ApiVersions` (api_key 18) : la première requête envoyée par tout client
//! Kafka. Le broker y annonce, pour chaque API qu'il supporte, la plage de
//! versions qu'il sait traiter. Le client choisit ensuite, pour chaque API,
//! la version la plus haute supportée des deux côtés.

use kafka_protocol::ResponseError;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse};

use crate::{Broker, apis};

impl Broker {
    pub(super) fn handle_api_versions(&self, request: ApiVersionsRequest) -> ApiVersionsResponse {
        tracing::debug!(
            client = %request.client_software_name.as_str(),
            version = %request.client_software_version.as_str(),
            "ApiVersions"
        );
        ApiVersionsResponse::default()
            .with_error_code(0)
            .with_api_keys(supported_api_versions())
    }
}

/// Réponse à un `ApiVersions` dont la version n'est pas supportée.
pub(super) fn unsupported_version_response() -> ApiVersionsResponse {
    ApiVersionsResponse::default()
        .with_error_code(ResponseError::UnsupportedVersion.code())
        .with_api_keys(supported_api_versions())
}

fn supported_api_versions() -> Vec<ApiVersion> {
    apis::SUPPORTED_APIS
        .iter()
        .map(|api| {
            ApiVersion::default()
                .with_api_key(api.key as i16)
                .with_min_version(api.min_version)
                .with_max_version(api.max_version)
        })
        .collect()
}
