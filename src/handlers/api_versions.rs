//! `ApiVersions` (api_key 18) : la première requête envoyée par tout client
//! Kafka. Le broker y annonce, pour chaque API qu'il supporte, la plage de
//! versions qu'il sait traiter. Le client choisit ensuite, pour chaque API,
//! la version la plus haute supportée des deux côtés.

#![allow(dead_code)]

use kafka_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse};

use crate::Broker;

impl Broker {
    /// Construit la réponse à partir de `crate::apis::SUPPORTED_APIS`.
    ///
    /// Indice : chaque entrée de `ApiVersionsResponse::api_keys` est un
    /// `kafka_protocol::messages::api_versions_response::ApiVersion`. Les
    /// types générés se construisent avec `Default::default()` et des
    /// méthodes `with_xxx(...)`.
    pub(super) fn handle_api_versions(&self, request: ApiVersionsRequest) -> ApiVersionsResponse {
        let _ = request;
        todo!("ch.0 : réponse ApiVersions")
    }
}

/// Réponse à un `ApiVersions` dont la version n'est pas supportée : code
/// d'erreur `UNSUPPORTED_VERSION` et liste des API supportées.
///
/// Indice : `kafka_protocol::ResponseError::UnsupportedVersion.code()`.
pub(super) fn unsupported_version_response() -> ApiVersionsResponse {
    todo!("ch.0 : réponse UNSUPPORTED_VERSION")
}
