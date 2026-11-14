//! Les API que le broker sait traiter, avec la plage de versions supportée.
//!
//! Cette liste est renvoyée telle quelle dans la réponse `ApiVersions`.
//! Ajoutez-y une ligne à chaque fois que vous implémentez une nouvelle API.

use kafka_protocol::messages::ApiKey;

/// Une API supportée et sa plage de versions (bornes incluses).
#[derive(Debug, Clone, Copy)]
pub struct SupportedApi {
    pub key: ApiKey,
    pub min_version: i16,
    pub max_version: i16,
}

pub const SUPPORTED_APIS: &[SupportedApi] = &[
    // ch.0
    SupportedApi { key: ApiKey::ApiVersions, min_version: 0, max_version: 4 },
    // ch.1
    SupportedApi { key: ApiKey::Metadata, min_version: 1, max_version: 13 },
    SupportedApi { key: ApiKey::CreateTopics, min_version: 2, max_version: 7 },
    SupportedApi { key: ApiKey::DescribeConfigs, min_version: 1, max_version: 4 },
    // ch.2 : à compléter
];

/// Renvoie la plage supportée pour `key`, ou `None` si l'API n'est pas implémentée.
pub fn supported(key: ApiKey) -> Option<&'static SupportedApi> {
    SUPPORTED_APIS.iter().find(|api| api.key == key)
}

/// Indique si le broker supporte `key` dans la version `version`.
pub fn is_supported(key: ApiKey, version: i16) -> bool {
    supported(key).is_some_and(|api| (api.min_version..=api.max_version).contains(&version))
}
