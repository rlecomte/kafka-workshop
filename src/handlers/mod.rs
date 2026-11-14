//! Décodage des requêtes, dispatch vers le bon handler, encodage des réponses.
//!
//! Le contenu d'une trame de requête est :
//!
//! ```text
//! +----------------------------------+-----------------------------+
//! | request header (v0, v1 ou v2)    | corps de la requête         |
//! | api_key, api_version,            | (dépend de api_key et de    |
//! | correlation_id, client_id, ...   |  api_version)               |
//! +----------------------------------+-----------------------------+
//! ```
//!
//! Celui d'une trame de réponse :
//!
//! ```text
//! +----------------------------------+-----------------------------+
//! | response header (v0 ou v1)       | corps de la réponse         |
//! | correlation_id, ...              | (même api_version que la    |
//! |                                  |  requête)                   |
//! +----------------------------------+-----------------------------+
//! ```

mod api_versions;
mod create_topics;
mod describe_configs;
mod init_producer_id;
mod metadata;
mod produce;

use anyhow::{Context, Result, bail};
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, RequestHeader, RequestKind, ResponseHeader, ResponseKind,
};
use kafka_protocol::protocol::{Encodable, decode_request_header_from_buffer};

use crate::{Broker, apis};

impl Broker {
    /// Traite une trame de requête et renvoie le contenu de la trame de réponse,
    /// ou `None` si la requête n'appelle pas de réponse (Produce avec `acks = 0`).
    ///
    /// Renvoyer une erreur provoque la fermeture de la connexion : c'est ce
    /// que fait Kafka quand il reçoit une requête qu'il ne sait pas décoder.
    pub async fn handle_request(&self, mut frame: Bytes) -> Result<Option<Bytes>> {
        let header = decode_request_header_from_buffer(&mut frame)
            .context("request header illisible")?;
        let api_key = ApiKey::try_from(header.request_api_key)
            .map_err(|_| anyhow::anyhow!("api_key inconnue : {}", header.request_api_key))?;
        let version = header.request_api_version;
        tracing::debug!(?api_key, version, correlation_id = header.correlation_id, client_id = ?header.client_id, "requête reçue");

        if !apis::is_supported(api_key, version) {
            // Cas particulier : un client récent peut envoyer ApiVersions dans
            // une version plus haute que celles qu'on connaît. On ne sait pas
            // décoder son corps, mais on doit quand même répondre (en v0)
            // UNSUPPORTED_VERSION avec la liste de nos API, pour que le client
            // puisse se rabattre sur une version commune.
            if api_key == ApiKey::ApiVersions {
                let response = api_versions::unsupported_version_response();
                return encode_response(&header, ResponseKind::ApiVersions(response), 0).map(Some);
            }
            bail!("API non supportée : {api_key:?} v{version}");
        }

        let request = RequestKind::decode(api_key, &mut frame, version)
            .with_context(|| format!("corps de requête {api_key:?} v{version} illisible"))?;

        let response = match request {
            RequestKind::ApiVersions(req) => ResponseKind::ApiVersions(self.handle_api_versions(req)),
            RequestKind::Metadata(req) => ResponseKind::Metadata(self.handle_metadata(req)),
            RequestKind::CreateTopics(req) => ResponseKind::CreateTopics(self.handle_create_topics(req)),
            RequestKind::DescribeConfigs(req) => ResponseKind::DescribeConfigs(self.handle_describe_configs(req)),
            RequestKind::Produce(req) => match self.handle_produce(req).await {
                Some(response) => ResponseKind::Produce(response),
                None => return Ok(None),
            },
            RequestKind::InitProducerId(req) => ResponseKind::InitProducerId(self.handle_init_producer_id(req)),
            other => bail!("pas de handler pour {other:?}"),
        };

        encode_response(&header, response, version).map(Some)
    }
}

/// Encode le response header (avec le même `correlation_id` que la requête)
/// suivi du corps de la réponse, encodé dans la version `version`.
fn encode_response(request: &RequestHeader, response: ResponseKind, version: i16) -> Result<Bytes> {
    let api_key = ApiKey::try_from(request.request_api_key).expect("déjà validée");
    let header = ResponseHeader::default().with_correlation_id(request.correlation_id);

    let mut buf = BytesMut::new();
    header.encode(&mut buf, api_key.response_header_version(version))?;
    response.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

