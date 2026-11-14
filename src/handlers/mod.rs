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
mod describe_groups;
mod fetch;
mod find_coordinator;
mod groups;
mod init_producer_id;
mod list_offsets;
mod metadata;
mod offsets;
mod produce;

use std::net::SocketAddr;

use anyhow::{Context, Result, bail};
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, RequestHeader, RequestKind, ResponseHeader, ResponseKind,
};
use kafka_protocol::protocol::{Encodable, decode_request_header_from_buffer};

use crate::{Broker, apis};

/// Informations sur la requête utiles à certains handlers.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// Version de l'API utilisée par la requête.
    pub version: i16,
    pub client_id: String,
    /// Adresse du client, telle que l'affiche Kafka (`/127.0.0.1`).
    pub client_host: String,
}

impl Broker {
    /// Traite une trame de requête et renvoie le contenu de la trame de réponse,
    /// ou `None` si la requête n'appelle pas de réponse (Produce avec `acks = 0`).
    ///
    /// Renvoyer une erreur provoque la fermeture de la connexion : c'est ce
    /// que fait Kafka quand il reçoit une requête qu'il ne sait pas décoder.
    pub async fn handle_request(&self, mut frame: Bytes, peer: SocketAddr) -> Result<Option<Bytes>> {
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

        let ctx = RequestContext {
            version,
            client_id: header.client_id.as_deref().unwrap_or_default().to_string(),
            client_host: format!("/{}", peer.ip()),
        };
        let request = RequestKind::decode(api_key, &mut frame, version)
            .with_context(|| format!("corps de requête {api_key:?} v{version} illisible"))?;

        let response = match request {
            RequestKind::ApiVersions(req) => ResponseKind::ApiVersions(self.handle_api_versions(req)),
            RequestKind::Metadata(req) => ResponseKind::Metadata(self.handle_metadata(req).await),
            RequestKind::CreateTopics(req) => ResponseKind::CreateTopics(self.handle_create_topics(req).await),
            RequestKind::DescribeConfigs(req) => ResponseKind::DescribeConfigs(self.handle_describe_configs(req)),
            RequestKind::Produce(req) => match self.handle_produce(req).await {
                Some(response) => ResponseKind::Produce(response),
                None => return Ok(None),
            },
            RequestKind::InitProducerId(req) => ResponseKind::InitProducerId(self.handle_init_producer_id(req)),
            RequestKind::Fetch(req) => ResponseKind::Fetch(self.handle_fetch(req).await),
            RequestKind::ListOffsets(req) => ResponseKind::ListOffsets(self.handle_list_offsets(req)),
            RequestKind::FindCoordinator(req) => ResponseKind::FindCoordinator(self.handle_find_coordinator(req, &ctx).await),
            RequestKind::JoinGroup(req) => ResponseKind::JoinGroup(self.handle_join_group(req, &ctx).await),
            RequestKind::SyncGroup(req) => ResponseKind::SyncGroup(self.handle_sync_group(req).await),
            RequestKind::Heartbeat(req) => ResponseKind::Heartbeat(self.handle_heartbeat(req)),
            RequestKind::LeaveGroup(req) => ResponseKind::LeaveGroup(self.handle_leave_group(req, &ctx)),
            RequestKind::OffsetCommit(req) => ResponseKind::OffsetCommit(self.handle_offset_commit(req).await),
            RequestKind::OffsetFetch(req) => ResponseKind::OffsetFetch(self.handle_offset_fetch(req, &ctx)),
            RequestKind::ListGroups(req) => ResponseKind::ListGroups(self.handle_list_groups(req)),
            RequestKind::DescribeGroups(req) => ResponseKind::DescribeGroups(self.handle_describe_groups(req)),
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

