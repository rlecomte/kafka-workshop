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

use anyhow::Result;
use bytes::Bytes;

use crate::Broker;

impl Broker {
    /// Traite une trame de requête et renvoie le contenu de la trame de réponse.
    ///
    /// Renvoyer une erreur provoque la fermeture de la connexion : c'est ce
    /// que fait Kafka quand il reçoit une requête qu'il ne sait pas décoder.
    ///
    /// Étapes :
    /// 1. décoder le header : `kafka_protocol::protocol::decode_request_header_from_buffer` ;
    /// 2. convertir `request_api_key` en `kafka_protocol::messages::ApiKey` (`ApiKey::try_from`) ;
    /// 3. vérifier la version avec `crate::apis::is_supported`. Si elle n'est
    ///    pas supportée : pour `ApiVersions`, répondre en v0 avec l'erreur
    ///    UNSUPPORTED_VERSION (voir `api_versions.rs`) ; sinon, renvoyer une erreur ;
    /// 4. décoder le corps : `kafka_protocol::messages::RequestKind::decode` ;
    /// 5. appeler le handler correspondant (`self.handle_api_versions`) ;
    /// 6. encoder un `ResponseHeader` (même `correlation_id`) dans la version
    ///    donnée par `ApiKey::response_header_version`, puis le corps de la
    ///    réponse (`ResponseKind::encode`) dans la version de la requête.
    pub async fn handle_request(&self, frame: Bytes) -> Result<Bytes> {
        let _ = frame;
        todo!("ch.0 : décoder, dispatcher, encoder")
    }
}
