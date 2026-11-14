//! Chapitre 0 : réseau, framing et ApiVersions.
//!
//! Lancer avec : `cargo test --test ch00`

mod common;

use bytes::{BufMut, BytesMut};
use common::{Client, start_broker};
use kafka_protocol::ResponseError;
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, SaslHandshakeRequest};
use kafka_protocol::protocol::{Decodable, StrBytes};

fn api_versions_request() -> ApiVersionsRequest {
    ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("workshop-tests"))
        .with_client_software_version(StrBytes::from_static_str("0.1.0"))
}

/// Vérifie que la réponse annonce ApiVersions en v0..=4 au moins.
fn assert_announces_api_versions(response: &ApiVersionsResponse) {
    let entry = response
        .api_keys
        .iter()
        .find(|api| api.api_key == ApiKey::ApiVersions as i16)
        .expect("ApiVersions doit figurer dans la liste des API annoncées");
    assert_eq!(entry.min_version, 0, "ApiVersions doit être supportée à partir de la v0");
    assert!(entry.max_version >= 3, "ApiVersions doit être supportée au moins jusqu'à la v3");
}

#[tokio::test]
async fn api_versions_v0() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let response = client.send(api_versions_request(), 0).await.unwrap();

    assert_eq!(response.error_code, 0);
    assert_announces_api_versions(&response);
}

/// La v3 est la première version « flexible » d'ApiVersions : les chaînes
/// sont encodées en compact strings et il y a des tagged fields.
#[tokio::test]
async fn api_versions_v3() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let response = client.send(api_versions_request(), 3).await.unwrap();

    assert_eq!(response.error_code, 0);
    assert_announces_api_versions(&response);
}

/// Chaque API annoncée doit avoir une plage de versions cohérente.
#[tokio::test]
async fn announced_versions_are_consistent() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let response = client.send(api_versions_request(), 3).await.unwrap();

    for api in &response.api_keys {
        let name = common::api_name(api.api_key);
        assert!(ApiKey::try_from(api.api_key).is_ok(), "api_key inconnue annoncée : {name}");
        assert!(api.min_version <= api.max_version, "{name} : min_version > max_version");
        let valid = ApiKey::try_from(api.api_key).unwrap().valid_versions();
        assert!(
            api.min_version >= valid.min && api.max_version <= valid.max,
            "{name} : versions {}..={} hors de la plage du protocole {valid}",
            api.min_version,
            api.max_version
        );
    }
}

/// Un client plus récent que le broker peut envoyer ApiVersions dans une
/// version inconnue. Le broker doit répondre UNSUPPORTED_VERSION, en v0,
/// avec la liste des API qu'il supporte.
#[tokio::test]
async fn api_versions_unsupported_version() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    // Le corps est encodé en v4 mais le header annonce une version 42.
    let mut buf = BytesMut::new();
    buf.put_i16(ApiKey::ApiVersions as i16); // api_key
    buf.put_i16(42); // api_version
    buf.put_i32(7); // correlation_id
    buf.put_i16(-1); // client_id (nullable string, null)
    buf.put_u8(0); // tagged fields du header v2 (aucun)
    buf.put_u8(0); // corps : client_software_name (compact string vide)
    buf.put_u8(0); // corps : client_software_version (compact string vide)
    buf.put_u8(0); // corps : tagged fields
    client.send_raw(&buf).await.unwrap();

    let mut frame = client.receive_raw().await.unwrap().expect("le broker doit répondre");
    // Response header v0 : juste le correlation_id.
    let correlation_id = i32::from_be_bytes(frame.split_to(4)[..].try_into().unwrap());
    assert_eq!(correlation_id, 7);
    let response = ApiVersionsResponse::decode(&mut frame, 0).expect("le corps doit être encodé en v0");
    assert_eq!(response.error_code, ResponseError::UnsupportedVersion.code());
    assert_announces_api_versions(&response);
}

/// Les clients envoient souvent plusieurs requêtes sans attendre les
/// réponses. Le broker doit répondre à toutes, dans l'ordre.
#[tokio::test]
async fn pipelined_requests_are_answered_in_order() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let versions = [0, 3, 1, 2, 3];
    let mut correlation_ids = Vec::new();
    for &version in &versions {
        correlation_ids.push(client.send_only(&api_versions_request(), version).await.unwrap());
    }
    for (&version, &correlation_id) in versions.iter().zip(&correlation_ids) {
        let response = client
            .receive::<ApiVersionsRequest>(correlation_id, version)
            .await
            .unwrap();
        assert_eq!(response.error_code, 0);
    }
}

/// Une requête peut arriver en plusieurs morceaux sur la socket : le broker
/// doit attendre d'avoir la trame complète.
#[tokio::test]
async fn request_split_across_several_writes() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;

    let mut buf = BytesMut::new();
    buf.put_i16(ApiKey::ApiVersions as i16);
    buf.put_i16(0);
    buf.put_i32(99);
    buf.put_i16(-1);
    let mut frame = (buf.len() as i32).to_be_bytes().to_vec();
    frame.extend_from_slice(&buf);

    for byte in frame {
        client.write_bytes(&[byte]).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let response = client.receive::<ApiVersionsRequest>(99, 0).await.unwrap();
    assert_eq!(response.error_code, 0);
}

/// Plusieurs clients en parallèle : une connexion ne doit pas bloquer les autres.
#[tokio::test]
async fn concurrent_connections() {
    let broker = start_broker().await;

    // Un client se connecte mais n'envoie qu'une trame incomplète.
    let mut idle = Client::connect(broker.addr).await;
    idle.write_bytes(&[0, 0, 0, 100]).await.unwrap();

    let mut tasks = Vec::new();
    for _ in 0..10 {
        let addr = broker.addr;
        tasks.push(tokio::spawn(async move {
            let mut client = Client::connect(addr).await;
            for _ in 0..10 {
                let response = client.send(api_versions_request(), 3).await.unwrap();
                assert_eq!(response.error_code, 0);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

/// Une API non implémentée ferme la connexion, mais le broker continue de
/// servir les autres clients. (SaslHandshake : la sécurité est hors du
/// périmètre du workshop, cette API ne sera jamais implémentée.)
#[tokio::test]
async fn unsupported_api_closes_connection_only() {
    let broker = start_broker().await;

    let mut client = Client::connect(broker.addr).await;
    client.send_only(&SaslHandshakeRequest::default(), 1).await.unwrap();
    client.assert_closed().await;

    let mut other = Client::connect(broker.addr).await;
    let response = other.send(api_versions_request(), 3).await.unwrap();
    assert_eq!(response.error_code, 0);
}

/// Une trame avec une taille aberrante ferme la connexion sans faire tomber le broker.
#[tokio::test]
async fn invalid_frame_size_closes_connection() {
    let broker = start_broker().await;

    let mut client = Client::connect(broker.addr).await;
    client.write_bytes(&(-5i32).to_be_bytes()).await.unwrap();
    client.assert_closed().await;

    let mut other = Client::connect(broker.addr).await;
    let response = other.send(api_versions_request(), 3).await.unwrap();
    assert_eq!(response.error_code, 0);
}
