//! Outils partagés par les tests de tous les chapitres.
//!
//! Ce module contient un client Kafka minimal, volontairement indépendant du
//! code du broker : il fait son propre framing et utilise `kafka-protocol`
//! pour encoder les requêtes et décoder les réponses.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use broker::{Broker, Config};
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// Délai max d'attente d'une réponse : un test ne doit jamais rester bloqué.
pub const TIMEOUT: Duration = Duration::from_secs(5);

/// Un broker lancé dans la même process que le test, sur un port libre.
pub struct TestBroker {
    pub addr: SocketAddr,
    task: JoinHandle<Result<()>>,
}

impl Drop for TestBroker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn start_broker() -> TestBroker {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = Config { node_id: 0, host: "127.0.0.1".into(), port: addr.port() };
    let task = tokio::spawn(broker::server::serve(listener, Arc::new(Broker::new(config))));
    TestBroker { addr, task }
}

/// Un client Kafka minimal, une connexion TCP.
pub struct Client {
    stream: TcpStream,
    next_correlation_id: i32,
}

impl Client {
    pub async fn connect(addr: SocketAddr) -> Client {
        let stream = TcpStream::connect(addr).await.expect("connexion au broker impossible");
        Client { stream, next_correlation_id: 1 }
    }

    /// Envoie une requête typée et attend sa réponse, en vérifiant le correlation_id.
    pub async fn send<R: Request>(&mut self, request: R, version: i16) -> Result<R::Response> {
        let correlation_id = self.send_only(&request, version).await?;
        self.receive::<R>(correlation_id, version).await
    }

    /// Envoie une requête sans attendre la réponse (pour tester le pipelining).
    /// Renvoie le correlation_id utilisé.
    pub async fn send_only<R: Request>(&mut self, request: &R, version: i16) -> Result<i32> {
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id += 1;

        let header = RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("workshop-tests")));

        let mut buf = BytesMut::new();
        header.encode(&mut buf, R::header_version(version))?;
        request.encode(&mut buf, version)?;
        self.send_raw(&buf).await?;
        Ok(correlation_id)
    }

    /// Lit la prochaine réponse et la décode comme une réponse à `R`.
    pub async fn receive<R: Request>(&mut self, correlation_id: i32, version: i16) -> Result<R::Response> {
        let mut frame = self
            .receive_raw()
            .await?
            .context("le broker a fermé la connexion au lieu de répondre")?;
        let header = ResponseHeader::decode(&mut frame, R::Response::header_version(version))
            .context("response header illisible")?;
        if header.correlation_id != correlation_id {
            bail!("correlation_id inattendu : reçu {}, attendu {correlation_id}", header.correlation_id);
        }
        let response = R::Response::decode(&mut frame, version).context("corps de réponse illisible")?;
        if !frame.is_empty() {
            bail!("{} octets en trop à la fin de la réponse", frame.len());
        }
        Ok(response)
    }

    /// Envoie une trame brute (le préfixe de taille est ajouté ici).
    pub async fn send_raw(&mut self, payload: &[u8]) -> Result<()> {
        self.stream.write_i32(payload.len() as i32).await?;
        self.stream.write_all(payload).await?;
        Ok(())
    }

    /// Écrit des octets tels quels sur la socket, sans préfixe de taille.
    pub async fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream.write_all(bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Lit une trame brute. `Ok(None)` si le broker a fermé la connexion.
    pub async fn receive_raw(&mut self) -> Result<Option<Bytes>> {
        tokio::time::timeout(TIMEOUT, async {
            let size = match self.stream.read_i32().await {
                Ok(size) => size,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            let mut payload = vec![0u8; size as usize];
            self.stream.read_exact(&mut payload).await?;
            Ok(Some(Bytes::from(payload)))
        })
        .await
        .context("pas de réponse du broker (timeout)")?
    }

    /// Vérifie que le broker a fermé la connexion.
    pub async fn assert_closed(&mut self) {
        match self.receive_raw().await {
            Ok(None) => {}
            Ok(Some(_)) => panic!("le broker a répondu au lieu de fermer la connexion"),
            Err(e) => panic!("le broker n'a pas fermé la connexion : {e:#}"),
        }
    }
}

/// Nom lisible d'une api_key, pour les messages d'erreur.
pub fn api_name(key: i16) -> String {
    ApiKey::try_from(key).map(|k| format!("{k:?}")).unwrap_or_else(|_| key.to_string())
}
