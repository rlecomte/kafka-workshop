//! Un client Kafka minimal, utilisé par les followers pour envoyer des
//! `Fetch` à leur leader.

use std::net::SocketAddr;

use anyhow::{Context, Result, bail};
use bytes::BytesMut;
use kafka_protocol::messages::{RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use tokio::net::TcpStream;

use crate::server::{read_frame, write_frame};

pub struct BrokerClient {
    stream: TcpStream,
    client_id: StrBytes,
    next_correlation_id: i32,
}

impl BrokerClient {
    pub async fn connect(addr: SocketAddr, client_id: String) -> Result<BrokerClient> {
        let stream = TcpStream::connect(addr).await.with_context(|| format!("connexion à {addr} impossible"))?;
        stream.set_nodelay(true)?;
        Ok(BrokerClient { stream, client_id: StrBytes::from_string(client_id), next_correlation_id: 0 })
    }

    /// Envoie une requête et attend sa réponse.
    pub async fn send<R: Request>(&mut self, request: &R, version: i16) -> Result<R::Response> {
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id += 1;
        let header = RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(self.client_id.clone()));
        let mut buf = BytesMut::new();
        header.encode(&mut buf, R::header_version(version))?;
        request.encode(&mut buf, version)?;
        write_frame(&mut self.stream, &buf).await?;

        let mut frame = read_frame(&mut self.stream).await?.context("connexion fermée par le broker")?;
        let header = ResponseHeader::decode(&mut frame, R::Response::header_version(version))?;
        if header.correlation_id != correlation_id {
            bail!("correlation_id inattendu : {} au lieu de {correlation_id}", header.correlation_id);
        }
        Ok(R::Response::decode(&mut frame, version)?)
    }
}
