//! Outils partagés par les tests de tous les chapitres.
//!
//! Ce module contient un client Kafka minimal, volontairement indépendant du
//! code du broker : il fait son propre framing et utilise `kafka-protocol`
//! pour encoder les requêtes et décoder les réponses.
#![allow(dead_code)]

pub mod groups;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use broker::{Broker, Config};
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, BrokerId, RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// Délai max d'attente d'une réponse : un test ne doit jamais rester bloqué.
pub const TIMEOUT: Duration = Duration::from_secs(5);

/// Un broker lancé dans la même process que le test, sur un port libre.
pub struct TestBroker {
    pub addr: SocketAddr,
    /// Le répertoire de données, supprimé à la fin du test.
    pub data_dir: Arc<tempfile::TempDir>,
    pub config: Config,
    task: JoinHandle<Result<()>>,
}

impl TestBroker {
    /// Arrête le broker, puis en relance un sur le même répertoire de données.
    pub async fn restart(self) -> TestBroker {
        let config = self.config.clone();
        let data_dir = self.data_dir.clone();
        self.stop().await;
        start_broker_in(config, data_dir).await
    }

    /// Arrête le broker (les données restent sur disque).
    pub async fn stop(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
        // Laisse aux tâches annulées le temps de se terminer.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

impl Drop for TestBroker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn start_broker() -> TestBroker {
    start_broker_with(Config::default()).await
}

/// Lance un broker avec une configuration donnée (host, port et data_dir
/// sont écrasés).
pub async fn start_broker_with(config: Config) -> TestBroker {
    start_broker_in(config, Arc::new(tempfile::tempdir().unwrap())).await
}

/// Lance un broker qui stocke ses données dans `data_dir`.
pub async fn start_broker_in(config: Config, data_dir: Arc<tempfile::TempDir>) -> TestBroker {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config =
        Config { host: "127.0.0.1".into(), port: addr.port(), data_dir: data_dir.path().to_path_buf(), ..config };
    let broker = Broker::new(config.clone()).expect("démarrage du broker impossible");
    let task = tokio::spawn(broker::server::serve(listener, Arc::new(broker)));
    TestBroker { addr, data_dir, config, task }
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

// ---------------------------------------------------------------------------
// Topics et messages
// ---------------------------------------------------------------------------

use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::produce_response::PartitionProduceResponse;
use kafka_protocol::messages::{CreateTopicsRequest, ProduceRequest, TopicName};
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

pub fn topic_name(name: &str) -> TopicName {
    TopicName(StrBytes::from_string(name.to_string()))
}

/// Crée un topic et vérifie que la création a réussi.
pub async fn create_topic(client: &mut Client, name: &str, partitions: i32) {
    let request = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default().with_name(topic_name(name)).with_num_partitions(partitions).with_replication_factor(-1),
    ]);
    let response = client.send(request, 7).await.unwrap();
    assert_eq!(response.topics[0].error_code, 0, "création du topic {name}");
}

/// Un record avec une valeur (et une clé optionnelle).
pub fn record(key: Option<&str>, value: &str) -> Record {
    Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: -1,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: 1_700_000_000_000,
        key: key.map(|k| Bytes::from(k.to_string())),
        value: Some(Bytes::from(value.to_string())),
        headers: IndexMap::new(),
    }
}

/// Encode des records dans un batch v2, comme le ferait un producer.
pub fn encode_batch(mut records: Vec<Record>, compression: Compression) -> Bytes {
    // L'encodeur regroupe dans un même batch les records dont
    // `offset - sequence` est constant.
    for (i, r) in records.iter_mut().enumerate() {
        r.offset = i as i64;
        r.sequence = i as i32;
    }
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(&mut buf, &records, &RecordEncodeOptions { version: 2, compression }).unwrap();
    buf.freeze()
}

/// Un batch non compressé contenant ces valeurs.
pub fn batch(values: &[&str]) -> Bytes {
    encode_batch(values.iter().map(|v| record(None, v)).collect(), Compression::None)
}

/// Décode des batches (réponse Fetch, fichier de log...) en records.
pub fn decode_records(mut bytes: Bytes) -> Vec<Record> {
    let mut records = Vec::new();
    while !bytes.is_empty() {
        let set = RecordBatchDecoder::decode(&mut bytes).expect("batch illisible");
        records.extend(set.records);
    }
    records
}

/// Les valeurs des records, en texte.
pub fn values(records: &[Record]) -> Vec<String> {
    records.iter().map(|r| String::from_utf8_lossy(r.value.as_deref().unwrap_or_default()).into_owned()).collect()
}

pub fn produce_request(topic: &str, partition: i32, records: Bytes, acks: i16) -> ProduceRequest {
    ProduceRequest::default().with_acks(acks).with_timeout_ms(5000).with_topic_data(vec![
        TopicProduceData::default().with_name(topic_name(topic)).with_partition_data(vec![
            PartitionProduceData::default().with_index(partition).with_records(Some(records)),
        ]),
    ])
}

/// Envoie un Produce (acks = -1) et renvoie la réponse de la partition.
pub async fn produce(client: &mut Client, topic: &str, partition: i32, records: Bytes) -> PartitionProduceResponse {
    let response = client.send(produce_request(topic, partition, records, -1), 9).await.unwrap();
    response.responses[0].partition_responses[0].clone()
}

// ---------------------------------------------------------------------------
// Fetch et ListOffsets
// ---------------------------------------------------------------------------

use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::fetch_response::PartitionData;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::list_offsets_response::ListOffsetsPartitionResponse;
use kafka_protocol::messages::{FetchRequest, ListOffsetsRequest};

pub const FETCH_VERSION: i16 = 12;

/// Une requête Fetch « consumer » pour une partition, sans attente.
pub fn fetch_request(topic: &str, partition: i32, offset: i64, max_bytes: i32) -> FetchRequest {
    FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(0)
        .with_max_bytes(max_bytes)
        .with_session_id(0)
        .with_session_epoch(-1)
        .with_topics(vec![FetchTopic::default().with_topic(topic_name(topic)).with_partitions(vec![
            FetchPartition::default()
                .with_partition(partition)
                .with_fetch_offset(offset)
                .with_partition_max_bytes(max_bytes),
        ])])
}

/// Lit une partition à partir de `offset` (sans attente) et renvoie les
/// données de la partition.
pub async fn fetch(client: &mut Client, topic: &str, partition: i32, offset: i64) -> PartitionData {
    let response = client.send(fetch_request(topic, partition, offset, 1024 * 1024), FETCH_VERSION).await.unwrap();
    response.responses[0].partitions[0].clone()
}

/// Les records renvoyés par un Fetch.
pub fn fetched_records(data: &PartitionData) -> Vec<Record> {
    decode_records(data.records.clone().unwrap_or_default())
}

pub async fn list_offset(client: &mut Client, topic: &str, partition: i32, timestamp: i64) -> ListOffsetsPartitionResponse {
    let request = ListOffsetsRequest::default().with_replica_id(BrokerId(-1)).with_topics(vec![
        ListOffsetsTopic::default().with_name(topic_name(topic)).with_partitions(vec![
            ListOffsetsPartition::default().with_partition_index(partition).with_timestamp(timestamp),
        ]),
    ]);
    let response = client.send(request, 7).await.unwrap();
    response.topics[0].partitions[0].clone()
}
