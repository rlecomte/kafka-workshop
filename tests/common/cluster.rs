//! Un cluster de brokers lancés dans le processus de test, coordonnés par
//! etcd (`docker compose up -d etcd`).
//!
//! Chaque test utilise un préfixe etcd unique : les tests sont indépendants.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use broker::Config;
use broker::cluster::ClusterConfig;
use kafka_protocol::messages::{MetadataRequest, MetadataResponse};

use super::{Client, TestBroker, start_broker_in};

/// L'adresse d'etcd pour les tests (`WORKSHOP_ETCD_URL`, ou localhost).
pub async fn etcd_url() -> String {
    let url = std::env::var("WORKSHOP_ETCD_URL").unwrap_or_else(|_| "http://localhost:2379".into());
    let healthy = reqwest::Client::new()
        .get(format!("{url}/health"))
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .is_ok_and(|r| r.status().is_success());
    assert!(healthy, "etcd injoignable sur {url} : lancez `docker compose up -d etcd`");
    url
}

/// Attend qu'une condition soit vraie, au plus `timeout`.
pub async fn wait_until<F, Fut>(timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if condition().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub struct TestCluster {
    pub prefix: String,
    pub brokers: Vec<Option<TestBroker>>,
    configs: Vec<Config>,
    data_dirs: Vec<Arc<tempfile::TempDir>>,
}

impl TestCluster {
    /// Lance `n` brokers (identifiants 0 à n-1) et attend qu'ils se voient
    /// tous et qu'un controller soit élu.
    pub async fn start(n: usize) -> TestCluster {
        Self::start_with(n, |_| {}).await
    }

    pub async fn start_with(n: usize, tweak: impl Fn(&mut Config)) -> TestCluster {
        let url = etcd_url().await;
        let prefix = format!("/workshop-test-{}", uuid::Uuid::new_v4());
        let mut cluster = TestCluster { prefix: prefix.clone(), brokers: Vec::new(), configs: Vec::new(), data_dirs: Vec::new() };
        for id in 0..n {
            let mut config = Config {
                node_id: id as i32,
                cluster: Some(ClusterConfig { etcd_url: url.clone(), prefix: prefix.clone(), lease_ttl: Duration::from_secs(2) }),
                ..super::groups::group_config()
            };
            tweak(&mut config);
            let data_dir = Arc::new(tempfile::tempdir().unwrap());
            cluster.brokers.push(Some(start_broker_in(config.clone(), data_dir.clone()).await));
            cluster.configs.push(config);
            cluster.data_dirs.push(data_dir);
        }
        let ready = wait_until(Duration::from_secs(10), || async {
            let m = cluster.metadata().await;
            m.brokers.len() == n && m.controller_id.0 >= 0
        })
        .await;
        assert!(ready, "le cluster de {n} brokers ne s'est pas formé");
        cluster
    }

    pub fn addr(&self, id: usize) -> SocketAddr {
        self.brokers[id].as_ref().expect("broker arrêté").addr
    }

    /// L'adresse d'un broker vivant.
    pub fn any_addr(&self) -> SocketAddr {
        self.brokers.iter().flatten().next().expect("aucun broker vivant").addr
    }

    pub async fn client(&self, id: usize) -> Client {
        Client::connect(self.addr(id)).await
    }

    /// Les métadonnées de tous les topics, vues par un broker vivant.
    pub async fn metadata(&self) -> MetadataResponse {
        let mut client = Client::connect(self.any_addr()).await;
        client.send(MetadataRequest::default().with_topics(None), 12).await.unwrap()
    }

    pub async fn metadata_from(&self, id: usize) -> MetadataResponse {
        let mut client = self.client(id).await;
        client.send(MetadataRequest::default().with_topics(None), 12).await.unwrap()
    }

    pub async fn controller(&self) -> i32 {
        self.metadata().await.controller_id.0
    }

    /// Arrête brutalement un broker (son lease expirera après 2 s).
    pub async fn kill(&mut self, id: usize) {
        if let Some(broker) = self.brokers[id].take() {
            broker.stop().await;
        }
    }

    /// Relance un broker arrêté, avec ses données.
    pub async fn restart(&mut self, id: usize) {
        assert!(self.brokers[id].is_none(), "le broker {id} tourne déjà");
        self.brokers[id] = Some(start_broker_in(self.configs[id].clone(), self.data_dirs[id].clone()).await);
    }

    /// Le répertoire de données d'un broker.
    pub fn data_dir(&self, id: usize) -> &std::path::Path {
        self.data_dirs[id].path()
    }

    /// Attend que tous les brokers vivants voient `n` brokers.
    pub async fn wait_for_brokers(&self, n: usize) -> bool {
        wait_until(Duration::from_secs(10), || async {
            let mut ok = true;
            for id in 0..self.brokers.len() {
                if self.brokers[id].is_some() {
                    ok &= self.metadata_from(id).await.brokers.len() == n;
                }
            }
            ok
        })
        .await
    }
}
