use std::sync::Arc;

use anyhow::Context;
use broker::{Broker, Config};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

/// Lit une variable d'environnement, avec une valeur par défaut.
fn env_or<T: std::str::FromStr>(name: &str, default: T) -> anyhow::Result<T>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(v) => v.parse().map_err(|e| anyhow::anyhow!("{name}={v} invalide : {e}")),
        Err(_) => Ok(default),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let defaults = Config::default();
    let config = Config {
        node_id: env_or("BROKER_NODE_ID", defaults.node_id)?,
        host: env_or("BROKER_HOST", defaults.host)?,
        port: env_or("BROKER_PORT", defaults.port)?,
        cluster_id: env_or("BROKER_CLUSTER_ID", defaults.cluster_id)?,
        default_num_partitions: env_or("BROKER_NUM_PARTITIONS", defaults.default_num_partitions)?,
        auto_create_topics: env_or("BROKER_AUTO_CREATE_TOPICS", defaults.auto_create_topics)?,
    };

    let listener = TcpListener::bind(("0.0.0.0", config.port))
        .await
        .with_context(|| format!("impossible d'écouter sur le port {}", config.port))?;
    tracing::info!(?config, "broker démarré");

    broker::server::serve(listener, Arc::new(Broker::new(config))).await
}
