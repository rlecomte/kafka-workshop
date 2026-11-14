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

    let config = Config {
        node_id: env_or("BROKER_NODE_ID", 0)?,
        host: env_or("BROKER_HOST", "localhost".to_string())?,
        port: env_or("BROKER_PORT", 9092)?,
    };

    let listener = TcpListener::bind(("0.0.0.0", config.port))
        .await
        .with_context(|| format!("impossible d'écouter sur le port {}", config.port))?;
    tracing::info!(?config, "broker démarré");

    broker::server::serve(listener, Arc::new(Broker::new(config))).await
}
