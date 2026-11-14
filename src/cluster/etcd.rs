//! Un client etcd minimal, qui utilise l'API v3 d'etcd en HTTP/JSON (la
//! passerelle gRPC, activée par défaut).
//!
//! Les clés et les valeurs sont encodées en base64, les entiers 64 bits sont
//! des chaînes. On peut faire les mêmes appels avec `curl` :
//!
//! ```text
//! curl -s -X POST http://localhost:2379/v3/kv/range \
//!      -d "{\"key\": \"$(printf /workshop/brokers/0 | base64)\"}"
//! ```

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct Etcd {
    http: reqwest::Client,
    url: String,
}

/// Une paire clé-valeur stockée dans etcd.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValue {
    pub key: String,
    pub value: Vec<u8>,
    /// Révision à laquelle la clé a été créée.
    pub create_revision: i64,
    /// Révision de la dernière modification de la clé.
    pub mod_revision: i64,
}

/// Une condition d'une transaction (toujours une égalité).
#[derive(Debug, Clone)]
pub enum Compare {
    /// La clé a été créée à cette révision (0 : la clé n'existe pas).
    CreateRevision(String, i64),
    /// La clé a été modifiée pour la dernière fois à cette révision.
    ModRevision(String, i64),
}

/// Une opération d'une transaction.
#[derive(Debug, Clone)]
pub enum Op {
    Put { key: String, value: Vec<u8>, lease: Option<i64> },
    Delete { key: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Put,
    Delete,
}

/// Un changement observé par un watch.
#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub kind: EventKind,
    pub kv: KeyValue,
}

/// La fin d'une plage de clés couvrant toutes les clés qui commencent par
/// `prefix` : le préfixe avec son dernier octet incrémenté.
pub fn prefix_end(prefix: &str) -> Vec<u8> {
    let mut end = prefix.as_bytes().to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return end;
        }
    }
    vec![0]
}

fn b64(bytes: &[u8]) -> String {
    BASE64.encode(bytes)
}

/// Les entiers 64 bits sont des chaînes en JSON ; les champs à zéro sont omis.
fn int(value: &Value) -> i64 {
    match value {
        Value::String(s) => s.parse().unwrap_or(0),
        Value::Number(n) => n.as_i64().unwrap_or(0),
        _ => 0,
    }
}

fn parse_kv(value: &Value) -> Result<KeyValue> {
    let decode = |field: &str| -> Result<Vec<u8>> {
        match value.get(field).and_then(Value::as_str) {
            Some(s) => BASE64.decode(s).context("base64 invalide"),
            None => Ok(Vec::new()),
        }
    };
    Ok(KeyValue {
        key: String::from_utf8(decode("key")?).context("clé non UTF-8")?,
        value: decode("value")?,
        create_revision: int(&value["create_revision"]),
        mod_revision: int(&value["mod_revision"]),
    })
}

fn op_json(op: &Op) -> Value {
    match op {
        Op::Put { key, value, lease } => {
            let mut put = json!({ "key": b64(key.as_bytes()), "value": b64(value) });
            if let Some(lease) = lease {
                put["lease"] = json!(lease.to_string());
            }
            json!({ "request_put": put })
        }
        Op::Delete { key } => json!({ "request_delete_range": { "key": b64(key.as_bytes()) } }),
    }
}

fn compare_json(compare: &Compare) -> Value {
    match compare {
        Compare::CreateRevision(key, rev) => {
            json!({ "key": b64(key.as_bytes()), "target": "CREATE", "result": "EQUAL", "create_revision": rev.to_string() })
        }
        Compare::ModRevision(key, rev) => {
            json!({ "key": b64(key.as_bytes()), "target": "MOD", "result": "EQUAL", "mod_revision": rev.to_string() })
        }
    }
}

impl Etcd {
    /// `url` : l'adresse d'etcd, par exemple `http://localhost:2379`.
    pub fn new(url: &str) -> Etcd {
        Etcd { http: reqwest::Client::new(), url: url.trim_end_matches('/').to_string() }
    }

    async fn call(&self, path: &str, body: Value) -> Result<Value> {
        let response = self
            .http
            .post(format!("{}{path}", self.url))
            .json(&body)
            .send()
            .await
            .with_context(|| format!("etcd injoignable ({})", self.url))?;
        let status = response.status();
        let value: Value = response.json().await.context("réponse etcd illisible")?;
        if !status.is_success() {
            bail!("etcd {path} : {status} {value}");
        }
        Ok(value)
    }

    /// Lit une clé.
    pub async fn get(&self, key: &str) -> Result<Option<KeyValue>> {
        let response = self.call("/v3/kv/range", json!({ "key": b64(key.as_bytes()) })).await?;
        match response.get("kvs").and_then(Value::as_array).and_then(|kvs| kvs.first()) {
            Some(kv) => Ok(Some(parse_kv(kv)?)),
            None => Ok(None),
        }
    }

    /// Lit toutes les clés qui commencent par `prefix`. Renvoie aussi la
    /// révision courante d'etcd, à partir de laquelle surveiller les
    /// changements suivants.
    pub async fn get_prefix(&self, prefix: &str) -> Result<(Vec<KeyValue>, i64)> {
        let body = json!({ "key": b64(prefix.as_bytes()), "range_end": b64(&prefix_end(prefix)) });
        let response = self.call("/v3/kv/range", body).await?;
        let revision = int(&response["header"]["revision"]);
        let kvs = match response.get("kvs").and_then(Value::as_array) {
            Some(kvs) => kvs.iter().map(parse_kv).collect::<Result<_>>()?,
            None => Vec::new(),
        };
        Ok((kvs, revision))
    }

    /// Écrit une clé, éventuellement attachée à un lease.
    pub async fn put(&self, key: &str, value: &[u8], lease: Option<i64>) -> Result<()> {
        self.call("/v3/kv/put", op_json(&Op::Put { key: key.into(), value: value.into(), lease })["request_put"].clone())
            .await?;
        Ok(())
    }

    /// Supprime une clé.
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.call("/v3/kv/deleterange", json!({ "key": b64(key.as_bytes()) })).await?;
        Ok(())
    }

    /// Exécute une transaction : si toutes les conditions sont vraies, les
    /// opérations `success` sont appliquées, sinon les opérations `failure`.
    /// Le tout de façon atomique. Renvoie `true` si les conditions étaient vraies.
    pub async fn txn(&self, compare: &[Compare], success: &[Op], failure: &[Op]) -> Result<bool> {
        let body = json!({
            "compare": compare.iter().map(compare_json).collect::<Vec<_>>(),
            "success": success.iter().map(op_json).collect::<Vec<_>>(),
            "failure": failure.iter().map(op_json).collect::<Vec<_>>(),
        });
        let response = self.call("/v3/kv/txn", body).await?;
        Ok(response.get("succeeded").and_then(Value::as_bool).unwrap_or(false))
    }

    /// Crée un lease de `ttl_secs` secondes. Les clés attachées à un lease
    /// sont supprimées quand il expire.
    pub async fn grant_lease(&self, ttl_secs: i64) -> Result<i64> {
        let response = self.call("/v3/lease/grant", json!({ "TTL": ttl_secs.to_string() })).await?;
        let id = int(&response["ID"]);
        if id == 0 {
            bail!("lease refusé : {response}");
        }
        Ok(id)
    }

    /// Prolonge un lease. Renvoie `false` s'il a déjà expiré.
    pub async fn keep_alive(&self, lease: i64) -> Result<bool> {
        let response = self.call("/v3/lease/keepalive", json!({ "ID": lease.to_string() })).await?;
        Ok(int(&response["result"]["TTL"]) > 0)
    }

    /// Révoque un lease : ses clés sont supprimées immédiatement.
    pub async fn revoke_lease(&self, lease: i64) -> Result<()> {
        self.call("/v3/lease/revoke", json!({ "ID": lease.to_string() })).await?;
        Ok(())
    }

    /// Surveille les clés qui commencent par `prefix`, à partir de la révision
    /// `start_revision`. Chaque message reçu est un lot de changements ; une
    /// erreur termine le watch.
    ///
    /// Une tâche lit la réponse HTTP (un flux d'objets JSON, un par ligne) ;
    /// elle s'arrête quand le `Receiver` est abandonné.
    pub async fn watch_prefix(&self, prefix: &str, start_revision: i64) -> Result<mpsc::Receiver<Result<Vec<WatchEvent>>>> {
        let body = json!({ "create_request": {
            "key": b64(prefix.as_bytes()),
            "range_end": b64(&prefix_end(prefix)),
            "start_revision": start_revision.to_string(),
        }});
        let response = self
            .http
            .post(format!("{}/v3/watch", self.url))
            .json(&body)
            .send()
            .await
            .with_context(|| format!("etcd injoignable ({})", self.url))?;
        if !response.status().is_success() {
            bail!("watch refusé : {}", response.status());
        }

        let (sender, receiver) = mpsc::channel(16);
        tokio::spawn(async move {
            let mut bytes = response.bytes_stream();
            let mut buffer: Vec<u8> = Vec::new();
            loop {
                let chunk = tokio::select! {
                    chunk = bytes.next() => chunk,
                    _ = sender.closed() => return,
                };
                let chunk = match chunk {
                    Some(Ok(chunk)) => chunk,
                    Some(Err(e)) => {
                        let _ = sender.send(Err(anyhow::Error::from(e).context("watch interrompu"))).await;
                        return;
                    }
                    None => {
                        let _ = sender.send(Err(anyhow::anyhow!("watch terminé par etcd"))).await;
                        return;
                    }
                };
                buffer.extend_from_slice(&chunk);
                while let Some(newline) = buffer.iter().position(|&b| b == b'\n') {
                    let line: Vec<u8> = buffer.drain(..=newline).collect();
                    if line.iter().all(u8::is_ascii_whitespace) {
                        continue;
                    }
                    let events = parse_watch_line(&line);
                    let failed = events.is_err();
                    if sender.send(events).await.is_err() || failed {
                        return;
                    }
                }
            }
        });
        Ok(receiver)
    }
}

fn parse_watch_line(line: &[u8]) -> Result<Vec<WatchEvent>> {
    let value: Value = serde_json::from_slice(line).context("message de watch illisible")?;
    if let Some(error) = value.get("error") {
        bail!("erreur de watch : {error}");
    }
    let result = &value["result"];
    if result.get("canceled").and_then(Value::as_bool) == Some(true) {
        bail!("watch annulé : {result}");
    }
    let events = match result.get("events").and_then(Value::as_array) {
        Some(events) => events,
        None => return Ok(Vec::new()),
    };
    events
        .iter()
        .map(|e| {
            let kind = if e.get("type").and_then(Value::as_str) == Some("DELETE") { EventKind::Delete } else { EventKind::Put };
            Ok(WatchEvent { kind, kv: parse_kv(&e["kv"])? })
        })
        .collect()
}
