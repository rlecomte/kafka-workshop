//! Chapitre 4 : segments, index, recovery et rétention.
//!
//! Lancer avec : `cargo test --test ch04`

mod common;

use std::path::Path;
use std::time::Duration;

use broker::log::{Log, LogConfig, ReadError};
use broker::record_batch::{RecordBatchHeader, parse_batches};
use common::*;
use kafka_protocol::records::Compression;

/// Une config de log avec de petits segments, pour en avoir plusieurs.
fn small_segments() -> LogConfig {
    LogConfig { segment_bytes: 1000, index_interval_bytes: 200, retention_ms: None, retention_bytes: None }
}

fn append(log: &mut Log, values: &[&str]) -> i64 {
    log.append(parse_batches(batch(values)).unwrap()).unwrap()
}

fn append_at(log: &mut Log, timestamp: i64, value: &str) -> i64 {
    let mut r = record(None, value);
    r.timestamp = timestamp;
    log.append(parse_batches(encode_batch(vec![r], Compression::None)).unwrap()).unwrap()
}

/// Remplit un log avec `n` batches d'un record de ~100 octets.
fn fill(log: &mut Log, n: usize) {
    for i in 0..n {
        append(log, &[format!("{i:03}-{}", "x".repeat(100)).as_str()]);
    }
}

fn read_all(log: &Log, from: i64) -> Vec<i64> {
    let mut offsets = Vec::new();
    let mut offset = from;
    loop {
        let bytes = log.read(offset, 1024 * 1024, i64::MAX).unwrap();
        if bytes.is_empty() {
            return offsets;
        }
        for r in decode_records(bytes) {
            if r.offset >= offset {
                offsets.push(r.offset);
            }
        }
        offset = *offsets.last().unwrap() + 1;
    }
}

fn files(dir: &Path, extension: &str) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(extension))
        .collect();
    names.sort();
    names
}

#[test]
fn log_rolls_new_segments() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = Log::open(dir.path(), small_segments()).unwrap();

    fill(&mut log, 30);

    let segments = files(dir.path(), ".log");
    assert!(segments.len() > 3, "des segments de 1000 octets doivent être créés : {segments:?}");
    assert_eq!(segments[0], "00000000000000000000.log");
    // Chaque segment est nommé d'après l'offset de son premier batch.
    for name in &segments {
        let base: i64 = name.trim_end_matches(".log").parse().unwrap();
        let content = std::fs::read(dir.path().join(name)).unwrap();
        assert!(content.len() <= 1000, "{name} dépasse segment_bytes : {} octets", content.len());
        let header = RecordBatchHeader::parse(&content).unwrap();
        assert_eq!(header.base_offset, base, "{name} doit commencer par le batch d'offset {base}");
    }
    assert_eq!(read_all(&log, 0), (0..30).collect::<Vec<_>>(), "tous les offsets sont lisibles");
}

#[test]
fn reading_any_offset_finds_its_batch() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = Log::open(dir.path(), small_segments()).unwrap();
    for i in 0..20 {
        append(&mut log, &[format!("{i}-a").as_str(), format!("{i}-b").as_str(), format!("{i}-c").as_str()]);
    }

    for offset in 0..60 {
        let records = decode_records(log.read(offset, 1, i64::MAX).unwrap());
        let found: Vec<i64> = records.iter().map(|r| r.offset).collect();
        assert!(found.contains(&offset), "lecture à l'offset {offset} : batch {found:?}");
        assert_eq!(found.len(), 3, "un seul batch (max_bytes = 1)");
    }
}

/// L'index contient une entrée environ tous les index_interval_bytes octets,
/// et chaque entrée pointe sur le début d'un batch.
#[test]
fn index_points_to_batch_starts() {
    let dir = tempfile::tempdir().unwrap();
    let config = LogConfig { segment_bytes: 100_000, index_interval_bytes: 300, ..small_segments() };
    let mut log = Log::open(dir.path(), config).unwrap();
    fill(&mut log, 50);

    let index = std::fs::read(dir.path().join("00000000000000000000.index")).unwrap();
    assert_eq!(index.len() % 8, 0, "chaque entrée fait 8 octets");
    let entries = index.len() / 8;
    // ~170 octets par batch, 50 batches, une entrée tous les 300 octets.
    assert!((20..=40).contains(&entries), "nombre d'entrées inattendu : {entries}");

    let content = std::fs::read(dir.path().join("00000000000000000000.log")).unwrap();
    let mut previous = None;
    for entry in index.chunks_exact(8) {
        let relative = u32::from_be_bytes(entry[0..4].try_into().unwrap()) as i64;
        let position = u32::from_be_bytes(entry[4..8].try_into().unwrap()) as usize;
        let header = RecordBatchHeader::parse(&content[position..]).unwrap();
        assert_eq!(header.base_offset, relative, "l'entrée ({relative}, {position}) ne pointe pas sur ce batch");
        assert!(previous < Some(relative), "les entrées doivent être croissantes");
        previous = Some(relative);
    }
}

#[test]
fn reopen_keeps_everything() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(dir.path(), small_segments()).unwrap();
        fill(&mut log, 25);
    }

    let mut log = Log::open(dir.path(), small_segments()).unwrap();
    assert_eq!(log.log_end_offset(), 25, "le log end offset est retrouvé");
    assert_eq!(log.log_start_offset(), 0);
    assert_eq!(append(&mut log, &["après"]), 25, "les écritures reprennent à la suite");
    assert_eq!(read_all(&log, 0), (0..26).collect::<Vec<_>>());
}

/// Un arrêt brutal pendant une écriture laisse un batch incomplet à la fin
/// du dernier segment : il doit être supprimé à l'ouverture.
#[test]
fn recovery_truncates_a_partial_batch() {
    let dir = tempfile::tempdir().unwrap();
    let valid_size;
    {
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        append(&mut log, &["a", "b"]);
        append(&mut log, &["c"]);
        valid_size = std::fs::metadata(dir.path().join("00000000000000000000.log")).unwrap().len();
    }
    // La moitié d'un batch, comme si l'écriture avait été interrompue.
    let partial = batch(&["perdu"]);
    let path = dir.path().join("00000000000000000000.log");
    let mut content = std::fs::read(&path).unwrap();
    content.extend_from_slice(&partial[..partial.len() / 2]);
    std::fs::write(&path, content).unwrap();

    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    assert_eq!(log.log_end_offset(), 3);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_size, "le fichier est tronqué");
    assert_eq!(append(&mut log, &["d"]), 3);
    assert_eq!(values(&decode_records(log.read(0, 1 << 20, i64::MAX).unwrap())), vec!["a", "b", "c", "d"]);
}

#[test]
fn recovery_truncates_a_corrupt_batch() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        append(&mut log, &["a"]);
        append(&mut log, &["b"]);
        append(&mut log, &["c"]);
    }
    // Un octet modifié dans le dernier batch : son CRC est invalide.
    let path = dir.path().join("00000000000000000000.log");
    let mut content = std::fs::read(&path).unwrap();
    let last = content.len() - 2;
    content[last] ^= 0xff;
    std::fs::write(&path, content).unwrap();

    let log = Log::open(dir.path(), LogConfig::default()).unwrap();
    assert_eq!(log.log_end_offset(), 2, "le batch corrompu et ce qui suit sont supprimés");
    assert_eq!(values(&decode_records(log.read(0, 1 << 20, i64::MAX).unwrap())), vec!["a", "b"]);
}

#[test]
fn missing_index_is_rebuilt() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(dir.path(), small_segments()).unwrap();
        fill(&mut log, 30);
    }
    let indexes = files(dir.path(), ".index");
    let before: Vec<Vec<u8>> = indexes.iter().map(|f| std::fs::read(dir.path().join(f)).unwrap()).collect();
    for f in &indexes {
        std::fs::remove_file(dir.path().join(f)).unwrap();
    }

    let log = Log::open(dir.path(), small_segments()).unwrap();
    assert_eq!(read_all(&log, 0), (0..30).collect::<Vec<_>>());
    let after: Vec<Vec<u8>> = indexes.iter().map(|f| std::fs::read(dir.path().join(f)).unwrap()).collect();
    assert_eq!(before, after, "les index reconstruits sont identiques aux originaux");
}

#[test]
fn corrupt_index_is_rebuilt() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(dir.path(), small_segments()).unwrap();
        fill(&mut log, 30);
    }
    for f in files(dir.path(), ".index") {
        std::fs::write(dir.path().join(f), b"n'importe quoi").unwrap();
    }

    let log = Log::open(dir.path(), small_segments()).unwrap();
    assert_eq!(read_all(&log, 0), (0..30).collect::<Vec<_>>());
}

#[test]
fn retention_by_size_deletes_oldest_segments() {
    let dir = tempfile::tempdir().unwrap();
    let config = LogConfig { retention_bytes: Some(2500), ..small_segments() };
    let mut log = Log::open(dir.path(), config).unwrap();
    fill(&mut log, 40);
    let before = files(dir.path(), ".log").len();

    let deleted = log.apply_retention(0).unwrap();

    let after = files(dir.path(), ".log");
    assert!(deleted > 0, "des segments doivent être supprimés");
    assert_eq!(after.len(), before - deleted);
    assert_eq!(files(dir.path(), ".index").len(), after.len(), "les index sont supprimés avec leur segment");
    let start: i64 = after[0].trim_end_matches(".log").parse().unwrap();
    assert_eq!(log.log_start_offset(), start, "le log commence au premier segment restant");
    assert_eq!(log.log_end_offset(), 40);
    assert!(matches!(log.read(0, 1000, i64::MAX), Err(ReadError::OffsetOutOfRange { .. })));
    assert_eq!(read_all(&log, start), (start..40).collect::<Vec<_>>());
}

#[test]
fn retention_by_time_deletes_old_segments() {
    let dir = tempfile::tempdir().unwrap();
    let config = LogConfig { retention_ms: Some(60_000), ..small_segments() };
    let mut log = Log::open(dir.path(), config).unwrap();
    let now = 10_000_000;
    let padding = "x".repeat(300);
    for i in 0..6 {
        append_at(&mut log, now - 3_600_000, &format!("vieux {i} {padding}"));
    }
    for i in 0..6 {
        append_at(&mut log, now - 1_000, &format!("récent {i} {padding}"));
    }

    log.apply_retention(now).unwrap();

    let start = log.log_start_offset();
    assert!(start > 0, "les segments anciens doivent être supprimés");
    assert!(start <= 6, "les segments récents doivent être conservés (début : {start})");
}

/// Le segment actif n'est jamais supprimé, même s'il est trop vieux.
#[test]
fn retention_keeps_the_active_segment() {
    let dir = tempfile::tempdir().unwrap();
    let config = LogConfig { retention_ms: Some(1), retention_bytes: Some(1), ..LogConfig::default() };
    let mut log = Log::open(dir.path(), config).unwrap();
    append_at(&mut log, 0, "très vieux");

    log.apply_retention(i64::MAX / 2).unwrap();

    assert_eq!(files(dir.path(), ".log").len(), 1);
    assert_eq!(log.log_end_offset(), 1);
    assert_eq!(append(&mut log, &["nouveau"]), 1);
}

/// Après un redémarrage, le broker retrouve ses topics et leurs données.
#[tokio::test]
async fn broker_restart_keeps_topics_and_data() {
    let broker = start_broker().await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "durable", 2).await;
    create_topic(&mut client, "with-dashes-in-name", 1).await;
    produce(&mut client, "durable", 0, batch(&["a", "b"])).await;
    produce(&mut client, "durable", 1, batch(&["c"])).await;
    let topic_id = topic_ids(&mut client).await;
    drop(client);

    let broker = broker.restart().await;
    let mut client = Client::connect(broker.addr).await;

    assert_eq!(topic_ids(&mut client).await, topic_id, "mêmes topics, mêmes identifiants");
    assert_eq!(values(&fetched_records(&fetch(&mut client, "durable", 0, 0).await)), vec!["a", "b"]);
    assert_eq!(values(&fetched_records(&fetch(&mut client, "durable", 1, 0).await)), vec!["c"]);
    let response = produce(&mut client, "durable", 0, batch(&["d"])).await;
    assert_eq!(response.base_offset, 2, "les offsets reprennent à la suite");
}

/// La rétention tourne en tâche de fond.
#[tokio::test]
async fn retention_runs_in_the_background() {
    let config = broker::Config {
        log: LogConfig { retention_bytes: Some(1500), ..small_segments() },
        retention_check_interval: Duration::from_millis(100),
        ..Default::default()
    };
    let broker = start_broker_with(config).await;
    let mut client = Client::connect(broker.addr).await;
    create_topic(&mut client, "retained", 1).await;
    for i in 0..30 {
        let value = format!("{i}-{}", "x".repeat(100));
        produce(&mut client, "retained", 0, batch(&[value.as_str()])).await;
    }

    let mut earliest = 0;
    for _ in 0..30 {
        earliest = list_offset(&mut client, "retained", 0, -2).await.offset;
        if earliest > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(earliest > 0, "la rétention doit avoir supprimé les premiers segments");
    // Laisse la rétention se stabiliser (elle a pu tourner pendant la production).
    tokio::time::sleep(Duration::from_millis(300)).await;
    let earliest = list_offset(&mut client, "retained", 0, -2).await.offset;
    let data = fetch(&mut client, "retained", 0, earliest).await;
    assert_eq!(data.log_start_offset, earliest);
    assert_eq!(fetched_records(&data)[0].offset, earliest);
}

async fn topic_ids(client: &mut Client) -> Vec<(String, uuid::Uuid, usize)> {
    use kafka_protocol::messages::MetadataRequest;
    let response = client.send(MetadataRequest::default().with_topics(None), 12).await.unwrap();
    let mut topics: Vec<_> = response
        .topics
        .iter()
        .map(|t| (t.name.as_ref().unwrap().to_string(), t.topic_id, t.partitions.len()))
        .collect();
    topics.sort();
    topics
}

