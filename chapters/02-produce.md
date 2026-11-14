# Chapitre 2 : Produce et le format RecordBatch

**Objectif :** `kafka-console-producer.sh` écrit des messages dans votre
broker, qui les stocke sur disque.

## Concepts

### Le broker ne lit pas les messages

Un producer regroupe ses messages en **batches** (par partition) et envoie
chaque batch dans une requête `Produce`. Le broker ne décode pas les
messages un par un : il valide l'en-tête du batch, lui attribue ses offsets,
et l'écrit **tel quel** sur disque. Lors d'un `Fetch`, il renverra ces mêmes
octets au consumer.

C'est l'une des raisons de la performance de Kafka : pas de
désérialisation, pas de recopie message par message, et le même format sur
le réseau et sur le disque. Un batch compressé reste compressé de bout en
bout, le broker ne le décompresse jamais.

### Le format RecordBatch v2

Depuis Kafka 0.11, les messages sont transportés dans des **RecordBatch**
(format « magic 2 »). L'en-tête fait 61 octets :

```text
offset  taille  champ
  0       8     base_offset             ┐
  8       4     batch_length            │ hors du CRC
 12       4     partition_leader_epoch  ┘
 16       1     magic (= 2)
 17       4     crc
 21       2     attributes
 23       4     last_offset_delta
 27       8     base_timestamp
 35       8     max_timestamp
 43       8     producer_id
 51       2     producer_epoch
 53       4     base_sequence
 57       4     records_count
 61       ...   records (éventuellement compressés)
```

- `batch_length` compte les octets **après** ce champ : la taille totale du
  batch est `12 + batch_length`.
- Le **CRC** est un CRC-32C (Castagnoli) calculé sur les octets 21 à la fin.
  Les trois premiers champs n'en font pas partie : le broker peut donc les
  réécrire sans recalculer le CRC.
- Les records sont numérotés relativement au batch : le premier a l'offset
  `base_offset`, le dernier `base_offset + last_offset_delta`.
- `attributes` : les bits 0 à 2 donnent la compression (0 aucune, 1 gzip,
  2 snappy, 3 lz4, 4 zstd), le bit 4 indique un batch transactionnel, le
  bit 5 un batch de contrôle.

Le champ `records` d'une requête Produce peut contenir **plusieurs
batches** mis bout à bout.

### L'attribution des offsets

Le producer envoie ses batches avec `base_offset = 0`. C'est le broker qui
attribue les offsets : il réécrit `base_offset` avec le prochain offset
libre de la partition (le **log end offset**), puis avance celui-ci de
`last_offset_delta + 1`. Chaque partition a sa propre séquence d'offsets.

### Le log sur disque

Chaque partition a un répertoire `<data_dir>/<topic>-<partition>/`, qui
contient des **segments** : des fichiers `.log` nommés d'après l'offset de
leur premier batch, sur 20 chiffres. Pour l'instant, un seul segment
suffit : `00000000000000000000.log`, dans lequel les batches sont ajoutés
les uns après les autres. Le découpage en plusieurs segments, l'index et
le rechargement au démarrage viendront au chapitre 4.

Kafka n'appelle pas `fsync` à chaque écriture : il compte sur le cache de
pages du système et sur la réplication pour la durabilité. On fait de même.

### `acks`

| `acks` | Signification |
|---|---|
| `0` | Le producer n'attend pas de réponse. **Le broker n'en envoie pas.** |
| `1` | Réponse dès que le leader a écrit les messages. |
| `-1` (`all`) | Réponse quand toutes les répliques de l'ISR les ont écrits. Avec un seul broker, c'est équivalent à `1`. Ce sera différent au chapitre 7. |

Toute autre valeur donne `INVALID_REQUIRED_ACKS`.

### Les erreurs

Comme pour `CreateTopics`, chaque partition a son propre code d'erreur :

| Situation | Code d'erreur |
|---|---|
| Topic ou partition inconnus | `UNKNOWN_TOPIC_OR_PARTITION` |
| Batch tronqué, longueur incohérente, CRC invalide | `CORRUPT_MESSAGE` |
| `magic` différent de 2 | `UNSUPPORTED_FOR_MESSAGE_FORMAT` |
| `records_count` ≠ `last_offset_delta + 1` | `INVALID_RECORD` |
| Batch plus grand que `Config::max_message_bytes` | `MESSAGE_TOO_LARGE` |
| Erreur d'écriture sur disque | `KAFKA_STORAGE_ERROR` |

Un batch refusé ne doit rien écrire et ne consomme pas d'offset.

### `InitProducerId`

Depuis Kafka 3.0, les producers Java sont **idempotents** par défaut : au
démarrage, ils demandent un identifiant de producer (`InitProducerId`), puis
numérotent leurs batches (`producer_id`, `base_sequence`) pour que le
broker puisse éliminer les doublons en cas de renvoi. Sans cette API, le
producer Java refuse de démarrer.

Ici, il suffit d'attribuer des identifiants uniques (un compteur). La
déduplication est un bonus. Les transactions (`transactional_id` non nul)
sont hors du périmètre du workshop : répondez avec une erreur.

## Ce qui est fourni

- `src/lib.rs` : `Config` a deux nouveaux champs (`data_dir`,
  `max_message_bytes`) et `Broker` deux nouveaux champs (`replicas`,
  `next_producer_id`).
- `src/record_batch.rs` : les types `RecordBatchHeader`, `RecordBatch` et
  `BatchError`, avec la conversion vers les codes d'erreur Kafka.
- `src/replica.rs` : `Replica`, la réplique locale d'une partition (son
  log, derrière un `Mutex`), et `ReplicaManager`, qui les range par
  (topic, partition).
- `handle_request` renvoie maintenant `Result<Option<Bytes>>` : `None`
  quand la requête n'appelle pas de réponse, et `handle_connection` n'écrit
  alors rien.

## À faire

### 1. Le format RecordBatch : `src/record_batch.rs`

- `RecordBatchHeader::parse` : décoder les 61 octets d'en-tête.
  `bytes::Buf` fournit `get_i64`, `get_i32`, etc., en big-endian.
- `compute_crc` : le CRC-32C des octets 21 à la fin, avec
  `crc32c::crc32c`.
- `RecordBatch::set_base_offset` : réécrire le champ dans l'en-tête décodé
  et dans les octets.
- `parse_batches` : découper le champ `records` en batches et valider
  chacun d'eux. Lisez le `magic` (octet 16) **avant** l'en-tête complet :
  les anciens formats ont un en-tête plus court.

### 2. Le log : `src/log/mod.rs` et `src/log/segment.rs`

- `Segment::create` et `Segment::append`.
- `Log::open` : créer le répertoire et un premier segment vide.
- `Log::append` : attribuer les offsets et écrire les batches dans le
  segment actif.

### 3. Les répliques : `src/replica.rs`

- `ReplicaManager::create_replicas` : ouvrir un log par partition.
- `Broker::create_topic` (dans `src/lib.rs`) : enregistrer le topic puis
  créer ses répliques. Les handlers `CreateTopics` et `Metadata` (création
  automatique) l'utilisent déjà.

### 4. Les handlers

- `src/handlers/produce.rs` : `handle_produce`, qui renvoie `None` quand
  `acks = 0`.
- `src/handlers/init_producer_id.rs` : `handle_init_producer_id`.
- Annoncez `Produce` (versions 3 à 12) et `InitProducerId` (0 à 5) dans
  `src/apis.rs`.

## Vérifier

```bash
cargo test --test ch02
```

Puis avec un vrai client :

```bash
cargo run
# dans un autre terminal
docker compose run --rm -T kafka-tools kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic demo
> hello
> world
# Ctrl-D pour terminer
ls -l data/demo-0/
xxd data/demo-0/00000000000000000000.log | head
```

Dans le dump hexadécimal, vous reconnaissez l'en-tête du batch (offset,
longueur, magic `02`...) suivi de vos messages en clair.

Les fichiers de données sont écrasés à chaque redémarrage du broker
jusqu'au chapitre 4.

### Et kcat ?

`kcat -P` échoue pour l'instant avec `Invalid message`. Dans les logs (en
`RUST_LOG=debug`), vous voyez que librdkafka envoie des messages au **format
v0** (magic 0) : il n'utilise le format v2 que si le broker annonce à la
fois `Produce` v3+ et `Fetch` v4+. Ce sera le cas au prochain chapitre.

## Pour aller plus loin

- Envoyez des messages compressés
  (`--producer-property compression.type=zstd`) et vérifiez avec `xxd` que
  le broker les stocke compressés.
- Consultez `kafka-dump-log.sh --files data/demo-0/00000000000000000000.log
  --print-data-log` (dans le conteneur `kafka-tools`, en montant le
  répertoire `data`) : l'outil officiel sait lire vos fichiers.
