# Chapitre 4 : segments, index, recovery et rétention

**Objectif :** un stockage qui survit aux redémarrages, aux arrêts brutaux,
et qui ne grossit pas indéfiniment.

## Concepts

### Les segments

Un log unique de plusieurs centaines de gigaoctets serait ingérable : on ne
peut pas supprimer le début d'un fichier. Kafka découpe donc chaque log en
**segments**. On écrit toujours dans le dernier, le segment **actif**.
Quand il dépasse `segment_bytes` (`log.segment.bytes`, 1 Gio par défaut), on
en ouvre un nouveau.

Chaque segment est nommé d'après l'offset de son premier batch :

```text
data/orders-0/
├── 00000000000000000000.log     offsets 0 à 79
├── 00000000000000000000.index
├── 00000000000000000080.log     offsets 80 à 151
├── 00000000000000000080.index
├── 00000000000000000152.log     segment actif
├── 00000000000000000152.index
└── partition.metadata
```

Pour trouver le segment qui contient un offset, il suffit de chercher le
dernier segment dont l'offset de base est inférieur ou égal :
`BTreeMap::range(..=offset).next_back()`.

Une règle : on ne crée un nouveau segment que si le segment actif n'est pas
vide. Un batch plus gros que `segment_bytes` a donc son propre segment.

### L'index d'offsets

Au chapitre 3, trouver un offset demandait de parcourir le segment depuis
le début. Avec des segments de 1 Gio, c'est beaucoup trop lent.

Chaque segment a donc un **index** (fichier `.index`) qui associe des
offsets à des positions dans le fichier `.log`. Il est **clairsemé** : une
entrée tous les `index_interval_bytes` octets (`log.index.interval.bytes`,
4 Kio par défaut), pas une par batch. Pour trouver un offset :

1. chercher dans l'index (par dichotomie) la dernière entrée dont l'offset
   est inférieur ou égal ;
2. parcourir le segment à partir de sa position, comme avant, sur au plus
   quelques kilo-octets.

Chaque entrée fait 8 octets, en big-endian : l'offset **relatif** au
segment (`offset - base_offset`, sur un `u32`) et la position dans le
fichier `.log` (`u32`). C'est exactement le format de Kafka :
`kafka-dump-log.sh` sait lire vos index.

Quand ajouter une entrée : au moment d'écrire un batch, si au moins
`index_interval_bytes` octets ont été écrits depuis la dernière entrée,
l'entrée pointe sur le début de ce batch.

### La recovery

Le broker peut s'arrêter brutalement au milieu d'une écriture (`kill -9`,
coupure de courant). La fin du segment actif peut alors contenir un batch
incomplet ou corrompu.

À l'ouverture d'un log, on **vérifie le dernier segment** batch par batch
(taille, magic, CRC), et on tronque le fichier au premier batch invalide.
Kafka garde un *recovery point* sur disque pour savoir jusqu'où les données
sont sûres. Plus simplement, on ne vérifie que le dernier segment et on
suppose les autres intacts.

L'index peut lui aussi être absent ou incohérent (taille non multiple de
8, entrées non croissantes, positions hors du fichier). Dans ce cas, on le
**reconstruit** en parcourant le segment.

### Recharger les topics

Au redémarrage, le broker doit retrouver ses topics. Chaque répertoire de
partition contient un fichier `partition.metadata`, écrit à la création :

```text
version: 0
topic_id: 6d8e9a3c-...
```

Au démarrage, on parcourt le répertoire de données : chaque
sous-répertoire `<topic>-<partition>` avec un `partition.metadata` est une
réplique à rouvrir. Attention, un nom de topic peut contenir des tirets :
coupez au **dernier** `-`.

### La rétention

Les plus vieux segments sont supprimés selon deux critères :

- **par durée** (`retention_ms`, 7 jours par défaut) : un segment dont le
  plus grand timestamp est plus vieux que `now - retention_ms` ;
- **par taille** (`retention_bytes`, illimitée par défaut) : tant que le
  log reste plus gros que `retention_bytes` sans son plus vieux segment.

On supprime toujours des segments **entiers**, en partant du plus ancien,
et jamais le segment actif. Le `log_start_offset` avance au premier
segment restant. Un consumer qui demande un offset supprimé reçoit
`OFFSET_OUT_OF_RANGE`.

La rétention tourne en tâche de fond, toutes les
`retention_check_interval` (5 minutes par défaut).

### Les tâches de fond et l'arrêt du broker

C'est la première tâche de fond du broker, il y en aura d'autres. Le
`Broker` a maintenant un `CancellationToken` (`shutdown`) :

- `Broker::spawn` lance une tâche qui s'arrête quand le token est annulé ;
- `serve` annule le token quand elle se termine, grâce à un *drop guard* :
  `broker.shutdown.clone().drop_guard()`.

Les connexions sont elles aussi lancées avec `Broker::spawn` : arrêter
`serve` arrête tout le broker. C'est ce qu'utilisent les tests pour
simuler un redémarrage.

## Ce qui est fourni

- `Config::log` (`LogConfig` : `segment_bytes`, `index_interval_bytes`,
  `retention_ms`, `retention_bytes`) et
  `Config::retention_check_interval`.
- `Broker::shutdown`, `Broker::spawn` et `Broker::start_background_tasks`.
- `OffsetIndex::create` et `OffsetIndex::open` (`src/log/index.rs`) :
  l'ouverture du fichier et la vérification de son contenu.
- `Topics::restore`, pour réenregistrer un topic rechargé.
- `serve` : elle lance les tâches de fond, puis les connexions avec
  `Broker::spawn`, et annule `shutdown` quand elle se termine (drop guard).

## À faire

### 1. L'index : `src/log/index.rs`

`OffsetIndex::append`, `OffsetIndex::lookup` (par dichotomie :
`partition_point` fait l'affaire) et `OffsetIndex::truncate_to_position`.

### 2. Les segments : `src/log/segment.rs`

- `Segment::create` crée aussi l'index ; `Segment::append` ajoute les
  entrées d'index.
- `Segment::open` : ouvrir un segment existant, en parcourant ses en-têtes
  pour retrouver `next_offset` et `max_timestamp`, et en reconstruisant
  l'index s'il est invalide.
- `Segment::recover` : vérifier chaque batch et tronquer la fin invalide.
- `Segment::read` : partir de la position donnée par l'index.
- `Segment::delete`.

Ouvrez les fichiers en mode *append* (`OpenOptions::append`) : après un
`set_len` qui raccourcit le fichier, les écritures suivantes doivent aller à
la nouvelle fin, pas à l'ancienne position du curseur.

### 3. Le log : `src/log/mod.rs`

- `Log::open` recharge les segments existants et fait la recovery du
  dernier.
- `Log::append` crée un nouveau segment quand le segment actif est plein.
- `Log::apply_retention`.

### 4. Le broker

- `ReplicaManager::open_replica` écrit `partition.metadata`, et
  `ReplicaManager::load_existing` recharge les répliques.
- `Broker::new` recharge les topics (il renvoie maintenant un `Result`).
- `ReplicaManager::apply_retention` et `Replica::apply_retention`,
  appelées par la tâche de fond.

## Vérifier

```bash
cargo test --test ch04
```

Puis avec de vrais clients, avec de petits segments pour bien les voir :

```bash
BROKER_SEGMENT_BYTES=2000 BROKER_INDEX_INTERVAL_BYTES=300 cargo run
# dans un autre terminal
seq 1 200 | sed 's/^/message-/' | docker compose run --rm -T kafka-tools \
    kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo \
    --producer-property batch.size=200
ls data/demo-0/
```

Arrêtez le broker (`Ctrl-C`, ou `kill -9` pour tester la recovery),
relancez-le, et relisez :

```bash
docker compose run --rm kcat -b localhost:9092 -C -t demo -o 190 -e
```

Les outils officiels savent lire vos fichiers :

```bash
docker run --rm -v $PWD/data:/data apache/kafka:4.1.0 /opt/kafka/bin/kafka-dump-log.sh \
    --files /data/demo-0/00000000000000000000.log --print-data-log
docker run --rm -v $PWD/data:/data apache/kafka:4.1.0 /opt/kafka/bin/kafka-dump-log.sh \
    --files /data/demo-0/00000000000000000000.index
```

Pour voir la rétention à l'œuvre :

```bash
BROKER_SEGMENT_BYTES=2000 BROKER_RETENTION_BYTES=5000 BROKER_RETENTION_CHECK_INTERVAL_MS=1000 cargo run
```

## Pour aller plus loin

- Reprenez la mesure du chapitre 3 (fetch vers la fin d'un gros log) et
  comparez.
- Kafka a aussi un index temporel (`.timeindex`), qui accélère
  `ListOffsets` par timestamp. Implémentez-le sur le même modèle.
- Kafka n'écrit pas l'index entrée par entrée : il le préalloue et le
  `mmap`. Essayez avec la crate `memmap2`.
