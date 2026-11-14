# Chapitre 3 : Fetch et ListOffsets

**Objectif :** lire les messages avec `kafka-console-consumer.sh` et
`kcat`.

## Concepts

### Le consumer tire les données

Dans Kafka, le broker ne pousse rien : c'est le consumer qui demande
régulièrement « donne-moi ce qui suit l'offset N dans la partition P »
avec une requête `Fetch`. C'est lui qui suit sa position.

### Fetch renvoie des batches entiers

Le broker renvoie les **mêmes octets** que ceux reçus lors du `Produce` :
des batches entiers, lus directement dans le fichier de segment. Kafka va
jusqu'à utiliser `sendfile` pour copier le fichier vers la socket sans
passer par la mémoire de l'application (*zero-copy*).

Conséquence : si l'offset demandé tombe au milieu d'un batch, le broker
renvoie le batch **entier**, à partir de son premier record. C'est le
consumer qui ignore les records qui précèdent l'offset demandé.

### Trouver un offset dans le log

Pour trouver le batch qui contient l'offset demandé, on parcourt le
segment en sautant de batch en batch : on lit l'en-tête à la position
courante, puis on avance de `12 + batch_length` octets. Le premier batch
dont `last_offset` (`base_offset + last_offset_delta`) est supérieur ou
égal à l'offset demandé est le bon.

C'est un parcours linéaire : plus le segment est gros, plus c'est lent. Le
chapitre 4 ajoutera un index pour sauter directement au bon endroit.

### Les limites de taille

- `partition_max_bytes` limite la quantité lue par partition ;
- `max_bytes` limite la taille totale de la réponse.

On ne coupe jamais un batch : on s'arrête avant le premier batch qui ne
tient plus dans la limite. Exception : si le **premier** batch à renvoyer
est plus gros que la limite, on le renvoie quand même en entier. Sinon un
consumer resterait bloqué indéfiniment devant un gros batch (KIP-74).

### Les cas limites

- `fetch_offset` égal au **log end offset** : rien à lire, la réponse est
  vide mais ce n'est pas une erreur.
- `fetch_offset` avant le début du log ou après sa fin :
  `OFFSET_OUT_OF_RANGE`. Le consumer réagit selon son
  `auto.offset.reset` (`earliest` ou `latest`).

### Le high watermark

Chaque réponse indique le **high watermark** de la partition : l'offset
jusqu'où les données sont visibles par les consumers. Un consumer ne lit
jamais au-delà.

Avec une seule réplique, c'est simplement le log end offset. Avec la
réplication (chapitre 7), ce sera l'offset jusqu'où **toutes les répliques
de l'ISR** ont copié les données : un message n'est visible qu'une fois
répliqué. Préparez le terrain : lisez toujours jusqu'à
`Replica::high_watermark()`, pas jusqu'au log end offset.

### Le long polling

Si un consumer est à jour, ses `Fetch` reviennent vides. Pour ne pas le
faire tourner en boucle, la requête contient :

- `min_bytes` : le nombre d'octets minimum avant de répondre ;
- `max_wait_ms` : le temps d'attente maximum.

Si moins de `min_bytes` octets sont disponibles, le broker **attend** que de
nouvelles données arrivent, ou que `max_wait_ms` soit écoulé, avant de
répondre. Une erreur sur une partition fait répondre immédiatement.

Pour être réveillé quand des données arrivent, on utilise un
`tokio::sync::Notify` partagé, notifié à chaque écriture. Attention au
réveil perdu : une écriture qui arrive **entre** la lecture et le début de
l'attente ne doit pas être manquée. Créez le `Notified` et appelez
`enable()` **avant** de lire :

```rust
let notified = self.replicas.new_data.notified();
tokio::pin!(notified);
notified.as_mut().enable();
// ... lire ...
// ... puis attendre `notified` (avec un timeout) si besoin
```

Pendant l'attente, la connexion ne traite pas d'autre requête : c'est
aussi le comportement de Kafka, qui traite les requêtes d'une connexion une
par une.

### Les sessions de fetch

Depuis KIP-227, un consumer peut ouvrir une **session de fetch** pour ne
pas renvoyer la liste complète de ses partitions à chaque requête. Le broker
peut refuser en répondant `session_id = 0` : le client continue alors avec
des requêtes complètes. C'est ce qu'on fait.

### ListOffsets

`ListOffsets` traduit un « temps » en offset :

| `timestamp` | Offset renvoyé |
|---|---|
| `-2` (earliest) | Le premier offset du log (`log_start_offset`) |
| `-1` (latest) | Le high watermark : l'offset du prochain message |
| `≥ 0` | Le premier offset dont le timestamp est supérieur ou égal |

Pour un timestamp, un parcours des en-têtes suffit : renvoyez le
`base_offset` du premier batch dont `max_timestamp` est supérieur ou égal
(une approximation à la granularité du batch). Si aucun batch n'est assez
récent, renvoyez l'offset `-1`.

## Ce qui est fourni

- `src/log/mod.rs` : le type `ReadError`.
- `src/replica.rs` : le type `FetchData` et le `Notify` partagé
  (`ReplicaManager::new_data`, transmis à chaque `Replica`).

## À faire

### 1. Lire le log

- `Segment::read_header` et `Segment::read` (`src/log/segment.rs`).
  `Segment::headers`, qui parcourt les en-têtes à partir d'une position, est
  fourni. `std::os::unix::fs::FileExt::read_exact_at`
  permet de lire à une position donnée sans déplacer le curseur du fichier,
  donc sans conflit avec les écritures.
- `Log::read` et `Log::offset_for_timestamp` (`src/log/mod.rs`).
- `Replica::read`, `Replica::high_watermark` et
  `Replica::offset_for_timestamp` (`src/replica.rs`), et la notification
  dans `Replica::append`.

### 2. Les handlers

- `src/handlers/fetch.rs` : `handle_fetch` (asynchrone).
- `src/handlers/list_offsets.rs` : `handle_list_offsets`.
- Annoncez `Fetch` (versions 4 à 12) et `ListOffsets` (1 à 7). À partir
  de la v13, `Fetch` identifie les topics par leur id et non plus par leur
  nom : on s'arrête à la v12 pour rester simple.

## Vérifier

```bash
cargo test --test ch03
```

Puis avec de vrais clients :

```bash
cargo run
# dans un autre terminal
printf 'k1:v1\nk2:v2\n' | docker compose run --rm -T kcat -b localhost:9092 -P -t demo -K:
docker compose run --rm kcat -b localhost:9092 -C -t demo -o beginning -f '%o %k=%s\n'

docker compose run --rm kafka-tools kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic demo --partition 0 --offset earliest
```

Laissez un consumer tourner et produisez des messages depuis un autre
terminal : ils apparaissent immédiatement grâce au long polling.

`kcat -P` fonctionne maintenant : le broker annonce `Fetch` v4+, librdkafka
utilise donc le format v2 pour produire.

Sans `--partition`, `kafka-console-consumer.sh` utilise un consumer group :
ce sera le chapitre 5.

## Pour aller plus loin

- Produisez quelques centaines de milliers de messages
  (`kafka-producer-perf-test.sh`), puis mesurez le temps d'un fetch vers la
  fin du log. Gardez ce chiffre : vous le comparerez après le chapitre 4.
