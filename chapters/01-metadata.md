# Chapitre 1 : métadonnées et création de topics

**Objectif :** créer des topics avec `kafka-topics.sh --create`, puis les
lister et les décrire.

## Concepts

### Topics, partitions, répliques

Un **topic** est découpé en **partitions**, numérotées à partir de 0. Chaque
partition est un log indépendant et ordonné : c'est l'unité de parallélisme
de Kafka.

Chaque partition est stockée sur un ou plusieurs brokers, ses
**répliques**. Le nombre de répliques est le **facteur de réplication**.
Parmi les répliques, l'une est le **leader** : c'est elle qui reçoit les
écritures et sert les lectures. Les autres la suivent. L'**ISR**
(*in-sync replicas*) est l'ensemble des répliques à jour.

Pour l'instant, le cluster n'a qu'un broker : il est leader de toutes les
partitions, seule réplique et seul membre de l'ISR. La réplication arrive au
chapitre 7.

### À quoi sert `Metadata`

Un client ne connaît au départ qu'une adresse : le *bootstrap server*. Il
lui demande alors les **métadonnées** du cluster :

- la liste des brokers, avec leur adresse ;
- le broker qui fait office de **controller** (celui qui gère les topics) ;
- pour chaque topic demandé, ses partitions et, pour chacune, son leader,
  ses répliques et son ISR.

Avec ces informations, le client sait à quel broker envoyer chaque requête :
les `Produce` et `Fetch` d'une partition vont à son leader, les
`CreateTopics` au controller.

L'adresse renvoyée pour chaque broker est celle que les clients utiliseront
ensuite. Elle doit donc être joignable par eux : c'est le rôle de
`advertised.listeners` dans Kafka, et ici de `Config::host` et
`Config::port`.

### Le champ `topics` de la requête

- `null` : le client demande **tous** les topics (c'est ce que fait
  `kafka-topics.sh --list`).
- une liste : seulement ces topics. La réponse contient une entrée par
  topic demandé. Un topic inconnu a le code d'erreur
  `UNKNOWN_TOPIC_OR_PARTITION` dans son entrée. La requête elle-même ne
  réussit ou n'échoue pas globalement.
- À partir de la v10, un topic peut être demandé par son **identifiant**
  (`topic_id`) plutôt que par son nom (`name` vaut alors `null`). Un id
  inconnu donne `UNKNOWN_TOPIC_ID`.

### Création automatique

Si `allow_auto_topic_creation` est vrai dans la requête **et** que le broker
l'autorise (`auto.create.topics.enable` dans Kafka, ici
`Config::auto_create_topics`), un topic inconnu demandé par son nom est créé
à la volée avec le nombre de partitions par défaut. C'est ce qui permet à
`kafka-console-producer` d'écrire dans un topic qui n'existe pas encore.

### Les identifiants de topic

Depuis Kafka 2.8 (KIP-516), chaque topic reçoit un **UUID** à sa création.
Il permet de distinguer deux topics successifs portant le même nom (supprimé
puis recréé). Utilisez `Uuid::new_v4()` de la crate `uuid`.

### `CreateTopics`

La requête contient une liste de topics à créer. Chacun est traité
**indépendamment** et a son propre code d'erreur dans la réponse :

| Situation | Code d'erreur |
|---|---|
| Nom vide, `.` ou `..`, plus de 249 caractères, ou caractère hors de `[a-zA-Z0-9._-]` | `INVALID_TOPIC_EXCEPTION` |
| Nombre de partitions inférieur à 1 | `INVALID_PARTITIONS` |
| Facteur de réplication inférieur à 1 ou supérieur au nombre de brokers | `INVALID_REPLICATION_FACTOR` |
| Le topic existe déjà | `TOPIC_ALREADY_EXISTS` |
| Assignation manuelle des partitions (`assignments` non vide) | `INVALID_REQUEST` (non supporté ici) |

Quelques subtilités :

- `-1` pour `num_partitions` ou `replication_factor` signifie « valeur par
  défaut du broker » : `Config::default_num_partitions` partitions et une
  seule réplique.
- `validate_only` : on vérifie tout, on renvoie la réponse qu'on aurait
  renvoyée, mais on ne crée rien.
- Le champ `error_message` est affiché tel quel par les outils Kafka :
  profitez-en pour donner des messages utiles.
- Les `configs` d'un topic (`retention.ms`...) peuvent être ignorées pour
  l'instant.

### `DescribeConfigs`

`kafka-topics.sh --describe` demande aussi la configuration du topic. Le
broker n'en a pas encore : renvoyez une liste vide pour un topic existant
(type de ressource 2) et `UNKNOWN_TOPIC_OR_PARTITION` pour un topic inconnu.
Pour le broker lui-même (type 4, dont le nom est le `node_id`), renvoyez
aussi une liste vide.

## Ce qui est fourni

- `src/lib.rs` : `Config` a de nouveaux champs (`cluster_id`,
  `default_num_partitions`, `auto_create_topics`) et `Broker` a un champ
  `topics`.
- `src/topics.rs` : les types `Topic`, `Partition`, `NewTopic`,
  `TopicError` (avec la conversion vers le code d'erreur Kafka) et la
  structure `Topics`.

## À faire

### 1. Le registre des topics : `src/topics.rs`

Implémentez `validate_name`, puis les méthodes de `Topics` : `create`,
`get`, `get_by_id` et `list`. Le registre est partagé entre toutes les
connexions : il est protégé par un `RwLock`.

Attention à la concurrence : deux clients peuvent créer le même topic au
même moment. La vérification « le topic existe-t-il ? » et l'insertion
doivent se faire sous le même verrou en écriture.

### 2. Les handlers

- `src/handlers/metadata.rs` : `handle_metadata`
- `src/handlers/create_topics.rs` : `handle_create_topics`
- `src/handlers/describe_configs.rs` : `handle_describe_configs`

### 3. Les brancher

- Ajoutez les trois API dans `src/apis.rs` :

  | API | Versions |
  |---|---|
  | `Metadata` | 1 à 13 |
  | `CreateTopics` | 2 à 7 |
  | `DescribeConfigs` | 1 à 4 |

Le dispatch de `handle_request` (`src/handlers/mod.rs`) appelle déjà les
trois handlers.

## Vérifier

```bash
cargo test --test ch01
cargo test   # les tests du chapitre 0 doivent toujours passer
```

Puis avec de vrais clients :

```bash
cargo run
# dans un autre terminal
alias kt='docker compose run --rm -T kafka-tools kafka-topics.sh --bootstrap-server localhost:9092'
kt --create --topic orders --partitions 3
kt --list
kt --describe --topic orders
kt --create --topic orders     # TopicExistsException, avec votre message
docker compose run --rm kcat -b localhost:9092 -L
```

`--describe` doit afficher :

```text
Topic: orders  TopicId: ...  PartitionCount: 3  ReplicationFactor: 1  Configs:
    Topic: orders  Partition: 0  Leader: 0  Replicas: 0  Isr: 0 ...
```

## Pour aller plus loin

- Lancez deux brokers (`BROKER_PORT=9093 BROKER_NODE_ID=1 cargo run`) et
  créez un topic sur chacun. Chacun a sa propre vision du « cluster » :
  c'est le problème que résoudra etcd au chapitre 6.
- Arrêtez le broker et relancez-le : les topics ont disparu. Ils seront
  persistés sur disque au chapitre 3.
- Implémentez `DeleteTopics` (api_key 20) pour faire fonctionner
  `kafka-topics.sh --delete`.
