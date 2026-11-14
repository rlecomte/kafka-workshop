# Chapitre 6 : un cluster coordonné par etcd

**Objectif :** trois brokers forment un cluster. Les partitions d'un topic
sont réparties entre eux, et les clients s'adressent au bon broker pour
chaque partition.

## Concepts

### Le problème

Lancez deux brokers du chapitre 5 : chacun a sa propre liste de topics,
chacun se croit seul. Pour former un cluster, les brokers doivent partager :

- **qui est vivant** : la liste des brokers et leurs adresses ;
- **les métadonnées** : les topics, et pour chaque partition ses répliques,
  son leader et son ISR ;
- **qui décide** : un broker, le **controller**, prend les décisions qui
  concernent tout le cluster (au chapitre 8, il élira les nouveaux leaders).

Tous doivent avoir la **même** vision, même en cas de panne ou de partition
réseau. C'est un problème de consensus. Kafka l'a d'abord délégué à
ZooKeeper, puis l'a intégré avec KRaft (une implémentation de Raft). On ne
réimplémente pas Raft : on délègue à **etcd**, qui l'implémente pour nous.

### Ce qu'etcd apporte

etcd est une base clé-valeur répliquée et cohérente. Quatre de ses
primitives suffisent :

| Primitive | Usage |
|---|---|
| **Lease** : une durée de vie, prolongée par des *keepalives*. Les clés attachées sont supprimées quand le lease expire. | Détecter les brokers morts. |
| **Transaction** : « si telle condition est vraie, applique ces écritures, sinon celles-là », de façon atomique. | Élire un controller, créer un topic une seule fois. |
| **Révisions** : chaque écriture incrémente une révision globale ; chaque clé connaît sa révision de création et de modification. | Écritures conditionnelles (« seulement si personne ne l'a modifiée depuis »). |
| **Watch** : un flux des changements à partir d'une révision. | Garder une copie locale de l'état à jour. |

### Les clés du cluster

```text
/workshop/brokers/<id>              {"node_id":0,"host":"localhost","port":9092}   (lease)
/workshop/controller                {"node_id":2}                                   (lease)
/workshop/topics/<topic>            {"id":"...","partitions":[[2,0,1],[0,1,2],...]}
/workshop/partitions/<topic>/<p>    {"leader":2,"leader_epoch":0,"isr":[2,0,1]}
```

- `/brokers/<id>` est attachée au lease du broker. Un broker arrêté
  brutalement disparaît de la liste quand son lease expire (après
  `lease_ttl`, 6 secondes par défaut).
- `/topics/<topic>` décrit l'**assignation** : la liste des répliques de
  chaque partition. Elle ne change pas après la création.
- `/partitions/<topic>/<p>` décrit l'**état** de la partition : leader,
  epoch, ISR. Il changera aux chapitres 7 et 8.

Vous pouvez tout inspecter :

```bash
docker compose exec etcd etcdctl get --prefix /workshop/
docker compose exec etcd etcdctl watch --prefix /workshop/
```

### Rejoindre le cluster

Au démarrage, un broker :

1. crée un **lease** et écrit `/brokers/<id>` avec ce lease ;
2. lit tout le préfixe `/workshop/` : c'est son état initial ;
3. lance un **watch** sur le préfixe, à partir de la révision lue + 1 : il
   ne manque aucun changement ;
4. lance une tâche qui envoie des **keepalives** (toutes les `lease_ttl / 3`) ;
5. se présente à l'**élection du controller**.

À chaque changement observé, il met à jour sa copie de l'état
(`ClusterState`), puis le registre des topics, et ouvre les répliques
locales des partitions qui lui sont assignées.

### Élire un controller

L'élection est une simple transaction :

```text
si  create_revision("/workshop/controller") == 0     (la clé n'existe pas)
alors put("/workshop/controller", {"node_id": moi}, lease = mon lease)
```

Un seul broker peut réussir. Si le controller meurt, son lease expire, la
clé disparaît, les autres le voient grâce au watch et retentent leur
chance.

### Créer un topic

`CreateTopics` est traité par le controller ; les autres brokers répondent
`NOT_CONTROLLER`. Les clients connaissent le controller grâce à `Metadata`
(`controller_id`) et y envoient leurs `CreateTopics`.

Le controller :

1. vérifie le nom, le nombre de partitions, et que le facteur de
   réplication ne dépasse pas le nombre de brokers vivants ;
2. calcule l'**assignation** : les répliques de chaque partition sont sur
   des brokers différents, et les leaders (première réplique) sont répartis
   en tourniquet ;
3. écrit le topic et l'état initial de chaque partition (leader = première
   réplique, ISR = toutes les répliques) dans **une seule transaction**,
   conditionnée à l'absence du topic : deux créations simultanées ne
   peuvent pas réussir toutes les deux.

Le topic apparaît ensuite chez tous les brokers grâce au watch.

Par simplicité, la création automatique (via `Metadata`) et la création de
`__consumer_offsets` peuvent se faire depuis n'importe quel broker : la
transaction etcd garantit qu'un topic n'est créé qu'une fois. Kafka, lui,
les fait faire au controller.

### Le routage des requêtes

Chaque partition a un leader. Les `Produce`, `Fetch` et `ListOffsets`
doivent lui être adressés ; un autre broker répond
`NOT_LEADER_OR_FOLLOWER`, et le client rafraîchit ses métadonnées.

De même, un consumer group est géré par le leader de sa partition de
`__consumer_offsets`. `FindCoordinator` renvoie ce broker ; les autres
répondent `NOT_COORDINATOR` aux requêtes du groupe.

Les followers n'ont pour l'instant que des logs vides : la réplication est
l'objet du chapitre 7.

### Le mode autonome reste possible

Sans `BROKER_ETCD_URL`, le broker fonctionne comme avant, seul. Le code
masque la différence derrière quelques méthodes de `Broker` :
`live_brokers`, `controller_id`, `leader_replica`, `coordinator_for`,
`create_topic`.

## Ce qui est fourni

- `src/cluster/etcd.rs` : un client etcd minimal qui utilise l'API v3 en
  HTTP/JSON (`get`, `get_prefix`, `put`, `txn`, `grant_lease`,
  `keep_alive`, `watch_prefix`...). Aucune dépendance système : pas de
  `protoc`, pas de TLS.
- `src/cluster/mod.rs` : `ClusterConfig`, `Keys`, les enregistrements JSON
  (`BrokerRecord`, `ControllerRecord`, `TopicRecord`, `PartitionRecord`) et
  `ClusterState`.
- `topics::Partition` a de nouveaux champs : `replicas`, `leader`,
  `leader_epoch`, `isr`.
- `Config::cluster` et `Broker::cluster`.
- `scripts/cluster.sh` : lance un cluster local.

## À faire

### 1. L'état du cluster : `src/cluster/mod.rs`

- `ClusterState::apply` : appliquer un changement (put ou delete) selon le
  type de clé.
- `assign_replicas` (testée par `cargo test --lib`).

### 2. Le nœud : `src/cluster/node.rs`

- `register` et `keep_alive_loop` ;
- `join_cluster` (lecture initiale, puis lancement des tâches) et
  `watch_loop` ;
- `controller_loop` et `is_controller` ;
- `on_cluster_change` : reconstruire le registre des topics, ouvrir les
  répliques assignées, charger ou décharger les offsets des partitions de
  `__consumer_offsets` dont ce broker devient (ou cesse d'être) leader ;
- `create_topic_in_cluster`.

### 3. Le broker : `src/lib.rs` et les handlers

- `live_brokers`, `controller_id`, `leader_replica`, `coordinator_for`,
  `is_coordinator`, `create_topic`, `ensure_offsets_topic` (3 répliques au
  plus).
- `Metadata` : brokers vivants, controller, leader, répliques, ISR et
  répliques hors ligne de chaque partition (`LEADER_NOT_AVAILABLE` si une
  partition n'a pas de leader).
- `CreateTopics` : `NOT_CONTROLLER` hors du controller.
- `FindCoordinator` : le leader de la partition du groupe.

Sont fournis : `Broker::start`, qui rejoint le cluster avant que `serve`
n'accepte les connexions ; l'utilisation de `leader_replica` dans `Produce`,
`Fetch` et `ListOffsets` ; la vérification de `is_coordinator` dans les
requêtes de groupe (`NOT_COORDINATOR`) ; les handlers rendus asynchrones.

## Vérifier

```bash
docker compose up -d etcd
cargo test --test ch06
cargo test   # le mode autonome fonctionne toujours
```

Puis avec un vrai cluster :

```bash
scripts/cluster.sh start 3
alias kt='docker compose run --rm -T kafka-tools'
kt kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders \
    --partitions 6 --replication-factor 3
kt kafka-topics.sh --bootstrap-server localhost:9093 --describe --topic orders
seq 1 30 | kt kafka-console-producer.sh --bootstrap-server localhost:9094 --topic orders
kt kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders \
    --group g1 --from-beginning
docker compose exec etcd etcdctl get --prefix /workshop/
```

Arrêtez le controller (`scripts/cluster.sh kill <id>`) : après quelques
secondes, un autre broker est élu. Les partitions dont il était leader
n'ont plus de leader joignable : c'est le chapitre 8.

`scripts/cluster.sh stop` arrête le cluster.
`docker compose exec etcd etcdctl del --prefix /workshop/` efface son état.

## Pour aller plus loin

- Observez le watch en direct (`etcdctl watch --prefix /workshop/`) pendant
  que vous créez des topics et arrêtez des brokers.
- Coupez etcd (`docker compose stop etcd`) puis relancez-le : les brokers se
  réenregistrent et relancent leur watch.
- Comparez avec KRaft : les métadonnées y sont un log (le topic
  `__cluster_metadata`) répliqué par Raft entre les controllers.
