# Chapitre 5 : consumer groups

**Objectif :** plusieurs `kafka-console-consumer.sh` d'un même groupe se
partagent les partitions d'un topic, et reprennent où ils en étaient après
un redémarrage.

Le chapitre est découpé en trois parties, chacune avec ses tests :

| Partie | Sujet | Tests |
|---|---|---|
| 5a | Appartenance au groupe et rebalance | `cargo test --test ch05a` |
| 5b | Commit et lecture des offsets | `cargo test --test ch05b` |
| 5c | Persistance des offsets dans `__consumer_offsets` | `cargo test --test ch05c` |

## Concepts

### À quoi sert un groupe

Les consumers d'un même **groupe** (même `group.id`) se répartissent les
partitions d'un topic : chaque partition est lue par un seul membre du
groupe. Ajouter des consumers permet de lire plus vite, jusqu'à un consumer
par partition. Si un consumer s'arrête, ses partitions sont redistribuées
aux autres.

Chaque groupe est géré par un broker, son **coordinateur**.

### Le broker ne décide pas de l'assignation

Avec le protocole « classic », c'est l'un des consumers, le **leader** du
groupe, qui calcule l'assignation des partitions. Le broker se contente de :

1. rassembler les membres ;
2. choisir un leader et lui transmettre la liste des membres ;
3. redistribuer à chacun ce que le leader a décidé.

Les stratégies d'assignation (`range`, `roundrobin`, `cooperative-sticky`...)
sont donc implémentées dans les clients. Pour le broker, les métadonnées des
membres et les assignations sont des octets opaques.

Kafka 4 propose un nouveau protocole (KIP-848) où c'est le broker qui
assigne. Les consumers Java utilisent encore le protocole classic par
défaut.

### Le rebalance

```text
            JoinGroup                tous les membres         SyncGroup
           (1er membre)              ont rejoint              du leader
   Empty ───────────────▶ Preparing ─────────────▶ Completing ─────────▶ Stable
     ▲                    Rebalance                Rebalance               │
     │  dernier membre parti  ▲                        │                   │
     └────────────────────────┴── JoinGroup, LeaveGroup, session expirée ──┘
```

1. **JoinGroup** : chaque membre envoie la liste des stratégies qu'il
   supporte. Le broker **ne répond pas tout de suite** : il attend que tous
   les membres connus aient envoyé leur `JoinGroup`, au plus le
   `rebalance_timeout`. Les membres qui ne rejoignent pas à temps sont
   exclus.
2. Le broker incrémente la **génération**, choisit une stratégie commune à
   tous les membres et un leader, puis répond à tous. Seul le leader reçoit
   la liste des membres et de leurs métadonnées.
3. **SyncGroup** : le leader envoie l'assignation de chaque membre. Les
   autres membres envoient un `SyncGroup` vide, qui **attend** celui du
   leader. Puis chacun reçoit sa part, et le groupe devient **Stable**.

Un rebalance est déclenché quand un membre arrive, part (`LeaveGroup`) ou
disparaît.

### Les réponses différées

`JoinGroup` et `SyncGroup` ne reçoivent pas forcément de réponse
immédiate. Pour chaque membre en attente, on garde un
`tokio::sync::oneshot::Sender` ; le handler attend le `Receiver`
correspondant. Quand le groupe change d'état, on envoie leur réponse à tous
les membres en attente.

Les échéances (fin du délai de rebalance, expiration des sessions) sont
vérifiées par une tâche de fond toutes les 100 ms
(`GroupCoordinator::tick`).

### Heartbeat et sessions

Un membre envoie des **heartbeats** réguliers (toutes les 3 secondes par
défaut). Sans nouvelle de lui pendant `session_timeout`, il est exclu et un
rebalance commence. Pendant un rebalance, le heartbeat répond
`REBALANCE_IN_PROGRESS` : c'est ainsi que les membres apprennent qu'ils
doivent renvoyer un `JoinGroup`.

Un membre qui attend une réponse à son `JoinGroup` ou à son `SyncGroup` ne
peut pas envoyer de heartbeat : il ne doit pas expirer pendant ce temps.

### `MEMBER_ID_REQUIRED`

À partir de la v4 de `JoinGroup`, un nouveau membre (`member_id` vide)
reçoit d'abord l'erreur `MEMBER_ID_REQUIRED` avec un identifiant généré. Il
renvoie ensuite son `JoinGroup` avec cet identifiant. Cela évite qu'un
client qui réessaie après une perte de connexion crée des membres fantômes.

### Le délai initial

Quand plusieurs consumers démarrent en même temps, le premier arrivé
déclencherait un rebalance, puis le second un autre, etc. Pour l'éviter, le
premier rebalance d'un groupe vide attend `initial_rebalance_delay`
(3 secondes par défaut) avant de se terminer. C'est pourquoi un
`kafka-console-consumer.sh` met 3 secondes à démarrer.

### Les offsets commités

Chaque consumer **commite** régulièrement sa position (`OffsetCommit`) :
« dans le groupe G, j'ai traité la partition P jusqu'à l'offset N ». Au
redémarrage, ou quand une partition change de propriétaire, le nouveau
consumer relit cette position (`OffsetFetch`) et reprend à partir de là.

Un commit n'est accepté que d'un membre de la génération courante. Sinon,
`ILLEGAL_GENERATION` ou `UNKNOWN_MEMBER_ID` : un consumer exclu ne doit pas
écraser le travail de celui qui a repris ses partitions. Exception : un
groupe sans membre accepte les commits « simples » (génération `-1`, sans
`member_id`), utilisés par les consumers qui s'assignent eux-mêmes leurs
partitions et par les outils d'administration.

### `__consumer_offsets`

Où stocker les offsets ? Kafka les stocke… dans Kafka. Le topic interne
`__consumer_offsets` (50 partitions dans Kafka, 10 ici) reçoit un record
par commit. La partition d'un groupe est `abs(hashCode(group_id)) %
partitions`, avec le `hashCode` des chaînes Java.

- À chaque commit, on écrit un record dans la partition du groupe, **puis**
  on met à jour les offsets en mémoire.
- Au démarrage, on relit ces partitions du début à la fin. Le dernier
  record d'une clé gagne.

Kafka utilise un format binaire versionné. Ici, clé et valeur sont en JSON,
pour pouvoir les lire facilement :

```text
{"group":"billing","topic":"orders","partition":0} => {"offset":40,...}
```

Le topic est créé au premier `FindCoordinator`. Il n'est pas soumis à la
rétention : Kafka le **compacte** à la place (il ne garde que le dernier
record de chaque clé). La compaction est un bonus.

Le coordinateur d'un groupe est le leader de sa partition de
`__consumer_offsets`. Avec un seul broker, c'est toujours lui. Mais ce choix
paiera au chapitre 8 : quand un broker tombe, les groupes qu'il coordonnait
passent au nouveau leader de ces partitions, qui a déjà leurs offsets.

## Ce qui est fourni

- `src/groups/group.rs` : les types (`GroupState`, `GroupConfig`, `Member`,
  `JoinRequest`, `JoinResult`, `SyncResult`, `Outcome`, `CommittedOffset`)
  et la structure `Group`.
- `src/groups/mod.rs` : `GroupCoordinator`, qui range les groupes derrière un
  `Mutex` (`with_group`, `with_existing_group`, `for_each_group`).
- `src/groups/offsets.rs` : les types `OffsetKey` et `OffsetValue`, et
  l'encodage et le décodage des records (`encode_commits`,
  `decode_commits`).
- `src/handlers/describe_groups.rs` : `ListGroups` et `DescribeGroups`,
  pour `kafka-consumer-groups.sh`.
- `RequestContext` (`src/handlers/mod.rs`) : la version de la requête, le
  `client_id` et l'adresse du client. `handle_request` reçoit maintenant
  l'adresse du client (`peer`).
- `Config::group` et `Config::offsets_topic_partitions`.

## À faire

### 5a. Appartenance et rebalance

- `src/groups/group.rs` : `Group::join`, `prepare_rebalance`,
  `maybe_complete_join`, `complete_join`, `select_protocol`, `sync`,
  `heartbeat`, `leave`, `remove_member`, `tick`.
- `GroupCoordinator::tick`, appelée par une tâche de fond toutes les 100 ms
  (la tâche est fournie, dans `Broker::start_background_tasks`).
- `src/handlers/find_coordinator.rs` : `handle_find_coordinator` (une clé
  jusqu'à la v3, une liste à partir de la v4).
- `src/handlers/groups.rs` : `handle_join_group`, `handle_sync_group`,
  `handle_heartbeat`, `handle_leave_group`.

Choix de la stratégie : parmi les stratégies supportées par **tous** les
membres, chaque membre vote pour sa préférée (la première de sa liste) ; la
plus populaire l'emporte.

### 5b. Offsets en mémoire

- `Group::check_commit`.
- `src/handlers/offsets.rs` : `handle_offset_commit` et
  `handle_offset_fetch`. À partir de la v8, `OffsetFetch` accepte plusieurs
  groupes ; `topics = null` demande tous les offsets du groupe.

### 5c. Persistance

- `groups::offsets::partition_for` (testée par `cargo test --lib`).
- `Broker::ensure_offsets_topic`, `Broker::store_commits` (écrire dans le log
  puis appliquer) et `Broker::load_offsets` (relire au démarrage).
- `GroupCoordinator::apply_commits`.
- `Replica::read_all`, pour relire une partition entière.

Les topics internes ne sont pas soumis à la rétention : c'est fourni, dans
`ReplicaManager::open_replica`.

Annoncez les nouvelles API dans `src/apis.rs` :

| API | Versions |
|---|---|
| `FindCoordinator` | 0 à 4 |
| `JoinGroup` | 0 à 9 |
| `SyncGroup` | 0 à 5 |
| `Heartbeat` | 0 à 4 |
| `LeaveGroup` | 0 à 5 |
| `OffsetCommit` | 2 à 9 |
| `OffsetFetch` | 1 à 9 |
| `ListGroups` | 0 à 5 |
| `DescribeGroups` | 0 à 5 |

## Vérifier

```bash
cargo test --test ch05a --test ch05b --test ch05c
```

Puis avec de vrais clients :

```bash
cargo run
# dans un autre terminal
alias kt='docker compose run --rm -T kafka-tools'
kt kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 4
seq 1 1000 | kt kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders

# deux consumers du même groupe, dans deux terminaux
kt kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders \
    --group billing --from-beginning --property print.partition=true

# dans un troisième terminal
kt kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group billing
```

`--describe` affiche, pour chaque partition, l'offset commité, le log end
offset, le lag et le membre qui la lit.

Arrêtez un consumer : après le rebalance, l'autre reprend ses partitions.
Arrêtez le broker, relancez-le, relancez un consumer : il reprend où le
groupe s'était arrêté.

Les commits sont lisibles directement :

```bash
docker compose run --rm kcat -b localhost:9092 -C -t __consumer_offsets -f '%k => %s\n'
```

## Pour aller plus loin

- Lancez trois consumers sur un topic à deux partitions : l'un d'eux n'a
  rien à lire.
- Réinitialisez les offsets d'un groupe arrêté :
  `kafka-consumer-groups.sh --reset-offsets --to-earliest --execute`.
- Implémentez la compaction de `__consumer_offsets` (voir le bonus).
- Lisez KIP-848 : en quoi le nouveau protocole simplifie-t-il le rôle des
  clients ?
