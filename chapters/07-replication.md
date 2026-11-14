# Chapitre 7 : réplication, high watermark et ISR

**Objectif :** chaque message est copié sur plusieurs brokers avant d'être
confirmé au producer et visible par les consumers.

## Concepts

### Les followers sont des consumers

La réplication de Kafka est d'une grande simplicité : chaque **follower**
copie le log de son leader en lui envoyant des `Fetch`, exactement comme un
consumer. Deux différences :

- le champ `replica_id` contient l'identifiant du follower (`-1` pour un
  consumer) ;
- le follower écrit les batches reçus **tels quels**, avec les offsets
  attribués par le leader, au lieu de leur en attribuer de nouveaux.

Le `fetch_offset` d'un follower est son log end offset : il dit au leader
« j'ai tout ce qui précède cet offset ». C'est ainsi que le leader suit la
progression de chaque follower, sans message supplémentaire.

Chaque broker lance un **fetcher** par partition dont il est follower : une
tâche qui envoie des `Fetch` au leader en boucle, avec du long polling
(`max_wait_ms = 500`). Quand le leader change, le fetcher est arrêté et un
autre est lancé vers le nouveau leader.

### Le high watermark

Le leader connaît le log end offset de chaque follower. Le **high
watermark** est le plus petit log end offset parmi les répliques de
l'**ISR** :

```text
               offsets :  0   1   2   3   4   5   6
leader     (LEO = 7)     [■] [■] [■] [■] [■] [■] [■]
follower 1 (LEO = 6)     [■] [■] [■] [■] [■] [■]
follower 2 (LEO = 4)     [■] [■] [■] [■]
                                         ▲
                              high watermark = 4
```

Les messages avant le high watermark sont sur **toutes** les répliques de
l'ISR : ils survivront à la perte du leader. C'est pourquoi :

- un consumer ne lit **jamais** au-delà du high watermark (ce que vous
  faites depuis le chapitre 3, où il valait le log end offset) ;
- un producer en `acks=all` reçoit sa réponse quand le high watermark
  dépasse ses messages.

Le leader transmet le high watermark aux followers dans ses réponses
`Fetch` : chaque follower le connaît, avec un léger retard. Il servira au
chapitre 8.

Le high watermark d'un leader ne recule jamais.

### L'ISR

L'ISR (*in-sync replicas*) est l'ensemble des répliques à jour. Un follower
arrêté ou trop lent bloquerait le high watermark, donc tous les producers
en `acks=all`. Le leader le retire donc de l'ISR s'il n'a pas rattrapé le
leader depuis `replica_lag_time_max` (`replica.lag.time.max.ms`, 30 s dans
Kafka, 10 s ici). « Rattraper » signifie : envoyer un `Fetch` dont le
`fetch_offset` est au moins le log end offset du leader.

À l'inverse, un follower dont le log end offset atteint le high watermark
revient dans l'ISR.

L'ISR fait partie de l'état de la partition dans etcd. Le leader la modifie
avec une **écriture conditionnelle** : « remplace `/partitions/t/p` si sa
révision de modification est toujours celle que je connais ». Si le
controller a élu un autre leader entre-temps, l'écriture échoue et le
leader déchu ne peut pas écraser la décision. Dans KRaft, le leader envoie
une requête `AlterPartition` au controller, avec la même protection.

### `acks=all` et `min.insync.replicas`

`acks=all` signifie « toute l'**ISR** », pas « toutes les répliques ». Si
l'ISR se réduit au seul leader, `acks=all` ne protège plus de rien.
`min.insync.replicas` fixe une taille minimale : en dessous, le leader
refuse les `Produce` en `acks=all` avec `NOT_ENOUGH_REPLICAS`. Avec trois
répliques, la configuration classique est `min.insync.replicas = 2`.

Si l'ISR ne rattrape pas les messages avant le `timeout_ms` de la requête,
le producer reçoit `REQUEST_TIMED_OUT`.

Les commits d'offsets dans `__consumer_offsets` attendent eux aussi d'être
répliqués.

### Quand un follower diverge

Un follower peut se retrouver avec un `fetch_offset` refusé
(`OFFSET_OUT_OF_RANGE`) :

- **avant** le début du log du leader (la rétention a supprimé ces
  données) : il vide son log et repart du `log_start_offset` du leader ;
- **après** la fin du log du leader : il a des messages que le leader n'a
  pas (un ancien leader qui revient). Il les supprime en tronquant son log
  au high watermark du leader. Le chapitre 8 y revient.

## Ce qui est fourni

- `src/replication/client.rs` : un client Kafka minimal pour les fetchers.
- `src/replica.rs` : les types `ReplicationState`, `FollowerState`,
  `AppendInfo` (avec `next_offset`).
- `Config::min_insync_replicas` et `Config::replica_lag_time_max`.
- `Broker::spawn_abortable`, pour les tâches qu'on arrête individuellement.

## À faire

### 1. Le log : `src/log/mod.rs` et `src/log/segment.rs`

- `Log::append_replicated` : écrire des batches qui ont déjà leurs offsets
  (le premier doit commencer au log end offset).
- `Log::append_with_epoch` : inscrire l'epoch du leader dans chaque batch
  (`partition_leader_epoch`).
- `Log::truncate_to`, `Segment::truncate_to` et `Log::reset_to`.

### 2. La réplique : `src/replica.rs`

- `append` (leader) et `append_as_follower` ;
- `read` (jusqu'au high watermark) et `read_for_follower` (jusqu'au log end
  offset) ;
- `update_partition_state` : le rôle de la réplique, d'après les
  métadonnées du cluster ;
- `record_follower_fetch`, `update_high_watermark`, `isr_to_shrink`.

### 3. La réplication : `src/replication/mod.rs`

- `sync_fetchers` : exactement un fetcher par partition suivie, vers le
  leader et l'epoch actuels ;
- `fetch_loop` et `fetch_from` ;
- `shrink_isrs`, appelée périodiquement (dans `Broker::start`).

### 4. Le cluster et les handlers

- `Broker::alter_isr` (`src/cluster/node.rs`). `on_cluster_change`, fourni,
  met à jour le rôle des répliques et appelle `sync_fetchers`.
- `Fetch` : avec `replica_id >= 0`, enregistrer la position du follower,
  lire jusqu'au log end offset, et l'ajouter à l'ISR s'il a rattrapé.
- `Produce` : `min_insync_replicas`, puis attente du high watermark avec
  `acks=all`.
- `Broker::wait_for_high_watermark` (`src/lib.rs`), sur le modèle du long
  polling du chapitre 3.
- `store_commits` attend la réplication du commit.

## Vérifier

```bash
docker compose up -d etcd
cargo test --test ch07
```

Puis avec un vrai cluster :

```bash
BROKER_REPLICA_LAG_TIME_MAX_MS=2000 scripts/cluster.sh start 3
alias kt='docker compose run --rm -T kafka-tools'
kt kafka-topics.sh --bootstrap-server localhost:9092 --create --topic payments \
    --partitions 3 --replication-factor 3
kt kafka-producer-perf-test.sh --topic payments --num-records 100000 --record-size 100 \
    --throughput -1 --producer-props bootstrap.servers=localhost:9092 acks=all
du -sh cluster/data-*/payments-*
```

Les trois brokers ont les mêmes données. Arrêtez un follower
(`scripts/cluster.sh kill 1`) et observez l'ISR rétrécir
(`kafka-topics.sh --describe`), puis relancez-le
(`scripts/cluster.sh restart 1`) : il rattrape son retard et revient dans
l'ISR.

Les partitions dont le broker arrêté était leader n'ont plus de leader :
c'est le chapitre 8.

## Pour aller plus loin

- Comparez les débits en `acks=0`, `acks=1` et `acks=all`.
- Kafka n'utilise pas un fetcher par partition mais un par broker leader,
  qui demande toutes les partitions de ce leader dans un seul `Fetch`.
  Regroupez vos fetchers.
- Les followers peuvent servir les consumers (KIP-392, *fetch from
  follower*) : un consumer lit sur la réplique la plus proche de lui.
  Quelles précautions faut-il prendre ?
