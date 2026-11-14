# Chapitre 8 : failover

**Objectif :** tuer le leader d'une partition (`kill -9`) sans que les
producers et les consumers ne perdent un seul message confirmé.

## Concepts

### Qui détecte la panne ?

Quand un broker meurt, son lease etcd expire après `lease_ttl`, et sa clé
`/brokers/<id>` disparaît. Tous les brokers le voient grâce au watch, et en
particulier le **controller**, qui doit réagir.

Si c'est le controller qui meurt, sa clé `/controller` disparaît aussi : un
autre broker est élu (chapitre 6), et reprend le travail. Comme il a pu
manquer des événements, il ne raisonne pas sur les événements mais sur
**l'état** : à chaque changement, et au moins une fois par seconde, il
parcourt toutes les partitions et corrige ce qui doit l'être. Ce style
(comparer l'état observé à l'état voulu, puis corriger) s'appelle une
boucle de **réconciliation**.

### Élire un nouveau leader

Pour chaque partition dont le leader est mort, le controller choisit le
**premier membre vivant de l'ISR** :

```text
avant :  leader = 2, epoch = 4, ISR = [2, 0, 1]      (le broker 2 meurt)
après :  leader = 0, epoch = 5, ISR = [0, 1]
```

Pourquoi l'ISR ? Ses membres ont tous les messages jusqu'au high
watermark, donc **tous les messages confirmés** (`acks=all`). N'importe
lequel peut reprendre sans perte.

L'**epoch** est incrémentée à chaque élection. Elle permet de distinguer
les leaders successifs : une requête ou une écriture qui porte une
ancienne epoch vient d'un leader déchu.

La mise à jour dans etcd est conditionnelle (révision de modification),
comme pour l'ISR au chapitre 7 : si le leader a modifié l'ISR entre-temps,
le controller recommence avec le nouvel état.

Le controller retire aussi les brokers morts des ISR, sans attendre que le
leader les juge en retard.

Le nouveau leader a tous les messages confirmés, mais son high watermark,
appris quand il était follower, peut avoir un échange de retard : les
derniers messages confirmés restent invisibles aux consumers pendant un
instant, jusqu'à ce que les followers aient refait un `Fetch` auprès de
lui. Kafka a le même comportement.

### Quand personne ne peut reprendre

Si aucun membre de l'ISR n'est vivant, la partition passe **hors ligne**
(leader `-1`). `Metadata` renvoie `LEADER_NOT_AVAILABLE` pour cette
partition, et les clients attendent. On garde la dernière ISR connue : le
premier de ses membres qui revient redevient leader.

Une réplique vivante hors de l'ISR pourrait reprendre, mais elle n'a pas
tous les messages confirmés : ce serait une perte de données. C'est
l'**élection impure** (`unclean.leader.election.enable`), désactivée par
défaut. Elle est disponible ici avec `Config::unclean_leader_election`.

### Côté brokers

Chaque broker applique les changements d'état comme au chapitre 7 :

- le nouveau leader commence à accepter les `Produce` et les `Fetch`. Les
  clients, qui reçoivent `NOT_LEADER_OR_FOLLOWER` de l'ancien leader ou ne
  peuvent plus le joindre, rafraîchissent leurs métadonnées et le trouvent ;
- les autres répliques arrêtent leur fetcher vers l'ancien leader et en
  démarrent un vers le nouveau.

### La troncature

Un ancien leader qui revient a peut-être des messages que personne d'autre
n'a : écrits juste avant sa mort, jamais répliqués, donc jamais confirmés
en `acks=all`. Le nouveau leader a pu écrire d'autres messages aux mêmes
offsets. Les logs ont **divergé** :

```text
offsets :            0   1   2   3
ancien leader (2)   [a] [b] [c] [x]      x : écrit en acks=1, jamais répliqué
nouveau leader (0)  [a] [b] [c] [y]      y : écrit après l'élection
high watermark = 3
```

Règle simple : quand une réplique devient follower d'un nouveau leader,
elle **tronque son log à son high watermark** avant de recopier le leader.
Tout ce qui précède le high watermark est commun à toutes les répliques ;
ce qui suit est soit recopié depuis le leader, soit perdu s'il n'avait
jamais été confirmé.

Le high watermark doit donc survivre à un redémarrage. Kafka le sauvegarde
régulièrement dans un fichier (`replication-offset-checkpoint`). On fait de
même, partition par partition (fichier `high-watermark`, toutes les
secondes). Un broker qui redémarre en mode cluster ne connaît pas encore
son rôle : il attend les métadonnées du cluster avant de servir.

Attention : une partition hors ligne (leader `-1`) ne fait pas de ses
répliques des followers. Ne tronquez pas dans ce cas : cette réplique est
peut-être la seule à avoir les données.

Cette règle a des failles connues en cas de pannes multiples rapprochées.
Kafka l'a remplacée (KIP-101, KIP-279) par une troncature fondée sur les
epochs : le follower demande au leader (`OffsetForLeaderEpoch`) où se
termine, dans le log du leader, l'epoch de ses derniers messages, et
tronque exactement là.

### Les consumer groups

Le coordinateur d'un groupe est le leader de sa partition de
`__consumer_offsets`. Quand il meurt, cette partition change de leader
comme les autres. Le nouveau leader relit la partition (elle est répliquée
depuis le chapitre 7) et devient coordinateur, avec tous les offsets
commités. Les consumers reçoivent `NOT_COORDINATOR`, refont un
`FindCoordinator`, puis rejoignent le groupe auprès du nouveau
coordinateur.

Un nouveau leader relit son log **jusqu'au log end offset**, pas jusqu'au
high watermark : son high watermark peut être en retard d'un échange avec
l'ancien leader, et tout ce qui est dans son log finira répliqué.

## Ce qui est fourni

- `Config::unclean_leader_election`.
- La sauvegarde périodique du high watermark (dans `Broker::start`) et sa
  relecture à l'ouverture d'une réplique.
- `stop_stale_fetchers` et `start_missing_fetchers` (`src/replication/mod.rs`).

## À faire

### 1. L'élection : `src/cluster/node.rs`

- `elect` : le nouvel état d'une partition, ou `None` s'il n'y a rien à
  changer. Des tests unitaires l'accompagnent :
  `cargo test --lib cluster::node`.
- `reconcile_partitions` : appliquer `elect` à toutes les partitions et
  écrire les changements dans etcd ; l'appeler dans `controller_loop` tant
  que ce broker est controller.

### 2. Les brokers

- `Replica::update_partition_state` : en plus de ce qu'elle faisait au
  chapitre 7, tronquer au high watermark quand la réplique devient follower
  d'un leader.

`on_cluster_change` (fourni) arrête les fetchers obsolètes, applique les
nouveaux rôles, puis démarre les nouveaux fetchers. L'ordre compte : sinon
un fetcher vers l'ancien leader pourrait écrire après la troncature.
- `Replica::checkpoint_high_watermark`.
- `Replica::read_all` lit jusqu'au log end offset.

## Vérifier

```bash
docker compose up -d etcd
cargo test --test ch08
```

Puis la démonstration complète, avec un producer qui tourne pendant qu'on
tue le leader :

```bash
BROKER_MIN_INSYNC_REPLICAS=2 scripts/cluster.sh start 3
alias kt='docker compose run --rm -T kafka-tools'
kt kafka-topics.sh --bootstrap-server localhost:9092 --create --topic payments \
    --partitions 1 --replication-factor 3
kt kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic payments   # notez le leader

# dans un autre terminal : 10 000 messages, 500 par seconde
kt kafka-producer-perf-test.sh --topic payments --num-records 10000 --record-size 100 \
    --throughput 500 --producer-props bootstrap.servers=localhost:9092,localhost:9093,localhost:9094 acks=all

# pendant que le producer tourne
scripts/cluster.sh kill <leader>

# à la fin
kt kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic payments \
    --group audit --from-beginning --timeout-ms 10000 | wc -l       # 10000
```

Le producer s'interrompt pendant quelques secondes (le temps que le lease
du leader expire), puis reprend avec le nouveau leader. Aucun message n'est
perdu. Dans les logs : `nouveau leader élu ... old=2 new=0 epoch=1`.

Relancez le broker tué (`scripts/cluster.sh restart <id>`) : il revient
comme follower, rattrape son retard et rentre dans l'ISR.

## Pour aller plus loin

- Rejouez la démonstration en `acks=1` et comptez les messages perdus.
- Tuez deux brokers sur trois avec `min.insync.replicas = 2` : que se
  passe-t-il pour les producers ? Et en relançant l'un d'eux ?
- Activez l'élection impure (`BROKER_UNCLEAN_LEADER_ELECTION=true`) et
  construisez un scénario qui perd des messages confirmés.
- Après un failover, les leaders ne sont plus équilibrés. Implémentez
  l'élection du **leader préféré** (la première réplique) quand il revient
  dans l'ISR (`auto.leader.rebalance.enable` dans Kafka).
