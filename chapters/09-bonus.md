# Bonus : pour aller plus loin

Ces sujets n'ont ni squelette ni tests : à vous de concevoir. Chacun indique
les concepts en jeu et les pièges à éviter. Partez de `solution/ch08`.

## Producer idempotent

Depuis le chapitre 2, `InitProducerId` distribue des identifiants, mais le
broker ignore les numéros de séquence. Un producer qui renvoie un batch
après un timeout peut donc l'écrire deux fois.

- Pour chaque partition et chaque `producer_id`, gardez l'epoch et la
  séquence des derniers batches écrits (Kafka en garde 5, autant que de
  requêtes en vol autorisées).
- Un batch déjà vu est un doublon : répondez **succès**, avec l'offset du
  batch original, sans le réécrire.
- Une séquence qui saute est une erreur : `OUT_OF_ORDER_SEQUENCE_NUMBER`.
  Une epoch plus ancienne que la dernière vue : `INVALID_PRODUCER_EPOCH`.
- Cet état doit survivre aux redémarrages et aux failovers : il peut être
  reconstruit en relisant les en-têtes des batches du log (`producer_id`,
  `producer_epoch`, `base_sequence`, `last_offset_delta`). Kafka ajoute des
  instantanés (`.snapshot`) pour ne pas tout relire.

Test : `kafka-producer-perf-test.sh` avec
`enable.idempotence=true` pendant que vous tuez le leader, puis comptez les
doublons.

## Compaction

Un topic compacté (`cleanup.policy=compact`) ne garde que le **dernier
record de chaque clé**. Un record dont la valeur est nulle (un
*tombstone*) supprime la clé. C'est ce dont `__consumer_offsets` a besoin,
au lieu de grossir indéfiniment.

- La compaction réécrit les segments **inactifs** : pour chaque record, on
  le garde s'il est le plus récent de sa clé (une table clé → dernier
  offset, construite en lisant le log).
- Les offsets ne changent pas : un log compacté a des trous, et les
  consumers savent les sauter. Les batches réécrits gardent leur
  `base_offset` mais peuvent avoir moins de records.
- Il faut cette fois décoder les records (et les recompresser) :
  `kafka_protocol::records` sait le faire.
- Les tombstones eux-mêmes doivent disparaître après un délai
  (`delete.retention.ms`), sinon la clé ne disparaît jamais vraiment.
- Le remplacement d'un segment doit être atomique : écrivez un fichier
  `.cleaned`, puis renommez-le.

## Configuration des topics

Les `configs` de `CreateTopics` sont ignorées. Stockez-les (dans
`TopicRecord` en mode cluster, dans un fichier sinon) et utilisez-les :
`retention.ms`, `retention.bytes`, `segment.bytes`, `cleanup.policy`,
`min.insync.replicas`. Implémentez `DescribeConfigs` pour de vrai, puis
`IncrementalAlterConfigs` (`kafka-configs.sh --alter`).

## Suppression de topics

`DeleteTopics` (`kafka-topics.sh --delete`) : supprimez les clés du topic
dans etcd (transaction), puis chaque broker supprime ses répliques locales
quand le watch le lui apprend. Attention aux fetchers en cours et aux
clients qui produisent encore. Que se passe-t-il si on recrée un topic du
même nom juste après ? C'est le rôle du `topic_id`.

## Leader préféré

Après un failover, un broker relancé revient comme follower : les leaders
se concentrent sur les brokers restés vivants. Le **leader préféré** d'une
partition est sa première réplique. Quand il est vivant et dans l'ISR, le
controller peut lui rendre la main (epoch + 1). Kafka le fait
périodiquement (`auto.leader.rebalance.enable`), au-delà d'un seuil de
déséquilibre, pour éviter des transferts trop fréquents.

## Troncature par epoch (KIP-101)

Remplacez la troncature au high watermark du chapitre 8 par celle de
Kafka :

- chaque log garde un petit cache « epoch → premier offset de cette epoch »
  (fichier `leader-epoch-checkpoint`), alimenté par le champ
  `partition_leader_epoch` des batches ;
- un nouveau follower envoie `OffsetForLeaderEpoch` au leader avec l'epoch
  de son dernier batch ; le leader répond où cette epoch se termine dans
  son log ;
- le follower tronque exactement là.

Cherchez un scénario de pannes multiples où la troncature au high
watermark perd ou corrompt des données, et vérifiez que KIP-101 le corrige.

## Fetch sessions (KIP-227)

Un consumer qui lit des centaines de partitions envoie leur liste
complète à chaque `Fetch`. Avec une session, il n'envoie que les
changements, et le broker ne renvoie que les partitions qui ont de
nouvelles données. Implémentez le cache de sessions côté broker
(`session_id`, `session_epoch`, `forgotten_topics_data`).

## Regrouper les fetchers

Un fetcher par partition, c'est une connexion et une boucle par partition.
Kafka utilise un fetcher par **broker leader**, qui demande toutes les
partitions de ce leader dans un seul `Fetch`. Regroupez les vôtres, et
mesurez la différence avec quelques centaines de partitions.

## Observabilité

- Des métriques Prometheus : débit par topic, taille des logs, retard des
  followers, taille des ISR, nombre de partitions hors ligne.
- Branchez une console graphique sur votre cluster et listez les API
  qu'elle utilise et qui vous manquent (`DescribeCluster`,
  `DescribeLogDirs`, `DescribeConsumerGroupOffsets`...).

## Zero-copy

Kafka envoie les données d'un `Fetch` avec `sendfile` : du fichier vers la
socket, sans passer par la mémoire de l'application. Avec tokio, c'est
possible en écrivant l'en-tête de la réponse, puis en appelant `sendfile`
(crate `nix`) sur la socket brute. Il faut alors calculer la taille de la
réponse avant d'avoir lu les données. Mesurez le gain.
