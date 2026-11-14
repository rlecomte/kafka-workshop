# Kafka Workshop : écrire son propre broker Kafka en Rust

Ce workshop consiste à implémenter, chapitre par chapitre, un broker Kafka
**basique mais fonctionnel**. Le but est pédagogique : comprendre ce qui se
passe à l'intérieur de Kafka en le reconstruisant, pas produire un broker
prêt pour la production.

Le broker parle le **vrai protocole Kafka**. À chaque étape, vous le testez
avec de vrais clients : `kcat`, `kafka-console-producer.sh`,
`kafka-console-consumer.sh`, `kafka-consumer-groups.sh`...

On ne réimplémente pas d'algorithme de consensus : la coordination du
cluster (brokers vivants, élection du controller, état des partitions) est
déléguée à **etcd**. C'est l'architecture historique de Kafka, avec etcd à la
place de ZooKeeper.

Comptez quelques semaines à votre rythme.

## Les chapitres

| # | Chapitre | Ce que vous obtenez |
|---|---|---|
| 0 | [Réseau, framing et ApiVersions](chapters/00-protocol.md) | Un client Kafka se connecte et négocie les versions |
| 1 | [Métadonnées et création de topics](chapters/01-metadata.md) | `kafka-topics.sh --create`, `--list` et `--describe` |
| 2 | [Produce et le format RecordBatch](chapters/02-produce.md) | `kafka-console-producer.sh` écrit sur disque |
| 3 | [Fetch et ListOffsets](chapters/03-fetch.md) | `kafka-console-consumer.sh` et `kcat` lisent les messages |
| 4 | [Segments, index, recovery et rétention](chapters/04-storage.md) | Les données survivent aux redémarrages et aux arrêts brutaux |
| 5 | [Consumer groups](chapters/05-consumer-groups.md) | Des consumers se partagent les partitions et reprennent où ils en étaient |
| 6 | [Un cluster coordonné par etcd](chapters/06-cluster.md) | 3 brokers, des partitions réparties, un controller élu |
| 7 | [Réplication, high watermark et ISR](chapters/07-replication.md) | `acks=all`, chaque message copié sur plusieurs brokers |
| 8 | [Failover](chapters/08-failover.md) | `kill -9` sur un leader : aucun message confirmé perdu |
| + | [Bonus](chapters/09-bonus.md) | Idempotence, compaction, troncature par epoch... |

## Prérequis

- Rust stable récent (`rustup update`).
- Docker et Docker Compose, pour etcd (à partir du chapitre 6) et pour les
  clients Kafka (`kcat`, les outils officiels) si vous ne les installez pas.
- Être à l'aise avec Kafka côté utilisateur : topics, partitions, offsets,
  consumer groups.
- La pratique de `tokio` aide, sans être indispensable.

## Organisation du dépôt

```text
├── chapters/          énoncés des chapitres
├── src/               le broker
├── tests/             tests d'acceptation, un fichier par chapitre
├── scripts/cluster.sh lance un cluster local (chapitres 6 à 8)
└── docker-compose.yml etcd, kcat, outils Kafka
```

### Les branches

| Branche | Contenu |
|---|---|
| `main` | Le point de départ : le squelette du chapitre 0. |
| `chapitre/chNN` | Le point de départ du chapitre NN : les chapitres précédents sont résolus, le travail du chapitre est marqué `todo!()`. Contient les tests jusqu'au chapitre NN. |
| `solution/chNN` | Une solution complète jusqu'au chapitre NN. |

Les branches forment une chaîne :
`chapitre/ch00` → `solution/ch00` → `chapitre/ch01` → `solution/ch01` → ...

Commandes utiles :

```bash
git diff chapitre/ch03 solution/ch03 -- src/   # la solution du chapitre 3
git diff solution/ch02 chapitre/ch03 -- src/   # ce que le chapitre 3 ajoute au squelette
```

Le code évolue d'un chapitre à l'autre : un chapitre peut faire évoluer une
fonction écrite dans un chapitre précédent (par exemple, le log du
chapitre 2 apprend à gérer plusieurs segments au chapitre 4). L'énoncé le
précise à chaque fois.

## Déroulé

1. Lisez l'énoncé du chapitre dans `chapters/`.
2. Partez de votre code, ou de la branche `chapitre/chNN` :
   `git checkout -b mon-ch03 chapitre/ch03`.
3. Remplacez les `todo!()` (cherchez `todo!("ch.3`).
4. Lancez les tests du chapitre : `cargo test --test ch03`. Ajoutez
   `RUST_LOG=debug` pour voir les logs du broker, et `-- --nocapture` pour
   les afficher.
5. Vérifiez avec de vrais clients : `cargo run`, puis les commandes
   indiquées dans l'énoncé.
6. Lancez tous les tests (`cargo test`) : les chapitres précédents doivent
   toujours passer.

Si vous êtes bloqué, comparez avec `solution/chNN`, ou repartez de
`chapitre/chNN+1` pour le chapitre suivant.

Pour garder votre propre code d'un chapitre à l'autre, récupérez les
nouveaux tests et fichiers du chapitre suivant
(`git checkout chapitre/ch04 -- tests/`, puis les nouveaux fichiers de
`src/`) et intégrez les évolutions décrites dans l'énoncé.

## Lancer le broker

```bash
cargo run                                      # broker autonome, port 9092
RUST_LOG=debug cargo run                       # logs détaillés
BROKER_PORT=9093 BROKER_DATA_DIR=data-2 cargo run

docker compose up -d etcd                      # à partir du chapitre 6
scripts/cluster.sh start 3                     # cluster de 3 brokers (9092 à 9094)
```

La configuration passe par des variables d'environnement (`BROKER_*`), voir
`src/main.rs`.

Les clients Kafka, sans rien installer :

```bash
docker compose run --rm kcat -b localhost:9092 -L
docker compose run --rm -T kafka-tools kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## Références

- [Le guide du protocole Kafka](https://kafka.apache.org/protocol.html)
- [Les schémas JSON des messages](https://github.com/apache/kafka/tree/trunk/clients/src/main/resources/common/message),
  à partir desquels la crate `kafka-protocol` est générée
- [La documentation de `kafka-protocol`](https://docs.rs/kafka-protocol)
- [La documentation de Kafka](https://kafka.apache.org/documentation/),
  en particulier les sections *Design* et *Implementation*
- [L'API v3 d'etcd](https://etcd.io/docs/v3.5/learning/api/)
