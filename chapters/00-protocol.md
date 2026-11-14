# Chapitre 0 : réseau, framing et ApiVersions

**Objectif :** un vrai client Kafka se connecte à votre broker et négocie
les versions du protocole.

## Concepts

### Un protocole binaire, requête-réponse, sur TCP

Un client Kafka ouvre une connexion TCP vers le broker et y envoie des
**requêtes**. Le broker répond à chacune par une **réponse**. Tout est
binaire, les entiers sont en big-endian.

Le client peut envoyer plusieurs requêtes sans attendre les réponses
(*pipelining*). Le broker doit répondre **dans l'ordre** des requêtes sur une
même connexion.

### Le framing

TCP transporte un flux d'octets, pas des messages : une requête peut
arriver en plusieurs morceaux, ou deux requêtes dans le même paquet. Kafka
découpe le flux en **trames**, préfixées par leur taille :

```text
+----------------+---------------------------------+
| size: i32 (BE) | payload: [u8; size]             |
+----------------+---------------------------------+
```

Pour lire une trame : lire exactement 4 octets, les interpréter comme un
`i32`, puis lire exactement `size` octets.

### Le header de requête

Le contenu d'une trame de requête commence par un **request header** :

| Champ | Type | Rôle |
|---|---|---|
| `request_api_key` | `i16` | Quelle API : 0 = Produce, 1 = Fetch, 3 = Metadata, 18 = ApiVersions... |
| `request_api_version` | `i16` | Version de cette API utilisée pour encoder le corps |
| `correlation_id` | `i32` | Identifiant choisi par le client, renvoyé tel quel dans la réponse |
| `client_id` | `string` (nullable) | Nom du client, utile pour les logs |
| *tagged fields* | | Uniquement dans le header v2 |

Le corps de la requête suit immédiatement. Son format dépend du couple
(`api_key`, `api_version`).

La réponse commence par un **response header**, qui contient le
`correlation_id` (et des tagged fields en v1), suivi du corps de la réponse,
encodé dans **la même version** que la requête.

### Les versions

Chaque API évolue indépendamment, avec un numéro de version. Un broker et un
client de versions différentes doivent pouvoir se parler. D'où la toute
première requête de chaque connexion : **`ApiVersions`**. Le broker y répond
avec, pour chaque API qu'il supporte, la plage de versions qu'il accepte.
Pour chaque API, le client utilise ensuite la version la plus haute
supportée des deux côtés.

Si une API n'apparaît pas dans la réponse, le client considère que le broker
ne la supporte pas.

### Versions « flexibles »

À partir d'une certaine version (v3 pour ApiVersions, v9 pour Metadata...),
une API devient **flexible** : les chaînes et tableaux utilisent un encodage
compact (longueur en varint), et chaque structure se termine par des
**tagged fields**, des champs optionnels qui permettent d'ajouter des données
sans changer de version. Le header de requête passe alors en v2 et celui de
réponse en v1.

Vous n'aurez pas à écrire cet encodage : la crate
[`kafka-protocol`](https://docs.rs/kafka-protocol) génère tous les types à
partir des schémas officiels et s'en charge. Elle choisit aussi la bonne
version de header pour chaque API et chaque version :
`ApiKey::request_header_version` et `ApiKey::response_header_version`.

### Le cas particulier d'ApiVersions

Deux règles spéciales s'appliquent à `ApiVersions` :

- **Le response header est toujours en v0**, même pour les versions
  flexibles. Sinon un client ne pourrait pas lire la réponse avant de savoir
  quelles versions le broker supporte. `kafka-protocol` le gère pour vous.
- **Une version trop récente ne ferme pas la connexion.** Si un client
  envoie `ApiVersions` dans une version que le broker ne connaît pas, le
  broker répond quand même, avec le code d'erreur `UNSUPPORTED_VERSION` (35)
  et la liste de ses API, le tout encodé en v0. Le client se rabat alors sur
  une version commune. C'est à vous de gérer ce cas.

Pour toutes les autres API, une requête que le broker ne sait pas traiter
provoque la fermeture de la connexion.

## Ce qui est fourni

- `src/main.rs` : lecture de la configuration et démarrage du serveur.
- `src/lib.rs` : les structures `Config` et `Broker`.
- `src/apis.rs` : la liste des API supportées et leurs versions, avec des
  fonctions utilitaires. Vous y ajouterez une ligne à chaque nouvelle API.

## À faire

### 1. La couche réseau : `src/server.rs`

- `serve` : accepter les connexions en boucle et traiter chacune dans sa
  propre tâche `tokio`. Une connexion lente ou bloquée ne doit pas empêcher
  les autres d'être servies.
- `handle_connection` : boucler sur lire une trame, la passer à
  `Broker::handle_request`, écrire la réponse.
- `read_frame` et `write_frame` : le framing décrit plus haut.
  - Un EOF avant le premier octet signifie que le client a fermé
    proprement la connexion : renvoyez `Ok(None)`.
  - Refusez les tailles négatives ou supérieures à `MAX_FRAME_SIZE`.

### 2. Le dispatch : `src/handlers/mod.rs`

`Broker::handle_request` reçoit le contenu d'une trame et doit :

1. décoder le request header ;
2. vérifier que l'API et sa version sont supportées (`apis::is_supported`),
   en gérant le cas particulier d'`ApiVersions` ;
3. décoder le corps de la requête ;
4. appeler le bon handler ;
5. encoder le response header (même `correlation_id`) puis le corps de la
   réponse.

Fonctions utiles de la crate :

- `kafka_protocol::protocol::decode_request_header_from_buffer`
- `kafka_protocol::messages::RequestKind::decode`, qui décode n'importe
  quel corps de requête à partir de l'`ApiKey` et de la version
- `ResponseHeader`, `ResponseKind`, et les traits `Encodable` / `Decodable`
- `ApiKey::response_header_version`

### 3. Le handler ApiVersions : `src/handlers/api_versions.rs`

Construire la réponse à partir de `apis::SUPPORTED_APIS`, ainsi que la
réponse d'erreur `UNSUPPORTED_VERSION`. Le code d'erreur s'obtient avec
`kafka_protocol::ResponseError::UnsupportedVersion.code()`.

## Vérifier

```bash
cargo test --test ch00
```

Puis avec un vrai client :

```bash
RUST_LOG=debug cargo run
# dans un autre terminal
kcat -b localhost:9092 -L
# ou, sans installer kcat :
docker compose run --rm kcat -b localhost:9092 -L
```

`kcat` doit échouer avec :

```text
Metadata request failed: ... Required feature not supported by broker
```

C'est le résultat attendu. Dans les logs du broker, vous voyez passer la
requête `ApiVersions` de librdkafka. `kcat` a bien lu la réponse, mais il
constate que le broker n'annonce pas l'API `Metadata` et abandonne. Ce sera
l'objet du chapitre 1.

## Pour aller plus loin

- Lancez le broker avec `RUST_LOG=debug` et observez la version
  d'`ApiVersions` qu'envoient différents clients (`kcat`, les outils Java de
  Kafka).
- Capturez le trafic avec Wireshark, qui sait décoder le protocole Kafka.
- Lisez le schéma JSON d'`ApiVersionsRequest` dans le dépôt Kafka et
  comparez-le aux types générés par `kafka-protocol`.
