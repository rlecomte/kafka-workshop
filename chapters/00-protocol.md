# Chapitre 0 : réseau, framing et ApiVersions

Bienvenue dans ce premier chapitre ! Avant de stocker le moindre message, il
faut qu'un client Kafka puisse **parler** à votre broker. C'est ce que nous
allons construire ici : un serveur TCP qui comprend le protocole de Kafka et
répond à la toute première question de chaque client : « quelles API
supportes-tu ? ».

À la fin du chapitre, un vrai client (`kcat`) se connectera à votre broker
et négociera avec lui les versions du protocole.

## Concepts

### Un protocole binaire, requête-réponse, sur TCP

Un client Kafka ouvre une connexion TCP vers le broker et y envoie des
**requêtes**. Le broker répond à chacune par une **réponse**. Tout est
binaire, les entiers sont en big-endian.

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

Le corps de la requête suit immédiatement. Son format dépend du couple
(`api_key`, `api_version`).

La réponse commence par un **response header**, qui contient le
`correlation_id`, suivi du corps de la réponse, encodé dans **la même
version** que la requête.

Vous n'aurez pas à écrire l'encodage de ces structures : la crate
[`kafka-protocol`](https://docs.rs/kafka-protocol) les génère toutes à partir
des schémas officiels de Kafka.

### Les versions et ApiVersions

Chaque API évolue indépendamment, avec un numéro de version. Un broker et un
client de versions différentes doivent pouvoir se parler. D'où la toute
première requête de chaque connexion : **`ApiVersions`**. Le broker y répond
avec, pour chaque API qu'il supporte, la plage de versions qu'il accepte.
Pour chaque API, le client utilise ensuite la version la plus haute
supportée des deux côtés.

Si une API n'apparaît pas dans la réponse, le client considère que le broker
ne la supporte pas. C'est pratique pour nous : le broker n'annonce que ce
qu'il sait faire, et cette liste grandira à chaque chapitre.

## Ce qui est fourni

- `src/main.rs` : lecture de la configuration et démarrage du serveur.
- `src/lib.rs` : les structures `Config` et `Broker`.
- `src/apis.rs` : la liste des API supportées et leurs versions, avec des
  fonctions utilitaires. Vous y ajouterez une ligne à chaque nouvelle API.

## À faire

Nous allons construire le broker de bas en haut, en trois étapes. D'abord
la couche réseau, qui transforme un flux TCP en trames. Ensuite le
dispatch, qui décode chaque trame et l'aiguille vers le bon handler. Enfin
le premier handler, `ApiVersions`. Chaque étape a ses `todo!()` dans le
code, avec des indices en commentaire.

### 1. La couche réseau : `src/server.rs`

C'est la plomberie : accepter des connexions, puis lire et écrire des
trames. Rien de propre à Kafka à part le préfixe de taille, mais c'est sur
cette base que reposera tout le reste.

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

Chaque trame contient une requête. Le dispatch lit son header pour savoir
de quelle API il s'agit, décode le corps, appelle le handler correspondant,
puis emballe sa réponse. C'est le point d'entrée de toutes les API : vous y
ajouterez une ligne à chaque chapitre.

`Broker::handle_request` reçoit le contenu d'une trame et doit :

1. décoder le request header ;
2. vérifier que l'API et sa version sont supportées
   (`apis::is_supported`). Sinon, renvoyer une erreur : la connexion sera
   fermée. Une exception pour `ApiVersions`, décrite dans
   [« Sous le capot »](#sous-le-capot) ;
3. décoder le corps de la requête ;
4. appeler le bon handler ;
5. encoder le response header (même `correlation_id`) puis le corps de la
   réponse.

Fonctions utiles de la crate :

- `kafka_protocol::protocol::decode_request_header_from_buffer`
- `kafka_protocol::messages::RequestKind::decode`, qui décode n'importe
  quel corps de requête à partir de l'`ApiKey` et de la version
- `ResponseHeader`, `ResponseKind`, et les traits `Encodable` / `Decodable`
- `ApiKey::response_header_version`, qui donne la version du response header
  à utiliser

### 3. Le handler ApiVersions : `src/handlers/api_versions.rs`

Votre premier handler, et le plus simple : il ne fait que décrire le broker.
Il y a deux fonctions à écrire, qui construisent toutes les deux une
`ApiVersionsResponse`.

- `handle_api_versions` : la réponse normale. Son `error_code` vaut 0, et
  son champ `api_keys` contient une entrée par API de
  `apis::SUPPORTED_APIS`, avec sa plage de versions (`api_key`,
  `min_version`, `max_version`). Pour l'instant, la réponse dit donc
  seulement « je sais traiter `ApiVersions`, de la v0 à la v4 ». Comme elle
  est construite à partir de la liste, elle s'enrichira d'elle-même à chaque
  chapitre.
- `unsupported_version_response` : la même réponse, mais avec le code
  d'erreur `UNSUPPORTED_VERSION`
  (`kafka_protocol::ResponseError::UnsupportedVersion.code()`). La liste des
  API y figure aussi : c'est grâce à elle que le client peut réessayer dans
  une version que le broker accepte.

Le handler n'appelle pas cette seconde fonction. C'est le dispatch
(étape 2) qui l'utilise, quand un client envoie `ApiVersions` dans une
version trop récente : il ne décode pas la requête et encode cette réponse
en v0. Les détails sont dans [« Sous le capot »](#sous-le-capot).

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

## Sous le capot

Ces détails ne sont pas nécessaires pour comprendre Kafka, mais vous les
croiserez dans le code et dans les tests.

### Une version trop récente d'ApiVersions

Un client plus récent que le broker peut envoyer `ApiVersions` dans une
version que le broker ne connaît pas. Le broker ne peut pas décoder le
corps, mais il ne ferme pas la connexion pour autant : il répond avec le
code d'erreur `UNSUPPORTED_VERSION` (35) et la liste de ses API, le tout
encodé en **v0**, la version que tout client sait lire. Le client se rabat
alors sur une version commune. C'est à vous de gérer ce cas dans le
dispatch.

Pour toutes les autres API, une requête que le broker ne sait pas traiter
provoque la fermeture de la connexion.

### Les versions « flexibles »

À partir d'une certaine version (v3 pour ApiVersions, v9 pour Metadata...),
une API devient **flexible** : les chaînes et tableaux utilisent un encodage
plus compact, et chaque structure peut porter des champs optionnels (les
*tagged fields*), ajoutés sans changer de version. Les headers changent
aussi de version : v2 pour le request header, v1 pour le response header.

`kafka-protocol` gère tout cela, y compris le choix de la version du header
(`ApiKey::request_header_version`, `ApiKey::response_header_version`). Une
seule exception, qu'elle gère aussi : la réponse à `ApiVersions` a toujours
un response header v0, pour qu'un client puisse la lire avant de connaître
les versions du broker.

### Plusieurs requêtes sur une connexion

Un client peut envoyer plusieurs requêtes à la suite sans attendre les
réponses. Le broker doit alors répondre **dans l'ordre** des requêtes. Une
boucle qui lit une requête, répond, puis lit la suivante le garantit
d'elle-même.

## Pour aller plus loin

- Lancez le broker avec `RUST_LOG=debug` et observez la version
  d'`ApiVersions` qu'envoient différents clients (`kcat`, les outils Java de
  Kafka).
- Capturez le trafic avec Wireshark, qui sait décoder le protocole Kafka.
- Lisez le schéma JSON d'`ApiVersionsRequest` dans le dépôt Kafka et
  comparez-le aux types générés par `kafka-protocol`.
