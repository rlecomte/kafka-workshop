//! La couche réseau : accepter des connexions TCP et découper le flux
//! d'octets en trames Kafka.
//!
//! Une trame Kafka est un entier signé big-endian sur 4 octets (la taille),
//! suivi de exactement `taille` octets de contenu :
//!
//! ```text
//! +----------------+---------------------------------+
//! | size: i32 (BE) | payload: [u8; size]             |
//! +----------------+---------------------------------+
//! ```

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};

use crate::Broker;

/// Taille maximale acceptée pour une trame (100 Mio, comme
/// `socket.request.max.bytes` dans Kafka). Au-delà, on coupe la connexion
/// plutôt que d'allouer n'importe quoi.
pub const MAX_FRAME_SIZE: usize = 100 * 1024 * 1024;

/// Accepte les connexions en boucle et traite chacune dans sa propre tâche tokio.
///
/// Indices :
/// - `listener.accept()` renvoie la socket et l'adresse du client ;
/// - `tokio::spawn` lance une tâche concurrente ;
/// - une erreur sur une connexion ne doit pas arrêter le serveur : loggez-la
///   (`tracing::warn!`) et passez à la suite.
pub async fn serve(listener: TcpListener, broker: Arc<Broker>) -> Result<()> {
    let _ = (listener, broker);
    todo!("ch.0 : accepter les connexions")
}

/// Traite une connexion : lit une requête, calcule la réponse avec
/// [`Broker::handle_request`], l'écrit, et recommence jusqu'à ce que le
/// client ferme la connexion.
///
/// Les réponses doivent partir dans l'ordre des requêtes : c'est ce qui
/// permet aux clients d'envoyer plusieurs requêtes d'affilée (pipelining).
#[allow(dead_code)]
async fn handle_connection(socket: TcpStream, broker: Arc<Broker>) -> Result<()> {
    let _ = (socket, broker);
    todo!("ch.0 : boucle requête-réponse")
}

/// Lit une trame complète. Renvoie `Ok(None)` si le client a fermé
/// proprement la connexion (EOF avant le premier octet de taille).
///
/// Indices :
/// - `tokio::io::AsyncReadExt` fournit `read_i32` (big-endian) et `read_exact` ;
/// - un EOF se traduit par une erreur de kind `std::io::ErrorKind::UnexpectedEof` ;
/// - refusez les tailles négatives ou supérieures à [`MAX_FRAME_SIZE`].
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Bytes>> {
    let _ = reader;
    todo!("ch.0 : lire une trame")
}

/// Écrit une trame : la taille sur 4 octets, puis le contenu.
///
/// Indice : `tokio::io::AsyncWriteExt` fournit `write_i32`, `write_all` et `flush`.
pub async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, payload: &[u8]) -> Result<()> {
    let _ = (writer, payload);
    todo!("ch.0 : écrire une trame")
}
