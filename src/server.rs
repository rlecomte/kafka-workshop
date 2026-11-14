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

use anyhow::{Result, bail};
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::Broker;

/// Taille maximale acceptée pour une trame (100 Mio, comme
/// `socket.request.max.bytes` dans Kafka). Au-delà, on coupe la connexion
/// plutôt que d'allouer n'importe quoi.
pub const MAX_FRAME_SIZE: usize = 100 * 1024 * 1024;

/// Accepte les connexions en boucle et traite chacune dans sa propre tâche tokio.
pub async fn serve(listener: TcpListener, broker: Arc<Broker>) -> Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        tracing::debug!(%peer, "nouvelle connexion");
        let broker = broker.clone();
        tokio::spawn(async move {
            match handle_connection(socket, broker).await {
                Ok(()) => tracing::debug!(%peer, "connexion fermée par le client"),
                Err(e) => tracing::warn!(%peer, "connexion fermée : {e:#}"),
            }
        });
    }
}

/// Traite une connexion : lit une requête, calcule la réponse, l'écrit, et
/// recommence jusqu'à ce que le client ferme la connexion.
///
/// Les réponses doivent partir dans l'ordre des requêtes : c'est ce qui
/// permet aux clients d'envoyer plusieurs requêtes d'affilée (pipelining).
async fn handle_connection(mut socket: TcpStream, broker: Arc<Broker>) -> Result<()> {
    socket.set_nodelay(true)?;
    while let Some(frame) = read_frame(&mut socket).await? {
        let response = broker.handle_request(frame).await?;
        write_frame(&mut socket, &response).await?;
    }
    Ok(())
}

/// Lit une trame complète. Renvoie `Ok(None)` si le client a fermé
/// proprement la connexion (EOF avant le premier octet de taille).
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Bytes>> {
    let size = match reader.read_i32().await {
        Ok(size) => size,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if size < 0 || size as usize > MAX_FRAME_SIZE {
        bail!("taille de trame invalide : {size}");
    }
    let mut payload = vec![0u8; size as usize];
    reader.read_exact(&mut payload).await?;
    Ok(Some(Bytes::from(payload)))
}

/// Écrit une trame : la taille sur 4 octets, puis le contenu.
pub async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, payload: &[u8]) -> Result<()> {
    let size = i32::try_from(payload.len())?;
    writer.write_i32(size).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}
