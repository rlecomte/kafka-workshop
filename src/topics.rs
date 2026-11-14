//! Le registre des topics du broker.
//!
//! Pour l'instant, un topic n'est qu'un nom, un identifiant et une liste de
//! partitions vides. Les partitions recevront leur log au chapitre 2.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use kafka_protocol::ResponseError;
use uuid::Uuid;

/// Longueur maximale d'un nom de topic dans Kafka.
pub const MAX_TOPIC_NAME_LEN: usize = 249;

#[derive(Debug)]
pub struct Topic {
    pub name: String,
    /// Identifiant unique, attribué à la création (KIP-516). Il permet de
    /// distinguer deux topics successifs portant le même nom.
    pub id: Uuid,
    pub partitions: Vec<Partition>,
}

#[derive(Debug)]
pub struct Partition {
    pub index: i32,
}

/// Les raisons pour lesquelles une création de topic peut échouer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicError {
    InvalidName(String),
    InvalidPartitions(i32),
    InvalidReplicationFactor(i16),
    AlreadyExists(String),
}

impl TopicError {
    /// Le code d'erreur Kafka correspondant.
    pub fn error(&self) -> ResponseError {
        match self {
            TopicError::InvalidName(_) => ResponseError::InvalidTopicException,
            TopicError::InvalidPartitions(_) => ResponseError::InvalidPartitions,
            TopicError::InvalidReplicationFactor(_) => ResponseError::InvalidReplicationFactor,
            TopicError::AlreadyExists(_) => ResponseError::TopicAlreadyExists,
        }
    }
}

impl std::fmt::Display for TopicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicError::InvalidName(reason) => write!(f, "nom de topic invalide : {reason}"),
            TopicError::InvalidPartitions(n) => write!(f, "nombre de partitions invalide : {n}"),
            TopicError::InvalidReplicationFactor(n) => {
                write!(f, "facteur de réplication invalide : {n} (un seul broker disponible)")
            }
            TopicError::AlreadyExists(name) => write!(f, "le topic {name} existe déjà"),
        }
    }
}

impl std::error::Error for TopicError {}

/// Les paramètres d'une création de topic, après résolution des valeurs par
/// défaut (-1 dans la requête).
#[derive(Debug, Clone)]
pub struct NewTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
}

/// Le registre des topics, partagé entre toutes les connexions.
///
/// Un `BTreeMap` garde les topics triés par nom, ce qui rend les listings
/// stables. Le verrou est un `std::sync::RwLock` : on ne le garde jamais
/// pendant un `.await`, donc pas besoin d'un verrou asynchrone.
#[derive(Debug, Default)]
pub struct Topics {
    topics: RwLock<BTreeMap<String, Arc<Topic>>>,
}

impl Topics {
    /// Vérifie qu'un topic peut être créé, et le crée sauf si `validate_only`.
    /// Renvoie le topic (créé, ou tel qu'il aurait été créé).
    pub fn create(&self, new: NewTopic, validate_only: bool) -> Result<Arc<Topic>, TopicError> {
        todo!("ch.1 : create")
    }

    pub fn get(&self, name: &str) -> Option<Arc<Topic>> {
        todo!("ch.1 : get")
    }

    pub fn get_by_id(&self, id: Uuid) -> Option<Arc<Topic>> {
        todo!("ch.1 : get_by_id")
    }

    /// Tous les topics, triés par nom.
    pub fn list(&self) -> Vec<Arc<Topic>> {
        todo!("ch.1 : list")
    }
}

/// Règles de nommage de Kafka : 1 à 249 caractères parmi `[a-zA-Z0-9._-]`,
/// et ni `.` ni `..`.
pub fn validate_name(name: &str) -> Result<(), TopicError> {
    todo!("ch.1 : validate_name")
}
