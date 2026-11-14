//! Un consumer group et sa machine à états (protocole « classic »).
//!
//! ```text
//!            JoinGroup                tous les membres         SyncGroup
//!           (1er membre)              ont rejoint              du leader
//!   Empty ───────────────▶ Preparing ─────────────▶ Completing ─────────▶ Stable
//!     ▲                    Rebalance                Rebalance               │
//!     │                        ▲                        │                   │
//!     │  dernier membre parti  │  JoinGroup, LeaveGroup, session expirée   │
//!     └────────────────────────┴────────────────────────┴───────────────────┘
//! ```
//!
//! - **PreparingRebalance** : on attend que tous les membres connus
//!   renvoient un `JoinGroup` (au plus le `rebalance_timeout`). Les
//!   `JoinGroup` restent en attente de réponse.
//! - **CompletingRebalance** : la génération a été incrémentée, chaque membre
//!   a reçu sa réponse `JoinGroup`, le leader a reçu la liste des membres. On
//!   attend que le leader envoie l'assignation des partitions (`SyncGroup`).
//! - **Stable** : chaque membre connaît ses partitions.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::ResponseError;
use tokio::sync::oneshot;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

impl GroupState {
    /// Le nom de l'état tel que Kafka l'affiche (`DescribeGroups`, `ListGroups`).
    pub fn name(&self) -> &'static str {
        match self {
            GroupState::Empty => "Empty",
            GroupState::PreparingRebalance => "PreparingRebalance",
            GroupState::CompletingRebalance => "CompletingRebalance",
            GroupState::Stable => "Stable",
        }
    }
}

/// Paramètres des groupes.
#[derive(Debug, Clone)]
pub struct GroupConfig {
    /// Délai avant le premier rebalance d'un groupe vide, pour laisser le temps
    /// aux autres membres d'arriver (`group.initial.rebalance.delay.ms`).
    pub initial_rebalance_delay: Duration,
    /// Bornes acceptées pour le `session_timeout_ms` des membres
    /// (`group.min.session.timeout.ms`, `group.max.session.timeout.ms`).
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
}

impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            initial_rebalance_delay: Duration::from_secs(3),
            min_session_timeout: Duration::from_secs(6),
            max_session_timeout: Duration::from_secs(1800),
        }
    }
}

/// La réponse à un `JoinGroup`.
#[derive(Debug, Clone, Default)]
pub struct JoinResult {
    pub error: Option<ResponseError>,
    pub generation_id: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader: String,
    pub member_id: String,
    /// Les membres et leurs métadonnées : uniquement dans la réponse du leader.
    pub members: Vec<(String, Bytes)>,
}

impl JoinResult {
    pub fn error(error: ResponseError, member_id: &str) -> Self {
        JoinResult { error: Some(error), generation_id: -1, member_id: member_id.to_string(), ..Default::default() }
    }
}

/// La réponse à un `SyncGroup`.
#[derive(Debug, Clone, Default)]
pub struct SyncResult {
    pub error: Option<ResponseError>,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub assignment: Bytes,
}

impl SyncResult {
    pub fn error(error: ResponseError) -> Self {
        SyncResult { error: Some(error), ..Default::default() }
    }
}

/// Soit une réponse immédiate, soit une réponse qui arrivera plus tard.
#[derive(Debug)]
pub enum Outcome<T> {
    Now(T),
    Later(oneshot::Receiver<T>),
}

/// Une demande d'entrée dans le groupe (le contenu d'un `JoinGroup`).
#[derive(Debug, Clone)]
pub struct JoinRequest {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    pub protocol_type: String,
    /// Les stratégies d'assignation proposées par le membre, par ordre de
    /// préférence, avec leurs métadonnées (opaques pour le broker).
    pub protocols: Vec<(String, Bytes)>,
    /// À partir de la v4, un nouveau membre doit d'abord obtenir un
    /// identifiant (`MEMBER_ID_REQUIRED`) puis renvoyer sa demande avec.
    pub require_known_member_id: bool,
}

#[derive(Debug)]
pub struct Member {
    pub id: String,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    pub protocols: Vec<(String, Bytes)>,
    /// L'assignation reçue du leader (opaque pour le broker).
    pub assignment: Bytes,
    /// Dernier signe de vie (JoinGroup, SyncGroup, Heartbeat, OffsetCommit).
    last_seen: Instant,
    join_waiter: Option<oneshot::Sender<JoinResult>>,
    sync_waiter: Option<oneshot::Sender<SyncResult>>,
}

impl Member {
    /// Les métadonnées du membre pour un protocole donné.
    pub fn metadata(&self, protocol: &str) -> Bytes {
        self.protocols.iter().find(|(name, _)| name == protocol).map(|(_, m)| m.clone()).unwrap_or_default()
    }
}

/// Un offset commité.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOffset {
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: String,
}

#[derive(Debug)]
pub struct Group {
    pub id: String,
    pub state: GroupState,
    pub generation_id: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader: Option<String>,
    pub members: BTreeMap<String, Member>,
    /// Identifiants donnés via `MEMBER_ID_REQUIRED`, pas encore utilisés,
    /// avec leur date d'expiration.
    pending_members: HashMap<String, Instant>,
    /// Fin de l'attente des `JoinGroup` en PreparingRebalance.
    rebalance_deadline: Option<Instant>,
    /// Premier rebalance d'un groupe vide : on attend toujours jusqu'à la
    /// fin du délai initial, même si tous les membres connus ont rejoint.
    initial_rebalance: bool,
    /// Les offsets commités, par (topic, partition).
    pub offsets: HashMap<(String, i32), CommittedOffset>,
}

impl Group {
    pub fn new(id: &str) -> Group {
        Group {
            id: id.to_string(),
            state: GroupState::Empty,
            generation_id: 0,
            protocol_type: None,
            protocol_name: None,
            leader: None,
            members: BTreeMap::new(),
            pending_members: HashMap::new(),
            rebalance_deadline: None,
            initial_rebalance: false,
            offsets: HashMap::new(),
        }
    }

    /// Le groupe peut-il être oublié (ni membre, ni offset) ?
    pub fn is_dead(&self) -> bool {
        self.state == GroupState::Empty && self.members.is_empty() && self.pending_members.is_empty() && self.offsets.is_empty()
    }

    // -----------------------------------------------------------------------
    // JoinGroup
    // -----------------------------------------------------------------------

    pub fn join(&mut self, request: JoinRequest, config: &GroupConfig, now: Instant) -> Outcome<JoinResult> {
        todo!("ch.5 : join")
    }

    /// Le protocole d'un nouveau membre doit être compatible avec le groupe :
    /// même type, et au moins une stratégie en commun avec tous les membres.
    fn is_compatible(&self, request: &JoinRequest) -> bool {
        if request.protocols.is_empty() || request.protocol_type.is_empty() {
            return false;
        }
        // Seuls les autres membres comptent (un membre peut changer ses
        // propres stratégies en rejoignant).
        if self.members.keys().all(|id| *id == request.member_id) {
            return true;
        }
        if self.protocol_type.as_deref() != Some(request.protocol_type.as_str()) {
            return false;
        }
        let candidates = self.candidate_protocols(Some(&request.member_id));
        request.protocols.iter().any(|(name, _)| candidates.contains(name))
    }

    /// Les stratégies supportées par tous les membres (sauf `except`).
    fn candidate_protocols(&self, except: Option<&str>) -> Vec<String> {
        let mut members = self.members.values().filter(|m| Some(m.id.as_str()) != except);
        let Some(first) = members.next() else { return Vec::new() };
        let mut candidates: Vec<String> = first.protocols.iter().map(|(n, _)| n.clone()).collect();
        for member in members {
            candidates.retain(|c| member.protocols.iter().any(|(n, _)| n == c));
        }
        candidates
    }

    /// Démarre un rebalance : on attend que tous les membres renvoient un
    /// JoinGroup. Les SyncGroup en attente sont annulés.
    fn prepare_rebalance(&mut self, config: &GroupConfig, now: Instant) {
        todo!("ch.5 : prepare_rebalance")
    }

    /// Termine la phase de join si tous les membres ont rejoint.
    fn maybe_complete_join(&mut self, now: Instant) {
        todo!("ch.5 : maybe_complete_join")
    }

    /// Fin de la phase de join : les membres qui n'ont pas rejoint sont
    /// exclus, une nouvelle génération commence, et chaque membre reçoit sa
    /// réponse JoinGroup.
    fn complete_join(&mut self, now: Instant) {
        todo!("ch.5 : complete_join")
    }

    /// Chaque membre vote pour sa stratégie préférée parmi celles supportées
    /// par tous ; la plus populaire l'emporte.
    fn select_protocol(&self) -> String {
        todo!("ch.5 : select_protocol")
    }

    // -----------------------------------------------------------------------
    // SyncGroup
    // -----------------------------------------------------------------------

    pub fn sync(
        &mut self,
        member_id: &str,
        generation_id: i32,
        assignments: Vec<(String, Bytes)>,
        now: Instant,
    ) -> Outcome<SyncResult> {
        todo!("ch.5 : sync")
    }

    fn sync_result(&self, member_id: &str) -> SyncResult {
        SyncResult {
            error: None,
            protocol_type: self.protocol_type.clone(),
            protocol_name: self.protocol_name.clone(),
            assignment: self.members[member_id].assignment.clone(),
        }
    }

    // -----------------------------------------------------------------------
    // Heartbeat, LeaveGroup, OffsetCommit
    // -----------------------------------------------------------------------

    /// Vérifie qu'un membre existe et qu'il est dans la génération courante.
    fn check_member(&self, member_id: &str, generation_id: i32) -> Result<(), ResponseError> {
        if !self.members.contains_key(member_id) {
            return Err(ResponseError::UnknownMemberId);
        }
        if generation_id != self.generation_id {
            return Err(ResponseError::IllegalGeneration);
        }
        Ok(())
    }

    pub fn heartbeat(&mut self, member_id: &str, generation_id: i32, now: Instant) -> Result<(), ResponseError> {
        todo!("ch.5 : heartbeat")
    }

    pub fn leave(&mut self, member_id: &str, config: &GroupConfig, now: Instant) -> Result<(), ResponseError> {
        todo!("ch.5 : leave")
    }

    /// Retire un membre. S'il en reste d'autres, ils doivent se répartir ses
    /// partitions : nouveau rebalance.
    fn remove_member(&mut self, member_id: &str, config: &GroupConfig, now: Instant) {
        todo!("ch.5 : remove_member")
    }

    /// Vérifie qu'un commit d'offsets est autorisé.
    ///
    /// Un commit « simple » (génération -1, sans membre) n'est accepté que si
    /// le groupe n'a pas de membre : c'est le cas des consumers sans group
    /// management (`assign()`) et des outils d'administration.
    pub fn check_commit(&mut self, member_id: &str, generation_id: i32, now: Instant) -> Result<(), ResponseError> {
        todo!("ch.5 : check_commit")
    }

    // -----------------------------------------------------------------------
    // Échéances
    // -----------------------------------------------------------------------

    /// Appelée régulièrement : fin du délai de rebalance, expiration des
    /// sessions et des identifiants en attente.
    pub fn tick(&mut self, config: &GroupConfig, now: Instant) {
        todo!("ch.5 : tick")
    }
}
