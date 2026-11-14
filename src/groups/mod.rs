//! Le coordinateur de groupes : il gère les consumer groups dont ce broker
//! est le coordinateur, et leurs offsets commités.

pub mod group;
pub mod offsets;

use std::collections::HashMap;
use std::sync::Mutex;

use group::{Group, GroupConfig};
use offsets::{OffsetKey, OffsetValue};
use tokio::time::Instant;

#[derive(Debug)]
pub struct GroupCoordinator {
    pub config: GroupConfig,
    groups: Mutex<HashMap<String, Group>>,
}

impl GroupCoordinator {
    pub fn new(config: GroupConfig) -> Self {
        Self { config, groups: Mutex::default() }
    }

    /// Exécute `f` sur le groupe `group_id`, en le créant s'il n'existe pas.
    pub fn with_group<T>(&self, group_id: &str, f: impl FnOnce(&mut Group, &GroupConfig, Instant) -> T) -> T {
        let mut groups = self.groups.lock().unwrap();
        let group = groups.entry(group_id.to_string()).or_insert_with(|| Group::new(group_id));
        f(group, &self.config, Instant::now())
    }

    /// Exécute `f` sur le groupe `group_id` s'il existe.
    pub fn with_existing_group<T>(&self, group_id: &str, f: impl FnOnce(&mut Group, &GroupConfig, Instant) -> T) -> Option<T> {
        let mut groups = self.groups.lock().unwrap();
        groups.get_mut(group_id).map(|group| f(group, &self.config, Instant::now()))
    }

    /// Exécute `f` sur chaque groupe.
    pub fn for_each_group(&self, mut f: impl FnMut(&Group)) {
        for group in self.groups.lock().unwrap().values() {
            f(group);
        }
    }

    /// Applique des commits (relus depuis `__consumer_offsets`, ou qui viennent
    /// d'y être écrits) aux offsets en mémoire.
    pub fn apply_commits(&self, commits: Vec<(OffsetKey, OffsetValue)>) {
        let mut groups = self.groups.lock().unwrap();
        for (key, value) in commits {
            let group = groups.entry(key.group.clone()).or_insert_with(|| Group::new(&key.group));
            group.offsets.insert((key.topic, key.partition), (&value).into());
        }
    }

    /// Gère les échéances de tous les groupes, et oublie les groupes morts.
    pub fn tick(&self) {
        let now = Instant::now();
        let mut groups = self.groups.lock().unwrap();
        for group in groups.values_mut() {
            group.tick(&self.config, now);
        }
        groups.retain(|_, g| !g.is_dead());
    }
}
