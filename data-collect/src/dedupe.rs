use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};

use sha2::{Digest as _, Sha256};

pub const POLYMARKET_MAX_ACTIVE_LIFECYCLES: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FingerprintDecision {
    Unique,
    Duplicate,
}

pub struct LifecycleFingerprintSets {
    per_lifecycle_limit: usize,
    fingerprints: BTreeMap<String, HashSet<[u8; 32]>>,
}

impl LifecycleFingerprintSets {
    #[must_use]
    pub fn with_limit(per_lifecycle_limit: usize) -> Self {
        Self {
            per_lifecycle_limit,
            fingerprints: BTreeMap::new(),
        }
    }

    pub fn replace_lifecycles(&mut self, lifecycles: BTreeSet<String>) -> Result<(), String> {
        if lifecycles.len() > POLYMARKET_MAX_ACTIVE_LIFECYCLES {
            return Err(format!(
                "too many active Polymarket lifecycles: {}",
                lifecycles.len()
            ));
        }
        self.fingerprints
            .retain(|lifecycle, _| lifecycles.contains(lifecycle));
        for lifecycle in lifecycles {
            self.fingerprints.entry(lifecycle).or_default();
        }
        Ok(())
    }

    pub fn observe(
        &mut self,
        lifecycle: &str,
        source_native_payload: &[u8],
    ) -> Result<FingerprintDecision, String> {
        let fingerprint = sha256(source_native_payload);
        self.observe_fingerprint(lifecycle, fingerprint)
    }

    pub fn observe_fingerprint(
        &mut self,
        lifecycle: &str,
        fingerprint: [u8; 32],
    ) -> Result<FingerprintDecision, String> {
        let fingerprints = self
            .fingerprints
            .get_mut(lifecycle)
            .ok_or_else(|| format!("inactive Polymarket lifecycle: {lifecycle}"))?;
        if fingerprints.contains(&fingerprint) {
            return Ok(FingerprintDecision::Duplicate);
        }
        if fingerprints.len() >= self.per_lifecycle_limit {
            return Err(format!(
                "Polymarket lifecycle fingerprint limit reached: lifecycle={lifecycle} limit={}",
                self.per_lifecycle_limit
            ));
        }
        fingerprints.insert(fingerprint);
        Ok(FingerprintDecision::Unique)
    }

    #[must_use]
    pub fn lifecycle_count(&self) -> usize {
        self.fingerprints.len()
    }

    #[must_use]
    pub fn contains_lifecycle(&self, lifecycle: &str) -> bool {
        self.fingerprints.contains_key(lifecycle)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinanceIdDecision {
    Unique,
    Duplicate,
    Conflict,
    OutsideWindow,
}

pub struct BinanceRecentIdWindow {
    limit: usize,
    fingerprints: BTreeMap<u64, HashSet<[u8; 32]>>,
}

impl BinanceRecentIdWindow {
    #[must_use]
    pub fn with_limit(limit: usize) -> Self {
        Self {
            limit,
            fingerprints: BTreeMap::new(),
        }
    }

    pub fn observe(&mut self, native_id: u64, payload: &[u8]) -> BinanceIdDecision {
        let fingerprint = sha256(payload);
        if let Some(existing) = self.fingerprints.get_mut(&native_id) {
            if existing.contains(&fingerprint) {
                return BinanceIdDecision::Duplicate;
            }
            existing.insert(fingerprint);
            return BinanceIdDecision::Conflict;
        }
        if self.fingerprints.len() >= self.limit
            && self
                .fingerprints
                .first_key_value()
                .is_some_and(|(oldest_id, _)| native_id < *oldest_id)
        {
            return BinanceIdDecision::OutsideWindow;
        }
        self.fingerprints
            .insert(native_id, HashSet::from([fingerprint]));
        while self.fingerprints.len() > self.limit {
            self.fingerprints.pop_first();
        }
        BinanceIdDecision::Unique
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.fingerprints.len()
    }

    pub fn remove(&mut self, native_id: u64) {
        self.fingerprints.remove(&native_id);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fingerprints.is_empty()
    }
}

pub struct TimedFingerprintWindow {
    window_ms: i64,
    limit: usize,
    first_received: HashMap<[u8; 32], i64>,
    expiry_queue: BinaryHeap<Reverse<(i64, [u8; 32])>>,
}

impl TimedFingerprintWindow {
    #[must_use]
    pub fn with_limits(window_ms: i64, limit: usize) -> Self {
        Self {
            window_ms,
            limit,
            first_received: HashMap::new(),
            expiry_queue: BinaryHeap::new(),
        }
    }

    pub fn observe(
        &mut self,
        source_native_payload: &[u8],
        recv_time_utc_ms: i64,
    ) -> Result<FingerprintDecision, String> {
        self.expire(recv_time_utc_ms);
        let fingerprint = sha256(source_native_payload);
        if let Some(first_received) = self.first_received.get_mut(&fingerprint) {
            if recv_time_utc_ms < *first_received {
                *first_received = recv_time_utc_ms;
                self.expiry_queue
                    .push(Reverse((recv_time_utc_ms, fingerprint)));
            }
            return Ok(FingerprintDecision::Duplicate);
        }
        if self.first_received.len() >= self.limit {
            return Err(format!(
                "timed fingerprint limit reached: limit={}",
                self.limit
            ));
        }
        self.first_received.insert(fingerprint, recv_time_utc_ms);
        self.expiry_queue
            .push(Reverse((recv_time_utc_ms, fingerprint)));
        Ok(FingerprintDecision::Unique)
    }

    fn expire(&mut self, now_ms: i64) {
        while let Some(Reverse((first_received, _))) = self.expiry_queue.peek() {
            if now_ms.saturating_sub(*first_received) < self.window_ms {
                break;
            }
            let Reverse((first_received, fingerprint)) =
                self.expiry_queue.pop().expect("heap entry exists");
            if self.first_received.get(&fingerprint) == Some(&first_received) {
                self.first_received.remove(&fingerprint);
            }
        }
    }
}

#[must_use]
pub fn sha256(payload: &[u8]) -> [u8; 32] {
    Sha256::digest(payload).into()
}
