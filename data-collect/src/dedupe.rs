use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};

use sha2::{Digest as _, Sha256};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FingerprintDecision {
    Unique,
    Duplicate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinanceIdDecision {
    Unique,
    Duplicate,
    Conflict,
    OutsideWindow,
}

const BINANCE_MAX_FINGERPRINTS_PER_ID: usize = 8;

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
            if existing.len() < BINANCE_MAX_FINGERPRINTS_PER_ID {
                existing.insert(fingerprint);
            }
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

    #[cfg(test)]
    pub(crate) fn fingerprint_count(&self, native_id: u64) -> usize {
        self.fingerprints.get(&native_id).map_or(0, HashSet::len)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binance_conflict_fingerprints_are_bounded_without_dropping_conflicts() {
        let mut window = BinanceRecentIdWindow::with_limit(1);
        assert_eq!(window.observe(7, b"variant-0"), BinanceIdDecision::Unique);
        for variant in 1..64 {
            assert_eq!(
                window.observe(7, format!("variant-{variant}").as_bytes()),
                BinanceIdDecision::Conflict
            );
        }
        assert_eq!(window.fingerprint_count(7), BINANCE_MAX_FINGERPRINTS_PER_ID);
        assert_eq!(
            window.observe(7, b"variant-63"),
            BinanceIdDecision::Conflict,
            "an uncached overflow variant remains preserved as a conflict"
        );
        assert_eq!(
            window.observe(7, b"variant-1"),
            BinanceIdDecision::Duplicate,
            "cached variants remain structural duplicates"
        );
    }
}
