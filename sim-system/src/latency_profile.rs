use std::fmt;
use std::sync::{Arc, OnceLock};

use contracts::Sha256Hex;
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub const PROXY_LATENCY_PROFILE_SHA256_V1: &str =
    "98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596";
pub const PROXY_LATENCY_SAMPLER_VERSION_V1: &str = "sha256_multiply_high_empirical_rank_v1";

const EMBEDDED_PROFILE_BYTES: &[u8] = include_bytes!(
    "../config/latency-profiles/98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596/profile.json"
);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DelaySampleDomainV1 {
    RequestOutbound,
    DirectResponseInbound,
    LifecycleFeedbackInbound,
}

impl DelaySampleDomainV1 {
    fn identifier(self) -> &'static [u8] {
        match self {
            Self::RequestOutbound => b"REQUEST_OUTBOUND_V1",
            Self::DirectResponseInbound => b"DIRECT_RESPONSE_INBOUND_V1",
            Self::LifecycleFeedbackInbound => b"LIFECYCLE_FEEDBACK_INBOUND_V1",
        }
    }
}

#[derive(Clone, Debug)]
pub struct EmpiricalDelayProfile {
    file_sha256: Sha256Hex,
    sample_count: u64,
    histogram: Vec<u64>,
    cumulative_counts: Vec<u64>,
}

impl EmpiricalDelayProfile {
    pub fn file_sha256(&self) -> &Sha256Hex {
        &self.file_sha256
    }

    pub fn sample_delay_ms(
        &self,
        session_seed: &[u8],
        stable_identity: &str,
        domain: DelaySampleDomainV1,
    ) -> u64 {
        let mut hasher = Sha256::new();
        update_len_prefixed(&mut hasher, PROXY_LATENCY_SAMPLER_VERSION_V1.as_bytes());
        update_len_prefixed(&mut hasher, self.file_sha256.0.as_bytes());
        update_len_prefixed(&mut hasher, session_seed);
        update_len_prefixed(&mut hasher, stable_identity.as_bytes());
        update_len_prefixed(&mut hasher, domain.identifier());
        let digest = hasher.finalize();
        let mut word = [0_u8; 8];
        word.copy_from_slice(&digest[..8]);
        let rank = ((u64::from_be_bytes(word) as u128 * self.sample_count as u128) >> 64) as u64;
        let index = histogram_index_for_rank(&self.cumulative_counts, rank);
        self.histogram[index]
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LatencyProfileError {
    Json(String),
    Invalid(&'static str),
}

impl fmt::Display for LatencyProfileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(message) => write!(formatter, "代理时延配置档 JSON 非法：{message}"),
            Self::Invalid(message) => write!(formatter, "代理时延配置档语义非法：{message}"),
        }
    }
}

impl std::error::Error for LatencyProfileError {}

pub fn load_embedded_proxy_latency_profile()
-> Result<Arc<EmpiricalDelayProfile>, LatencyProfileError> {
    static PROFILE: OnceLock<Result<Arc<EmpiricalDelayProfile>, LatencyProfileError>> =
        OnceLock::new();
    PROFILE
        .get_or_init(|| parse_profile(EMBEDDED_PROFILE_BYTES).map(Arc::new))
        .clone()
}

#[derive(Deserialize)]
struct ProfileWire {
    histogram: Vec<HistogramWire>,
}
#[derive(Deserialize)]
struct HistogramWire {
    delay_ms: u64,
    count: u64,
}

fn parse_profile(bytes: &[u8]) -> Result<EmpiricalDelayProfile, LatencyProfileError> {
    let wire: ProfileWire = serde_json::from_slice(bytes)
        .map_err(|error| LatencyProfileError::Json(error.to_string()))?;
    let mut histogram = Vec::with_capacity(wire.histogram.len());
    let mut cumulative_counts = Vec::with_capacity(wire.histogram.len());
    let mut cumulative = 0_u64;
    for bin in wire.histogram {
        cumulative = cumulative
            .checked_add(bin.count)
            .ok_or(LatencyProfileError::Invalid("直方图计数和溢出"))?;
        histogram.push(bin.delay_ms);
        cumulative_counts.push(cumulative);
    }
    Ok(EmpiricalDelayProfile {
        file_sha256: Sha256Hex(PROXY_LATENCY_PROFILE_SHA256_V1.to_owned()),
        sample_count: cumulative,
        histogram,
        cumulative_counts,
    })
}

fn histogram_index_for_rank(cumulative_counts: &[u64], rank: u64) -> usize {
    cumulative_counts.partition_point(|cumulative| *cumulative <= rank)
}

fn update_len_prefixed(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_profile_json_reports_parse_error() {
        assert!(matches!(
            parse_profile(b"broken"),
            Err(LatencyProfileError::Json(_))
        ));
    }

    #[test]
    fn empirical_rank_boundaries_select_the_first_count_strictly_above_rank() {
        let cumulative_counts = [2, 5, 9];
        assert_eq!(histogram_index_for_rank(&cumulative_counts, 0), 0);
        assert_eq!(histogram_index_for_rank(&cumulative_counts, 1), 0);
        assert_eq!(histogram_index_for_rank(&cumulative_counts, 2), 1);
        assert_eq!(histogram_index_for_rank(&cumulative_counts, 4), 1);
        assert_eq!(histogram_index_for_rank(&cumulative_counts, 5), 2);
        assert_eq!(histogram_index_for_rank(&cumulative_counts, 8), 2);
    }
}
