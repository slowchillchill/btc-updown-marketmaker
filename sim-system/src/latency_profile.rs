use std::fmt;
use std::sync::{Arc, OnceLock};

use contracts::Sha256Hex;
use rand_xoshiro::{
    Xoshiro256PlusPlus,
    rand_core::{RngCore, SeedableRng},
};
use serde::{Deserialize, Serialize};

pub const PROXY_LATENCY_PROFILE_SHA256_V1: &str =
    "98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596";
pub const PROXY_LATENCY_SAMPLER_VERSION_V2: &str =
    "xoshiro256plusplus_jump_multiply_high_empirical_rank_v2";

const EMBEDDED_PROFILE_BYTES: &[u8] = include_bytes!(
    "../config/latency-profiles/98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596/profile.json"
);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DelaySampleDomainV1 {
    RequestOutbound,
    DirectResponseInbound,
    LifecycleFeedbackInbound,
}

/// 每个会话的三个非密码学随机序列；状态随原会话保存。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionDelaySamplerV2 {
    outbound: Xoshiro256PlusPlus,
    response: Xoshiro256PlusPlus,
    lifecycle: Xoshiro256PlusPlus,
}
impl SessionDelaySamplerV2 {
    /// 新版种子合同：64位十六进制表示32字节，仅开户时解析。
    pub fn from_hex(seed: &str) -> Result<Self, &'static str> {
        if seed.len() != 64 {
            return Err("session_seed 必须为64位十六进制");
        }
        let mut bytes = [0; 32];
        for (out, pair) in bytes.iter_mut().zip(seed.as_bytes().chunks_exact(2)) {
            let digit = |c: u8| match c {
                b'0'..=b'9' => Some(c - b'0'),
                b'a'..=b'f' => Some(c - b'a' + 10),
                b'A'..=b'F' => Some(c - b'A' + 10),
                _ => None,
            };
            *out = (digit(pair[0]).ok_or("session_seed 含非十六进制字符")? << 4)
                | digit(pair[1]).ok_or("session_seed 含非十六进制字符")?;
        }
        let outbound = Xoshiro256PlusPlus::from_seed(bytes);
        let mut response = outbound.clone();
        response.jump();
        let mut lifecycle = response.clone();
        lifecycle.jump();
        Ok(Self {
            outbound,
            response,
            lifecycle,
        })
    }
    pub fn sample(&mut self, profile: &EmpiricalDelayProfile, domain: DelaySampleDomainV1) -> u64 {
        let rng = match domain {
            DelaySampleDomainV1::RequestOutbound => &mut self.outbound,
            DelaySampleDomainV1::DirectResponseInbound => &mut self.response,
            DelaySampleDomainV1::LifecycleFeedbackInbound => &mut self.lifecycle,
        };
        profile.delay_for_word(rng.next_u64())
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

    fn delay_for_word(&self, word: u64) -> u64 {
        let rank = ((word as u128 * self.sample_count as u128) >> 64) as u64;
        self.histogram[histogram_index_for_rank(&self.cumulative_counts, rank)]
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

#[cfg(test)]
mod tests {
    use super::*;

    // 锁定 rand_xoshiro 0.7.0 上游 reference() 的同一字节种子和参考输出。
    #[test]
    fn parsed_seed_matches_upstream_xoshiro_reference_words() {
        let mut sampler = SessionDelaySamplerV2::from_hex(
            "0100000000000000020000000000000003000000000000000400000000000000",
        )
        .unwrap();
        for word in [
            41943041,
            58720359,
            3588806011781223,
            3591011842654386,
            9228616714210784205,
            9973669472204895162,
            14011001112246962877,
            12406186145184390807,
            15849039046786891736,
            10450023813501588000,
        ] {
            assert_eq!(sampler.outbound.next_u64(), word);
        }
    }

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
