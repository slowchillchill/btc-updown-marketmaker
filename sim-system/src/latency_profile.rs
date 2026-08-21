use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use contracts::Sha256Hex;
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub const PROXY_LATENCY_PROFILE_SCHEMA_V1: &str = "sim_proxy_latency_profile_v1";
pub const PROXY_LATENCY_PROFILE_ID_V1: &str = "btc_updown_5m_collector_observed_delay_20260803_v1";
pub const PROXY_LATENCY_PROFILE_VERSION_V1: u32 = 1;
pub const PROXY_LATENCY_PROFILE_SHA256_V1: &str =
    "98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596";
pub const PROXY_LATENCY_PROFILE_BYTES_V1: usize = 280_853;
pub const PROXY_LATENCY_SAMPLER_VERSION_V1: &str = "sha256_multiply_high_empirical_rank_v1";

const DATASET_MANIFEST_SHA256: &str =
    "342d2fa2c55a1c4b9446090cffd57e31814554abb319ff412e72200a7e5aabdf";
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LatencyProfileStatisticsV1 {
    pub sample_count: u64,
    pub missing_source_timestamp_count: u64,
    pub negative_delay_count: u64,
    pub min_delay_ms: u64,
    pub p50_delay_ms: u64,
    pub p85_delay_ms: u64,
    pub p90_delay_ms: u64,
    pub p95_delay_ms: u64,
    pub p99_delay_ms: u64,
    pub max_delay_ms: u64,
    pub histogram_bin_count: u64,
}

#[derive(Clone, Debug)]
pub struct EmpiricalDelayProfile {
    profile_id: String,
    profile_version: u32,
    file_sha256: Sha256Hex,
    statistics: LatencyProfileStatisticsV1,
    histogram: Vec<HistogramBin>,
    cumulative_counts: Vec<u64>,
}

impl EmpiricalDelayProfile {
    pub(crate) fn canonical_file_bytes(&self) -> &'static [u8] {
        EMBEDDED_PROFILE_BYTES
    }

    pub fn profile_id(&self) -> &str {
        &self.profile_id
    }

    pub fn profile_version(&self) -> u32 {
        self.profile_version
    }

    pub fn file_sha256(&self) -> &Sha256Hex {
        &self.file_sha256
    }

    pub fn statistics(&self) -> LatencyProfileStatisticsV1 {
        self.statistics
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
        let rank = ((u64::from_be_bytes(word) as u128 * self.statistics.sample_count as u128) >> 64)
            as u64;
        let index = histogram_index_for_rank(&self.cumulative_counts, rank);
        self.histogram[index].delay_ms
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LatencyProfileError {
    ByteLength { expected: usize, actual: usize },
    FileSha256 { expected: String, actual: String },
    Json(String),
    Invalid(&'static str),
}

impl fmt::Display for LatencyProfileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ByteLength { expected, actual } => {
                write!(
                    formatter,
                    "代理时延配置档字节数应为 {expected}，实际为 {actual}"
                )
            }
            Self::FileSha256 { expected, actual } => {
                write!(
                    formatter,
                    "代理时延配置档 SHA-256 应为 {expected}，实际为 {actual}"
                )
            }
            Self::Json(message) => write!(formatter, "代理时延配置档 JSON 非法：{message}"),
            Self::Invalid(message) => write!(formatter, "代理时延配置档语义非法：{message}"),
        }
    }
}

impl std::error::Error for LatencyProfileError {}

pub fn load_embedded_proxy_latency_profile()
-> Result<Arc<EmpiricalDelayProfile>, LatencyProfileError> {
    validate_profile_bytes(
        EMBEDDED_PROFILE_BYTES,
        PROXY_LATENCY_PROFILE_BYTES_V1,
        PROXY_LATENCY_PROFILE_SHA256_V1,
    )
    .map(Arc::new)
}

#[derive(Clone, Copy, Debug)]
struct HistogramBin {
    delay_ms: u64,
    count: u64,
}

#[derive(Deserialize)]
struct ProfileWire {
    schema_version: String,
    profile_id: String,
    profile_version: u32,
    application_policy: String,
    evidence_kind: String,
    lineage: LineageWire,
    limitations: Vec<String>,
    sample_definition: SampleDefinitionWire,
    sampler: SamplerWire,
    statistics: StatisticsWire,
    histogram: Vec<HistogramWire>,
}

#[derive(Deserialize)]
struct LineageWire {
    dataset_manifest_sha256: String,
    file_count: u64,
    market_count: u64,
    source_kind: String,
    source_schema_version: String,
    utc_day: String,
    selected_files: Vec<SelectedFileWire>,
}

#[derive(Deserialize)]
struct SelectedFileWire {
    bytes: u64,
    path: String,
    rows: u64,
    sha256: String,
    utc_hour: String,
}

#[derive(Deserialize)]
struct SampleDefinitionWire {
    formula: String,
    percentile_method: String,
    unit: String,
}

#[derive(Deserialize)]
struct SamplerWire {
    algorithm_version: String,
    domains: Vec<String>,
    historical_public_events_resampled: bool,
    itode_included: bool,
    physical_retry_resamples: bool,
}

#[derive(Deserialize)]
struct StatisticsWire {
    sample_count: u64,
    missing_source_timestamp_count: u64,
    negative_delay_count: u64,
    min_delay_ms: u64,
    p50_delay_ms: u64,
    p85_delay_ms: u64,
    p90_delay_ms: u64,
    p95_delay_ms: u64,
    p99_delay_ms: u64,
    max_delay_ms: u64,
    histogram_bin_count: u64,
}

#[derive(Deserialize)]
struct HistogramWire {
    delay_ms: u64,
    count: u64,
}

fn validate_profile_bytes(
    bytes: &[u8],
    expected_bytes: usize,
    expected_sha256: &str,
) -> Result<EmpiricalDelayProfile, LatencyProfileError> {
    if bytes.len() != expected_bytes {
        return Err(LatencyProfileError::ByteLength {
            expected: expected_bytes,
            actual: bytes.len(),
        });
    }
    let actual_sha256 = sha256_hex(bytes);
    if actual_sha256 != expected_sha256 {
        return Err(LatencyProfileError::FileSha256 {
            expected: expected_sha256.to_owned(),
            actual: actual_sha256,
        });
    }
    let wire: ProfileWire = serde_json::from_slice(bytes)
        .map_err(|error| LatencyProfileError::Json(error.to_string()))?;
    validate_profile_wire(wire, expected_sha256)
}

fn validate_profile_wire(
    wire: ProfileWire,
    file_sha256: &str,
) -> Result<EmpiricalDelayProfile, LatencyProfileError> {
    if wire.schema_version != PROXY_LATENCY_PROFILE_SCHEMA_V1
        || wire.profile_id != PROXY_LATENCY_PROFILE_ID_V1
        || wire.profile_version != PROXY_LATENCY_PROFILE_VERSION_V1
    {
        return Err(LatencyProfileError::Invalid("schema 或配置档身份不匹配"));
    }
    if wire.application_policy != "PROJECT_ASSUMPTION"
        || wire.evidence_kind != "COLLECTOR_OBSERVED_FEED_DELAY"
    {
        return Err(LatencyProfileError::Invalid("证据类别或应用策略不匹配"));
    }
    validate_lineage(&wire.lineage, wire.statistics.sample_count)?;
    validate_sample_definition(&wire.sample_definition)?;
    validate_sampler(&wire.sampler)?;
    validate_limitations(&wire.limitations)?;

    let statistics = LatencyProfileStatisticsV1 {
        sample_count: wire.statistics.sample_count,
        missing_source_timestamp_count: wire.statistics.missing_source_timestamp_count,
        negative_delay_count: wire.statistics.negative_delay_count,
        min_delay_ms: wire.statistics.min_delay_ms,
        p50_delay_ms: wire.statistics.p50_delay_ms,
        p85_delay_ms: wire.statistics.p85_delay_ms,
        p90_delay_ms: wire.statistics.p90_delay_ms,
        p95_delay_ms: wire.statistics.p95_delay_ms,
        p99_delay_ms: wire.statistics.p99_delay_ms,
        max_delay_ms: wire.statistics.max_delay_ms,
        histogram_bin_count: wire.statistics.histogram_bin_count,
    };
    let histogram = validate_histogram(wire.histogram, statistics)?;
    let mut cumulative_counts = Vec::with_capacity(histogram.len());
    let mut cumulative = 0_u64;
    for bin in &histogram {
        cumulative = cumulative
            .checked_add(bin.count)
            .ok_or(LatencyProfileError::Invalid("直方图计数和溢出"))?;
        cumulative_counts.push(cumulative);
    }

    Ok(EmpiricalDelayProfile {
        profile_id: wire.profile_id,
        profile_version: wire.profile_version,
        file_sha256: Sha256Hex(file_sha256.to_owned()),
        statistics,
        histogram,
        cumulative_counts,
    })
}

fn validate_lineage(lineage: &LineageWire, sample_count: u64) -> Result<(), LatencyProfileError> {
    if lineage.dataset_manifest_sha256 != DATASET_MANIFEST_SHA256
        || lineage.file_count != 24
        || lineage.market_count != 290
        || lineage.source_kind != "polymarket_market"
        || lineage.source_schema_version != "polymarket_orderbook_v3"
        || lineage.utc_day != "2026-08-03"
        || lineage.selected_files.len() != 24
    {
        return Err(LatencyProfileError::Invalid("固定输入谱系不匹配"));
    }

    let mut paths = BTreeSet::new();
    let mut hours = BTreeSet::new();
    let mut row_sum = 0_u64;
    for member in &lineage.selected_files {
        if member.bytes == 0
            || member.rows == 0
            || !is_sha256_hex(&member.sha256)
            || !paths.insert(member.path.as_str())
            || !hours.insert(member.utc_hour.as_str())
        {
            return Err(LatencyProfileError::Invalid("固定输入成员非法或重复"));
        }
        row_sum = row_sum
            .checked_add(member.rows)
            .ok_or(LatencyProfileError::Invalid("固定输入成员行数溢出"))?;
    }
    if row_sum != sample_count {
        return Err(LatencyProfileError::Invalid(
            "固定输入成员行数与样本数不一致",
        ));
    }
    Ok(())
}

fn validate_sample_definition(
    definition: &SampleDefinitionWire,
) -> Result<(), LatencyProfileError> {
    if definition.formula != "timestamp_received_utc_ms - source_event_timestamp_utc_ms"
        || definition.percentile_method != "nearest_rank"
        || definition.unit != "decoded_websocket_message"
    {
        return Err(LatencyProfileError::Invalid("样本定义不匹配"));
    }
    Ok(())
}

fn validate_sampler(sampler: &SamplerWire) -> Result<(), LatencyProfileError> {
    let expected_domains = [
        "REQUEST_OUTBOUND_V1",
        "DIRECT_RESPONSE_INBOUND_V1",
        "LIFECYCLE_FEEDBACK_INBOUND_V1",
    ];
    if sampler.algorithm_version != PROXY_LATENCY_SAMPLER_VERSION_V1
        || sampler
            .domains
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != expected_domains
        || sampler.historical_public_events_resampled
        || sampler.itode_included
        || sampler.physical_retry_resamples
    {
        return Err(LatencyProfileError::Invalid("采样算法或采样边界不匹配"));
    }
    Ok(())
}

fn validate_limitations(limitations: &[String]) -> Result<(), LatencyProfileError> {
    let expected = BTreeSet::from([
        "BIDIRECTIONAL_REUSE_IS_PROJECT_ASSUMPTION",
        "HISTORICAL_PUBLIC_EVENTS_ARE_NOT_RESAMPLED",
        "ITODE_IS_EXCLUDED",
        "MARGINAL_DISTRIBUTION_ONLY",
        "NOT_EXACT_NETWORK_DELAY",
        "NO_SHARED_CLOCK_AUTHORITY_PROOF",
    ]);
    let actual = limitations
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if actual != expected || actual.len() != limitations.len() {
        return Err(LatencyProfileError::Invalid("科学限制声明不完整或重复"));
    }
    Ok(())
}

fn validate_histogram(
    wire: Vec<HistogramWire>,
    statistics: LatencyProfileStatisticsV1,
) -> Result<Vec<HistogramBin>, LatencyProfileError> {
    if wire.len() as u64 != statistics.histogram_bin_count
        || statistics.sample_count == 0
        || statistics.missing_source_timestamp_count != 0
        || statistics.negative_delay_count != 0
    {
        return Err(LatencyProfileError::Invalid("样本计数或缺失边界不匹配"));
    }
    let mut histogram = Vec::with_capacity(wire.len());
    let mut previous_delay = None;
    let mut total = 0_u64;
    for entry in wire {
        if entry.count == 0 || previous_delay.is_some_and(|previous| entry.delay_ms <= previous) {
            return Err(LatencyProfileError::Invalid("直方图必须严格升序且频数非零"));
        }
        total = total
            .checked_add(entry.count)
            .ok_or(LatencyProfileError::Invalid("直方图计数和溢出"))?;
        previous_delay = Some(entry.delay_ms);
        histogram.push(HistogramBin {
            delay_ms: entry.delay_ms,
            count: entry.count,
        });
    }
    if total != statistics.sample_count
        || histogram.first().map(|bin| bin.delay_ms) != Some(statistics.min_delay_ms)
        || histogram.last().map(|bin| bin.delay_ms) != Some(statistics.max_delay_ms)
    {
        return Err(LatencyProfileError::Invalid("直方图总数或极值不匹配"));
    }
    let percentiles = [
        nearest_rank_from_bins(&histogram, total, 50),
        nearest_rank_from_bins(&histogram, total, 85),
        nearest_rank_from_bins(&histogram, total, 90),
        nearest_rank_from_bins(&histogram, total, 95),
        nearest_rank_from_bins(&histogram, total, 99),
    ];
    if percentiles
        != [
            statistics.p50_delay_ms,
            statistics.p85_delay_ms,
            statistics.p90_delay_ms,
            statistics.p95_delay_ms,
            statistics.p99_delay_ms,
        ]
    {
        return Err(LatencyProfileError::Invalid("直方图重算分位数不匹配"));
    }
    Ok(histogram)
}

fn nearest_rank_from_bins(histogram: &[HistogramBin], total: u64, percentile: u64) -> u64 {
    let rank = (total as u128 * percentile as u128).div_ceil(100) as u64;
    let mut cumulative = 0_u64;
    for bin in histogram {
        cumulative += bin.count;
        if cumulative >= rank {
            return bin.delay_ms;
        }
    }
    unreachable!("validated non-empty histogram must cover every nearest rank")
}

fn histogram_index_for_rank(cumulative_counts: &[u64], rank: u64) -> usize {
    cumulative_counts.partition_point(|cumulative| *cumulative <= rank)
}

fn update_len_prefixed(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changed_bytes_fail_before_json_is_trusted() {
        let mut corrupted = EMBEDDED_PROFILE_BYTES.to_vec();
        corrupted[0] ^= 1;
        let error = validate_profile_bytes(
            &corrupted,
            PROXY_LATENCY_PROFILE_BYTES_V1,
            PROXY_LATENCY_PROFILE_SHA256_V1,
        )
        .expect_err("内容损坏必须被文件摘要拒绝");
        assert!(matches!(error, LatencyProfileError::FileSha256 { .. }));
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
