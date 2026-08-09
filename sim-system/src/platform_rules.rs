use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use contracts::{ClobMarketId, ConditionId, Sha256Hex, TokenId};
use polymarket_client_sdk_v2::types::{B256, U256};
use reqwest::StatusCode;
use reqwest::redirect::Policy;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rustix::fs::{AtFlags, Mode, OFlags, RenameFlags, open, openat, renameat_with, unlinkat};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

pub const PLATFORM_RULE_SNAPSHOT_SCHEMA_V2: &str = "platform_rule_snapshot_v2";
const PLATFORM_RULE_CONFIG_SCHEMA_V1: &str = "platform_rules_config_v1";
const PLATFORM_RULE_CONFIG_PATH: &str = "sim-system/config/platform-rules-v1.toml";
const PLATFORM_RULE_CONFIG_BYTES: &[u8] = include_bytes!("../config/platform-rules-v1.toml");
const OFFICIAL_CLOB_MARKET_BASE: &str = "https://clob.polymarket.com/clob-markets";
const OFFICIAL_GAMMA_KEYSET_URL: &str = "https://gamma-api.polymarket.com/markets/keyset";
const MAX_RULE_HTTP_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_RULE_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const PROCESS_RULE_INITIALIZATION_TIMEOUT: Duration = Duration::from_secs(30);
const BTC_UPDOWN_SLUG_PREFIX: &str = "btc-updown-5m-";
const PROCESS_RULE_TEMPLATE_VERSION: &str = "BTC_UPDOWN_5M_PROCESS_RULE_TEMPLATE_V1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceOutcomeSpecV1 {
    pub token_id: TokenId,
    pub outcome: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceMarketSpecV1 {
    pub condition_id: ConditionId,
    pub clob_market_id: ClobMarketId,
    pub slug: String,
    pub outcomes: Vec<ReferenceOutcomeSpecV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImplementationIdentityV1 {
    pub git_commit: String,
    pub source_identity_sha256: Sha256Hex,
    pub cargo_lock_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentProcessSnapshotRequestV1 {
    pub reference_market: ReferenceMarketSpecV1,
    pub target_path: PathBuf,
    pub implementation_identity: ImplementationIdentityV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoricalProcessSnapshotRequestV1 {
    pub snapshot_path: PathBuf,
    pub expected_file_sha256: Sha256Hex,
    pub replay_causal_start_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryProcessSnapshotRequestV1 {
    pub snapshot_path: PathBuf,
    pub expected_file_sha256: Sha256Hex,
    pub expected_implementation_identity: ImplementationIdentityV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SnapshotAccessMode {
    CurrentQuery,
    HistoricalAsOf,
    Recovery,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SnapshotCreationModeV1 {
    CurrentQuery,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedProcessRuleSnapshot {
    path: PathBuf,
    file_sha256: Sha256Hex,
    access_mode: SnapshotAccessMode,
    snapshot: PlatformRuleSnapshotV2,
}

impl SealedProcessRuleSnapshot {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn file_sha256(&self) -> &Sha256Hex {
        &self.file_sha256
    }

    #[must_use]
    pub fn access_mode(&self) -> SnapshotAccessMode {
        self.access_mode
    }

    #[must_use]
    pub fn snapshot(&self) -> &PlatformRuleSnapshotV2 {
        &self.snapshot
    }

    #[must_use]
    pub fn synchronized_rules(&self) -> &SynchronizedPlatformRulesV1 {
        &self.snapshot.synchronized_platform_rules
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuleDecimal(Decimal);

impl RuleDecimal {
    fn new(value: Decimal) -> Self {
        Self(value.normalize())
    }

    #[must_use]
    pub fn value(&self) -> Decimal {
        self.0
    }
}

impl Serialize for RuleDecimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.normalize().to_string())
    }
}

impl<'de> Deserialize<'de> for RuleDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let lexical = String::deserialize(deserializer)?;
        parse_decimal(&lexical)
            .map(Self::new)
            .map_err(D::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RuleEvidenceCategory {
    OfficialPlatformRule,
    OfficialSdkContract,
    LocalSdkPatchContract,
    ObservedPlatformBehavior,
    UserConfirmedPlatformBehavior,
    RuleDerivation,
    ProjectAssumption,
    UncertaintyProfile,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleEvidenceV1 {
    pub category: RuleEvidenceCategory,
    pub source: String,
    pub reviewed_on_utc_date: String,
    pub scope: String,
    pub limitation: String,
    pub version: String,
    pub failure_semantics: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceBackedRuleV1<T> {
    pub value: T,
    pub evidence: RuleEvidenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TakerDelayRuleV1 {
    pub enabled_only_when_itode: bool,
    pub applies_only_to_fok_fak_market_buy: bool,
    pub delay_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchRuleV1 {
    pub max_items: u32,
    pub per_item_results: bool,
    pub request_is_atomic: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GtdTimingRuleV1 {
    pub minimum_lead_seconds: u64,
    pub safety_offset_seconds: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeComputationRuleV1 {
    pub formula_version: String,
    pub precision_decimal_places: u32,
    pub minimum_nonzero_fee_pusd: RuleDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MakerRebatePaymentRuleV1 {
    pub attribution_day_boundary: String,
    pub minimum_payment_pusd: RuleDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImmediateBuyMinimumRuleV1 {
    pub minimum_principal_pusd: RuleDecimal,
    pub applies_to_marketable_limit_buy: bool,
    pub applies_to_non_marketable_limit_buy: bool,
    pub applies_to_fok_or_fak: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedOrderStatusRuleV1 {
    pub raw_status: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentedPlatformRulesV1 {
    pub taker_delay: EvidenceBackedRuleV1<TakerDelayRuleV1>,
    pub post_orders_batch: EvidenceBackedRuleV1<BatchRuleV1>,
    pub cancel_orders_batch: EvidenceBackedRuleV1<BatchRuleV1>,
    pub gtd_timing: EvidenceBackedRuleV1<GtdTimingRuleV1>,
    pub fee_computation: EvidenceBackedRuleV1<FeeComputationRuleV1>,
    pub maker_rebate_payment: EvidenceBackedRuleV1<MakerRebatePaymentRuleV1>,
    pub immediate_gtc_buy_minimum: EvidenceBackedRuleV1<ImmediateBuyMinimumRuleV1>,
    pub immediate_gtd_buy_minimum: EvidenceBackedRuleV1<ImmediateBuyMinimumRuleV1>,
    pub resolved_order_status: EvidenceBackedRuleV1<ResolvedOrderStatusRuleV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct L2QueueModelRuleV1 {
    pub model_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleAccountRuleV1 {
    pub venue_dust_upper_exclusive_shares: RuleDecimal,
    pub venue_dust_requires_positive_fill: bool,
    pub reject_new_orders_at_end_at: bool,
    pub existing_orders_queryable_after_end_at: bool,
    pub existing_orders_cancelable_after_end_at: bool,
    pub existing_orders_matchable_after_end_at: bool,
    pub auto_redeem_only_after_qualified_market_resolved: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebateSettlementAssumptionV1 {
    pub fixed_taker_rebate_rate: RuleDecimal,
    pub fixed_maker_rebate_rate: RuleDecimal,
    pub dynamic_rebate_tiers_enabled: bool,
    pub one_time_bonuses_enabled: bool,
    pub liquidity_rewards_enabled: bool,
    pub taker_cash_available_at_utc: String,
    pub maker_cash_available_at_utc: String,
    pub payment_floor_decimal_places: u32,
    pub truncated_remainder_stays_locked: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectAccountRulesV1 {
    pub l2_queue_model: EvidenceBackedRuleV1<L2QueueModelRuleV1>,
    pub lifecycle_account_behavior: EvidenceBackedRuleV1<LifecycleAccountRuleV1>,
    pub rebate_settlement: EvidenceBackedRuleV1<RebateSettlementAssumptionV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeScheduleV1 {
    pub rate: RuleDecimal,
    pub exponent: u32,
    pub taker_only: bool,
    pub rebate_rate: RuleDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SynchronizedPlatformRulesV1 {
    pub minimum_order_size_shares: RuleDecimal,
    pub taker_order_delay_enabled: bool,
    pub fees_enabled: bool,
    pub fee_schedule: FeeScheduleV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BtcUpdownProcessRulePolicyV1 {
    pub policy_version: String,
    pub supported_slug_prefix: String,
    pub reuse_reference_values_until_process_exit: bool,
    pub per_market_requery_allowed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HttpEndpointKindV1 {
    ClobMarketInfo,
    GammaMarketsKeyset,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpAttemptEvidenceV1 {
    pub attempt: u8,
    pub started_at_utc_ms: i64,
    pub completed_at_utc_ms: i64,
    pub status_code: Option<u16>,
    pub response_bytes: Option<u64>,
    pub raw_response_sha256: Option<Sha256Hex>,
    pub transport_error_kind: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpRequestEvidenceV1 {
    pub endpoint_kind: HttpEndpointKindV1,
    pub method: String,
    pub request_url: String,
    pub condition_id: ConditionId,
    pub closed: Option<bool>,
    pub attempts: Vec<HttpAttemptEvidenceV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticConfigEvidenceV1 {
    pub path: String,
    pub schema_version: String,
    pub raw_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyIdentityV1 {
    pub name: String,
    pub version: String,
    pub source: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceValidationV1 {
    pub exact_reference_identity: bool,
    pub token_outcome_match: bool,
    pub enabled_fee_schedule_match: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformRuleProvenanceV2 {
    pub created_via: SnapshotCreationModeV1,
    pub query_started_at_utc_ms: i64,
    pub query_completed_at_utc_ms: i64,
    pub rules_observed_at_utc_ms: i64,
    pub reference_market: ReferenceMarketSpecV1,
    pub reference_observation_evidence: RuleEvidenceV1,
    pub application_policy: EvidenceBackedRuleV1<BtcUpdownProcessRulePolicyV1>,
    pub http_requests: Vec<HttpRequestEvidenceV1>,
    pub static_config: StaticConfigEvidenceV1,
    pub implementation_identity: ImplementationIdentityV1,
    pub dependencies: Vec<DependencyIdentityV1>,
    pub reference_validation: ReferenceValidationV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformRuleSnapshotV2 {
    pub schema_version: String,
    pub content_sha256: Sha256Hex,
    pub synchronized_platform_rules: SynchronizedPlatformRulesV1,
    pub documented_platform_rules: DocumentedPlatformRulesV1,
    pub project_account_rules: ProjectAccountRulesV1,
    pub provenance: PlatformRuleProvenanceV2,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RuleSnapshotError {
    InvalidReferenceMarket(String),
    InvalidConfig(String),
    ClientBuild(String),
    QueryBuild(String),
    HttpTransport {
        url: String,
        message: String,
        attempts: Vec<HttpAttemptEvidenceV1>,
    },
    HttpStatus {
        url: String,
        status: u16,
        attempts: Vec<HttpAttemptEvidenceV1>,
    },
    ResponseTooLarge {
        url: String,
        limit: usize,
    },
    ResponseDecode {
        url: String,
        raw_sha256: Sha256Hex,
        message: String,
    },
    InvalidRules(String),
    InitializationTimeout,
    Io {
        operation: &'static str,
        message: String,
    },
    SnapshotAlreadyExists(PathBuf),
    SnapshotCandidateExists(PathBuf),
    SnapshotTooLarge {
        limit: usize,
    },
    SnapshotIntegrity(String),
    HistoricalNotApplicable {
        observed_at_utc_ms: i64,
        causal_start_utc_ms: i64,
    },
    IncompatibleImplementation,
    DurabilityUncertain {
        path: PathBuf,
        message: String,
    },
}

impl fmt::Display for RuleSnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidReferenceMarket(message) => {
                write!(formatter, "invalid reference market: {message}")
            }
            Self::InvalidConfig(message) => write!(formatter, "invalid rule config: {message}"),
            Self::ClientBuild(message) => write!(formatter, "HTTP client build failed: {message}"),
            Self::QueryBuild(message) => write!(formatter, "HTTP query build failed: {message}"),
            Self::HttpTransport { url, message, .. } => {
                write!(formatter, "HTTP transport failed for {url}: {message}")
            }
            Self::HttpStatus { url, status, .. } => {
                write!(formatter, "HTTP status {status} for {url}")
            }
            Self::ResponseTooLarge { url, limit } => {
                write!(formatter, "HTTP response for {url} exceeds {limit} bytes")
            }
            Self::ResponseDecode {
                url,
                raw_sha256,
                message,
            } => write!(
                formatter,
                "response decode failed for {url} at {}: {message}",
                raw_sha256.0
            ),
            Self::InvalidRules(message) => write!(formatter, "invalid platform rules: {message}"),
            Self::InitializationTimeout => {
                write!(formatter, "process rule initialization timed out")
            }
            Self::Io { operation, message } => write!(formatter, "{operation}: {message}"),
            Self::SnapshotAlreadyExists(path) => {
                write!(formatter, "snapshot already exists at {}", path.display())
            }
            Self::SnapshotCandidateExists(path) => {
                write!(
                    formatter,
                    "snapshot candidate already exists at {}",
                    path.display()
                )
            }
            Self::SnapshotTooLarge { limit } => {
                write!(formatter, "snapshot exceeds {limit} bytes")
            }
            Self::SnapshotIntegrity(message) => {
                write!(formatter, "snapshot integrity failure: {message}")
            }
            Self::HistoricalNotApplicable {
                observed_at_utc_ms,
                causal_start_utc_ms,
            } => write!(
                formatter,
                "snapshot observed at {observed_at_utc_ms} after replay causal start {causal_start_utc_ms}"
            ),
            Self::IncompatibleImplementation => {
                write!(
                    formatter,
                    "snapshot implementation identity is incompatible"
                )
            }
            Self::DurabilityUncertain { path, message } => write!(
                formatter,
                "snapshot appeared at {} but parent sync failed: {message}",
                path.display()
            ),
        }
    }
}

impl Error for RuleSnapshotError {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlatformRulesConfigV1 {
    schema_version: String,
    documented_platform_rules: DocumentedPlatformRulesV1,
    project_account_rules: ProjectAccountRulesV1,
}

#[derive(Clone, Debug)]
struct ExactDecimalWire(Decimal);

impl<'de> Deserialize<'de> for ExactDecimalWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let lexical = match raw.get().as_bytes().first() {
            Some(b'"') => serde_json::from_str::<String>(raw.get()).map_err(D::Error::custom)?,
            Some(b'-' | b'0'..=b'9') => raw.get().to_owned(),
            _ => return Err(D::Error::custom("expected a JSON number or decimal string")),
        };
        parse_decimal(&lexical).map(Self).map_err(D::Error::custom)
    }
}

#[derive(Debug, Deserialize)]
struct ClobTokenWire {
    t: String,
    o: String,
}

#[derive(Debug, Deserialize)]
struct ClobFeeDetailsWire {
    r: ExactDecimalWire,
    e: ExactDecimalWire,
    to: bool,
}

#[derive(Debug, Deserialize)]
struct ClobMarketWire {
    #[serde(default)]
    c: Option<String>,
    t: Vec<ClobTokenWire>,
    mos: ExactDecimalWire,
    #[serde(default)]
    itode: bool,
    #[serde(default)]
    fd: Option<ClobFeeDetailsWire>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaFeeScheduleWire {
    rate: ExactDecimalWire,
    exponent: ExactDecimalWire,
    taker_only: bool,
    rebate_rate: ExactDecimalWire,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarketWire {
    id: String,
    condition_id: String,
    slug: String,
    outcomes: String,
    clob_token_ids: String,
    closed: bool,
    fees_enabled: bool,
    #[serde(default)]
    fee_schedule: Option<GammaFeeScheduleWire>,
}

#[derive(Debug, Deserialize)]
struct GammaKeysetWire {
    markets: Vec<GammaMarketWire>,
}

#[derive(Debug)]
struct HashedResponse {
    url: String,
    bytes: Vec<u8>,
    sha256: Sha256Hex,
    evidence: HttpRequestEvidenceV1,
}

#[derive(Clone, Debug)]
struct LoaderOptions {
    clob_market_base: String,
    gamma_keyset_url: String,
    https_only: bool,
    connect_timeout: Duration,
    request_timeout: Duration,
    initialization_timeout: Duration,
    max_response_bytes: usize,
}

impl LoaderOptions {
    fn production() -> Self {
        Self {
            clob_market_base: OFFICIAL_CLOB_MARKET_BASE.to_owned(),
            gamma_keyset_url: OFFICIAL_GAMMA_KEYSET_URL.to_owned(),
            https_only: true,
            connect_timeout: CONNECT_TIMEOUT,
            request_timeout: REQUEST_TIMEOUT,
            initialization_timeout: PROCESS_RULE_INITIALIZATION_TIMEOUT,
            max_response_bytes: MAX_RULE_HTTP_RESPONSE_BYTES,
        }
    }
}

struct PlatformRuleHttpLoader {
    client: reqwest::Client,
    options: LoaderOptions,
    now_utc_ms: fn() -> Result<i64, RuleSnapshotError>,
}

impl PlatformRuleHttpLoader {
    fn new(
        options: LoaderOptions,
        now_utc_ms: fn() -> Result<i64, RuleSnapshotError>,
    ) -> Result<Self, RuleSnapshotError> {
        let client = reqwest::Client::builder()
            .https_only(options.https_only)
            .redirect(Policy::none())
            .connect_timeout(options.connect_timeout)
            .timeout(options.request_timeout)
            .user_agent("sim-system/0.1.0 process-rule-initialization")
            .build()
            .map_err(|error| RuleSnapshotError::ClientBuild(error.to_string()))?;
        Ok(Self {
            client,
            options,
            now_utc_ms,
        })
    }

    async fn load(
        &self,
        reference: &ReferenceMarketSpecV1,
    ) -> Result<LoadedHttpRules, RuleSnapshotError> {
        let clob_url = format!(
            "{}/{}",
            self.options.clob_market_base, reference.condition_id.0
        );
        let clob_response = self
            .fetch_with_retry(
                HttpEndpointKindV1::ClobMarketInfo,
                clob_url,
                reference.condition_id.clone(),
                None,
            )
            .await?;
        let clob: ClobMarketWire = decode_hashed_response(&clob_response)?;
        let mut requests = vec![clob_response.evidence];

        let mut gamma = Vec::new();
        for closed in [false, true] {
            let url = build_gamma_request_url(
                &self.client,
                &self.options.gamma_keyset_url,
                &reference.condition_id,
                closed,
            )?;
            let response = self
                .fetch_with_retry(
                    HttpEndpointKindV1::GammaMarketsKeyset,
                    url.to_string(),
                    reference.condition_id.clone(),
                    Some(closed),
                )
                .await?;
            let parsed: GammaKeysetWire = decode_hashed_response(&response)?;
            for market in &parsed.markets {
                if market.closed != closed {
                    return Err(RuleSnapshotError::InvalidRules(format!(
                        "Gamma returned closed={} for closed={closed} query",
                        market.closed
                    )));
                }
            }
            gamma.extend(parsed.markets);
            requests.push(response.evidence);
        }

        Ok(LoadedHttpRules {
            clob,
            gamma,
            requests,
        })
    }

    async fn fetch_with_retry(
        &self,
        endpoint_kind: HttpEndpointKindV1,
        url: String,
        condition_id: ConditionId,
        closed: Option<bool>,
    ) -> Result<HashedResponse, RuleSnapshotError> {
        let mut attempts = Vec::new();
        for attempt in 1..=2 {
            let started_at_utc_ms = (self.now_utc_ms)()?;
            let response = self
                .client
                .get(&url)
                .header("accept", "application/json")
                .send()
                .await;
            match response {
                Ok(mut response) => {
                    let status = response.status();
                    let body =
                        match read_bounded_body(&mut response, self.options.max_response_bytes)
                            .await
                        {
                            Ok(body) => body,
                            Err(BoundedBodyError::TooLarge) => {
                                return Err(RuleSnapshotError::ResponseTooLarge {
                                    url,
                                    limit: self.options.max_response_bytes,
                                });
                            }
                            Err(BoundedBodyError::Transport { error, partial_len }) => {
                                let completed_at_utc_ms = (self.now_utc_ms)()?;
                                attempts.push(HttpAttemptEvidenceV1 {
                                    attempt,
                                    started_at_utc_ms,
                                    completed_at_utc_ms,
                                    status_code: Some(status.as_u16()),
                                    response_bytes: Some(partial_len as u64),
                                    raw_response_sha256: None,
                                    transport_error_kind: Some(
                                        transport_error_kind(&error).to_owned(),
                                    ),
                                });
                                if attempt == 1 && is_transient_transport_error(&error) {
                                    continue;
                                }
                                return Err(RuleSnapshotError::HttpTransport {
                                    url,
                                    message: error.to_string(),
                                    attempts,
                                });
                            }
                        };
                    let completed_at_utc_ms = (self.now_utc_ms)()?;
                    let raw_sha256 = sha256_hex(&body);
                    attempts.push(HttpAttemptEvidenceV1 {
                        attempt,
                        started_at_utc_ms,
                        completed_at_utc_ms,
                        status_code: Some(status.as_u16()),
                        response_bytes: Some(body.len() as u64),
                        raw_response_sha256: Some(raw_sha256.clone()),
                        transport_error_kind: None,
                    });
                    if status == StatusCode::OK {
                        return Ok(HashedResponse {
                            url: url.clone(),
                            bytes: body,
                            sha256: raw_sha256,
                            evidence: HttpRequestEvidenceV1 {
                                endpoint_kind,
                                method: "GET".to_owned(),
                                request_url: url,
                                condition_id,
                                closed,
                                attempts,
                            },
                        });
                    }
                    if attempt == 1
                        && matches!(
                            status,
                            StatusCode::INTERNAL_SERVER_ERROR | StatusCode::SERVICE_UNAVAILABLE
                        )
                    {
                        continue;
                    }
                    return Err(RuleSnapshotError::HttpStatus {
                        url,
                        status: status.as_u16(),
                        attempts,
                    });
                }
                Err(error) => {
                    let completed_at_utc_ms = (self.now_utc_ms)()?;
                    attempts.push(HttpAttemptEvidenceV1 {
                        attempt,
                        started_at_utc_ms,
                        completed_at_utc_ms,
                        status_code: None,
                        response_bytes: None,
                        raw_response_sha256: None,
                        transport_error_kind: Some(transport_error_kind(&error).to_owned()),
                    });
                    if attempt == 1 && is_transient_transport_error(&error) {
                        continue;
                    }
                    return Err(RuleSnapshotError::HttpTransport {
                        url,
                        message: error.to_string(),
                        attempts,
                    });
                }
            }
        }
        unreachable!("the two-attempt loop always returns")
    }
}

struct LoadedHttpRules {
    clob: ClobMarketWire,
    gamma: Vec<GammaMarketWire>,
    requests: Vec<HttpRequestEvidenceV1>,
}

pub async fn query_and_seal_current_process_snapshot(
    request: CurrentProcessSnapshotRequestV1,
) -> Result<Arc<SealedProcessRuleSnapshot>, RuleSnapshotError> {
    query_and_seal_current_process_snapshot_with(
        request,
        LoaderOptions::production(),
        PLATFORM_RULE_CONFIG_BYTES,
        system_now_utc_ms,
    )
    .await
}

pub fn load_historical_process_snapshot(
    request: HistoricalProcessSnapshotRequestV1,
) -> Result<Arc<SealedProcessRuleSnapshot>, RuleSnapshotError> {
    let loaded = read_and_validate_snapshot(
        &request.snapshot_path,
        &request.expected_file_sha256,
        SnapshotAccessMode::HistoricalAsOf,
        MAX_RULE_SNAPSHOT_BYTES,
    )?;
    if loaded.snapshot.provenance.rules_observed_at_utc_ms > request.replay_causal_start_utc_ms {
        return Err(RuleSnapshotError::HistoricalNotApplicable {
            observed_at_utc_ms: loaded.snapshot.provenance.rules_observed_at_utc_ms,
            causal_start_utc_ms: request.replay_causal_start_utc_ms,
        });
    }
    Ok(Arc::new(loaded))
}

pub fn recover_process_snapshot(
    request: RecoveryProcessSnapshotRequestV1,
) -> Result<Arc<SealedProcessRuleSnapshot>, RuleSnapshotError> {
    validate_implementation_identity(&request.expected_implementation_identity)?;
    let loaded = read_and_validate_snapshot(
        &request.snapshot_path,
        &request.expected_file_sha256,
        SnapshotAccessMode::Recovery,
        MAX_RULE_SNAPSHOT_BYTES,
    )?;
    if loaded.snapshot.provenance.implementation_identity
        != request.expected_implementation_identity
    {
        return Err(RuleSnapshotError::IncompatibleImplementation);
    }
    Ok(Arc::new(loaded))
}

async fn query_and_seal_current_process_snapshot_with(
    request: CurrentProcessSnapshotRequestV1,
    options: LoaderOptions,
    config_bytes: &[u8],
    now_utc_ms: fn() -> Result<i64, RuleSnapshotError>,
) -> Result<Arc<SealedProcessRuleSnapshot>, RuleSnapshotError> {
    let reference_market = canonical_reference_market(request.reference_market)?;
    validate_implementation_identity(&request.implementation_identity)?;
    let config = parse_and_validate_config(config_bytes)?;
    reject_existing_target_before_query(&request.target_path)?;

    let query_started_at_utc_ms = now_utc_ms()?;
    let loader = PlatformRuleHttpLoader::new(options.clone(), now_utc_ms)?;
    let loaded = tokio::time::timeout(
        options.initialization_timeout,
        loader.load(&reference_market),
    )
    .await
    .map_err(|_| RuleSnapshotError::InitializationTimeout)??;
    let query_completed_at_utc_ms = now_utc_ms()?;
    let synchronized_platform_rules = merge_and_validate_reference_rules(
        &reference_market,
        loaded.clob,
        loaded.gamma,
        &config.project_account_rules,
    )?;
    let provenance = PlatformRuleProvenanceV2 {
        created_via: SnapshotCreationModeV1::CurrentQuery,
        query_started_at_utc_ms,
        query_completed_at_utc_ms,
        rules_observed_at_utc_ms: query_completed_at_utc_ms,
        reference_market,
        reference_observation_evidence: reference_observation_evidence(),
        application_policy: process_rule_application_policy(),
        http_requests: loaded.requests,
        static_config: StaticConfigEvidenceV1 {
            path: PLATFORM_RULE_CONFIG_PATH.to_owned(),
            schema_version: config.schema_version,
            raw_sha256: sha256_hex(config_bytes),
        },
        implementation_identity: request.implementation_identity,
        dependencies: direct_dependency_identities(),
        reference_validation: ReferenceValidationV1 {
            exact_reference_identity: true,
            token_outcome_match: true,
            enabled_fee_schedule_match: true,
        },
    };
    let mut snapshot = PlatformRuleSnapshotV2 {
        schema_version: PLATFORM_RULE_SNAPSHOT_SCHEMA_V2.to_owned(),
        content_sha256: Sha256Hex(String::new()),
        synchronized_platform_rules,
        documented_platform_rules: config.documented_platform_rules,
        project_account_rules: config.project_account_rules,
        provenance,
    };
    snapshot.content_sha256 = compute_snapshot_content_sha256(&snapshot)?;
    validate_snapshot(&snapshot)?;
    let (file_sha256, path) =
        seal_snapshot(&request.target_path, &snapshot, MAX_RULE_SNAPSHOT_BYTES)?;
    Ok(Arc::new(SealedProcessRuleSnapshot {
        path,
        file_sha256,
        access_mode: SnapshotAccessMode::CurrentQuery,
        snapshot,
    }))
}

fn build_gamma_request_url(
    client: &reqwest::Client,
    base_url: &str,
    condition_id: &ConditionId,
    closed: bool,
) -> Result<reqwest::Url, RuleSnapshotError> {
    let query = [
        ("limit", "100".to_owned()),
        ("condition_ids", condition_id.0.clone()),
        ("closed", closed.to_string()),
    ];
    client
        .get(base_url)
        .query(&query)
        .build()
        .map(|request| request.url().clone())
        .map_err(|error| RuleSnapshotError::QueryBuild(error.to_string()))
}

enum BoundedBodyError {
    TooLarge,
    Transport {
        error: reqwest::Error,
        partial_len: usize,
    },
}

async fn read_bounded_body(
    response: &mut reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, BoundedBodyError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(BoundedBodyError::TooLarge);
    }
    let mut bytes = Vec::new();
    loop {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(error) => {
                return Err(BoundedBodyError::Transport {
                    error,
                    partial_len: bytes.len(),
                });
            }
        };
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(BoundedBodyError::TooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn decode_hashed_response<T>(response: &HashedResponse) -> Result<T, RuleSnapshotError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_slice(&response.bytes).map_err(|error| RuleSnapshotError::ResponseDecode {
        url: response.url.clone(),
        raw_sha256: response.sha256.clone(),
        message: error.to_string(),
    })
}

fn merge_and_validate_reference_rules(
    reference: &ReferenceMarketSpecV1,
    clob: ClobMarketWire,
    gamma: Vec<GammaMarketWire>,
    project_rules: &ProjectAccountRulesV1,
) -> Result<SynchronizedPlatformRulesV1, RuleSnapshotError> {
    if let Some(returned) = clob.c.as_deref()
        && canonical_condition_id(returned)? != reference.condition_id
    {
        return Err(RuleSnapshotError::InvalidRules(
            "CLOB condition identity does not match the reference market".to_owned(),
        ));
    }
    let clob_outcomes =
        canonical_wire_outcomes(clob.t.into_iter().map(|item| (item.t, item.o)).collect())?;
    if clob_outcomes != reference.outcomes {
        return Err(RuleSnapshotError::InvalidRules(
            "CLOB token/outcome identity does not match the reference market".to_owned(),
        ));
    }
    if clob.mos.0 <= Decimal::ZERO {
        return Err(RuleSnapshotError::InvalidRules(
            "CLOB mos must be positive".to_owned(),
        ));
    }

    if gamma.len() != 1 {
        return Err(RuleSnapshotError::InvalidRules(format!(
            "Gamma must return the reference market exactly once, received {} records",
            gamma.len()
        )));
    }
    let gamma = gamma
        .into_iter()
        .next()
        .expect("length checked before extracting Gamma market");
    if canonical_condition_id(&gamma.condition_id)? != reference.condition_id
        || canonical_clob_market_id(&gamma.id)? != reference.clob_market_id
        || gamma.slug != reference.slug
    {
        return Err(RuleSnapshotError::InvalidRules(
            "Gamma identity does not match the reference market".to_owned(),
        ));
    }
    let outcome_labels = serde_json::from_str::<Vec<String>>(&gamma.outcomes).map_err(|error| {
        RuleSnapshotError::InvalidRules(format!("invalid Gamma outcomes: {error}"))
    })?;
    let token_ids =
        serde_json::from_str::<Vec<String>>(&gamma.clob_token_ids).map_err(|error| {
            RuleSnapshotError::InvalidRules(format!("invalid Gamma clobTokenIds: {error}"))
        })?;
    if outcome_labels.len() != token_ids.len() {
        return Err(RuleSnapshotError::InvalidRules(
            "Gamma token/outcome lengths differ".to_owned(),
        ));
    }
    let gamma_outcomes =
        canonical_wire_outcomes(token_ids.into_iter().zip(outcome_labels).collect())?;
    if gamma_outcomes != reference.outcomes {
        return Err(RuleSnapshotError::InvalidRules(
            "Gamma token/outcome identity does not match the reference market".to_owned(),
        ));
    }
    if !gamma.fees_enabled {
        return Err(RuleSnapshotError::InvalidRules(
            "reference market must have fees enabled".to_owned(),
        ));
    }
    let gamma_fee = gamma.fee_schedule.ok_or_else(|| {
        RuleSnapshotError::InvalidRules(
            "reference market feeSchedule is required when fees are enabled".to_owned(),
        )
    })?;
    let exponent = decimal_to_u32(&gamma_fee.exponent.0, "Gamma fee exponent")?;
    let expected_maker_rate = project_rules
        .rebate_settlement
        .value
        .fixed_maker_rebate_rate
        .value();
    if gamma_fee.rate.0 <= Decimal::ZERO
        || exponent == 0
        || !gamma_fee.taker_only
        || gamma_fee.rebate_rate.0 != expected_maker_rate
    {
        return Err(RuleSnapshotError::InvalidRules(
            "reference market feeSchedule conflicts with the fixed process policy".to_owned(),
        ));
    }
    let clob_fee = clob.fd.ok_or_else(|| {
        RuleSnapshotError::InvalidRules(
            "CLOB fd is required for the fee-enabled reference market".to_owned(),
        )
    })?;
    let clob_exponent = decimal_to_u32(&clob_fee.e.0, "CLOB fee exponent")?;
    if clob_fee.r.0 != gamma_fee.rate.0
        || clob_exponent != exponent
        || clob_fee.to != gamma_fee.taker_only
    {
        return Err(RuleSnapshotError::InvalidRules(
            "CLOB and Gamma fee schedules conflict".to_owned(),
        ));
    }

    Ok(SynchronizedPlatformRulesV1 {
        minimum_order_size_shares: RuleDecimal::new(clob.mos.0),
        taker_order_delay_enabled: clob.itode,
        fees_enabled: true,
        fee_schedule: FeeScheduleV1 {
            rate: RuleDecimal::new(gamma_fee.rate.0),
            exponent,
            taker_only: true,
            rebate_rate: RuleDecimal::new(gamma_fee.rebate_rate.0),
        },
    })
}

fn canonical_reference_market(
    reference: ReferenceMarketSpecV1,
) -> Result<ReferenceMarketSpecV1, RuleSnapshotError> {
    let condition_id = canonical_condition_id(&reference.condition_id.0)?;
    let clob_market_id = canonical_clob_market_id(&reference.clob_market_id.0)?;
    if reference.slug.trim() != reference.slug
        || !reference.slug.starts_with(BTC_UPDOWN_SLUG_PREFIX)
        || reference.slug.len() == BTC_UPDOWN_SLUG_PREFIX.len()
    {
        return Err(RuleSnapshotError::InvalidReferenceMarket(
            "slug must identify one btc-updown-5m market".to_owned(),
        ));
    }
    if reference.outcomes.len() != 2 {
        return Err(RuleSnapshotError::InvalidReferenceMarket(
            "reference market must have exactly two outcomes".to_owned(),
        ));
    }
    let outcomes = canonical_wire_outcomes(
        reference
            .outcomes
            .into_iter()
            .map(|outcome| (outcome.token_id.0, outcome.outcome))
            .collect(),
    )?;
    if outcomes
        .iter()
        .map(|outcome| outcome.outcome.as_str())
        .collect::<Vec<_>>()
        != ["Up", "Down"]
    {
        return Err(RuleSnapshotError::InvalidReferenceMarket(
            "reference outcomes must be ordered Up then Down".to_owned(),
        ));
    }
    Ok(ReferenceMarketSpecV1 {
        condition_id,
        clob_market_id,
        slug: reference.slug,
        outcomes,
    })
}

fn canonical_wire_outcomes(
    outcomes: Vec<(String, String)>,
) -> Result<Vec<ReferenceOutcomeSpecV1>, RuleSnapshotError> {
    let mut canonical = Vec::with_capacity(outcomes.len());
    let mut tokens = BTreeSet::new();
    let mut labels = BTreeSet::new();
    for (token_id, outcome) in outcomes {
        if outcome.trim() != outcome || outcome.is_empty() {
            return Err(RuleSnapshotError::InvalidReferenceMarket(
                "outcome labels must be non-empty and canonical".to_owned(),
            ));
        }
        let token_id = canonical_token_id(&token_id)?;
        if !tokens.insert(token_id.0.clone()) || !labels.insert(outcome.clone()) {
            return Err(RuleSnapshotError::InvalidReferenceMarket(
                "token and outcome identities must be unique".to_owned(),
            ));
        }
        canonical.push(ReferenceOutcomeSpecV1 { token_id, outcome });
    }
    Ok(canonical)
}

fn canonical_condition_id(value: &str) -> Result<ConditionId, RuleSnapshotError> {
    B256::from_str(value)
        .map(|condition| ConditionId(condition.to_string()))
        .map_err(|error| {
            RuleSnapshotError::InvalidReferenceMarket(format!(
                "invalid condition id {value}: {error}"
            ))
        })
}

fn canonical_token_id(value: &str) -> Result<TokenId, RuleSnapshotError> {
    U256::from_str(value)
        .map(|token| TokenId(token.to_string()))
        .map_err(|error| {
            RuleSnapshotError::InvalidReferenceMarket(format!("invalid token id {value}: {error}"))
        })
}

fn canonical_clob_market_id(value: &str) -> Result<ClobMarketId, RuleSnapshotError> {
    let parsed = value.parse::<u64>().map_err(|error| {
        RuleSnapshotError::InvalidReferenceMarket(format!(
            "invalid CLOB market id {value}: {error}"
        ))
    })?;
    if parsed == 0 {
        return Err(RuleSnapshotError::InvalidReferenceMarket(
            "CLOB market id must be positive".to_owned(),
        ));
    }
    Ok(ClobMarketId(parsed.to_string()))
}

fn process_rule_application_policy() -> EvidenceBackedRuleV1<BtcUpdownProcessRulePolicyV1> {
    EvidenceBackedRuleV1 {
        value: BtcUpdownProcessRulePolicyV1 {
            policy_version: PROCESS_RULE_TEMPLATE_VERSION.to_owned(),
            supported_slug_prefix: BTC_UPDOWN_SLUG_PREFIX.to_owned(),
            reuse_reference_values_until_process_exit: true,
            per_market_requery_allowed: false,
        },
        evidence: RuleEvidenceV1 {
            category: RuleEvidenceCategory::ProjectAssumption,
            source: "user-decision:2026-08-09:single-reference-process-freeze".to_owned(),
            reviewed_on_utc_date: "2026-08-09".to_owned(),
            scope: "one sim-system process and btc-updown-5m sessions".to_owned(),
            limitation: "the official responses prove only the reference market at the observation time; reuse for later markets is a project assumption".to_owned(),
            version: PROCESS_RULE_TEMPLATE_VERSION.to_owned(),
            failure_semantics: "invalid reference rules fail process initialization; sealed rules are never refreshed or extended".to_owned(),
        },
    }
}

fn reference_observation_evidence() -> RuleEvidenceV1 {
    RuleEvidenceV1 {
        category: RuleEvidenceCategory::OfficialPlatformRule,
        source: "the three official CLOB and Gamma responses bound by provenance.http_requests"
            .to_owned(),
        reviewed_on_utc_date: "2026-08-09".to_owned(),
        scope: "the reference btc-updown-5m market at rules_observed_at_utc_ms".to_owned(),
        limitation: "the responses do not prove that later markets retain the same rules"
            .to_owned(),
        version: "REFERENCE_MARKET_OBSERVATION_V1".to_owned(),
        failure_semantics:
            "missing, incomplete, conflicting, or invalid reference evidence fails initialization"
                .to_owned(),
    }
}

fn parse_and_validate_config(bytes: &[u8]) -> Result<PlatformRulesConfigV1, RuleSnapshotError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|error| RuleSnapshotError::InvalidConfig(error.to_string()))?;
    let config: PlatformRulesConfigV1 = toml::from_str(text)
        .map_err(|error| RuleSnapshotError::InvalidConfig(error.to_string()))?;
    validate_config(&config)?;
    Ok(config)
}

fn validate_config(config: &PlatformRulesConfigV1) -> Result<(), RuleSnapshotError> {
    if config.schema_version != PLATFORM_RULE_CONFIG_SCHEMA_V1 {
        return Err(RuleSnapshotError::InvalidConfig(format!(
            "unexpected schema {}",
            config.schema_version
        )));
    }
    let documented = &config.documented_platform_rules;
    validate_evidence(
        &documented.taker_delay.evidence,
        RuleEvidenceCategory::OfficialPlatformRule,
    )?;
    validate_evidence(
        &documented.post_orders_batch.evidence,
        RuleEvidenceCategory::OfficialSdkContract,
    )?;
    validate_evidence(
        &documented.cancel_orders_batch.evidence,
        RuleEvidenceCategory::UncertaintyProfile,
    )?;
    validate_evidence(
        &documented.gtd_timing.evidence,
        RuleEvidenceCategory::OfficialSdkContract,
    )?;
    validate_evidence(
        &documented.fee_computation.evidence,
        RuleEvidenceCategory::OfficialPlatformRule,
    )?;
    validate_evidence(
        &documented.maker_rebate_payment.evidence,
        RuleEvidenceCategory::OfficialPlatformRule,
    )?;
    validate_evidence(
        &documented.immediate_gtc_buy_minimum.evidence,
        RuleEvidenceCategory::ObservedPlatformBehavior,
    )?;
    validate_evidence(
        &documented.immediate_gtd_buy_minimum.evidence,
        RuleEvidenceCategory::RuleDerivation,
    )?;
    validate_evidence(
        &documented.resolved_order_status.evidence,
        RuleEvidenceCategory::OfficialSdkContract,
    )?;
    if documented.taker_delay.value
        != (TakerDelayRuleV1 {
            enabled_only_when_itode: true,
            applies_only_to_fok_fak_market_buy: true,
            delay_ms: 250,
        })
        || documented.post_orders_batch.value
            != (BatchRuleV1 {
                max_items: 15,
                per_item_results: true,
                request_is_atomic: false,
            })
        || documented.cancel_orders_batch.value
            != (BatchRuleV1 {
                max_items: 1_000,
                per_item_results: true,
                request_is_atomic: false,
            })
        || documented.gtd_timing.value
            != (GtdTimingRuleV1 {
                minimum_lead_seconds: 180,
                safety_offset_seconds: 60,
            })
        || documented.fee_computation.value.formula_version != "POLYMARKET_FEE_CURVE_V1"
        || documented.fee_computation.value.precision_decimal_places != 5
        || documented
            .fee_computation
            .value
            .minimum_nonzero_fee_pusd
            .value()
            != Decimal::new(1, 5)
        || documented
            .maker_rebate_payment
            .value
            .attribution_day_boundary
            != "UTC"
        || documented
            .maker_rebate_payment
            .value
            .minimum_payment_pusd
            .value()
            != Decimal::ONE
        || documented
            .immediate_gtc_buy_minimum
            .value
            .minimum_principal_pusd
            .value()
            != Decimal::ONE
        || documented
            .immediate_gtd_buy_minimum
            .value
            .minimum_principal_pusd
            .value()
            != Decimal::ONE
        || !documented
            .immediate_gtc_buy_minimum
            .value
            .applies_to_marketable_limit_buy
        || documented
            .immediate_gtc_buy_minimum
            .value
            .applies_to_non_marketable_limit_buy
        || documented
            .immediate_gtc_buy_minimum
            .value
            .applies_to_fok_or_fak
        || !documented
            .immediate_gtd_buy_minimum
            .value
            .applies_to_marketable_limit_buy
        || documented
            .immediate_gtd_buy_minimum
            .value
            .applies_to_non_marketable_limit_buy
        || documented
            .immediate_gtd_buy_minimum
            .value
            .applies_to_fok_or_fak
        || documented.resolved_order_status.value.raw_status
            != "ORDER_STATUS_CANCELED_MARKET_RESOLVED"
    {
        return Err(RuleSnapshotError::InvalidConfig(
            "documented rule values differ from the approved V1 contract".to_owned(),
        ));
    }

    let project = &config.project_account_rules;
    validate_evidence(
        &project.l2_queue_model.evidence,
        RuleEvidenceCategory::ProjectAssumption,
    )?;
    validate_evidence(
        &project.lifecycle_account_behavior.evidence,
        RuleEvidenceCategory::UserConfirmedPlatformBehavior,
    )?;
    validate_evidence(
        &project.rebate_settlement.evidence,
        RuleEvidenceCategory::ProjectAssumption,
    )?;
    let rebate = &project.rebate_settlement.value;
    if project.l2_queue_model.value.model_version != "L2_BOOK_DELTA_FIFO_ESTIMATE_V1"
        || project
            .lifecycle_account_behavior
            .value
            .venue_dust_upper_exclusive_shares
            .value()
            != Decimal::new(1, 2)
        || !project
            .lifecycle_account_behavior
            .value
            .venue_dust_requires_positive_fill
        || !project
            .lifecycle_account_behavior
            .value
            .reject_new_orders_at_end_at
        || !project
            .lifecycle_account_behavior
            .value
            .existing_orders_queryable_after_end_at
        || !project
            .lifecycle_account_behavior
            .value
            .existing_orders_cancelable_after_end_at
        || project
            .lifecycle_account_behavior
            .value
            .existing_orders_matchable_after_end_at
        || !project
            .lifecycle_account_behavior
            .value
            .auto_redeem_only_after_qualified_market_resolved
        || rebate.fixed_taker_rebate_rate.value() != Decimal::new(8, 2)
        || rebate.fixed_maker_rebate_rate.value() != Decimal::new(2, 1)
        || rebate.dynamic_rebate_tiers_enabled
        || rebate.one_time_bonuses_enabled
        || rebate.liquidity_rewards_enabled
        || rebate.taker_cash_available_at_utc != "00:10:00"
        || rebate.maker_cash_available_at_utc != "00:45:00"
        || rebate.payment_floor_decimal_places != 4
        || !rebate.truncated_remainder_stays_locked
    {
        return Err(RuleSnapshotError::InvalidConfig(
            "project/account rule values differ from the approved V1 contract".to_owned(),
        ));
    }
    Ok(())
}

fn validate_evidence(
    evidence: &RuleEvidenceV1,
    expected_category: RuleEvidenceCategory,
) -> Result<(), RuleSnapshotError> {
    if evidence.category != expected_category {
        return Err(RuleSnapshotError::InvalidConfig(format!(
            "evidence category {:?} does not match {:?}",
            evidence.category, expected_category
        )));
    }
    if evidence.source.is_empty()
        || evidence.scope.is_empty()
        || evidence.limitation.is_empty()
        || evidence.version.is_empty()
        || evidence.failure_semantics.is_empty()
        || !is_utc_date(&evidence.reviewed_on_utc_date)
    {
        return Err(RuleSnapshotError::InvalidConfig(
            "rule evidence metadata is incomplete".to_owned(),
        ));
    }
    Ok(())
}

fn validate_implementation_identity(
    identity: &ImplementationIdentityV1,
) -> Result<(), RuleSnapshotError> {
    if identity.git_commit.len() != 40
        || !identity
            .git_commit
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(RuleSnapshotError::InvalidReferenceMarket(
            "implementation git commit must be 40 hexadecimal characters".to_owned(),
        ));
    }
    validate_sha256(&identity.source_identity_sha256)?;
    validate_sha256(&identity.cargo_lock_sha256)
}

fn validate_snapshot(snapshot: &PlatformRuleSnapshotV2) -> Result<(), RuleSnapshotError> {
    if snapshot.schema_version != PLATFORM_RULE_SNAPSHOT_SCHEMA_V2 {
        return Err(RuleSnapshotError::SnapshotIntegrity(format!(
            "unexpected schema {}",
            snapshot.schema_version
        )));
    }
    validate_sha256(&snapshot.content_sha256)?;
    if snapshot.content_sha256 != compute_snapshot_content_sha256(snapshot)? {
        return Err(RuleSnapshotError::SnapshotIntegrity(
            "content SHA-256 mismatch".to_owned(),
        ));
    }

    let canonical_reference =
        canonical_reference_market(snapshot.provenance.reference_market.clone())
            .map_err(|error| RuleSnapshotError::SnapshotIntegrity(error.to_string()))?;
    if canonical_reference != snapshot.provenance.reference_market {
        return Err(RuleSnapshotError::SnapshotIntegrity(
            "reference market is not canonical".to_owned(),
        ));
    }
    validate_implementation_identity(&snapshot.provenance.implementation_identity)
        .map_err(|error| RuleSnapshotError::SnapshotIntegrity(error.to_string()))?;
    let config = PlatformRulesConfigV1 {
        schema_version: snapshot.provenance.static_config.schema_version.clone(),
        documented_platform_rules: snapshot.documented_platform_rules.clone(),
        project_account_rules: snapshot.project_account_rules.clone(),
    };
    validate_config(&config)
        .map_err(|error| RuleSnapshotError::SnapshotIntegrity(error.to_string()))?;
    validate_sha256(&snapshot.provenance.static_config.raw_sha256)?;
    if snapshot.provenance.static_config.path != PLATFORM_RULE_CONFIG_PATH {
        return Err(RuleSnapshotError::SnapshotIntegrity(
            "static config path is invalid".to_owned(),
        ));
    }
    validate_synchronized_rules(
        &snapshot.synchronized_platform_rules,
        &snapshot.project_account_rules,
    )?;

    if snapshot.provenance.created_via != SnapshotCreationModeV1::CurrentQuery
        || snapshot.provenance.query_started_at_utc_ms < 0
        || snapshot.provenance.query_completed_at_utc_ms
            < snapshot.provenance.query_started_at_utc_ms
        || snapshot.provenance.rules_observed_at_utc_ms
            != snapshot.provenance.query_completed_at_utc_ms
        || !snapshot
            .provenance
            .reference_validation
            .exact_reference_identity
        || !snapshot.provenance.reference_validation.token_outcome_match
        || !snapshot
            .provenance
            .reference_validation
            .enabled_fee_schedule_match
    {
        return Err(RuleSnapshotError::SnapshotIntegrity(
            "reference provenance invariants are invalid".to_owned(),
        ));
    }
    let expected_policy = process_rule_application_policy();
    if snapshot.provenance.application_policy != expected_policy {
        return Err(RuleSnapshotError::SnapshotIntegrity(
            "process rule application policy is invalid".to_owned(),
        ));
    }
    if snapshot.provenance.reference_observation_evidence != reference_observation_evidence() {
        return Err(RuleSnapshotError::SnapshotIntegrity(
            "reference observation evidence is invalid".to_owned(),
        ));
    }
    validate_dependency_identities(&snapshot.provenance.dependencies)?;
    validate_http_provenance(
        &snapshot.provenance.http_requests,
        &snapshot.provenance.reference_market,
        snapshot.provenance.query_started_at_utc_ms,
        snapshot.provenance.query_completed_at_utc_ms,
    )
}

fn validate_synchronized_rules(
    rules: &SynchronizedPlatformRulesV1,
    project: &ProjectAccountRulesV1,
) -> Result<(), RuleSnapshotError> {
    if rules.minimum_order_size_shares.value() <= Decimal::ZERO
        || !rules.fees_enabled
        || rules.fee_schedule.rate.value() <= Decimal::ZERO
        || rules.fee_schedule.exponent == 0
        || !rules.fee_schedule.taker_only
        || rules.fee_schedule.rebate_rate.value()
            != project
                .rebate_settlement
                .value
                .fixed_maker_rebate_rate
                .value()
    {
        return Err(RuleSnapshotError::SnapshotIntegrity(
            "synchronized platform rules violate the process policy".to_owned(),
        ));
    }
    Ok(())
}

fn validate_http_provenance(
    requests: &[HttpRequestEvidenceV1],
    reference: &ReferenceMarketSpecV1,
    query_started_at_utc_ms: i64,
    query_completed_at_utc_ms: i64,
) -> Result<(), RuleSnapshotError> {
    if requests.len() != 3 {
        return Err(RuleSnapshotError::SnapshotIntegrity(
            "exactly three reference-market requests are required".to_owned(),
        ));
    }
    let expected = [
        (HttpEndpointKindV1::ClobMarketInfo, None),
        (HttpEndpointKindV1::GammaMarketsKeyset, Some(false)),
        (HttpEndpointKindV1::GammaMarketsKeyset, Some(true)),
    ];
    for (request, (endpoint, closed)) in requests.iter().zip(expected) {
        if request.endpoint_kind != endpoint
            || request.method != "GET"
            || request.condition_id != reference.condition_id
            || request.closed != closed
            || request.attempts.is_empty()
            || request.attempts.len() > 2
        {
            return Err(RuleSnapshotError::SnapshotIntegrity(
                "HTTP request provenance is invalid".to_owned(),
            ));
        }
        let url = reqwest::Url::parse(&request.request_url).map_err(|error| {
            RuleSnapshotError::SnapshotIntegrity(format!("invalid request URL: {error}"))
        })?;
        let valid_origin = official_or_test_endpoint_origin(&url, endpoint);
        let valid_route = match endpoint {
            HttpEndpointKindV1::ClobMarketInfo => {
                url.path() == format!("/clob-markets/{}", reference.condition_id.0)
                    && url.query().is_none()
            }
            HttpEndpointKindV1::GammaMarketsKeyset => {
                let pairs = url.query_pairs().collect::<Vec<_>>();
                url.path() == "/markets/keyset"
                    && pairs.len() == 3
                    && pairs[0] == ("limit".into(), "100".into())
                    && pairs[1]
                        == (
                            "condition_ids".into(),
                            reference.condition_id.0.as_str().into(),
                        )
                    && pairs[2]
                        == (
                            "closed".into(),
                            if closed == Some(true) {
                                "true".into()
                            } else {
                                "false".into()
                            },
                        )
            }
        };
        if !valid_origin || !valid_route {
            return Err(RuleSnapshotError::SnapshotIntegrity(
                "HTTP request origin or route does not match the approved reference endpoint"
                    .to_owned(),
            ));
        }
        for (index, attempt) in request.attempts.iter().enumerate() {
            if attempt.attempt as usize != index + 1
                || attempt.completed_at_utc_ms < attempt.started_at_utc_ms
                || attempt.started_at_utc_ms < query_started_at_utc_ms
                || attempt.completed_at_utc_ms > query_completed_at_utc_ms
                || attempt
                    .response_bytes
                    .is_some_and(|bytes| bytes > MAX_RULE_HTTP_RESPONSE_BYTES as u64)
                || index > 0
                    && request.attempts[index - 1].completed_at_utc_ms > attempt.started_at_utc_ms
            {
                return Err(RuleSnapshotError::SnapshotIntegrity(
                    "HTTP attempt provenance is invalid".to_owned(),
                ));
            }
        }
        for retryable in &request.attempts[..request.attempts.len() - 1] {
            let retryable_status = matches!(retryable.status_code, Some(500 | 503))
                && retryable.response_bytes.is_some()
                && retryable.raw_response_sha256.is_some()
                && retryable.transport_error_kind.is_none();
            let retryable_transport =
                retryable.transport_error_kind.is_some() && retryable.raw_response_sha256.is_none();
            if !retryable_status && !retryable_transport {
                return Err(RuleSnapshotError::SnapshotIntegrity(
                    "HTTP retry provenance is invalid".to_owned(),
                ));
            }
            if let Some(raw_sha256) = &retryable.raw_response_sha256 {
                validate_sha256(raw_sha256)?;
            }
        }
        let final_attempt = request
            .attempts
            .last()
            .expect("non-empty attempts checked above");
        if final_attempt.status_code != Some(StatusCode::OK.as_u16())
            || final_attempt.response_bytes.is_none_or(|bytes| bytes == 0)
            || final_attempt.raw_response_sha256.is_none()
            || final_attempt.transport_error_kind.is_some()
        {
            return Err(RuleSnapshotError::SnapshotIntegrity(
                "sealed snapshot lacks a complete successful response".to_owned(),
            ));
        }
        validate_sha256(
            final_attempt
                .raw_response_sha256
                .as_ref()
                .expect("presence checked above"),
        )?;
    }
    Ok(())
}

fn official_or_test_endpoint_origin(url: &reqwest::Url, endpoint: HttpEndpointKindV1) -> bool {
    let official_host = match endpoint {
        HttpEndpointKindV1::ClobMarketInfo => "clob.polymarket.com",
        HttpEndpointKindV1::GammaMarketsKeyset => "gamma-api.polymarket.com",
    };
    if url.scheme() == "https"
        && url.host_str() == Some(official_host)
        && url.port().is_none()
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
    {
        return true;
    }
    #[cfg(test)]
    {
        url.scheme() == "http"
            && url.host_str() == Some("127.0.0.1")
            && url.port().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none()
    }
    #[cfg(not(test))]
    false
}

fn validate_dependency_identities(
    dependencies: &[DependencyIdentityV1],
) -> Result<(), RuleSnapshotError> {
    let expected_names = direct_dependency_identities()
        .into_iter()
        .map(|dependency| dependency.name)
        .collect::<BTreeSet<_>>();
    let actual_names = dependencies
        .iter()
        .map(|dependency| dependency.name.clone())
        .collect::<BTreeSet<_>>();
    if dependencies.len() != actual_names.len()
        || actual_names != expected_names
        || dependencies
            .iter()
            .any(|dependency| dependency.version.is_empty() || dependency.source.is_empty())
    {
        return Err(RuleSnapshotError::SnapshotIntegrity(
            "direct dependency identities are incomplete or duplicated".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Serialize)]
struct SnapshotHashContent<'a> {
    schema_version: &'a str,
    synchronized_platform_rules: &'a SynchronizedPlatformRulesV1,
    documented_platform_rules: &'a DocumentedPlatformRulesV1,
    project_account_rules: &'a ProjectAccountRulesV1,
    provenance: &'a PlatformRuleProvenanceV2,
}

fn compute_snapshot_content_sha256(
    snapshot: &PlatformRuleSnapshotV2,
) -> Result<Sha256Hex, RuleSnapshotError> {
    sha256_serialized(&SnapshotHashContent {
        schema_version: &snapshot.schema_version,
        synchronized_platform_rules: &snapshot.synchronized_platform_rules,
        documented_platform_rules: &snapshot.documented_platform_rules,
        project_account_rules: &snapshot.project_account_rules,
        provenance: &snapshot.provenance,
    })
}

fn reject_existing_target_before_query(target_path: &Path) -> Result<(), RuleSnapshotError> {
    match std::fs::symlink_metadata(target_path) {
        Ok(_) => {
            return Err(RuleSnapshotError::SnapshotAlreadyExists(
                target_path.to_path_buf(),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(RuleSnapshotError::Io {
                operation: "inspect snapshot target",
                message: error.to_string(),
            });
        }
    }
    let (parent, target_name) = split_snapshot_path(target_path)?;
    let candidate = parent.join(format!(".{target_name}.candidate"));
    match std::fs::symlink_metadata(&candidate) {
        Ok(_) => Err(RuleSnapshotError::SnapshotCandidateExists(candidate)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(RuleSnapshotError::Io {
            operation: "inspect snapshot candidate",
            message: error.to_string(),
        }),
    }
}

fn seal_snapshot(
    target_path: &Path,
    snapshot: &PlatformRuleSnapshotV2,
    max_snapshot_bytes: usize,
) -> Result<(Sha256Hex, PathBuf), RuleSnapshotError> {
    let mut bytes = serde_json::to_vec_pretty(snapshot)
        .map_err(|error| RuleSnapshotError::SnapshotIntegrity(error.to_string()))?;
    bytes.push(b'\n');
    if bytes.len() > max_snapshot_bytes {
        return Err(RuleSnapshotError::SnapshotTooLarge {
            limit: max_snapshot_bytes,
        });
    }

    let (parent_path, target_name) = split_snapshot_path(target_path)?;
    let parent_fd = open(
        parent_path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| io_error("open snapshot parent", error))?;
    let parent = File::from(parent_fd);
    let candidate_name = format!(".{target_name}.candidate");
    let candidate_fd = match openat(
        &parent,
        candidate_name.as_str(),
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    ) {
        Ok(candidate) => candidate,
        Err(error) if error == rustix::io::Errno::EXIST => {
            return Err(RuleSnapshotError::SnapshotCandidateExists(
                parent_path.join(&candidate_name),
            ));
        }
        Err(error) => return Err(io_error("create snapshot candidate", error)),
    };
    let mut candidate = File::from(candidate_fd);
    let write_result = candidate
        .write_all(&bytes)
        .and_then(|()| candidate.sync_all());
    drop(candidate);
    if let Err(error) = write_result {
        let _ = unlinkat(&parent, candidate_name.as_str(), AtFlags::empty());
        return Err(RuleSnapshotError::Io {
            operation: "write and sync snapshot candidate",
            message: error.to_string(),
        });
    }
    if let Err(error) = renameat_with(
        &parent,
        candidate_name.as_str(),
        &parent,
        target_name.as_str(),
        RenameFlags::NOREPLACE,
    ) {
        let _ = unlinkat(&parent, candidate_name.as_str(), AtFlags::empty());
        if error == rustix::io::Errno::EXIST {
            return Err(RuleSnapshotError::SnapshotAlreadyExists(
                target_path.to_path_buf(),
            ));
        }
        return Err(io_error("publish snapshot", error));
    }
    if let Err(error) = parent.sync_all() {
        return Err(RuleSnapshotError::DurabilityUncertain {
            path: target_path.to_path_buf(),
            message: error.to_string(),
        });
    }
    Ok((sha256_hex(&bytes), target_path.to_path_buf()))
}

fn read_and_validate_snapshot(
    path: &Path,
    expected_file_sha256: &Sha256Hex,
    access_mode: SnapshotAccessMode,
    max_snapshot_bytes: usize,
) -> Result<SealedProcessRuleSnapshot, RuleSnapshotError> {
    validate_sha256(expected_file_sha256)?;
    let (parent_path, target_name) = split_snapshot_path(path)?;
    let parent_fd = open(
        parent_path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| io_error("open snapshot parent", error))?;
    let parent = File::from(parent_fd);
    let snapshot_fd = openat(
        &parent,
        target_name.as_str(),
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| io_error("open sealed snapshot", error))?;
    let mut file = File::from(snapshot_fd);
    let metadata = file.metadata().map_err(|error| RuleSnapshotError::Io {
        operation: "inspect sealed snapshot",
        message: error.to_string(),
    })?;
    if !metadata.is_file() || metadata.len() > max_snapshot_bytes as u64 {
        return Err(RuleSnapshotError::SnapshotIntegrity(
            "snapshot is not a bounded regular file".to_owned(),
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(max_snapshot_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| RuleSnapshotError::Io {
            operation: "read sealed snapshot",
            message: error.to_string(),
        })?;
    if bytes.len() > max_snapshot_bytes {
        return Err(RuleSnapshotError::SnapshotTooLarge {
            limit: max_snapshot_bytes,
        });
    }
    let actual_file_sha256 = sha256_hex(&bytes);
    if &actual_file_sha256 != expected_file_sha256 {
        return Err(RuleSnapshotError::SnapshotIntegrity(
            "file SHA-256 mismatch".to_owned(),
        ));
    }
    let snapshot: PlatformRuleSnapshotV2 = serde_json::from_slice(&bytes)
        .map_err(|error| RuleSnapshotError::SnapshotIntegrity(error.to_string()))?;
    validate_snapshot(&snapshot)?;
    Ok(SealedProcessRuleSnapshot {
        path: path.to_path_buf(),
        file_sha256: actual_file_sha256,
        access_mode,
        snapshot,
    })
}

fn split_snapshot_path(path: &Path) -> Result<(&Path, String), RuleSnapshotError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty() && *name != "." && *name != "..")
        .ok_or_else(|| {
            RuleSnapshotError::SnapshotIntegrity(
                "snapshot path must end in one UTF-8 file name".to_owned(),
            )
        })?;
    Ok((parent, file_name.to_owned()))
}

fn direct_dependency_identities() -> Vec<DependencyIdentityV1> {
    [
        ("contracts", "0.1.0", "workspace"),
        (
            "polymarket_client_sdk_v2",
            "0.7.0",
            "workspace-patch:third_party/polymarket_client_sdk_v2-0.7.0-p1",
        ),
        ("reqwest", "0.13.4", "crates.io"),
        ("rust_decimal", "1.42.1", "crates.io"),
        ("rustix", "1.1.4", "crates.io"),
        ("serde", "1.0.228", "crates.io"),
        ("serde_json", "1.0.150", "crates.io"),
        ("sha2", "0.10.9", "crates.io"),
        ("tokio", "1.52.3", "crates.io"),
        ("toml", "1.1.4+spec-1.1.0", "crates.io"),
    ]
    .into_iter()
    .map(|(name, version, source)| DependencyIdentityV1 {
        name: name.to_owned(),
        version: version.to_owned(),
        source: source.to_owned(),
    })
    .collect()
}

fn parse_decimal(lexical: &str) -> Result<Decimal, String> {
    Decimal::from_str_exact(lexical)
        .or_else(|_| Decimal::from_scientific(lexical))
        .map_err(|error| format!("invalid exact decimal {lexical}: {error}"))
}

fn decimal_to_u32(value: &Decimal, name: &str) -> Result<u32, RuleSnapshotError> {
    if value.fract() != Decimal::ZERO {
        return Err(RuleSnapshotError::InvalidRules(format!(
            "{name} must be an integer"
        )));
    }
    value
        .to_u32()
        .ok_or_else(|| RuleSnapshotError::InvalidRules(format!("{name} is outside u32")))
}

fn sha256_hex(bytes: &[u8]) -> Sha256Hex {
    Sha256Hex(format!("{:x}", Sha256::digest(bytes)))
}

fn sha256_serialized<T: Serialize + ?Sized>(value: &T) -> Result<Sha256Hex, RuleSnapshotError> {
    serde_json::to_vec(value)
        .map(|bytes| sha256_hex(&bytes))
        .map_err(|error| RuleSnapshotError::SnapshotIntegrity(error.to_string()))
}

fn validate_sha256(value: &Sha256Hex) -> Result<(), RuleSnapshotError> {
    if value.0.len() == 64
        && value
            .0
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(RuleSnapshotError::SnapshotIntegrity(
            "SHA-256 must be 64 lowercase hexadecimal characters".to_owned(),
        ))
    }
}

fn is_utc_date(value: &str) -> bool {
    value.len() == 10
        && value.as_bytes()[4] == b'-'
        && value.as_bytes()[7] == b'-'
        && value
            .bytes()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
}

fn system_now_utc_ms() -> Result<i64, RuleSnapshotError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| RuleSnapshotError::InvalidRules(error.to_string()))?
        .as_millis();
    i64::try_from(millis).map_err(|error| RuleSnapshotError::InvalidRules(error.to_string()))
}

fn transport_error_kind(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "TIMEOUT"
    } else if error.is_connect() {
        "CONNECT"
    } else if error.is_body() || error.is_decode() {
        "BODY"
    } else if error.is_request() {
        "REQUEST"
    } else {
        "OTHER"
    }
}

fn is_transient_transport_error(error: &reqwest::Error) -> bool {
    error.is_timeout()
        || error.is_connect()
        || error.is_request()
        || error.is_body()
        || error.is_decode()
}

fn io_error(operation: &'static str, error: rustix::io::Errno) -> RuleSnapshotError {
    RuleSnapshotError::Io {
        operation,
        message: io::Error::from_raw_os_error(error.raw_os_error()).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::latency_profile::load_embedded_proxy_latency_profile;
    use crate::session::{
        ParsedMethodTimingV1, RequestContextV1, SIM_EXECUTION_CONTRACT_V1, SessionError,
        SessionSpecV1, SimCallerRoleV1, SimProcessRoot,
    };
    use contracts::EXECUTION_MARKET_EVENT_SCHEMA_V3;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    const CONDITION: &str = "0xd4346bcf419856fa3261d3c0705597d5c909ead020b9f1a0de468092704fc65f";
    const TOKEN_UP: &str =
        "45253098770861975037434792771412818336165758089047645530470012231811628219325";
    const TOKEN_DOWN: &str =
        "13103176462561566402126211408478811357460751704260150146723274072578196298541";
    const CLOB_REAL: &[u8] = include_bytes!("../tests/fixtures/clob-market-info-real-shape.json");
    const GAMMA_OPEN_EMPTY: &[u8] =
        include_bytes!("../tests/fixtures/gamma-keyset-open-empty-real-shape.json");
    const GAMMA_CLOSED_REAL: &[u8] =
        include_bytes!("../tests/fixtures/gamma-keyset-closed-real-shape.json");

    #[derive(Clone)]
    struct ResponseSpec {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
        declared_content_length: Option<usize>,
        chunked: bool,
    }

    impl ResponseSpec {
        fn json(status: u16, body: impl Into<Vec<u8>>) -> Self {
            Self {
                status,
                headers: vec![("content-type".to_owned(), "application/json".to_owned())],
                body: body.into(),
                declared_content_length: None,
                chunked: false,
            }
        }

        fn truncated(status: u16, body: impl Into<Vec<u8>>, declared_length: usize) -> Self {
            Self {
                status,
                headers: vec![("content-type".to_owned(), "application/json".to_owned())],
                body: body.into(),
                declared_content_length: Some(declared_length),
                chunked: false,
            }
        }

        fn chunked(status: u16, body: impl Into<Vec<u8>>) -> Self {
            Self {
                status,
                headers: vec![("content-type".to_owned(), "application/json".to_owned())],
                body: body.into(),
                declared_content_length: None,
                chunked: true,
            }
        }

        fn redirect(location: String) -> Self {
            Self {
                status: 302,
                headers: vec![("location".to_owned(), location)],
                body: Vec::new(),
                declared_content_length: None,
                chunked: false,
            }
        }
    }

    struct ScriptedServer {
        base_url: String,
        requests: Arc<Mutex<Vec<String>>>,
        task: JoinHandle<()>,
    }

    impl ScriptedServer {
        async fn start(responses: Vec<ResponseSpec>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind scripted server");
            let base_url = format!("http://{}", listener.local_addr().expect("local addr"));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let recorded = Arc::clone(&requests);
            let task = tokio::spawn(async move {
                let mut responses = VecDeque::from(responses);
                while let Some(response) = responses.pop_front() {
                    let (mut socket, _) = listener.accept().await.expect("accept request");
                    let mut request = Vec::new();
                    let mut chunk = [0_u8; 4096];
                    loop {
                        let count = socket.read(&mut chunk).await.expect("read request");
                        if count == 0 {
                            break;
                        }
                        request.extend_from_slice(&chunk[..count]);
                        if request.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                        assert!(request.len() <= 64 * 1024, "request headers are bounded");
                    }
                    let first_line = String::from_utf8_lossy(&request)
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .to_owned();
                    recorded.lock().expect("requests lock").push(first_line);
                    let reason = reqwest::StatusCode::from_u16(response.status)
                        .ok()
                        .and_then(|status| status.canonical_reason())
                        .unwrap_or("Test");
                    let framing = if response.chunked {
                        "Transfer-Encoding: chunked\r\n".to_owned()
                    } else {
                        format!(
                            "Content-Length: {}\r\n",
                            response
                                .declared_content_length
                                .unwrap_or(response.body.len())
                        )
                    };
                    let mut headers = format!(
                        "HTTP/1.1 {} {}\r\n{}Connection: close\r\n",
                        response.status, reason, framing
                    );
                    for (name, value) in response.headers {
                        headers.push_str(&format!("{name}: {value}\r\n"));
                    }
                    headers.push_str("\r\n");
                    socket
                        .write_all(headers.as_bytes())
                        .await
                        .expect("write headers");
                    if response.chunked {
                        let chunk_header = format!("{:x}\r\n", response.body.len());
                        socket
                            .write_all(chunk_header.as_bytes())
                            .await
                            .expect("write chunk header");
                        socket
                            .write_all(&response.body)
                            .await
                            .expect("write chunk body");
                        socket
                            .write_all(b"\r\n0\r\n\r\n")
                            .await
                            .expect("finish chunked body");
                    } else {
                        socket.write_all(&response.body).await.expect("write body");
                    }
                    socket.shutdown().await.expect("shutdown response");
                }
            });
            Self {
                base_url,
                requests,
                task,
            }
        }

        fn request_count(&self) -> usize {
            self.requests.lock().expect("requests lock").len()
        }

        fn request_lines(&self) -> Vec<String> {
            self.requests.lock().expect("requests lock").clone()
        }
    }

    impl Drop for ScriptedServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn test_reference_market() -> ReferenceMarketSpecV1 {
        ReferenceMarketSpecV1 {
            condition_id: ConditionId(CONDITION.to_owned()),
            clob_market_id: ClobMarketId("3278040".to_owned()),
            slug: "btc-updown-5m-1785715500".to_owned(),
            outcomes: vec![
                ReferenceOutcomeSpecV1 {
                    token_id: TokenId(TOKEN_UP.to_owned()),
                    outcome: "Up".to_owned(),
                },
                ReferenceOutcomeSpecV1 {
                    token_id: TokenId(TOKEN_DOWN.to_owned()),
                    outcome: "Down".to_owned(),
                },
            ],
        }
    }

    fn test_implementation_identity() -> ImplementationIdentityV1 {
        ImplementationIdentityV1 {
            git_commit: "a".repeat(40),
            source_identity_sha256: Sha256Hex("b".repeat(64)),
            cargo_lock_sha256: Sha256Hex("c".repeat(64)),
        }
    }

    fn fixed_now_utc_ms() -> Result<i64, RuleSnapshotError> {
        Ok(1_785_715_688_412)
    }

    fn test_loader_options(server: &ScriptedServer) -> LoaderOptions {
        LoaderOptions {
            clob_market_base: format!("{}/clob-markets", server.base_url),
            gamma_keyset_url: format!("{}/markets/keyset", server.base_url),
            https_only: false,
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            initialization_timeout: Duration::from_secs(2),
            max_response_bytes: 128 * 1024,
        }
    }

    fn current_request(path: PathBuf) -> CurrentProcessSnapshotRequestV1 {
        CurrentProcessSnapshotRequestV1 {
            reference_market: test_reference_market(),
            target_path: path,
            implementation_identity: test_implementation_identity(),
        }
    }

    fn successful_responses() -> Vec<ResponseSpec> {
        vec![
            ResponseSpec::json(200, CLOB_REAL),
            ResponseSpec::json(200, GAMMA_OPEN_EMPTY),
            ResponseSpec::json(200, GAMMA_CLOSED_REAL),
        ]
    }

    async fn create_successful_snapshot(
        server: &ScriptedServer,
        path: PathBuf,
    ) -> Arc<SealedProcessRuleSnapshot> {
        query_and_seal_current_process_snapshot_with(
            current_request(path),
            test_loader_options(server),
            PLATFORM_RULE_CONFIG_BYTES,
            fixed_now_utc_ms,
        )
        .await
        .expect("current query should seal a process snapshot")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn process_snapshot_queries_only_one_reference_and_is_shared_without_refresh() {
        let server = ScriptedServer::start(successful_responses()).await;
        let directory = tempfile::tempdir().expect("tempdir");
        let target = directory.path().join("platform-rule-snapshot.json");
        let sealed = create_successful_snapshot(&server, target.clone()).await;

        assert_eq!(server.request_count(), 3);
        let request_lines = server.request_lines();
        assert!(request_lines[0].starts_with(&format!("GET /clob-markets/{CONDITION} ")));
        assert!(request_lines[1].contains("limit=100"));
        assert!(request_lines[1].contains(&format!("condition_ids={CONDITION}")));
        assert!(request_lines[1].contains("closed=false"));
        assert!(request_lines[2].contains("closed=true"));
        assert!(request_lines.iter().all(|line| line.contains(CONDITION)));

        assert_eq!(sealed.access_mode(), SnapshotAccessMode::CurrentQuery);
        assert_eq!(
            sealed.snapshot().schema_version,
            PLATFORM_RULE_SNAPSHOT_SCHEMA_V2
        );
        let rules = sealed.synchronized_rules();
        assert_eq!(rules.minimum_order_size_shares.value(), Decimal::from(5));
        assert!(rules.taker_order_delay_enabled);
        assert!(rules.fees_enabled);
        assert_eq!(rules.fee_schedule.rate.value(), Decimal::new(7, 2));
        assert_eq!(rules.fee_schedule.exponent, 1);
        assert!(rules.fee_schedule.taker_only);
        assert_eq!(rules.fee_schedule.rebate_rate.value(), Decimal::new(2, 1));
        let rebate = &sealed
            .snapshot()
            .project_account_rules
            .rebate_settlement
            .value;
        assert_eq!(rebate.fixed_taker_rebate_rate.value(), Decimal::new(8, 2));
        assert_eq!(rebate.fixed_maker_rebate_rate.value(), Decimal::new(2, 1));
        assert!(!rebate.dynamic_rebate_tiers_enabled);
        assert!(!rebate.one_time_bonuses_enabled);
        assert!(!rebate.liquidity_rewards_enabled);

        let second_consumer = Arc::clone(&sealed);
        assert!(Arc::ptr_eq(&sealed, &second_consumer));
        assert_eq!(server.request_count(), 3);
        let bytes = std::fs::read(&target).expect("sealed bytes");
        assert_eq!(sealed.file_sha256(), &sha256_hex(&bytes));
        assert_eq!(
            sealed
                .snapshot()
                .provenance
                .reference_observation_evidence
                .category,
            RuleEvidenceCategory::OfficialPlatformRule
        );
        assert_eq!(
            sealed
                .snapshot()
                .provenance
                .application_policy
                .evidence
                .category,
            RuleEvidenceCategory::ProjectAssumption
        );

        let profile = load_embedded_proxy_latency_profile().expect("proxy latency profile");
        let mut root = SimProcessRoot::new(Arc::clone(&sealed), profile);
        let mut first = root
            .create_session(SessionSpecV1 {
                session_id: "session-a".to_owned(),
                account_id: "account-a".to_owned(),
                caller_role: SimCallerRoleV1::ModelTrain,
                caller_instance_id: "model-a".to_owned(),
                market_set_sha256: Sha256Hex("11".repeat(32)),
                scenario_sha256: Sha256Hex("22".repeat(32)),
                session_seed: Sha256Hex("33".repeat(32)),
                execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
                market_event_schema_version: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
            })
            .expect("first session");
        let second = root
            .create_session(SessionSpecV1 {
                session_id: "session-b".to_owned(),
                account_id: "account-b".to_owned(),
                caller_role: SimCallerRoleV1::OfflineEvaluation,
                caller_instance_id: "model-b".to_owned(),
                market_set_sha256: Sha256Hex("11".repeat(32)),
                scenario_sha256: Sha256Hex("22".repeat(32)),
                session_seed: Sha256Hex("44".repeat(32)),
                execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
                market_event_schema_version: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
            })
            .expect("second session");
        assert_eq!(root.registered_session_count(), 2);
        assert!(matches!(
            root.create_session(first.session_spec().clone()),
            Err(SessionError::DuplicateSessionId(id)) if id == "session-a"
        ));
        assert_eq!(root.registered_session_count(), 2);
        let mut wrong_version = second.session_spec().clone();
        wrong_version.session_id = "session-c".to_owned();
        wrong_version.execution_contract_version = "sim_execution_contract_v0".to_owned();
        assert_eq!(
            root.create_session(wrong_version).err(),
            Some(SessionError::InvalidSessionSpec("合同版本不匹配"))
        );
        assert_eq!(root.registered_session_count(), 2);
        assert!(Arc::ptr_eq(first.rule_snapshot(), second.rule_snapshot()));
        assert!(Arc::ptr_eq(
            first.latency_profile(),
            second.latency_profile()
        ));

        let ordinary = first
            .register_request(
                RequestContextV1 {
                    execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
                    session_id: "session-a".to_owned(),
                    account_id: "account-a".to_owned(),
                    caller_role: SimCallerRoleV1::ModelTrain,
                    caller_instance_id: "model-a".to_owned(),
                    global_request_id: "ordinary-request".to_owned(),
                    caller_correlation_id: None,
                    caller_sequence: 1,
                    request_sent_at_utc_ms: 1_000,
                    model_visible_cursor: first.model_visible_cursor(),
                },
                Sha256Hex("dd".repeat(32)),
            )
            .expect("ordinary request");
        first
            .enqueue_logical_action(
                "ordinary-action".to_owned(),
                "ordinary-request".to_owned(),
                0,
                ParsedMethodTimingV1::Ordinary,
            )
            .expect("schedule ordinary action");
        first
            .seal_logical_actions("ordinary-request")
            .expect("seal ordinary request");
        first
            .close_platform_prefix_through(ordinary.transit.platform_arrived_at_utc_ms, 1)
            .expect("close ordinary prefix");
        let ordinary_action = first
            .take_next_action_through(ordinary.transit.platform_arrived_at_utc_ms)
            .expect("release ordinary action")
            .expect("ordinary action");
        assert_eq!(
            ordinary_action.key.effective_processing_at_utc_ms,
            ordinary.transit.platform_arrived_at_utc_ms
        );
        first
            .complete_logical_item("ordinary-action")
            .expect("complete ordinary action");

        let market = first
            .register_request(
                RequestContextV1 {
                    execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
                    session_id: "session-a".to_owned(),
                    account_id: "account-a".to_owned(),
                    caller_role: SimCallerRoleV1::ModelTrain,
                    caller_instance_id: "model-a".to_owned(),
                    global_request_id: "fok-market-request".to_owned(),
                    caller_correlation_id: None,
                    caller_sequence: 2,
                    request_sent_at_utc_ms: 2_000,
                    model_visible_cursor: first.model_visible_cursor(),
                },
                Sha256Hex("ee".repeat(32)),
            )
            .expect("FOK market request");
        first
            .enqueue_logical_action(
                "fok-market-action".to_owned(),
                "fok-market-request".to_owned(),
                0,
                ParsedMethodTimingV1::FokFakMarketBuy,
            )
            .expect("schedule FOK market action");
        first
            .seal_logical_actions("fok-market-request")
            .expect("seal FOK market request");
        let held_until = market
            .transit
            .platform_arrived_at_utc_ms
            .checked_add(250)
            .expect("held time");
        first
            .close_platform_prefix_through(held_until, 2)
            .expect("close held prefix");
        assert!(
            first
                .take_next_action_through(market.transit.platform_arrived_at_utc_ms)
                .expect("before held time")
                .is_none()
        );
        let market_action = first
            .take_next_action_through(held_until)
            .expect("release held action")
            .expect("FOK market action");
        assert_eq!(market_action.key.effective_processing_at_utc_ms, held_until);
        first
            .complete_logical_item("fok-market-action")
            .expect("complete FOK market action");

        first
            .advance_public_visibility_through(1, 1)
            .expect("first public cursor");
        assert_eq!(
            first
                .public_visibility_cursor()
                .next_visibility_index_position(),
            1
        );
        assert_eq!(
            second
                .public_visibility_cursor()
                .next_visibility_index_position(),
            0
        );
    }

    async fn assert_rule_failure(clob: Vec<u8>, gamma_closed: Vec<u8>) {
        let server = ScriptedServer::start(vec![
            ResponseSpec::json(200, clob),
            ResponseSpec::json(200, GAMMA_OPEN_EMPTY),
            ResponseSpec::json(200, gamma_closed),
        ])
        .await;
        let directory = tempfile::tempdir().expect("tempdir");
        let target = directory.path().join("platform-rule-snapshot.json");
        let error = query_and_seal_current_process_snapshot_with(
            current_request(target.clone()),
            test_loader_options(&server),
            PLATFORM_RULE_CONFIG_BYTES,
            fixed_now_utc_ms,
        )
        .await
        .expect_err("invalid reference rules must fail");
        assert!(matches!(error, RuleSnapshotError::InvalidRules(_)));
        assert_eq!(server.request_count(), 3);
        assert!(!target.exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reference_identity_and_fixed_fee_policy_must_match() {
        let gamma = String::from_utf8(GAMMA_CLOSED_REAL.to_vec()).expect("fixture utf8");
        let wrong_identity = gamma.replace(
            CONDITION,
            "0x1111111111111111111111111111111111111111111111111111111111111111",
        );
        assert_rule_failure(CLOB_REAL.to_vec(), wrong_identity.into_bytes()).await;

        let fees_disabled = gamma.replace("\"feesEnabled\":true", "\"feesEnabled\":false");
        assert_rule_failure(CLOB_REAL.to_vec(), fees_disabled.into_bytes()).await;

        let wrong_maker_rate = gamma.replace("\"rebateRate\":0.20", "\"rebateRate\":0.25");
        assert_rule_failure(CLOB_REAL.to_vec(), wrong_maker_rate.into_bytes()).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn malformed_complete_response_is_hashed_before_decode_failure() {
        let malformed = b"{\"t\":[".to_vec();
        let server = ScriptedServer::start(vec![ResponseSpec::json(200, malformed.clone())]).await;
        let directory = tempfile::tempdir().expect("tempdir");
        let target = directory.path().join("platform-rule-snapshot.json");
        let error = query_and_seal_current_process_snapshot_with(
            current_request(target.clone()),
            test_loader_options(&server),
            PLATFORM_RULE_CONFIG_BYTES,
            fixed_now_utc_ms,
        )
        .await
        .expect_err("malformed JSON must fail");

        assert!(matches!(
            error,
            RuleSnapshotError::ResponseDecode { raw_sha256, .. }
                if raw_sha256 == sha256_hex(&malformed)
        ));
        assert_eq!(server.request_count(), 1);
        assert!(!target.exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn incomplete_response_never_claims_a_complete_raw_sha() {
        let server = ScriptedServer::start(vec![
            ResponseSpec::truncated(200, b"{\"t\":".to_vec(), 64),
            ResponseSpec::truncated(200, b"{\"t\":".to_vec(), 64),
        ])
        .await;
        let directory = tempfile::tempdir().expect("tempdir");
        let target = directory.path().join("platform-rule-snapshot.json");
        let error = query_and_seal_current_process_snapshot_with(
            current_request(target.clone()),
            test_loader_options(&server),
            PLATFORM_RULE_CONFIG_BYTES,
            fixed_now_utc_ms,
        )
        .await
        .expect_err("two incomplete bodies must fail");

        match error {
            RuleSnapshotError::HttpTransport { attempts, .. } => {
                assert_eq!(attempts.len(), 2);
                assert!(attempts.iter().all(|attempt| {
                    attempt.response_bytes.is_some()
                        && attempt.raw_response_sha256.is_none()
                        && attempt.transport_error_kind.as_deref() == Some("BODY")
                }));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(server.request_count(), 2);
        assert!(!target.exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn response_limit_applies_without_content_length() {
        let server = ScriptedServer::start(vec![ResponseSpec::chunked(200, CLOB_REAL)]).await;
        let directory = tempfile::tempdir().expect("tempdir");
        let target = directory.path().join("platform-rule-snapshot.json");
        let mut options = test_loader_options(&server);
        options.max_response_bytes = CLOB_REAL.len() - 1;
        let error = query_and_seal_current_process_snapshot_with(
            current_request(target.clone()),
            options,
            PLATFORM_RULE_CONFIG_BYTES,
            fixed_now_utc_ms,
        )
        .await
        .expect_err("streamed body over the limit must fail");

        assert!(matches!(error, RuleSnapshotError::ResponseTooLarge { .. }));
        assert_eq!(server.request_count(), 1);
        assert!(!target.exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retry_and_redirect_policy_are_bounded() {
        let retry = ScriptedServer::start(vec![
            ResponseSpec::json(503, b"temporary".to_vec()),
            ResponseSpec::json(200, CLOB_REAL),
            ResponseSpec::json(200, GAMMA_OPEN_EMPTY),
            ResponseSpec::json(200, GAMMA_CLOSED_REAL),
        ])
        .await;
        let directory = tempfile::tempdir().expect("tempdir");
        let target = directory.path().join("platform-rule-snapshot.json");
        let sealed = create_successful_snapshot(&retry, target).await;
        assert_eq!(retry.request_count(), 4);
        assert_eq!(
            sealed.snapshot().provenance.http_requests[0].attempts.len(),
            2
        );

        let redirect_target = ScriptedServer::start(vec![ResponseSpec::json(200, CLOB_REAL)]).await;
        let redirect_source = ScriptedServer::start(vec![ResponseSpec::redirect(format!(
            "{}/unexpected",
            redirect_target.base_url
        ))])
        .await;
        let directory = tempfile::tempdir().expect("tempdir");
        let target = directory.path().join("platform-rule-snapshot.json");
        let error = query_and_seal_current_process_snapshot_with(
            current_request(target.clone()),
            test_loader_options(&redirect_source),
            PLATFORM_RULE_CONFIG_BYTES,
            fixed_now_utc_ms,
        )
        .await
        .expect_err("redirect must fail");
        assert!(matches!(
            error,
            RuleSnapshotError::HttpStatus { status: 302, .. }
        ));
        assert_eq!(redirect_source.request_count(), 1);
        assert_eq!(redirect_target.request_count(), 0);
        assert!(!target.exists());

        let partial_content = ScriptedServer::start(vec![ResponseSpec::json(206, CLOB_REAL)]).await;
        let directory = tempfile::tempdir().expect("tempdir");
        let target = directory.path().join("platform-rule-snapshot.json");
        let error = query_and_seal_current_process_snapshot_with(
            current_request(target.clone()),
            test_loader_options(&partial_content),
            PLATFORM_RULE_CONFIG_BYTES,
            fixed_now_utc_ms,
        )
        .await
        .expect_err("206 must not masquerade as one complete response");
        assert!(matches!(
            error,
            RuleSnapshotError::HttpStatus { status: 206, .. }
        ));
        assert_eq!(partial_content.request_count(), 1);
        assert!(!target.exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn historical_and_recovery_are_offline_and_revalidate_snapshot_semantics() {
        let server = ScriptedServer::start(successful_responses()).await;
        let directory = tempfile::tempdir().expect("tempdir");
        let target = directory.path().join("platform-rule-snapshot.json");
        let sealed = create_successful_snapshot(&server, target.clone()).await;
        let request_count = server.request_count();

        let historical = load_historical_process_snapshot(HistoricalProcessSnapshotRequestV1 {
            snapshot_path: target.clone(),
            expected_file_sha256: sealed.file_sha256().clone(),
            replay_causal_start_utc_ms: fixed_now_utc_ms().expect("fixed time"),
        })
        .expect("historical load");
        assert_eq!(historical.access_mode(), SnapshotAccessMode::HistoricalAsOf);
        assert_eq!(historical.snapshot(), sealed.snapshot());

        let recovered = recover_process_snapshot(RecoveryProcessSnapshotRequestV1 {
            snapshot_path: target.clone(),
            expected_file_sha256: sealed.file_sha256().clone(),
            expected_implementation_identity: test_implementation_identity(),
        })
        .expect("recovery load");
        assert_eq!(recovered.access_mode(), SnapshotAccessMode::Recovery);
        assert_eq!(recovered.snapshot(), sealed.snapshot());
        assert_eq!(server.request_count(), request_count);

        let late = load_historical_process_snapshot(HistoricalProcessSnapshotRequestV1 {
            snapshot_path: target.clone(),
            expected_file_sha256: sealed.file_sha256().clone(),
            replay_causal_start_utc_ms: fixed_now_utc_ms().expect("fixed time") - 1,
        })
        .expect_err("future observation must not enter historical replay");
        assert!(matches!(
            late,
            RuleSnapshotError::HistoricalNotApplicable { .. }
        ));

        let mut forged = sealed.snapshot().clone();
        forged.synchronized_platform_rules.minimum_order_size_shares =
            RuleDecimal::new(Decimal::ZERO);
        forged.content_sha256 = compute_snapshot_content_sha256(&forged).expect("content hash");
        let mut forged_bytes = serde_json::to_vec_pretty(&forged).expect("serialize forged");
        forged_bytes.push(b'\n');
        let forged_path = directory.path().join("forged-rule-snapshot.json");
        std::fs::write(&forged_path, &forged_bytes).expect("write forged snapshot");
        let error = load_historical_process_snapshot(HistoricalProcessSnapshotRequestV1 {
            snapshot_path: forged_path,
            expected_file_sha256: sha256_hex(&forged_bytes),
            replay_causal_start_utc_ms: fixed_now_utc_ms().expect("fixed time"),
        })
        .expect_err("self-consistent hashes cannot bypass semantic validation");
        assert!(matches!(error, RuleSnapshotError::SnapshotIntegrity(_)));

        let mut forged_origin = sealed.snapshot().clone();
        forged_origin.provenance.http_requests[0].request_url =
            format!("https://unapproved.example/clob-markets/{CONDITION}");
        forged_origin.content_sha256 =
            compute_snapshot_content_sha256(&forged_origin).expect("content hash");
        let mut forged_origin_bytes =
            serde_json::to_vec_pretty(&forged_origin).expect("serialize forged origin");
        forged_origin_bytes.push(b'\n');
        let forged_origin_path = directory.path().join("forged-origin-snapshot.json");
        std::fs::write(&forged_origin_path, &forged_origin_bytes)
            .expect("write forged origin snapshot");
        let error = load_historical_process_snapshot(HistoricalProcessSnapshotRequestV1 {
            snapshot_path: forged_origin_path,
            expected_file_sha256: sha256_hex(&forged_origin_bytes),
            replay_causal_start_utc_ms: fixed_now_utc_ms().expect("fixed time"),
        })
        .expect_err("an unapproved endpoint origin must not become trusted provenance");
        assert!(matches!(error, RuleSnapshotError::SnapshotIntegrity(_)));
        assert_eq!(server.request_count(), request_count);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_write_and_read_limits_are_symmetric_and_precede_candidate_creation() {
        let server = ScriptedServer::start(successful_responses()).await;
        let directory = tempfile::tempdir().expect("tempdir");
        let target = directory.path().join("platform-rule-snapshot.json");
        let sealed = create_successful_snapshot(&server, target.clone()).await;

        let mut exact_bytes = serde_json::to_vec_pretty(sealed.snapshot()).expect("serialize");
        exact_bytes.push(b'\n');
        let exact_limit = exact_bytes.len();
        let exact_target = directory.path().join("exact-limit.json");
        let (exact_sha, _) = seal_snapshot(&exact_target, sealed.snapshot(), exact_limit)
            .expect("the exact byte limit is inclusive");
        read_and_validate_snapshot(
            &exact_target,
            &exact_sha,
            SnapshotAccessMode::Recovery,
            exact_limit,
        )
        .expect("read side accepts the same exact limit");

        let collision_target = directory.path().join("collision.json");
        let sentinel = b"existing-sealed-bytes";
        std::fs::write(&collision_target, sentinel).expect("write collision target");
        let error = seal_snapshot(&collision_target, sealed.snapshot(), exact_limit)
            .expect_err("RENAME_NOREPLACE must reject a final target collision");
        assert!(matches!(
            error,
            RuleSnapshotError::SnapshotAlreadyExists(path) if path == collision_target
        ));
        assert_eq!(
            std::fs::read(&collision_target).expect("collision sentinel"),
            sentinel
        );
        assert!(!directory.path().join(".collision.json.candidate").exists());

        let too_small_target = directory.path().join("too-small.json");
        let error = seal_snapshot(&too_small_target, sealed.snapshot(), exact_limit - 1)
            .expect_err("write-side size gate must fail before a candidate exists");
        assert!(matches!(error, RuleSnapshotError::SnapshotTooLarge { .. }));
        assert!(!too_small_target.exists());
        assert!(!directory.path().join(".too-small.json.candidate").exists());

        let error = read_and_validate_snapshot(
            &target,
            sealed.file_sha256(),
            SnapshotAccessMode::Recovery,
            exact_limit - 1,
        )
        .expect_err("read-side size gate must use the same boundary semantics");
        assert!(matches!(error, RuleSnapshotError::SnapshotIntegrity(_)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn existing_snapshot_is_rejected_before_network_and_never_overwritten() {
        let server = ScriptedServer::start(successful_responses()).await;
        let directory = tempfile::tempdir().expect("tempdir");
        let target = directory.path().join("platform-rule-snapshot.json");
        let sentinel = b"sealed-existing-snapshot";
        std::fs::write(&target, sentinel).expect("write sentinel");
        let error = query_and_seal_current_process_snapshot_with(
            current_request(target.clone()),
            test_loader_options(&server),
            PLATFORM_RULE_CONFIG_BYTES,
            fixed_now_utc_ms,
        )
        .await
        .expect_err("existing snapshot must reject current query");

        assert!(matches!(
            error,
            RuleSnapshotError::SnapshotAlreadyExists(path) if path == target
        ));
        assert_eq!(server.request_count(), 0);
        assert_eq!(std::fs::read(&target).expect("sentinel"), sentinel);

        let second_target = directory.path().join("second-snapshot.json");
        let candidate = directory.path().join(".second-snapshot.json.candidate");
        std::fs::write(&candidate, sentinel).expect("write candidate sentinel");
        let error = query_and_seal_current_process_snapshot_with(
            current_request(second_target.clone()),
            test_loader_options(&server),
            PLATFORM_RULE_CONFIG_BYTES,
            fixed_now_utc_ms,
        )
        .await
        .expect_err("a crash-left candidate must fail closed");
        assert!(matches!(
            error,
            RuleSnapshotError::SnapshotCandidateExists(path) if path == candidate
        ));
        assert_eq!(server.request_count(), 0);
        assert_eq!(
            std::fs::read(&candidate).expect("candidate sentinel"),
            sentinel
        );
        assert!(!second_target.exists());
    }
}
