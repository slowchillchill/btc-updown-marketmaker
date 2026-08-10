use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;

use contracts::{AssetIdentityV1, MarketIdentityV2, Sha256Hex};
use serde::Serialize;
use sha2::{Digest, Sha256};

pub const PLATFORM_SCENARIO_SCHEMA_V1: &str = "platform_scenario_v1";
pub const MARKET_SET_SCHEMA_V1: &str = "market_set_v1";
pub const BTC_UPDOWN_5M_MARKET_FAMILY_V1: &str = "BTC_UPDOWN_5M";

const MARKET_SET_HASH_DOMAIN_V1: &str = "SIM_MARKET_SET_SHA256_V1";
const PLATFORM_SCENARIO_HASH_DOMAIN_V1: &str = "SIM_PLATFORM_SCENARIO_SHA256_V1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlatformScenarioErrorV1 {
    EmptyMarketSet,
    InvalidConditionId,
    InvalidClobMarketId,
    InvalidMarketWindow,
    InvalidAssetIdentity,
    DuplicateAssetIdentity,
    DuplicateOutcomeIndex,
    DuplicateMarketIdentity,
    Serialization(String),
}

impl fmt::Display for PlatformScenarioErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyMarketSet => write!(formatter, "场景市场集不能为空"),
            Self::InvalidConditionId => write!(formatter, "场景 condition_id 非法"),
            Self::InvalidClobMarketId => write!(formatter, "场景 CLOB 市场身份非法"),
            Self::InvalidMarketWindow => write!(formatter, "场景市场时间窗口非法"),
            Self::InvalidAssetIdentity => write!(formatter, "场景资产身份非法"),
            Self::DuplicateAssetIdentity => write!(formatter, "场景市场的资产身份重复"),
            Self::DuplicateOutcomeIndex => write!(formatter, "场景市场的 outcome index 重复"),
            Self::DuplicateMarketIdentity => write!(formatter, "场景 condition_id 重复"),
            Self::Serialization(message) => write!(formatter, "场景规范序列化失败：{message}"),
        }
    }
}

impl std::error::Error for PlatformScenarioErrorV1 {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PlatformMarketV1 {
    market: MarketIdentityV2,
    assets: [AssetIdentityV1; 2],
}

impl PlatformMarketV1 {
    pub fn try_new(
        market: MarketIdentityV2,
        mut assets: [AssetIdentityV1; 2],
    ) -> Result<Self, PlatformScenarioErrorV1> {
        validate_market(&market)?;
        for asset in &assets {
            validate_asset(asset)?;
        }
        assets.sort_by(|left, right| compare_token_ids(&left.token_id.0, &right.token_id.0));
        if assets[0].token_id == assets[1].token_id {
            return Err(PlatformScenarioErrorV1::DuplicateAssetIdentity);
        }
        if assets[0].outcome.index == assets[1].outcome.index {
            return Err(PlatformScenarioErrorV1::DuplicateOutcomeIndex);
        }
        Ok(Self { market, assets })
    }

    #[must_use]
    pub fn market(&self) -> &MarketIdentityV2 {
        &self.market
    }

    #[must_use]
    pub fn assets(&self) -> &[AssetIdentityV1; 2] {
        &self.assets
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketSetV1 {
    markets: Vec<PlatformMarketV1>,
    sha256: Sha256Hex,
}

impl MarketSetV1 {
    pub fn try_new(mut markets: Vec<PlatformMarketV1>) -> Result<Self, PlatformScenarioErrorV1> {
        if markets.is_empty() {
            return Err(PlatformScenarioErrorV1::EmptyMarketSet);
        }
        markets.sort_by(|left, right| left.market.condition_id.0.cmp(&right.market.condition_id.0));
        let mut condition_ids = BTreeSet::new();
        if markets
            .iter()
            .any(|member| !condition_ids.insert(member.market.condition_id.0.as_str()))
        {
            return Err(PlatformScenarioErrorV1::DuplicateMarketIdentity);
        }
        let material = MarketSetIdentityMaterialV1 {
            schema_version: MARKET_SET_SCHEMA_V1,
            markets: &markets,
        };
        let sha256 = hash_json(MARKET_SET_HASH_DOMAIN_V1, &material)?;
        Ok(Self { markets, sha256 })
    }

    #[must_use]
    pub fn markets(&self) -> &[PlatformMarketV1] {
        &self.markets
    }

    #[must_use]
    pub fn sha256(&self) -> &Sha256Hex {
        &self.sha256
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlatformScenarioV1 {
    market_set: MarketSetV1,
    sha256: Sha256Hex,
}

impl PlatformScenarioV1 {
    pub fn try_new(markets: Vec<PlatformMarketV1>) -> Result<Self, PlatformScenarioErrorV1> {
        Self::from_market_set(MarketSetV1::try_new(markets)?)
    }

    pub fn from_market_set(market_set: MarketSetV1) -> Result<Self, PlatformScenarioErrorV1> {
        let material = PlatformScenarioIdentityMaterialV1 {
            schema_version: PLATFORM_SCENARIO_SCHEMA_V1,
            market_family: BTC_UPDOWN_5M_MARKET_FAMILY_V1,
            market_set_sha256: market_set.sha256(),
        };
        let sha256 = hash_json(PLATFORM_SCENARIO_HASH_DOMAIN_V1, &material)?;
        Ok(Self { market_set, sha256 })
    }

    #[must_use]
    pub fn market_set(&self) -> &MarketSetV1 {
        &self.market_set
    }

    #[must_use]
    pub fn market_set_sha256(&self) -> &Sha256Hex {
        self.market_set.sha256()
    }

    #[must_use]
    pub fn scenario_sha256(&self) -> &Sha256Hex {
        &self.sha256
    }
}

#[derive(Serialize)]
struct MarketSetIdentityMaterialV1<'a> {
    schema_version: &'static str,
    markets: &'a [PlatformMarketV1],
}

#[derive(Serialize)]
struct PlatformScenarioIdentityMaterialV1<'a> {
    schema_version: &'static str,
    market_family: &'static str,
    market_set_sha256: &'a Sha256Hex,
}

fn hash_json<T: Serialize>(domain: &str, value: &T) -> Result<Sha256Hex, PlatformScenarioErrorV1> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| PlatformScenarioErrorV1::Serialization(error.to_string()))?;
    let mut hash = Sha256::new();
    hash.update((domain.len() as u64).to_be_bytes());
    hash.update(domain.as_bytes());
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in hash.finalize() {
        use fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("写入 String 不会失败");
    }
    Ok(Sha256Hex(encoded))
}

fn validate_market(market: &MarketIdentityV2) -> Result<(), PlatformScenarioErrorV1> {
    let condition = market
        .condition_id
        .0
        .strip_prefix("0x")
        .ok_or(PlatformScenarioErrorV1::InvalidConditionId)?;
    if condition.len() != 64
        || !condition
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(PlatformScenarioErrorV1::InvalidConditionId);
    }
    if !is_unsigned_decimal_id(&market.clob_market_id.0) {
        return Err(PlatformScenarioErrorV1::InvalidClobMarketId);
    }
    if market.window.start_at_utc_ms >= market.window.end_at_utc_ms {
        return Err(PlatformScenarioErrorV1::InvalidMarketWindow);
    }
    Ok(())
}

fn validate_asset(asset: &AssetIdentityV1) -> Result<(), PlatformScenarioErrorV1> {
    if !is_unsigned_decimal_id(&asset.token_id.0) || asset.outcome.label.is_empty() {
        return Err(PlatformScenarioErrorV1::InvalidAssetIdentity);
    }
    Ok(())
}

fn is_unsigned_decimal_id(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && (value.len() == 1 || !value.starts_with('0'))
}

fn compare_token_ids(left: &str, right: &str) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

#[cfg(test)]
pub(crate) fn scenario_for_sim_tests() -> std::sync::Arc<PlatformScenarioV1> {
    use contracts::{
        AssetIdentityV1, ClobMarketId, ConditionId, MarketWindowV2, OutcomeIdentityV1, TokenId,
    };

    let market = MarketIdentityV2 {
        condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
        clob_market_id: ClobMarketId("123".to_owned()),
        window: MarketWindowV2 {
            start_at_utc_ms: 1_000,
            end_at_utc_ms: 2_000,
        },
    };
    let assets = [
        AssetIdentityV1 {
            token_id: TokenId("1".to_owned()),
            outcome: OutcomeIdentityV1 {
                index: 0,
                label: "Up".to_owned(),
            },
        },
        AssetIdentityV1 {
            token_id: TokenId("2".to_owned()),
            outcome: OutcomeIdentityV1 {
                index: 1,
                label: "Down".to_owned(),
            },
        },
    ];
    let member = PlatformMarketV1::try_new(market, assets).expect("test platform market");
    std::sync::Arc::new(PlatformScenarioV1::try_new(vec![member]).expect("test platform scenario"))
}
