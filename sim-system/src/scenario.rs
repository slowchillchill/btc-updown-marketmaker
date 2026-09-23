use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;

use contracts::{AssetIdentityV1, MarketIdentityV2, Sha256Hex};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::market_directory::{SessionMarketDirectoryErrorV1, SessionMarketDirectoryV1};

pub const PLATFORM_SCENARIO_SCHEMA_V1: &str = "platform_scenario_v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlatformScenarioErrorV1 {
    EmptyMarketSet,
    InvalidConditionId,
    InvalidMarketWindow,
    InvalidAssetIdentity,
    DuplicateAssetIdentity,
    DuplicateOutcomeIndex,
    DuplicateMarketIdentity,
}

impl fmt::Display for PlatformScenarioErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyMarketSet => write!(formatter, "场景市场集不能为空"),
            Self::InvalidConditionId => write!(formatter, "场景 condition_id 非法"),
            Self::InvalidMarketWindow => write!(formatter, "场景市场时间窗口非法"),
            Self::InvalidAssetIdentity => write!(formatter, "场景资产身份非法"),
            Self::DuplicateAssetIdentity => write!(formatter, "场景市场的资产身份重复"),
            Self::DuplicateOutcomeIndex => write!(formatter, "场景市场的 outcome index 重复"),
            Self::DuplicateMarketIdentity => write!(formatter, "场景 condition_id 重复"),
        }
    }
}

impl std::error::Error for PlatformScenarioErrorV1 {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlatformScenarioFileErrorV1 {
    Json(String),
    InvalidSchema,
    Scenario(PlatformScenarioErrorV1),
    MarketDirectory(SessionMarketDirectoryErrorV1),
}

impl fmt::Display for PlatformScenarioFileErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(message) => write!(formatter, "场景文件 JSON 非法：{message}"),
            Self::InvalidSchema => write!(formatter, "场景文件 schema 不匹配"),
            Self::Scenario(error) => write!(formatter, "场景文件业务内容非法：{error}"),
            Self::MarketDirectory(error) => write!(formatter, "场景文件市场目录非法：{error}"),
        }
    }
}

impl std::error::Error for PlatformScenarioFileErrorV1 {}

#[derive(Clone, Debug)]
pub struct SealedPlatformScenarioV1 {
    file_sha256: Sha256Hex,
    scenario: PlatformScenarioV1,
    initial_market_directory: SessionMarketDirectoryV1,
}

impl SealedPlatformScenarioV1 {
    pub fn decode_and_seal(bytes: &[u8]) -> Result<Self, PlatformScenarioFileErrorV1> {
        let artifact: SimulationScenarioArtifactV1 = serde_json::from_slice(bytes)
            .map_err(|error| PlatformScenarioFileErrorV1::Json(error.to_string()))?;
        let SimulationScenarioArtifactV1 {
            schema_version,
            markets,
        } = artifact;
        if schema_version != PLATFORM_SCENARIO_SCHEMA_V1 {
            return Err(PlatformScenarioFileErrorV1::InvalidSchema);
        }
        for market in &markets {
            market
                .validate_for_admission()
                .map_err(PlatformScenarioFileErrorV1::Scenario)?;
        }
        let scenario =
            PlatformScenarioV1::try_new(markets).map_err(PlatformScenarioFileErrorV1::Scenario)?;
        let initial_market_directory = SessionMarketDirectoryV1::try_from_scenario(&scenario)
            .map_err(PlatformScenarioFileErrorV1::MarketDirectory)?;
        Ok(Self {
            file_sha256: sha256_file_bytes(bytes),
            scenario,
            initial_market_directory,
        })
    }

    pub fn file_sha256(&self) -> &Sha256Hex {
        &self.file_sha256
    }

    pub fn scenario(&self) -> &PlatformScenarioV1 {
        &self.scenario
    }

    pub(crate) fn initial_market_directory(&self) -> &SessionMarketDirectoryV1 {
        &self.initial_market_directory
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationScenarioArtifactV1 {
    pub schema_version: String,
    pub markets: Vec<PlatformMarketV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

    pub(crate) fn validate_for_admission(&self) -> Result<(), PlatformScenarioErrorV1> {
        validate_market(&self.market)?;
        for asset in &self.assets {
            validate_asset(asset)?;
        }
        if self.assets[0].token_id == self.assets[1].token_id {
            return Err(PlatformScenarioErrorV1::DuplicateAssetIdentity);
        }
        if self.assets[0].outcome.index == self.assets[1].outcome.index {
            return Err(PlatformScenarioErrorV1::DuplicateOutcomeIndex);
        }
        if compare_token_ids(&self.assets[0].token_id.0, &self.assets[1].token_id.0)
            == Ordering::Greater
        {
            return Err(PlatformScenarioErrorV1::InvalidAssetIdentity);
        }
        Ok(())
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
}

impl MarketSetV1 {
    pub fn try_new(mut markets: Vec<PlatformMarketV1>) -> Result<Self, PlatformScenarioErrorV1> {
        markets.sort_by(|left, right| left.market.condition_id.0.cmp(&right.market.condition_id.0));
        let mut condition_ids = BTreeSet::new();
        if markets
            .iter()
            .any(|member| !condition_ids.insert(member.market.condition_id.0.as_str()))
        {
            return Err(PlatformScenarioErrorV1::DuplicateMarketIdentity);
        }
        Ok(Self { markets })
    }

    #[must_use]
    pub fn markets(&self) -> &[PlatformMarketV1] {
        &self.markets
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlatformScenarioV1 {
    market_set: MarketSetV1,
}

impl PlatformScenarioV1 {
    pub fn try_new(markets: Vec<PlatformMarketV1>) -> Result<Self, PlatformScenarioErrorV1> {
        Ok(Self::from_market_set(MarketSetV1::try_new(markets)?))
    }

    pub fn from_market_set(market_set: MarketSetV1) -> Self {
        Self { market_set }
    }

    #[must_use]
    pub fn market_set(&self) -> &MarketSetV1 {
        &self.market_set
    }
}

fn sha256_file_bytes(bytes: &[u8]) -> Sha256Hex {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("写入 String 不会失败");
    }
    Sha256Hex(encoded)
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

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::{
        AssetIdentityV1, ClobMarketId, ConditionId, MarketWindowV2, OutcomeIdentityV1, TokenId,
    };

    fn market_with_fields(
        clob_market_id: String,
        first_token_id: String,
        first_label: String,
    ) -> Result<PlatformMarketV1, PlatformScenarioErrorV1> {
        PlatformMarketV1::try_new(
            MarketIdentityV2 {
                condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
                clob_market_id: ClobMarketId(clob_market_id),
                window: MarketWindowV2 {
                    start_at_utc_ms: 1_000,
                    end_at_utc_ms: 2_000,
                },
            },
            [
                AssetIdentityV1 {
                    token_id: TokenId(first_token_id),
                    outcome: OutcomeIdentityV1 {
                        index: 0,
                        label: first_label,
                    },
                },
                AssetIdentityV1 {
                    token_id: TokenId("2".to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 1,
                        label: "Down".to_owned(),
                    },
                },
            ],
        )
    }

    fn market(
        condition_digit: char,
        clob_market_id: &str,
        first_token_id: &str,
        second_token_id: &str,
    ) -> PlatformMarketV1 {
        PlatformMarketV1::try_new(
            MarketIdentityV2 {
                condition_id: ConditionId(format!("0x{}", condition_digit.to_string().repeat(64))),
                clob_market_id: ClobMarketId(clob_market_id.to_owned()),
                window: MarketWindowV2 {
                    start_at_utc_ms: 1_000,
                    end_at_utc_ms: 2_000,
                },
            },
            [
                AssetIdentityV1 {
                    token_id: TokenId(first_token_id.to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 0,
                        label: "Up".to_owned(),
                    },
                },
                AssetIdentityV1 {
                    token_id: TokenId(second_token_id.to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 1,
                        label: "Down".to_owned(),
                    },
                },
            ],
        )
        .expect("构造测试市场")
    }

    #[test]
    fn empty_initial_market_set_round_trips_through_original_scenario_file() {
        let set = MarketSetV1::try_new(Vec::new()).unwrap();
        assert!(set.markets().is_empty());
        let scenario = PlatformScenarioV1::from_market_set(set);
        let bytes = serde_json::to_vec(&SimulationScenarioArtifactV1 {
            schema_version: PLATFORM_SCENARIO_SCHEMA_V1.into(),
            markets: Vec::new(),
        })
        .unwrap();
        let sealed = SealedPlatformScenarioV1::decode_and_seal(&bytes).unwrap();
        assert_eq!(sealed.scenario(), &scenario);
        assert_eq!(sealed.initial_market_directory().markets().len(), 0);
        assert_eq!(sealed.file_sha256(), &sha256_file_bytes(&bytes));
    }

    #[test]
    fn sealed_scenario_uses_file_bytes_without_trusting_legacy_object_digests() {
        let scenario = scenario_for_sim_tests();
        let artifact = |scenario_sha256: Sha256Hex, market_set_sha256: Sha256Hex| {
            serde_json::to_vec(&serde_json::json!({
                "schema_version": PLATFORM_SCENARIO_SCHEMA_V1,
                "scenario_sha256": scenario_sha256,
                "market_set_sha256": market_set_sha256,
                "markets": scenario.market_set().markets(),
            }))
            .expect("编码旧场景文件")
        };
        let first_bytes = artifact(Sha256Hex("a".repeat(64)), Sha256Hex("b".repeat(64)));
        let second_bytes = artifact(Sha256Hex("c".repeat(64)), Sha256Hex("d".repeat(64)));

        let first =
            SealedPlatformScenarioV1::decode_and_seal(&first_bytes).expect("解码第一份场景文件");
        let second =
            SealedPlatformScenarioV1::decode_and_seal(&second_bytes).expect("解码第二份场景文件");

        assert_eq!(first.scenario(), second.scenario());
        assert_ne!(first.file_sha256(), second.file_sha256());
        assert_eq!(first.file_sha256(), &sha256_file_bytes(&first_bytes));
    }

    #[test]
    fn sealed_scenario_builds_the_initial_directory_once_and_rejects_cross_market_token_reuse() {
        let artifact_bytes = |markets| {
            serde_json::to_vec(&SimulationScenarioArtifactV1 {
                schema_version: PLATFORM_SCENARIO_SCHEMA_V1.to_owned(),
                markets,
            })
            .expect("编码场景文件")
        };
        let valid_bytes = artifact_bytes(vec![
            market('1', "101", "201", "202"),
            market('2', "102", "203", "204"),
        ]);
        let sealed = SealedPlatformScenarioV1::decode_and_seal(&valid_bytes)
            .expect("冷加载时形成初始市场目录");
        assert_eq!(sealed.initial_market_directory().markets().len(), 2);

        let duplicate_token_bytes = artifact_bytes(vec![
            market('1', "101", "201", "202"),
            market('2', "102", "202", "204"),
        ]);
        assert_eq!(
            SealedPlatformScenarioV1::decode_and_seal(&duplicate_token_bytes)
                .expect_err("冷加载必须拒绝跨市场复用 token"),
            PlatformScenarioFileErrorV1::MarketDirectory(SessionMarketDirectoryErrorV1::Token(
                "202".to_owned()
            ))
        );
    }

    #[test]
    fn long_market_identity_and_asset_text_preserve_association_and_numeric_order() {
        let clob_id = format!("market-{}", "界".repeat(100));
        let token_id = "1".repeat(300);
        let label = "上".repeat(100);
        let market = market_with_fields(clob_id.clone(), token_id.clone(), label.clone()).unwrap();
        assert_eq!(market.market().clob_market_id.0, clob_id);
        assert_eq!(market.assets()[0].token_id.0, "2");
        assert_eq!(market.assets()[1].token_id.0, token_id);
        assert_eq!(market.assets()[1].outcome.label, label);
        market.validate_for_admission().unwrap();
    }

    #[test]
    fn sealed_scenario_rejects_deserialized_noncanonical_asset_order() {
        let canonical = market_with_fields("1".to_owned(), "10".to_owned(), "Up".to_owned())
            .expect("规范市场应成功构造");
        assert_eq!(canonical.assets()[0].token_id.0, "2");
        assert_eq!(canonical.assets()[1].token_id.0, "10");
        canonical
            .validate_for_admission()
            .expect("构造器产生的规范顺序应通过准入");

        let mut encoded = serde_json::to_value(&canonical).expect("市场应能序列化");
        encoded["assets"]
            .as_array_mut()
            .expect("资产字段应为数组")
            .reverse();
        let unchecked: PlatformMarketV1 =
            serde_json::from_value(encoded).expect("Serde 只负责结构反序列化");
        let bytes = serde_json::to_vec(&SimulationScenarioArtifactV1 {
            schema_version: PLATFORM_SCENARIO_SCHEMA_V1.to_owned(),
            markets: vec![unchecked],
        })
        .expect("编码损坏边界样本");

        assert_eq!(
            SealedPlatformScenarioV1::decode_and_seal(&bytes)
                .expect_err("冷加载必须拒绝非规范资产顺序"),
            PlatformScenarioFileErrorV1::Scenario(PlatformScenarioErrorV1::InvalidAssetIdentity)
        );
    }
}
