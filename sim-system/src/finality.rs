use std::collections::BTreeSet;
use std::fmt;

use contracts::{AccountInstanceKeyV1, Sha256Hex};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const TRADE_FINALITY_POLICY_SCHEMA_V1: &str = "trade_finality_policy_v1";
pub(crate) const TRADE_FINALITY_POLICY_SCHEMA_V2: &str = "trade_finality_policy_v2";

const TRADE_FINALITY_POLICY_HASH_DOMAIN_V1: &[u8] =
    b"SIM_TRADE_FINALITY_POLICY_CONTENT_SHA256_V1\0";
const TRADE_FINALITY_POLICY_ID_MAX_BYTES_V1: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeFinalityStateV1 {
    MatchedNotBroadcasted,
    Matched,
    Retrying,
    Mined,
    Confirmed,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeFinalitySelectorDomainV1 {
    Request,
    BatchItem,
    Fill,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeFinalityStepV1 {
    state: TradeFinalityStateV1,
    offset_ms: i64,
}

impl TradeFinalityStepV1 {
    #[must_use]
    pub fn state(&self) -> TradeFinalityStateV1 {
        self.state
    }

    #[must_use]
    pub fn offset_ms(&self) -> i64 {
        self.offset_ms
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeFinalityPathV1 {
    steps: Vec<TradeFinalityStepV1>,
    economic_application_ordinal: Option<u32>,
}

impl TradeFinalityPathV1 {
    #[must_use]
    pub fn steps(&self) -> &[TradeFinalityStepV1] {
        &self.steps
    }

    #[must_use]
    pub fn economic_application_ordinal(&self) -> Option<u32> {
        self.economic_application_ordinal
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TradeFinalitySelectorV1 {
    Request {
        account_instance_key: AccountInstanceKeyV1,
        global_request_id: String,
    },
    BatchItem {
        account_instance_key: AccountInstanceKeyV1,
        item_idempotency_identity: String,
    },
    Fill {
        account_instance_key: AccountInstanceKeyV1,
        fill_economic_delta_identity: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "selector_domain")]
pub(crate) enum TradeFinalitySelectorV2 {
    #[serde(rename = "REQUEST")]
    Request {
        account_instance_key: AccountInstanceKeyV1,
        global_request_id: String,
    },
    #[serde(rename = "BATCH_ITEM")]
    BatchItem {
        account_instance_key: AccountInstanceKeyV1,
        global_request_id: String,
        batch_item_ordinal: u16,
    },
    #[serde(rename = "FILL")]
    Fill {
        account_instance_key: AccountInstanceKeyV1,
        fill_sequence: u64,
    },
}

impl TradeFinalitySelectorV2 {
    #[must_use]
    pub(crate) fn domain(&self) -> TradeFinalitySelectorDomainV1 {
        match self {
            Self::Request { .. } => TradeFinalitySelectorDomainV1::Request,
            Self::BatchItem { .. } => TradeFinalitySelectorDomainV1::BatchItem,
            Self::Fill { .. } => TradeFinalitySelectorDomainV1::Fill,
        }
    }

    #[must_use]
    pub(crate) fn account_instance_key(&self) -> &AccountInstanceKeyV1 {
        match self {
            Self::Request {
                account_instance_key,
                ..
            }
            | Self::BatchItem {
                account_instance_key,
                ..
            }
            | Self::Fill {
                account_instance_key,
                ..
            } => account_instance_key,
        }
    }
}

impl TradeFinalitySelectorV1 {
    #[must_use]
    pub fn domain(&self) -> TradeFinalitySelectorDomainV1 {
        match self {
            Self::Request { .. } => TradeFinalitySelectorDomainV1::Request,
            Self::BatchItem { .. } => TradeFinalitySelectorDomainV1::BatchItem,
            Self::Fill { .. } => TradeFinalitySelectorDomainV1::Fill,
        }
    }

    #[must_use]
    pub fn account_instance_key(&self) -> &AccountInstanceKeyV1 {
        match self {
            Self::Request {
                account_instance_key,
                ..
            }
            | Self::BatchItem {
                account_instance_key,
                ..
            }
            | Self::Fill {
                account_instance_key,
                ..
            } => account_instance_key,
        }
    }

    #[must_use]
    pub fn stable_identity(&self) -> &str {
        match self {
            Self::Request {
                global_request_id, ..
            } => global_request_id,
            Self::BatchItem {
                item_idempotency_identity,
                ..
            } => item_idempotency_identity,
            Self::Fill {
                fill_economic_delta_identity,
                ..
            } => fill_economic_delta_identity,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradeFinalityFailureOverrideV1 {
    selector: TradeFinalitySelectorV1,
    path: TradeFinalityPathV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TradeFinalityFailureOverrideV2 {
    selector: TradeFinalitySelectorV2,
    path: TradeFinalityPathV1,
}

impl TradeFinalityFailureOverrideV2 {
    #[must_use]
    pub(crate) fn selector(&self) -> &TradeFinalitySelectorV2 {
        &self.selector
    }

    #[must_use]
    pub(crate) fn path(&self) -> &TradeFinalityPathV1 {
        &self.path
    }
}

impl TradeFinalityFailureOverrideV1 {
    #[must_use]
    pub fn selector(&self) -> &TradeFinalitySelectorV1 {
        &self.selector
    }

    #[must_use]
    pub fn path(&self) -> &TradeFinalityPathV1 {
        &self.path
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradeFinalityPolicyV1 {
    policy_id: String,
    content_sha256: Sha256Hex,
    normal_path: TradeFinalityPathV1,
    override_selector_domain: Option<TradeFinalitySelectorDomainV1>,
    failure_overrides: Vec<TradeFinalityFailureOverrideV1>,
    canonical_content_json: Vec<u8>,
}

impl TradeFinalityPolicyV1 {
    pub fn decode_and_seal(bytes: &[u8]) -> Result<Self, TradeFinalityPolicyErrorV1> {
        let wire: TradeFinalityPolicyWireV1 = serde_json::from_slice(bytes)
            .map_err(|error| TradeFinalityPolicyErrorV1::Json(error.to_string()))?;
        if wire.schema_version != TRADE_FINALITY_POLICY_SCHEMA_V1 {
            return Err(TradeFinalityPolicyErrorV1::UnsupportedSchema {
                actual: wire.schema_version,
            });
        }
        validate_bounded_policy_id(&wire.policy_id)
            .map_err(|_| TradeFinalityPolicyErrorV1::InvalidPolicyId)?;
        validate_sha256(&wire.content_sha256)?;

        let content = TradeFinalityPolicyHashContentV1 {
            schema_version: &wire.schema_version,
            policy_id: &wire.policy_id,
            normal_path: &wire.normal_path,
            failure_overrides: &wire.failure_overrides,
        };
        let canonical_content_json = serde_json::to_vec(&content)
            .map_err(|error| TradeFinalityPolicyErrorV1::Serialization(error.to_string()))?;
        let computed = hash_canonical_content(&canonical_content_json);
        if wire.content_sha256 != computed {
            return Err(TradeFinalityPolicyErrorV1::ContentSha256Mismatch {
                declared: wire.content_sha256,
                computed,
            });
        }

        let ValidatedTradeFinalityPolicyV1 {
            normal_path,
            override_selector_domain,
            failure_overrides,
        } = validate_policy_business(wire.normal_path, wire.failure_overrides)?;

        Ok(Self {
            policy_id: wire.policy_id,
            content_sha256: computed,
            normal_path,
            override_selector_domain,
            failure_overrides,
            canonical_content_json,
        })
    }

    #[must_use]
    pub fn policy_id(&self) -> &str {
        &self.policy_id
    }

    #[must_use]
    pub fn content_sha256(&self) -> &Sha256Hex {
        &self.content_sha256
    }

    #[must_use]
    pub fn normal_path(&self) -> &TradeFinalityPathV1 {
        &self.normal_path
    }

    #[must_use]
    pub fn override_selector_domain(&self) -> Option<TradeFinalitySelectorDomainV1> {
        self.override_selector_domain
    }

    #[must_use]
    pub fn failure_overrides(&self) -> &[TradeFinalityFailureOverrideV1] {
        &self.failure_overrides
    }

    #[must_use]
    pub fn canonical_content_json(&self) -> &[u8] {
        &self.canonical_content_json
    }

    pub fn select_path_for_fill(
        &self,
        account_instance_key: &AccountInstanceKeyV1,
        global_request_id: &str,
        item_idempotency_identity: &str,
        fill_economic_delta_identity: &str,
    ) -> &TradeFinalityPathV1 {
        select_path_for_fill(
            self.override_selector_domain,
            &self.failure_overrides,
            &self.normal_path,
            account_instance_key,
            global_request_id,
            item_idempotency_identity,
            fill_economic_delta_identity,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradeFinalityPolicyV2 {
    policy_id: Sha256Hex,
    normal_path: TradeFinalityPathV1,
    override_selector_domain: Option<TradeFinalitySelectorDomainV1>,
    failure_overrides: Vec<TradeFinalityFailureOverrideV2>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SelectedTradeFinalityPathV2 {
    Normal,
    FailureOverride(usize),
}

impl TradeFinalityPolicyV2 {
    pub fn decode_and_seal(bytes: &[u8]) -> Result<Self, TradeFinalityPolicyErrorV1> {
        let wire: TradeFinalityPolicyWireV2 = serde_json::from_slice(bytes)
            .map_err(|error| TradeFinalityPolicyErrorV1::Json(error.to_string()))?;
        if wire.schema_version != TRADE_FINALITY_POLICY_SCHEMA_V2 {
            return Err(TradeFinalityPolicyErrorV1::UnsupportedSchema {
                actual: wire.schema_version,
            });
        }
        let ValidatedTradeFinalityPolicyV2 {
            normal_path,
            override_selector_domain,
            failure_overrides,
        } = validate_policy_business_v2(wire.normal_path, wire.failure_overrides)?;

        Ok(Self {
            policy_id: sha256_file_bytes(bytes),
            normal_path,
            override_selector_domain,
            failure_overrides,
        })
    }

    #[must_use]
    pub fn policy_id(&self) -> &Sha256Hex {
        &self.policy_id
    }

    #[must_use]
    pub(crate) fn normal_path(&self) -> &TradeFinalityPathV1 {
        &self.normal_path
    }

    #[must_use]
    pub(crate) fn failure_overrides(&self) -> &[TradeFinalityFailureOverrideV2] {
        &self.failure_overrides
    }

    pub(crate) fn select_path_for_fill(
        &self,
        account_instance_key: &AccountInstanceKeyV1,
        global_request_id: &str,
        batch_item_ordinal: u16,
        fill_sequence: u64,
    ) -> &TradeFinalityPathV1 {
        self.path_for_selection(self.select_path_for_fill_once(
            account_instance_key,
            global_request_id,
            batch_item_ordinal,
            fill_sequence,
        ))
    }

    pub(crate) fn select_path_for_fill_once(
        &self,
        account_instance_key: &AccountInstanceKeyV1,
        global_request_id: &str,
        batch_item_ordinal: u16,
        fill_sequence: u64,
    ) -> SelectedTradeFinalityPathV2 {
        let Some(selector_domain) = self.override_selector_domain else {
            return SelectedTradeFinalityPathV2::Normal;
        };
        self.failure_overrides
            .iter()
            .position(|failure_override| {
                let selector = failure_override.selector();
                selector.account_instance_key() == account_instance_key
                    && match (selector_domain, selector) {
                        (
                            TradeFinalitySelectorDomainV1::Request,
                            TradeFinalitySelectorV2::Request {
                                global_request_id: selected,
                                ..
                            },
                        ) => selected == global_request_id,
                        (
                            TradeFinalitySelectorDomainV1::BatchItem,
                            TradeFinalitySelectorV2::BatchItem {
                                global_request_id: selected_request,
                                batch_item_ordinal: selected_ordinal,
                                ..
                            },
                        ) => {
                            selected_request == global_request_id
                                && *selected_ordinal == batch_item_ordinal
                        }
                        (
                            TradeFinalitySelectorDomainV1::Fill,
                            TradeFinalitySelectorV2::Fill {
                                fill_sequence: selected,
                                ..
                            },
                        ) => *selected == fill_sequence,
                        _ => false,
                    }
            })
            .map_or(SelectedTradeFinalityPathV2::Normal, |index| {
                SelectedTradeFinalityPathV2::FailureOverride(index)
            })
    }

    #[must_use]
    pub(crate) fn path_for_selection(
        &self,
        selection: SelectedTradeFinalityPathV2,
    ) -> &TradeFinalityPathV1 {
        match selection {
            SelectedTradeFinalityPathV2::Normal => &self.normal_path,
            SelectedTradeFinalityPathV2::FailureOverride(index) => {
                self.failure_overrides[index].path()
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TradeFinalityPolicyErrorV1 {
    Json(String),
    UnsupportedSchema {
        actual: String,
    },
    InvalidPolicyId,
    InvalidContentSha256,
    ContentSha256Mismatch {
        declared: Sha256Hex,
        computed: Sha256Hex,
    },
    Serialization(String),
    NormalPathNotSuccessfulTemplate,
    FailurePathNotFailureTemplate {
        override_index: usize,
    },
    NegativeOffset {
        path: String,
        ordinal: usize,
    },
    FirstOffsetNotZero {
        path: String,
    },
    OffsetNotStrictlyIncreasing {
        path: String,
        ordinal: usize,
    },
    MissingEconomicApplication {
        path: String,
    },
    EconomicApplicationOrdinalOutOfRange {
        path: String,
        ordinal: u32,
    },
    IllegalEconomicApplicationState {
        path: String,
        state: TradeFinalityStateV1,
    },
    OverrideDomainMismatch {
        override_index: usize,
        expected: TradeFinalitySelectorDomainV1,
        actual: TradeFinalitySelectorDomainV1,
    },
    InvalidAccountInstanceKey {
        override_index: usize,
        field: &'static str,
    },
    InvalidSelectorIdentity {
        override_index: usize,
    },
    DuplicateSelector {
        index: usize,
    },
}

impl fmt::Display for TradeFinalityPolicyErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(message) => write!(formatter, "成交最终性策略 JSON 非法：{message}"),
            Self::UnsupportedSchema { actual } => {
                write!(formatter, "不支持的成交最终性策略 schema：{actual}")
            }
            Self::InvalidPolicyId => write!(formatter, "成交最终性策略 policy_id 非法"),
            Self::InvalidContentSha256 => {
                write!(formatter, "成交最终性策略 SHA-256 必须是 64 位小写十六进制")
            }
            Self::ContentSha256Mismatch { declared, computed } => write!(
                formatter,
                "成交最终性策略内容摘要冲突：声明 {}，重算 {}",
                declared.0, computed.0
            ),
            Self::Serialization(message) => {
                write!(formatter, "成交最终性策略规范序列化失败：{message}")
            }
            Self::NormalPathNotSuccessfulTemplate => {
                write!(formatter, "普通成交路径不属于获准成功模板")
            }
            Self::FailurePathNotFailureTemplate { override_index } => {
                write!(
                    formatter,
                    "第 {override_index} 个故障覆盖不属于获准失败模板"
                )
            }
            Self::NegativeOffset { path, ordinal } => {
                write!(formatter, "{path} 第 {ordinal} 个逻辑偏移为负数")
            }
            Self::FirstOffsetNotZero { path } => write!(formatter, "{path} 的首个逻辑偏移必须为零"),
            Self::OffsetNotStrictlyIncreasing { path, ordinal } => {
                write!(formatter, "{path} 第 {ordinal} 个逻辑偏移没有严格递增")
            }
            Self::MissingEconomicApplication { path } => {
                write!(formatter, "{path} 缺少经济应用截点")
            }
            Self::EconomicApplicationOrdinalOutOfRange { path, ordinal } => {
                write!(formatter, "{path} 的经济应用截点 {ordinal} 越界")
            }
            Self::IllegalEconomicApplicationState { path, state } => {
                write!(formatter, "{path} 不能在 {state:?} 应用经济变化")
            }
            Self::OverrideDomainMismatch {
                override_index,
                expected,
                actual,
            } => write!(
                formatter,
                "第 {override_index} 个覆盖选择域为 {actual:?}，预期为 {expected:?}"
            ),
            Self::InvalidAccountInstanceKey {
                override_index,
                field,
            } => write!(
                formatter,
                "第 {override_index} 个覆盖的账户实例键字段 {field} 非法"
            ),
            Self::InvalidSelectorIdentity { override_index } => {
                write!(formatter, "第 {override_index} 个覆盖的稳定选择身份非法")
            }
            Self::DuplicateSelector { index } => {
                write!(formatter, "第 {index} 个故障覆盖的同域选择键重复")
            }
        }
    }
}

impl std::error::Error for TradeFinalityPolicyErrorV1 {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TradeFinalityPolicyWireV1 {
    schema_version: String,
    policy_id: String,
    content_sha256: Sha256Hex,
    normal_path: TradeFinalityPathWireV1,
    failure_overrides: Vec<TradeFinalityFailureOverrideWireV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TradeFinalityPolicyWireV2 {
    schema_version: String,
    normal_path: TradeFinalityPathWireV1,
    failure_overrides: Vec<TradeFinalityFailureOverrideWireV2>,
}

#[derive(Serialize)]
struct TradeFinalityPolicyHashContentV1<'a> {
    schema_version: &'a str,
    policy_id: &'a str,
    normal_path: &'a TradeFinalityPathWireV1,
    failure_overrides: &'a [TradeFinalityFailureOverrideWireV1],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TradeFinalityPathWireV1 {
    steps: Vec<TradeFinalityStepWireV1>,
    economic_application_ordinal: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TradeFinalityStepWireV1 {
    state: TradeFinalityStateV1,
    offset_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TradeFinalityFailureOverrideWireV1 {
    selector: TradeFinalitySelectorWireV1,
    path: TradeFinalityPathWireV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TradeFinalityFailureOverrideWireV2 {
    selector: TradeFinalitySelectorV2,
    path: TradeFinalityPathWireV1,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "selector_domain")]
enum TradeFinalitySelectorWireV1 {
    #[serde(rename = "REQUEST")]
    Request {
        account_instance_key: AccountInstanceKeyV1,
        global_request_id: String,
    },
    #[serde(rename = "BATCH_ITEM")]
    BatchItem {
        account_instance_key: AccountInstanceKeyV1,
        item_idempotency_identity: String,
    },
    #[serde(rename = "FILL")]
    Fill {
        account_instance_key: AccountInstanceKeyV1,
        fill_economic_delta_identity: String,
    },
}

struct ValidatedTradeFinalityPolicyV1 {
    normal_path: TradeFinalityPathV1,
    override_selector_domain: Option<TradeFinalitySelectorDomainV1>,
    failure_overrides: Vec<TradeFinalityFailureOverrideV1>,
}

struct ValidatedTradeFinalityPolicyV2 {
    normal_path: TradeFinalityPathV1,
    override_selector_domain: Option<TradeFinalitySelectorDomainV1>,
    failure_overrides: Vec<TradeFinalityFailureOverrideV2>,
}

fn validate_policy_business(
    normal_path: TradeFinalityPathWireV1,
    failure_overrides: Vec<TradeFinalityFailureOverrideWireV1>,
) -> Result<ValidatedTradeFinalityPolicyV1, TradeFinalityPolicyErrorV1> {
    let normal_path = validate_path(normal_path, PathKindV1::Normal)?;
    let override_selector_domain = derive_override_domain(&failure_overrides)?;
    let mut selector_keys = BTreeSet::new();
    let mut validated_overrides = Vec::with_capacity(failure_overrides.len());
    for (index, member) in failure_overrides.into_iter().enumerate() {
        if !selector_keys.insert(member.selector.clone()) {
            return Err(TradeFinalityPolicyErrorV1::DuplicateSelector { index });
        }
        let selector = validate_selector(member.selector, index)?;
        let path = validate_path(member.path, PathKindV1::FailureOverride { index })?;
        validated_overrides.push(TradeFinalityFailureOverrideV1 { selector, path });
    }
    Ok(ValidatedTradeFinalityPolicyV1 {
        normal_path,
        override_selector_domain,
        failure_overrides: validated_overrides,
    })
}

fn validate_policy_business_v2(
    normal_path: TradeFinalityPathWireV1,
    failure_overrides: Vec<TradeFinalityFailureOverrideWireV2>,
) -> Result<ValidatedTradeFinalityPolicyV2, TradeFinalityPolicyErrorV1> {
    let normal_path = validate_path(normal_path, PathKindV1::Normal)?;
    let override_selector_domain = derive_override_domain_v2(&failure_overrides)?;
    let mut selector_keys = BTreeSet::new();
    let mut validated_overrides = Vec::with_capacity(failure_overrides.len());
    for (index, member) in failure_overrides.into_iter().enumerate() {
        if !selector_keys.insert(member.selector.clone()) {
            return Err(TradeFinalityPolicyErrorV1::DuplicateSelector { index });
        }
        let selector = validate_selector_v2(member.selector, index)?;
        let path = validate_path(member.path, PathKindV1::FailureOverride { index })?;
        validated_overrides.push(TradeFinalityFailureOverrideV2 { selector, path });
    }
    Ok(ValidatedTradeFinalityPolicyV2 {
        normal_path,
        override_selector_domain,
        failure_overrides: validated_overrides,
    })
}

fn select_path_for_fill<'a>(
    override_selector_domain: Option<TradeFinalitySelectorDomainV1>,
    failure_overrides: &'a [TradeFinalityFailureOverrideV1],
    normal_path: &'a TradeFinalityPathV1,
    account_instance_key: &AccountInstanceKeyV1,
    global_request_id: &str,
    item_idempotency_identity: &str,
    fill_economic_delta_identity: &str,
) -> &'a TradeFinalityPathV1 {
    let Some(selector_domain) = override_selector_domain else {
        return normal_path;
    };
    let stable_identity = match selector_domain {
        TradeFinalitySelectorDomainV1::Request => global_request_id,
        TradeFinalitySelectorDomainV1::BatchItem => item_idempotency_identity,
        TradeFinalitySelectorDomainV1::Fill => fill_economic_delta_identity,
    };
    failure_overrides
        .iter()
        .find(|failure_override| {
            let selector = failure_override.selector();
            selector.account_instance_key() == account_instance_key
                && selector.stable_identity() == stable_identity
        })
        .map(TradeFinalityFailureOverrideV1::path)
        .unwrap_or(normal_path)
}

impl TradeFinalitySelectorWireV1 {
    fn domain(&self) -> TradeFinalitySelectorDomainV1 {
        match self {
            Self::Request { .. } => TradeFinalitySelectorDomainV1::Request,
            Self::BatchItem { .. } => TradeFinalitySelectorDomainV1::BatchItem,
            Self::Fill { .. } => TradeFinalitySelectorDomainV1::Fill,
        }
    }
}

#[derive(Clone, Copy)]
enum PathKindV1 {
    Normal,
    FailureOverride { index: usize },
}

impl PathKindV1 {
    fn label(self) -> String {
        match self {
            Self::Normal => "普通成交路径".to_owned(),
            Self::FailureOverride { index } => format!("第 {index} 个故障覆盖路径"),
        }
    }
}

fn validate_path(
    wire: TradeFinalityPathWireV1,
    kind: PathKindV1,
) -> Result<TradeFinalityPathV1, TradeFinalityPolicyErrorV1> {
    let label = kind.label();
    for (ordinal, step) in wire.steps.iter().enumerate() {
        if step.offset_ms < 0 {
            return Err(TradeFinalityPolicyErrorV1::NegativeOffset {
                path: label,
                ordinal,
            });
        }
        if ordinal == 0 && step.offset_ms != 0 {
            return Err(TradeFinalityPolicyErrorV1::FirstOffsetNotZero { path: label });
        }
        if ordinal > 0 && step.offset_ms <= wire.steps[ordinal - 1].offset_ms {
            return Err(TradeFinalityPolicyErrorV1::OffsetNotStrictlyIncreasing {
                path: label,
                ordinal,
            });
        }
    }

    let states = wire.steps.iter().map(|step| step.state).collect::<Vec<_>>();
    match kind {
        PathKindV1::Normal if !is_success_template(&states) => {
            return Err(TradeFinalityPolicyErrorV1::NormalPathNotSuccessfulTemplate);
        }
        PathKindV1::FailureOverride { index } if !is_failure_template(&states) => {
            return Err(TradeFinalityPolicyErrorV1::FailurePathNotFailureTemplate {
                override_index: index,
            });
        }
        _ => {}
    }

    validate_economic_application(&wire, kind)?;
    Ok(TradeFinalityPathV1 {
        steps: wire
            .steps
            .into_iter()
            .map(|step| TradeFinalityStepV1 {
                state: step.state,
                offset_ms: step.offset_ms,
            })
            .collect(),
        economic_application_ordinal: wire.economic_application_ordinal,
    })
}

fn validate_economic_application(
    path: &TradeFinalityPathWireV1,
    kind: PathKindV1,
) -> Result<(), TradeFinalityPolicyErrorV1> {
    let label = kind.label();
    let Some(ordinal) = path.economic_application_ordinal else {
        return match kind {
            PathKindV1::Normal => {
                Err(TradeFinalityPolicyErrorV1::MissingEconomicApplication { path: label })
            }
            PathKindV1::FailureOverride { .. } => Ok(()),
        };
    };
    let Some(step) = usize::try_from(ordinal)
        .ok()
        .and_then(|ordinal| path.steps.get(ordinal))
    else {
        return Err(
            TradeFinalityPolicyErrorV1::EconomicApplicationOrdinalOutOfRange {
                path: label,
                ordinal,
            },
        );
    };
    let legal = match kind {
        PathKindV1::Normal => matches!(
            step.state,
            TradeFinalityStateV1::Matched
                | TradeFinalityStateV1::Mined
                | TradeFinalityStateV1::Confirmed
        ),
        PathKindV1::FailureOverride { .. } => matches!(
            step.state,
            TradeFinalityStateV1::Matched | TradeFinalityStateV1::Mined
        ),
    };
    if !legal {
        return Err(
            TradeFinalityPolicyErrorV1::IllegalEconomicApplicationState {
                path: label,
                state: step.state,
            },
        );
    }
    Ok(())
}

fn derive_override_domain(
    overrides: &[TradeFinalityFailureOverrideWireV1],
) -> Result<Option<TradeFinalitySelectorDomainV1>, TradeFinalityPolicyErrorV1> {
    let Some(first) = overrides.first() else {
        return Ok(None);
    };
    let expected = first.selector.domain();
    for (index, member) in overrides.iter().enumerate().skip(1) {
        let actual = member.selector.domain();
        if actual != expected {
            return Err(TradeFinalityPolicyErrorV1::OverrideDomainMismatch {
                override_index: index,
                expected,
                actual,
            });
        }
    }
    Ok(Some(expected))
}

fn derive_override_domain_v2(
    overrides: &[TradeFinalityFailureOverrideWireV2],
) -> Result<Option<TradeFinalitySelectorDomainV1>, TradeFinalityPolicyErrorV1> {
    let Some(first) = overrides.first() else {
        return Ok(None);
    };
    let expected = first.selector.domain();
    for (index, member) in overrides.iter().enumerate().skip(1) {
        let actual = member.selector.domain();
        if actual != expected {
            return Err(TradeFinalityPolicyErrorV1::OverrideDomainMismatch {
                override_index: index,
                expected,
                actual,
            });
        }
    }
    Ok(Some(expected))
}

fn validate_selector(
    wire: TradeFinalitySelectorWireV1,
    index: usize,
) -> Result<TradeFinalitySelectorV1, TradeFinalityPolicyErrorV1> {
    let (account_instance_key, stable_identity) = match &wire {
        TradeFinalitySelectorWireV1::Request {
            account_instance_key,
            global_request_id,
        } => (account_instance_key, global_request_id),
        TradeFinalitySelectorWireV1::BatchItem {
            account_instance_key,
            item_idempotency_identity,
        } => (account_instance_key, item_idempotency_identity),
        TradeFinalitySelectorWireV1::Fill {
            account_instance_key,
            fill_economic_delta_identity,
        } => (account_instance_key, fill_economic_delta_identity),
    };
    validate_selector_account_key(account_instance_key, index)?;
    validate_selector_identity(stable_identity, index)?;

    Ok(match wire {
        TradeFinalitySelectorWireV1::Request {
            account_instance_key,
            global_request_id,
        } => TradeFinalitySelectorV1::Request {
            account_instance_key,
            global_request_id,
        },
        TradeFinalitySelectorWireV1::BatchItem {
            account_instance_key,
            item_idempotency_identity,
        } => TradeFinalitySelectorV1::BatchItem {
            account_instance_key,
            item_idempotency_identity,
        },
        TradeFinalitySelectorWireV1::Fill {
            account_instance_key,
            fill_economic_delta_identity,
        } => TradeFinalitySelectorV1::Fill {
            account_instance_key,
            fill_economic_delta_identity,
        },
    })
}

fn validate_selector_v2(
    selector: TradeFinalitySelectorV2,
    index: usize,
) -> Result<TradeFinalitySelectorV2, TradeFinalityPolicyErrorV1> {
    validate_selector_account_key(selector.account_instance_key(), index)?;
    match &selector {
        TradeFinalitySelectorV2::Request {
            global_request_id, ..
        }
        | TradeFinalitySelectorV2::BatchItem {
            global_request_id, ..
        } => validate_selector_identity(global_request_id, index)?,
        TradeFinalitySelectorV2::Fill { .. } => {}
    }
    Ok(selector)
}

fn validate_selector_account_key(
    account_instance_key: &AccountInstanceKeyV1,
    index: usize,
) -> Result<(), TradeFinalityPolicyErrorV1> {
    for (field, value) in [
        ("run_id", account_instance_key.run_id.as_str()),
        (
            "caller_instance_id",
            account_instance_key.caller_instance_id.as_str(),
        ),
        ("account_id", account_instance_key.account_id.as_str()),
    ] {
        validate_stable_text(value).map_err(|_| {
            TradeFinalityPolicyErrorV1::InvalidAccountInstanceKey {
                override_index: index,
                field,
            }
        })?;
    }
    Ok(())
}

fn validate_selector_identity(value: &str, index: usize) -> Result<(), TradeFinalityPolicyErrorV1> {
    validate_stable_text(value).map_err(|_| TradeFinalityPolicyErrorV1::InvalidSelectorIdentity {
        override_index: index,
    })
}

fn is_success_template(states: &[TradeFinalityStateV1]) -> bool {
    use TradeFinalityStateV1::{Confirmed, Matched, MatchedNotBroadcasted, Mined, Retrying};
    matches!(
        states,
        [Matched, Mined, Confirmed]
            | [MatchedNotBroadcasted, Matched, Mined, Confirmed]
            | [Matched, Retrying, Mined, Confirmed]
            | [MatchedNotBroadcasted, Matched, Retrying, Mined, Confirmed]
    )
}

fn is_failure_template(states: &[TradeFinalityStateV1]) -> bool {
    use TradeFinalityStateV1::{Failed, Matched, MatchedNotBroadcasted, Mined, Retrying};
    matches!(
        states,
        [MatchedNotBroadcasted, Failed]
            | [Matched, Failed]
            | [MatchedNotBroadcasted, Matched, Failed]
            | [Matched, Mined, Failed]
            | [MatchedNotBroadcasted, Matched, Mined, Failed]
            | [Matched, Retrying, Failed]
            | [MatchedNotBroadcasted, Matched, Retrying, Failed]
    )
}

fn validate_stable_text(value: &str) -> Result<(), ()> {
    if !value.is_empty() && value.trim() == value {
        Ok(())
    } else {
        Err(())
    }
}

fn validate_bounded_policy_id(value: &str) -> Result<(), ()> {
    validate_stable_text(value)?;
    if value.len() <= TRADE_FINALITY_POLICY_ID_MAX_BYTES_V1 {
        Ok(())
    } else {
        Err(())
    }
}

fn validate_sha256(value: &Sha256Hex) -> Result<(), TradeFinalityPolicyErrorV1> {
    if value.0.len() == 64
        && value
            .0
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(TradeFinalityPolicyErrorV1::InvalidContentSha256)
    }
}

fn hash_canonical_content(bytes: &[u8]) -> Sha256Hex {
    let mut hasher = Sha256::new();
    hasher.update(TRADE_FINALITY_POLICY_HASH_DOMAIN_V1);
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
    Sha256Hex(format!("{:x}", hasher.finalize()))
}

fn sha256_file_bytes(bytes: &[u8]) -> Sha256Hex {
    Sha256Hex(format!("{:x}", Sha256::digest(bytes)))
}

#[cfg(test)]
pub(crate) fn successful_policy_v2_for_tests() -> std::sync::Arc<TradeFinalityPolicyV2> {
    let wire = TradeFinalityPolicyWireV2 {
        schema_version: TRADE_FINALITY_POLICY_SCHEMA_V2.to_owned(),
        normal_path: TradeFinalityPathWireV1 {
            steps: vec![
                TradeFinalityStepWireV1 {
                    state: TradeFinalityStateV1::Matched,
                    offset_ms: 0,
                },
                TradeFinalityStepWireV1 {
                    state: TradeFinalityStateV1::Mined,
                    offset_ms: 10,
                },
                TradeFinalityStepWireV1 {
                    state: TradeFinalityStateV1::Confirmed,
                    offset_ms: 20,
                },
            ],
            economic_application_ordinal: Some(1),
        },
        failure_overrides: Vec::new(),
    };
    let bytes = serde_json::to_vec(&wire).expect("测试 V2 最终性策略必须可编码");
    std::sync::Arc::new(
        TradeFinalityPolicyV2::decode_and_seal(&bytes).expect("测试 V2 最终性策略必须合法"),
    )
}

#[cfg(test)]
pub(crate) fn successful_policy_for_tests() -> std::sync::Arc<TradeFinalityPolicyV1> {
    let normal_path = TradeFinalityPathWireV1 {
        steps: vec![
            TradeFinalityStepWireV1 {
                state: TradeFinalityStateV1::Matched,
                offset_ms: 0,
            },
            TradeFinalityStepWireV1 {
                state: TradeFinalityStateV1::Mined,
                offset_ms: 10,
            },
            TradeFinalityStepWireV1 {
                state: TradeFinalityStateV1::Confirmed,
                offset_ms: 20,
            },
        ],
        economic_application_ordinal: Some(1),
    };
    seal_test_policy_wire(TradeFinalityPolicyWireV1 {
        schema_version: TRADE_FINALITY_POLICY_SCHEMA_V1.to_owned(),
        policy_id: "stage3-successful-test-policy-v1".to_owned(),
        content_sha256: Sha256Hex("0".repeat(64)),
        normal_path,
        failure_overrides: Vec::new(),
    })
}

#[cfg(test)]
pub(crate) fn failure_policy_for_tests(
    selector_run_id: &str,
) -> std::sync::Arc<TradeFinalityPolicyV1> {
    let normal_path = TradeFinalityPathWireV1 {
        steps: vec![
            TradeFinalityStepWireV1 {
                state: TradeFinalityStateV1::Matched,
                offset_ms: 0,
            },
            TradeFinalityStepWireV1 {
                state: TradeFinalityStateV1::Mined,
                offset_ms: 10,
            },
            TradeFinalityStepWireV1 {
                state: TradeFinalityStateV1::Confirmed,
                offset_ms: 20,
            },
        ],
        economic_application_ordinal: Some(1),
    };
    let failure_override = TradeFinalityFailureOverrideWireV1 {
        selector: TradeFinalitySelectorWireV1::Request {
            account_instance_key: AccountInstanceKeyV1 {
                run_id: selector_run_id.to_owned(),
                caller_role: contracts::SimCallerRoleV1::ModelTrain,
                caller_instance_id: "task30-test-caller".to_owned(),
                account_id: "task30-test-account".to_owned(),
                platform_source: contracts::PlatformSourceV1::Simulated,
            },
            global_request_id: "task30-test-request".to_owned(),
        },
        path: TradeFinalityPathWireV1 {
            steps: vec![
                TradeFinalityStepWireV1 {
                    state: TradeFinalityStateV1::Matched,
                    offset_ms: 0,
                },
                TradeFinalityStepWireV1 {
                    state: TradeFinalityStateV1::Failed,
                    offset_ms: 10,
                },
            ],
            economic_application_ordinal: None,
        },
    };
    seal_test_policy_wire(TradeFinalityPolicyWireV1 {
        schema_version: TRADE_FINALITY_POLICY_SCHEMA_V1.to_owned(),
        policy_id: "stage3-failure-test-policy-v1".to_owned(),
        content_sha256: Sha256Hex("0".repeat(64)),
        normal_path,
        failure_overrides: vec![failure_override],
    })
}

#[cfg(test)]
pub(crate) type TestFinalityFailureOverrideV1 = (
    TradeFinalitySelectorV1,
    Vec<(TradeFinalityStateV1, i64)>,
    Option<u32>,
);

#[cfg(test)]
pub(crate) fn policy_for_tests(
    policy_id: &str,
    normal_steps: &[(TradeFinalityStateV1, i64)],
    normal_economic_application_ordinal: u32,
    failure_overrides: Vec<TestFinalityFailureOverrideV1>,
) -> std::sync::Arc<TradeFinalityPolicyV1> {
    let failure_overrides = failure_overrides
        .into_iter()
        .map(|(selector, steps, economic_application_ordinal)| {
            let selector = match selector {
                TradeFinalitySelectorV1::Request {
                    account_instance_key,
                    global_request_id,
                } => TradeFinalitySelectorWireV1::Request {
                    account_instance_key,
                    global_request_id,
                },
                TradeFinalitySelectorV1::BatchItem {
                    account_instance_key,
                    item_idempotency_identity,
                } => TradeFinalitySelectorWireV1::BatchItem {
                    account_instance_key,
                    item_idempotency_identity,
                },
                TradeFinalitySelectorV1::Fill {
                    account_instance_key,
                    fill_economic_delta_identity,
                } => TradeFinalitySelectorWireV1::Fill {
                    account_instance_key,
                    fill_economic_delta_identity,
                },
            };
            TradeFinalityFailureOverrideWireV1 {
                selector,
                path: TradeFinalityPathWireV1 {
                    steps: steps
                        .into_iter()
                        .map(|(state, offset_ms)| TradeFinalityStepWireV1 { state, offset_ms })
                        .collect(),
                    economic_application_ordinal,
                },
            }
        })
        .collect();
    seal_test_policy_wire(TradeFinalityPolicyWireV1 {
        schema_version: TRADE_FINALITY_POLICY_SCHEMA_V1.to_owned(),
        policy_id: policy_id.to_owned(),
        content_sha256: Sha256Hex("0".repeat(64)),
        normal_path: TradeFinalityPathWireV1 {
            steps: normal_steps
                .iter()
                .map(|(state, offset_ms)| TradeFinalityStepWireV1 {
                    state: *state,
                    offset_ms: *offset_ms,
                })
                .collect(),
            economic_application_ordinal: Some(normal_economic_application_ordinal),
        },
        failure_overrides,
    })
}

#[cfg(test)]
fn seal_test_policy_wire(
    mut wire: TradeFinalityPolicyWireV1,
) -> std::sync::Arc<TradeFinalityPolicyV1> {
    let content = TradeFinalityPolicyHashContentV1 {
        schema_version: &wire.schema_version,
        policy_id: &wire.policy_id,
        normal_path: &wire.normal_path,
        failure_overrides: &wire.failure_overrides,
    };
    let canonical = serde_json::to_vec(&content).expect("测试最终性策略规范内容必须可序列化");
    wire.content_sha256 = hash_canonical_content(&canonical);
    let bytes = serde_json::to_vec(&wire).expect("测试最终性策略必须可序列化");
    std::sync::Arc::new(
        TradeFinalityPolicyV1::decode_and_seal(&bytes).expect("测试最终性策略必须合法"),
    )
}

#[cfg(test)]
mod tests {
    use contracts::{PlatformSourceV1, SimCallerRoleV1};

    use super::*;

    fn account_key(account_id: &str) -> AccountInstanceKeyV1 {
        AccountInstanceKeyV1 {
            run_id: "run-a".to_owned(),
            caller_role: SimCallerRoleV1::ModelTrain,
            caller_instance_id: "trainer-a".to_owned(),
            account_id: account_id.to_owned(),
            platform_source: PlatformSourceV1::Simulated,
        }
    }

    fn path(
        states: &[TradeFinalityStateV1],
        economic_application_ordinal: Option<u32>,
    ) -> TradeFinalityPathWireV1 {
        TradeFinalityPathWireV1 {
            steps: states
                .iter()
                .enumerate()
                .map(|(ordinal, state)| TradeFinalityStepWireV1 {
                    state: *state,
                    offset_ms: i64::try_from(ordinal).expect("small ordinal") * 10,
                })
                .collect(),
            economic_application_ordinal,
        }
    }

    fn request_override(
        request_id: &str,
        failure_path: TradeFinalityPathWireV1,
    ) -> TradeFinalityFailureOverrideWireV1 {
        TradeFinalityFailureOverrideWireV1 {
            selector: TradeFinalitySelectorWireV1::Request {
                account_instance_key: account_key("account-a"),
                global_request_id: request_id.to_owned(),
            },
            path: failure_path,
        }
    }

    fn request_override_v2(
        request_id: &str,
        failure_path: TradeFinalityPathWireV1,
    ) -> TradeFinalityFailureOverrideWireV2 {
        TradeFinalityFailureOverrideWireV2 {
            selector: TradeFinalitySelectorV2::Request {
                account_instance_key: account_key("account-a"),
                global_request_id: request_id.to_owned(),
            },
            path: failure_path,
        }
    }

    fn batch_item_override_v2(
        request_id: &str,
        batch_item_ordinal: u16,
        failure_path: TradeFinalityPathWireV1,
    ) -> TradeFinalityFailureOverrideWireV2 {
        TradeFinalityFailureOverrideWireV2 {
            selector: TradeFinalitySelectorV2::BatchItem {
                account_instance_key: account_key("account-a"),
                global_request_id: request_id.to_owned(),
                batch_item_ordinal,
            },
            path: failure_path,
        }
    }

    fn fill_override_v2(
        fill_sequence: u64,
        failure_path: TradeFinalityPathWireV1,
    ) -> TradeFinalityFailureOverrideWireV2 {
        TradeFinalityFailureOverrideWireV2 {
            selector: TradeFinalitySelectorV2::Fill {
                account_instance_key: account_key("account-a"),
                fill_sequence,
            },
            path: failure_path,
        }
    }

    fn batch_item_override(
        item_idempotency_identity: &str,
        failure_path: TradeFinalityPathWireV1,
    ) -> TradeFinalityFailureOverrideWireV1 {
        TradeFinalityFailureOverrideWireV1 {
            selector: TradeFinalitySelectorWireV1::BatchItem {
                account_instance_key: account_key("account-a"),
                item_idempotency_identity: item_idempotency_identity.to_owned(),
            },
            path: failure_path,
        }
    }

    fn fill_override(
        fill_economic_delta_identity: &str,
        failure_path: TradeFinalityPathWireV1,
    ) -> TradeFinalityFailureOverrideWireV1 {
        TradeFinalityFailureOverrideWireV1 {
            selector: TradeFinalitySelectorWireV1::Fill {
                account_instance_key: account_key("account-a"),
                fill_economic_delta_identity: fill_economic_delta_identity.to_owned(),
            },
            path: failure_path,
        }
    }

    fn encode_policy(
        normal_path: TradeFinalityPathWireV1,
        failure_overrides: Vec<TradeFinalityFailureOverrideWireV1>,
    ) -> Vec<u8> {
        let mut wire = TradeFinalityPolicyWireV1 {
            schema_version: TRADE_FINALITY_POLICY_SCHEMA_V1.to_owned(),
            policy_id: "policy-a".to_owned(),
            content_sha256: Sha256Hex("0".repeat(64)),
            normal_path,
            failure_overrides,
        };
        let content = TradeFinalityPolicyHashContentV1 {
            schema_version: &wire.schema_version,
            policy_id: &wire.policy_id,
            normal_path: &wire.normal_path,
            failure_overrides: &wire.failure_overrides,
        };
        let canonical = serde_json::to_vec(&content).expect("canonical policy content");
        wire.content_sha256 = hash_canonical_content(&canonical);
        serde_json::to_vec(&wire).expect("canonical policy wire")
    }

    fn encode_policy_v2(
        normal_path: TradeFinalityPathWireV1,
        failure_overrides: Vec<TradeFinalityFailureOverrideWireV2>,
    ) -> Vec<u8> {
        serde_json::to_vec(&TradeFinalityPolicyWireV2 {
            schema_version: TRADE_FINALITY_POLICY_SCHEMA_V2.to_owned(),
            normal_path,
            failure_overrides,
        })
        .expect("V2 policy wire")
    }

    fn normal_success() -> TradeFinalityPathWireV1 {
        path(
            &[
                TradeFinalityStateV1::Matched,
                TradeFinalityStateV1::Mined,
                TradeFinalityStateV1::Confirmed,
            ],
            Some(1),
        )
    }

    #[test]
    fn decodes_both_normal_success_templates_and_all_failure_templates() {
        use TradeFinalityStateV1::{
            Confirmed, Failed, Matched, MatchedNotBroadcasted, Mined, Retrying,
        };
        let successful = [
            vec![Matched, Mined, Confirmed],
            vec![MatchedNotBroadcasted, Matched, Retrying, Mined, Confirmed],
        ];
        let failures = [
            vec![MatchedNotBroadcasted, Failed],
            vec![Matched, Failed],
            vec![Matched, Mined, Failed],
            vec![Matched, Retrying, Failed],
        ];

        for normal in successful {
            for (index, failure) in failures.iter().enumerate() {
                let failure_application = failure
                    .iter()
                    .position(|state| *state == Matched)
                    .and_then(|ordinal| u32::try_from(ordinal).ok());
                let bytes = encode_policy(
                    path(&normal, Some(1)),
                    vec![request_override(
                        &format!("request-{index}"),
                        path(failure, failure_application),
                    )],
                );
                let policy = TradeFinalityPolicyV1::decode_and_seal(&bytes)
                    .expect("allowed finality templates");
                assert_eq!(policy.failure_overrides().len(), 1);
            }
        }
    }

    #[test]
    fn json_layout_is_not_semantic_but_unknown_fields_and_digest_conflicts_fail() {
        let canonical = encode_policy(normal_success(), Vec::new());
        let compact =
            TradeFinalityPolicyV1::decode_and_seal(&canonical).expect("compact policy JSON");
        let value: serde_json::Value = serde_json::from_slice(&canonical).expect("policy JSON");
        let pretty = serde_json::to_vec_pretty(&value).expect("pretty policy JSON");
        let pretty =
            TradeFinalityPolicyV1::decode_and_seal(&pretty).expect("JSON layout is not semantic");
        assert_eq!(pretty.content_sha256(), compact.content_sha256());
        assert_eq!(
            pretty.canonical_content_json(),
            compact.canonical_content_json()
        );

        let mut unknown: serde_json::Value =
            serde_json::from_slice(&canonical).expect("policy JSON");
        unknown["override_selector_domain"] = serde_json::json!("REQUEST");
        let unknown = serde_json::to_vec(&unknown).expect("unknown field JSON");
        assert!(matches!(
            TradeFinalityPolicyV1::decode_and_seal(&unknown),
            Err(TradeFinalityPolicyErrorV1::Json(_))
        ));

        let mut digest_conflict: TradeFinalityPolicyWireV1 =
            serde_json::from_slice(&canonical).expect("policy wire");
        digest_conflict.content_sha256 = Sha256Hex("f".repeat(64));
        let digest_conflict = serde_json::to_vec(&digest_conflict).expect("conflicting digest");
        assert!(matches!(
            TradeFinalityPolicyV1::decode_and_seal(&digest_conflict),
            Err(TradeFinalityPolicyErrorV1::ContentSha256Mismatch { .. })
        ));

        let mut empty_id: TradeFinalityPolicyWireV1 =
            serde_json::from_slice(&canonical).expect("policy wire");
        empty_id.policy_id.clear();
        let empty_id = serde_json::to_vec(&empty_id).expect("empty policy ID JSON");
        assert_eq!(
            TradeFinalityPolicyV1::decode_and_seal(&empty_id),
            Err(TradeFinalityPolicyErrorV1::InvalidPolicyId)
        );

        let mut missing_field: serde_json::Value =
            serde_json::from_slice(&canonical).expect("policy JSON");
        missing_field
            .as_object_mut()
            .expect("policy object")
            .remove("normal_path");
        let missing_field = serde_json::to_vec(&missing_field).expect("missing field JSON");
        assert!(matches!(
            TradeFinalityPolicyV1::decode_and_seal(&missing_field),
            Err(TradeFinalityPolicyErrorV1::Json(_))
        ));
    }

    #[test]
    fn v2_uses_exact_file_sha_without_declared_or_canonical_content_identity() {
        let compact = br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10},{"state":"CONFIRMED","offset_ms":20}],"economic_application_ordinal":1},"failure_overrides":[]}"#;
        let compact_policy =
            TradeFinalityPolicyV2::decode_and_seal(compact).expect("compact V2 policy");
        assert_eq!(
            compact_policy.policy_id().0,
            "65295a7a83cb2aa5ed612cd48d84675e9798b2da2f5037f4466e2bc5c2159b62"
        );

        let value: serde_json::Value = serde_json::from_slice(compact).expect("V2 policy JSON");
        let object = value.as_object().expect("V2 policy object");
        assert_eq!(object.len(), 3);
        assert!(!object.contains_key("policy_id"));
        assert!(!object.contains_key("content_sha256"));
        let pretty = serde_json::to_vec_pretty(&value).expect("pretty V2 policy JSON");
        let pretty_policy =
            TradeFinalityPolicyV2::decode_and_seal(&pretty).expect("pretty V2 policy");
        assert_eq!(pretty_policy.normal_path(), compact_policy.normal_path());
        assert_ne!(pretty_policy.policy_id(), compact_policy.policy_id());
    }

    #[test]
    fn v2_reuses_business_validation_and_structured_selector_uniqueness() {
        let failure_path = path(
            &[TradeFinalityStateV1::Matched, TradeFinalityStateV1::Failed],
            None,
        );
        let duplicate = encode_policy_v2(
            normal_success(),
            vec![
                request_override_v2("request-a", failure_path.clone()),
                request_override_v2("request-a", failure_path),
            ],
        );
        assert!(matches!(
            TradeFinalityPolicyV2::decode_and_seal(&duplicate),
            Err(TradeFinalityPolicyErrorV1::DuplicateSelector { .. })
        ));

        let mut decreasing = normal_success();
        decreasing.steps[2].offset_ms = decreasing.steps[1].offset_ms;
        let decreasing = encode_policy_v2(decreasing, Vec::new());
        assert!(matches!(
            TradeFinalityPolicyV2::decode_and_seal(&decreasing),
            Err(TradeFinalityPolicyErrorV1::OffsetNotStrictlyIncreasing { .. })
        ));

        let failure_path = path(
            &[TradeFinalityStateV1::Matched, TradeFinalityStateV1::Failed],
            None,
        );
        let request_bytes = encode_policy_v2(
            normal_success(),
            vec![request_override_v2("request-a", failure_path.clone())],
        );
        let request_policy =
            TradeFinalityPolicyV2::decode_and_seal(&request_bytes).expect("V2 request policy");
        let matching_account = account_key("account-a");
        assert_eq!(
            request_policy.select_path_for_fill(&matching_account, "request-a", 0, 0),
            request_policy.failure_overrides()[0].path()
        );
        let mut other_account = matching_account.clone();
        other_account.caller_instance_id = "trainer-b".to_owned();
        assert_eq!(
            request_policy.select_path_for_fill(&other_account, "request-a", 0, 0),
            request_policy.normal_path()
        );
        assert_eq!(
            request_policy.select_path_for_fill(&matching_account, "request-b", 0, 0),
            request_policy.normal_path()
        );

        let batch_bytes = encode_policy_v2(
            normal_success(),
            vec![batch_item_override_v2("request-a", 2, failure_path.clone())],
        );
        let batch_json = std::str::from_utf8(&batch_bytes).expect("V2 batch policy UTF-8");
        assert!(!batch_json.contains("item_idempotency_identity"));
        let batch_policy =
            TradeFinalityPolicyV2::decode_and_seal(&batch_bytes).expect("V2 batch policy");
        assert_eq!(
            batch_policy.select_path_for_fill(&account_key("account-a"), "request-a", 2, 99),
            batch_policy.failure_overrides()[0].path()
        );
        assert_eq!(
            batch_policy.select_path_for_fill(&account_key("account-a"), "request-a", 3, 99),
            batch_policy.normal_path()
        );
        assert_eq!(
            batch_policy.select_path_for_fill(&account_key("account-a"), "request-b", 2, 99),
            batch_policy.normal_path()
        );

        let fill_bytes =
            encode_policy_v2(normal_success(), vec![fill_override_v2(7, failure_path)]);
        let fill_json = std::str::from_utf8(&fill_bytes).expect("V2 fill policy UTF-8");
        assert!(!fill_json.contains("fill_economic_delta_identity"));
        let fill_policy =
            TradeFinalityPolicyV2::decode_and_seal(&fill_bytes).expect("V2 fill policy");
        assert_eq!(
            fill_policy.select_path_for_fill(&account_key("account-a"), "other", 0, 7),
            fill_policy.failure_overrides()[0].path()
        );
        assert_eq!(
            fill_policy.select_path_for_fill(&account_key("account-a"), "other", 0, 8),
            fill_policy.normal_path()
        );

        let mixed = encode_policy_v2(
            normal_success(),
            vec![
                request_override_v2(
                    "request-a",
                    path(
                        &[TradeFinalityStateV1::Matched, TradeFinalityStateV1::Failed],
                        None,
                    ),
                ),
                fill_override_v2(
                    7,
                    path(
                        &[TradeFinalityStateV1::Matched, TradeFinalityStateV1::Failed],
                        None,
                    ),
                ),
            ],
        );
        assert!(matches!(
            TradeFinalityPolicyV2::decode_and_seal(&mixed),
            Err(TradeFinalityPolicyErrorV1::OverrideDomainMismatch { .. })
        ));
    }

    #[test]
    fn policy_id_is_bounded_by_utf8_bytes_before_sealing() {
        let encoded = encode_policy(normal_success(), Vec::new());
        let mut wire: TradeFinalityPolicyWireV1 =
            serde_json::from_slice(&encoded).expect("policy wire");

        wire.policy_id = "a".repeat(256);
        let content = TradeFinalityPolicyHashContentV1 {
            schema_version: &wire.schema_version,
            policy_id: &wire.policy_id,
            normal_path: &wire.normal_path,
            failure_overrides: &wire.failure_overrides,
        };
        wire.content_sha256 =
            hash_canonical_content(&serde_json::to_vec(&content).expect("256-byte policy content"));
        let at_limit = serde_json::to_vec(&wire).expect("256-byte policy wire");
        TradeFinalityPolicyV1::decode_and_seal(&at_limit)
            .expect("恰好达到 256 个 UTF-8 字节的 policy_id 应被接受");

        wire.policy_id.push('a');
        assert_eq!(wire.policy_id.len(), 257);
        let content = TradeFinalityPolicyHashContentV1 {
            schema_version: &wire.schema_version,
            policy_id: &wire.policy_id,
            normal_path: &wire.normal_path,
            failure_overrides: &wire.failure_overrides,
        };
        wire.content_sha256 =
            hash_canonical_content(&serde_json::to_vec(&content).expect("257-byte policy content"));
        let over_limit = serde_json::to_vec(&wire).expect("257-byte policy wire");
        assert_eq!(
            TradeFinalityPolicyV1::decode_and_seal(&over_limit),
            Err(TradeFinalityPolicyErrorV1::InvalidPolicyId)
        );
    }

    #[test]
    fn rejects_path_offset_and_economic_checkpoint_violations() {
        let bytes = encode_policy(
            path(
                &[
                    TradeFinalityStateV1::Matched,
                    TradeFinalityStateV1::Confirmed,
                ],
                Some(0),
            ),
            Vec::new(),
        );
        assert_eq!(
            TradeFinalityPolicyV1::decode_and_seal(&bytes),
            Err(TradeFinalityPolicyErrorV1::NormalPathNotSuccessfulTemplate)
        );

        let mut negative = normal_success();
        negative.steps[0].offset_ms = -1;
        let bytes = encode_policy(negative, Vec::new());
        assert!(matches!(
            TradeFinalityPolicyV1::decode_and_seal(&bytes),
            Err(TradeFinalityPolicyErrorV1::NegativeOffset { ordinal: 0, .. })
        ));

        let mut first_offset = normal_success();
        first_offset.steps[0].offset_ms = 1;
        let bytes = encode_policy(first_offset, Vec::new());
        assert!(matches!(
            TradeFinalityPolicyV1::decode_and_seal(&bytes),
            Err(TradeFinalityPolicyErrorV1::FirstOffsetNotZero { .. })
        ));

        let mut decreasing = normal_success();
        decreasing.steps[2].offset_ms = decreasing.steps[1].offset_ms;
        let bytes = encode_policy(decreasing, Vec::new());
        assert!(matches!(
            TradeFinalityPolicyV1::decode_and_seal(&bytes),
            Err(TradeFinalityPolicyErrorV1::OffsetNotStrictlyIncreasing { .. })
        ));

        let bytes = encode_policy(
            path(
                &[
                    TradeFinalityStateV1::Matched,
                    TradeFinalityStateV1::Mined,
                    TradeFinalityStateV1::Confirmed,
                ],
                Some(3),
            ),
            Vec::new(),
        );
        assert!(matches!(
            TradeFinalityPolicyV1::decode_and_seal(&bytes),
            Err(TradeFinalityPolicyErrorV1::EconomicApplicationOrdinalOutOfRange { .. })
        ));

        let bytes = encode_policy(
            normal_success(),
            vec![request_override(
                "request-a",
                path(
                    &[TradeFinalityStateV1::Matched, TradeFinalityStateV1::Failed],
                    Some(1),
                ),
            )],
        );
        assert!(matches!(
            TradeFinalityPolicyV1::decode_and_seal(&bytes),
            Err(
                TradeFinalityPolicyErrorV1::IllegalEconomicApplicationState {
                    state: TradeFinalityStateV1::Failed,
                    ..
                }
            )
        ));
    }

    #[test]
    fn rejects_mixed_and_duplicate_selector_domains() {
        let failure_path = path(
            &[TradeFinalityStateV1::Matched, TradeFinalityStateV1::Failed],
            None,
        );
        let mixed = encode_policy(
            normal_success(),
            vec![
                request_override("request-a", failure_path.clone()),
                TradeFinalityFailureOverrideWireV1 {
                    selector: TradeFinalitySelectorWireV1::Fill {
                        account_instance_key: account_key("account-a"),
                        fill_economic_delta_identity: "fill-a".to_owned(),
                    },
                    path: failure_path.clone(),
                },
            ],
        );
        assert!(matches!(
            TradeFinalityPolicyV1::decode_and_seal(&mixed),
            Err(TradeFinalityPolicyErrorV1::OverrideDomainMismatch { .. })
        ));

        let duplicate = encode_policy(
            normal_success(),
            vec![
                request_override("request-a", failure_path.clone()),
                request_override("request-a", failure_path),
            ],
        );
        assert!(matches!(
            TradeFinalityPolicyV1::decode_and_seal(&duplicate),
            Err(TradeFinalityPolicyErrorV1::DuplicateSelector { .. })
        ));
    }

    #[test]
    fn account_qualified_selectors_with_same_local_identity_remain_distinct() {
        let failure_path = path(
            &[TradeFinalityStateV1::Matched, TradeFinalityStateV1::Failed],
            None,
        );
        let mut second = request_override("request-a", failure_path.clone());
        let TradeFinalitySelectorWireV1::Request {
            account_instance_key,
            ..
        } = &mut second.selector
        else {
            unreachable!("request selector")
        };
        account_instance_key.account_id = "account-b".to_owned();
        let bytes = encode_policy(
            normal_success(),
            vec![request_override("request-a", failure_path), second],
        );
        let policy = TradeFinalityPolicyV1::decode_and_seal(&bytes)
            .expect("complete account key separates selectors");
        assert_eq!(policy.failure_overrides().len(), 2);
    }

    #[test]
    fn request_path_selection_requires_the_complete_account_key_and_allows_no_match() {
        let failure_path = path(
            &[TradeFinalityStateV1::Matched, TradeFinalityStateV1::Failed],
            None,
        );
        let bytes = encode_policy(
            normal_success(),
            vec![request_override("request-a", failure_path)],
        );
        let policy = TradeFinalityPolicyV1::decode_and_seal(&bytes).expect("request policy");
        let matching_account = account_key("account-a");
        let selected = policy.select_path_for_fill(
            &matching_account,
            "request-a",
            "unmatched-item",
            "unmatched-fill",
        );
        assert_eq!(selected, policy.failure_overrides()[0].path());

        let mut other_account = matching_account.clone();
        other_account.caller_instance_id = "trainer-b".to_owned();
        for (account, request_id) in [
            (&other_account, "request-a"),
            (&matching_account, "request-b"),
        ] {
            let selected = policy.select_path_for_fill(
                account,
                request_id,
                "unmatched-item",
                "unmatched-fill",
            );
            assert_eq!(selected, policy.normal_path());
        }
    }

    #[test]
    fn batch_item_path_selection_requires_the_complete_account_key_and_allows_no_match() {
        let failure_path = path(
            &[TradeFinalityStateV1::Matched, TradeFinalityStateV1::Failed],
            None,
        );
        let bytes = encode_policy(
            normal_success(),
            vec![batch_item_override("item-a", failure_path)],
        );
        let policy = TradeFinalityPolicyV1::decode_and_seal(&bytes).expect("batch item policy");
        let matching_account = account_key("account-a");
        let selected = policy.select_path_for_fill(
            &matching_account,
            "unmatched-request",
            "item-a",
            "unmatched-fill",
        );
        assert_eq!(selected, policy.failure_overrides()[0].path());

        let mut other_account = matching_account.clone();
        other_account.caller_instance_id = "trainer-b".to_owned();
        for (account, item_identity) in [(&other_account, "item-a"), (&matching_account, "item-b")]
        {
            let selected = policy.select_path_for_fill(
                account,
                "unmatched-request",
                item_identity,
                "unmatched-fill",
            );
            assert_eq!(selected, policy.normal_path());
        }
    }

    #[test]
    fn fill_path_selection_requires_the_complete_account_key_and_allows_no_match() {
        let failure_path = path(
            &[TradeFinalityStateV1::Matched, TradeFinalityStateV1::Failed],
            None,
        );
        let bytes = encode_policy(
            normal_success(),
            vec![fill_override("fill-a", failure_path)],
        );
        let policy = TradeFinalityPolicyV1::decode_and_seal(&bytes).expect("fill policy");
        let matching_account = account_key("account-a");
        let selected = policy.select_path_for_fill(
            &matching_account,
            "unmatched-request",
            "unmatched-item",
            "fill-a",
        );
        assert_eq!(selected, policy.failure_overrides()[0].path());

        let mut other_account = matching_account.clone();
        other_account.caller_instance_id = "trainer-b".to_owned();
        for (account, fill_identity) in [(&other_account, "fill-a"), (&matching_account, "fill-b")]
        {
            let selected = policy.select_path_for_fill(
                account,
                "unmatched-request",
                "unmatched-item",
                fill_identity,
            );
            assert_eq!(selected, policy.normal_path());
        }
    }
}
