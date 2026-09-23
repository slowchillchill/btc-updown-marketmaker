use std::collections::BTreeSet;
use std::fmt;

use contracts::{AccountInstanceKeyV1, Sha256Hex};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const TRADE_FINALITY_POLICY_SCHEMA_V2: &str = "trade_finality_policy_v2";

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
    #[cfg(test)]
    pub(crate) fn normal_path(&self) -> &TradeFinalityPathV1 {
        &self.normal_path
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn failure_overrides(&self) -> &[TradeFinalityFailureOverrideV2] {
        &self.failure_overrides
    }

    #[cfg(test)]
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
struct TradeFinalityPolicyWireV2 {
    schema_version: String,
    normal_path: TradeFinalityPathWireV1,
    failure_overrides: Vec<TradeFinalityFailureOverrideWireV2>,
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
struct TradeFinalityFailureOverrideWireV2 {
    selector: TradeFinalitySelectorV2,
    path: TradeFinalityPathWireV1,
}

struct ValidatedTradeFinalityPolicyV2 {
    normal_path: TradeFinalityPathV1,
    override_selector_domain: Option<TradeFinalitySelectorDomainV1>,
    failure_overrides: Vec<TradeFinalityFailureOverrideV2>,
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
}
