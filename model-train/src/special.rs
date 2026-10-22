//! D-0375学习信用：只读取公共证据，不写经济账本或Actor输入。
use contracts::{economics::EconomicRational as R, *};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use shared_run::DirectedConfirmationsReadV1;
use std::collections::BTreeMap;

pub const TARGET: &str = "confirmed-finite-j-special-v1";
pub const TRAJECTORY: &str = "learning-trajectory-special-v1";
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecialPolicyV1 {
    pub version: String,
    pub alpha: u8,
    pub frame_age_threshold_ms: i64,
    pub safe_notrade_credit_pusd: ExactDecimal,
    pub repair_credit_per_share_pusd: ExactDecimal,
    pub credit_bound_pusd: ExactDecimal,
}
impl SpecialPolicyV1 {
    pub fn adopted(alpha: u8) -> Self {
        Self {
            version: "D-0375-special-v1".into(),
            alpha,
            frame_age_threshold_ms: 100,
            safe_notrade_credit_pusd: ExactDecimal("0.01".into()),
            repair_credit_per_share_pusd: ExactDecimal("0.01".into()),
            credit_bound_pusd: ExactDecimal("0.10".into()),
        }
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.alpha > 1 || self.version != "D-0375-special-v1" {
            return Err("不支持的专项公式版本或alpha".into());
        }
        rational(&self.safe_notrade_credit_pusd)?;
        rational(&self.repair_credit_per_share_pusd)?;
        if rational(&self.credit_bound_pusd)? < R::zero() {
            return Err("专项裁剪边界B不得为负".into());
        }
        Ok(())
    }
}
fn rational(v: &ExactDecimal) -> Result<R, String> {
    Ok(R::from_decimal(
        Decimal::from_str_exact(&v.0).map_err(|e| e.to_string())?,
    ))
}
fn abs(v: &R) -> R {
    if v < &R::zero() {
        R::zero().sub(v)
    } else {
        v.clone()
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SpecialStateV1 {
    pub safe_notrade_accepted_call: Option<u64>,
    /// 只有原dispatch回执真正发送的专项Repair请求才进入该集合。
    pub repair_requests: BTreeMap<String, SpecialRepairOriginV1>,
    pub contributions: BTreeMap<String, ExactRationalV1>,
    pub necessary_unknown: Vec<String>,
    #[serde(default)]
    pub accepted_safety_unknown_calls: Vec<u64>,
    #[serde(skip)]
    last_confirmations: BTreeMap<
        ::execution_normalization::MemoryLogicalIdentityV1,
        (std::sync::Arc<ledger::TradeConfirmationReadV1>, String),
    >,
    #[serde(skip)]
    cached_sum: std::cell::RefCell<Option<R>>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpecialRepairOriginV1 {
    pub call: u64,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub sent_receipt: sim_system::execution::MutationRequestReceiptV2,
}
impl SpecialStateV1 {
    pub fn update(&mut self, facts: &DirectedConfirmationsReadV1) -> Result<(), String> {
        let mut seen = std::collections::BTreeSet::new();
        let mut unknown = Vec::new();
        for (request, origin) in &self.repair_requests {
            let Some(Some(trades)) = facts.requests.get(request) else {
                unknown.push(request.clone());
                continue;
            };
            for t in trades {
                // Registry收件尚未合法应用不是成功确认；不能在收件时提前授信或冻结锚。
                let Some(applied) = &t.applied else {
                    continue;
                };
                if applied.condition_id != origin.condition_id
                    || applied.token_id != origin.token_id
                {
                    unknown.push(format!("归属不符:{:?}", t.trade_identity));
                    continue;
                }
                if !seen.insert(t.trade_identity.clone()) {
                    unknown.push(format!("重复归属:{:?}", t.trade_identity));
                    continue;
                }
                if self
                    .last_confirmations
                    .get(&t.trade_identity)
                    .is_some_and(|(old, _)| {
                        std::sync::Arc::ptr_eq(old, applied) || **old == **applied
                    })
                {
                    continue;
                }
                let q = rational(&applied.current_final_success_shares)?;
                // 尚无成功确认是零贡献；已成功过却无首锚是必要历史缺口。
                if q.is_zero() {
                    let id = self
                        .last_confirmations
                        .get(&t.trade_identity)
                        .map(|(_, key)| key.clone())
                        .unwrap_or(
                            serde_json::to_string(&t.trade_identity).map_err(|e| e.to_string())?,
                        );
                    let value = R::zero().public();
                    if self.contributions.get(&id) != Some(&value) {
                        self.contributions.insert(id.clone(), value);
                        self.cached_sum.get_mut().take();
                    }
                    self.last_confirmations
                        .insert(t.trade_identity.clone(), (applied.clone(), id));
                    continue;
                }
                let Some(anchor) = &applied.first_success_anchor else {
                    unknown.push(format!("缺首锚:{:?}", t.trade_identity));
                    continue;
                };
                let Some(side) = anchor
                    .assets
                    .iter()
                    .position(|a| a.token_id == applied.token_id)
                else {
                    unknown.push(format!("缺方向:{:?}", t.trade_identity));
                    continue;
                };
                let d = R::try_from(&anchor.net_before_first_success_shares)
                    .map_err(|e| e.to_string())?;
                let signed = if side == 0 { q } else { R::zero().sub(&q) };
                let v = abs(&d).sub(&abs(&d.add(&signed)));
                let id = serde_json::to_string(&t.trade_identity).map_err(|e| e.to_string())?;
                let value = v.public();
                if self.contributions.get(&id) != Some(&value) {
                    self.contributions.insert(id.clone(), value);
                    self.cached_sum.get_mut().take();
                }
                self.last_confirmations
                    .insert(t.trade_identity.clone(), (applied.clone(), id));
            }
        }
        self.necessary_unknown = unknown;
        Ok(())
    }
    #[cfg(test)]
    fn replace_contribution(&mut self, id: String, value: ExactRationalV1) {
        if self.contributions.get(&id) != Some(&value) {
            self.contributions.insert(id, value);
            self.cached_sum.get_mut().take();
        }
    }
    pub fn unknown_reasons(&self) -> Vec<String> {
        self.necessary_unknown
            .iter()
            .cloned()
            .chain(
                self.accepted_safety_unknown_calls
                    .iter()
                    .map(|call| format!("原特殊NoTrade安全证明未知:call{call}")),
            )
            .collect()
    }
    pub fn credit(&self, policy: &SpecialPolicyV1) -> Result<(R, R), String> {
        if !self.necessary_unknown.is_empty() || !self.accepted_safety_unknown_calls.is_empty() {
            return Err("专项必要成交归属/首次确认参照未知，标签待定".into());
        }
        if self.cached_sum.borrow().is_none() {
            let mut sum = R::zero();
            for value in self.contributions.values() {
                sum = sum.add(&R::try_from(value).map_err(|e| e.to_string())?);
            }
            *self.cached_sum.borrow_mut() = Some(sum);
        }
        let sum = self
            .cached_sum
            .borrow()
            .as_ref()
            .expect("本次已计算信用和")
            .clone();
        let mut raw = sum.mul(&rational(&policy.repair_credit_per_share_pusd)?);
        if self.safe_notrade_accepted_call.is_some() {
            raw = raw.add(&rational(&policy.safe_notrade_credit_pusd)?);
        }
        let bound = rational(&policy.credit_bound_pusd)?;
        let clipped = raw.clone().min(bound.clone()).max(R::zero().sub(&bound));
        Ok((raw, clipped))
    }
}
/// R=0只在所有condition净量与每条可达义务都为零时成立。未知不授信，不代选动作。
pub fn safely_flat(facts: &DirectedConfirmationsReadV1) -> Result<Option<bool>, String> {
    let mut unknown = facts.unattributed_active_orders;
    for p in facts.positions.iter() {
        match &p.net_first_asset_shares {
            ExecutionReadValueV1::Known(q) => {
                if !R::try_from(q).map_err(|e| e.to_string())?.is_zero() {
                    return Ok(Some(false));
                }
            }
            _ => unknown = true,
        }
    }
    for c in &facts.commitments {
        if let (
            Some(min),
            BuyOrderIntentV1::GtcLimit { size_shares, .. }
            | BuyOrderIntentV1::GtdLimit { size_shares, .. },
        ) = (&c.minimum_shares, &c.original_order)
            && *size_shares < Decimal::from_str_exact(&min.0).map_err(|e| e.to_string())?
        {
            continue;
        }
        match &c.remaining_target_shares {
            ExecutionReadValueV1::Known(q) if rational(q)?.is_zero() => {}
            ExecutionReadValueV1::Known(_) if c.minimum_shares.is_some() => return Ok(Some(false)),
            ExecutionReadValueV1::NotApplicable => {}
            _ => unknown = true,
        }
    }
    Ok(if unknown { None } else { Some(true) })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn r(v: i64) -> ExactRationalV1 {
        R::from_decimal(v.into()).public()
    }
    #[test]
    fn configured_numbers_are_parsed_without_locking_the_adopted_preset() {
        let mut policy = SpecialPolicyV1::adopted(1);
        policy.frame_age_threshold_ms = 150;
        policy.safe_notrade_credit_pusd = ExactDecimal("0.02".into());
        policy.repair_credit_per_share_pusd = ExactDecimal("0.03".into());
        policy.credit_bound_pusd = ExactDecimal("0.20".into());
        policy.validate().unwrap();
        policy.credit_bound_pusd = ExactDecimal("-0.01".into());
        assert!(policy.validate().unwrap_err().contains("不得为负"));
        policy.credit_bound_pusd = ExactDecimal("invalid".into());
        assert!(policy.validate().is_err());
    }
    #[test]
    fn raw_credit_keeps_out_of_bound_history_and_replacements_retract() {
        let policy = SpecialPolicyV1::adopted(1);
        let mut state = SpecialStateV1 {
            safe_notrade_accepted_call: Some(2),
            ..Default::default()
        };
        state.replace_contribution("trade-original".into(), r(200));
        let (raw, c) = state.credit(&policy).unwrap();
        assert_eq!(raw, R::from_decimal(Decimal::new(201, 2)));
        assert_eq!(c, R::from_decimal(Decimal::new(1, 1)));
        state.replace_contribution("trade-original".into(), r(-98));
        let (raw, c) = state.credit(&policy).unwrap();
        assert_eq!(raw, R::from_decimal(Decimal::new(-97, 2)));
        assert_eq!(c, R::from_decimal(Decimal::new(-1, 1)));
        state.replace_contribution("trade-original".into(), r(0));
        assert_eq!(
            state.credit(&policy).unwrap().1,
            R::from_decimal(Decimal::new(1, 2))
        );
    }
    #[test]
    fn anchors_are_per_trade_not_replayed_against_later_ordinary_positions() {
        // 同一原Down2首锚+1：普通Up10后修正其有效量为Down1，仍使用+1计算。
        let contribution = |anchor: i64, delta: i64| {
            abs(&R::from_decimal(anchor.into()))
                .sub(&abs(&R::from_decimal((anchor + delta).into())))
        };
        assert_eq!(contribution(1, -2), R::zero());
        assert_eq!(contribution(1, -1), R::from_decimal(1.into()));
        // 若误用修正当下净仓9→10，则得到-1，与固定首锚的+1相反。
        assert_eq!(contribution(9, 1), R::from_decimal((-1).into()));
        assert_eq!(contribution(1, -100), R::from_decimal((-98).into()));
    }
    #[test]
    fn whole_account_old_condition_or_unknown_prevents_safe_credit_without_blocking_action() {
        let mut facts = DirectedConfirmationsReadV1 {
            positions: Default::default(),
            requests: Default::default(),
            commitments: vec![],
            unattributed_active_orders: false,
        };
        assert_eq!(safely_flat(&facts).unwrap(), Some(true));
        facts.positions = std::sync::Arc::new(vec![ledger::ConfirmedPositionReadV1 {
            condition_id: ConditionId("old-not-in-frame".into()),
            assets: None,
            net_first_asset_shares: ExecutionReadValueV1::Known(r(1)),
        }]);
        assert_eq!(safely_flat(&facts).unwrap(), Some(false));
        std::sync::Arc::make_mut(&mut facts.positions)[0].net_first_asset_shares =
            ExecutionReadValueV1::Unavailable;
        assert_eq!(safely_flat(&facts).unwrap(), None);
    }
}

#[cfg(test)]
mod export_tests {
    #[test]
    fn readonly_export_protocol_does_not_import_torch_or_construct_policy() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let result = std::process::Command::new("python3")
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .arg(root.join("tests/fixtures/frozen-export-check.py"))
            .arg(root.join("python/export_frozen.py"))
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
}

#[cfg(test)]
mod confirmation_tests {
    use super::*;
    use ::execution_normalization::{
        MemoryLogicalIdentityV1, NormalizationIdentity, SourceNeutralEntityKeyV1,
    };
    use contracts::execution_normalization::SourceNeutralAccountIdentityV1;
    use std::sync::Arc;
    fn id(n: &str) -> MemoryLogicalIdentityV1 {
        MemoryLogicalIdentityV1::entity(
            &Arc::new(SourceNeutralAccountIdentityV1 {
                source_category: "sim_v2".into(),
                environment: "synthetic".into(),
                run_scope: "special".into(),
                account_scope: "a".into(),
            }),
            &SourceNeutralEntityKeyV1::Trade {
                source_trade_id: n.into(),
            },
        )
        .unwrap()
    }
    #[test]
    fn original_trade_replacements_compensation_and_duplicate_reads_use_one_anchor() {
        let assets = std::array::from_fn(|i| AssetIdentityV1 {
            token_id: TokenId(if i == 0 { "up" } else { "down" }.into()),
            outcome: OutcomeIdentityV1 {
                index: i as u32,
                label: if i == 0 { "Up" } else { "Down" }.into(),
            },
        });
        let mut state = SpecialStateV1::default();
        let receipt=serde_json::from_value(serde_json::json!({"input_sequence":5,"platform_arrived_at_utc_ms":280,"scheduled_action_count":1})).unwrap();
        for (request, call) in [("old-special", 2), ("next-special", 4)] {
            state.repair_requests.insert(
                request.into(),
                SpecialRepairOriginV1 {
                    call,
                    condition_id: ConditionId("condition".into()),
                    token_id: TokenId("down".into()),
                    sent_receipt: receipt,
                },
            );
        }
        let trade = |name: &str, d: i64, q: i64| shared_run::DirectedTradeReadV1 {
            trade_identity: id(name),
            reported_final_success: q > 0,
            applied: Some(Arc::new(ledger::TradeConfirmationReadV1 {
                trade_identity: id(name),
                condition_id: ConditionId("condition".into()),
                token_id: TokenId("down".into()),
                first_success_seen: Some(true),
                first_success_anchor: Some(ledger::FinalSuccessAnchorV1 {
                    assets: assets.clone(),
                    net_before_first_success_shares: R::from_decimal(d.into()).public(),
                }),
                current_final_success_shares: ExactDecimal(q.to_string()),
            })),
        };
        let mut facts = DirectedConfirmationsReadV1 {
            positions: Arc::new(vec![]),
            commitments: vec![],
            unattributed_active_orders: false,
            requests: [
                ("old-special".into(), Some(vec![trade("e1", 1, 2)])),
                ("next-special".into(), Some(vec![])),
            ]
            .into_iter()
            .collect(),
        };
        let policy = SpecialPolicyV1::adopted(1);
        state.update(&facts).unwrap();
        assert_eq!(state.credit(&policy).unwrap().0, R::zero());
        // 普通Up10只改变当前净仓；同一原trade数量2→1仍用首次+1，不改锚。
        facts
            .requests
            .insert("old-special".into(), Some(vec![trade("e1", 1, 1)]));
        state.update(&facts).unwrap();
        assert_eq!(
            state.credit(&policy).unwrap().0,
            R::from_decimal(Decimal::new(1, 2))
        );
        facts
            .requests
            .insert("old-special".into(), Some(vec![trade("e1", 1, 2)]));
        state.update(&facts).unwrap();
        // 另一个新trade的首锚可以为+9，不能回写原e1的+1。
        facts
            .requests
            .insert("next-special".into(), Some(vec![trade("e2", 9, 1)]));
        state.update(&facts).unwrap();
        assert_eq!(
            state.credit(&policy).unwrap().0,
            R::from_decimal(Decimal::new(1, 2))
        );
        facts
            .requests
            .insert("old-special".into(), Some(vec![trade("e1", 1, 100)]));
        state.update(&facts).unwrap();
        assert_eq!(
            state.credit(&policy).unwrap().0,
            R::from_decimal(Decimal::new(-97, 2))
        );
        let before = serde_json::to_value(&state).unwrap();
        state.update(&facts).unwrap();
        assert_eq!(serde_json::to_value(&state).unwrap(), before);
        facts
            .requests
            .insert("old-special".into(), Some(vec![trade("e1", 1, 0)]));
        state.update(&facts).unwrap();
        assert_eq!(
            state.credit(&policy).unwrap().0,
            R::from_decimal(Decimal::new(1, 2))
        );
        let mut invalid = trade("e1", 1, 1);
        Arc::make_mut(invalid.applied.as_mut().unwrap()).condition_id =
            ConditionId("different".into());
        facts
            .requests
            .insert("old-special".into(), Some(vec![invalid]));
        state.update(&facts).unwrap();
        assert!(state.credit(&policy).is_err());
    }
    #[test]
    fn safety_uses_original_request_mos_and_does_not_erase_legal_small_remainder() {
        let mut facts = DirectedConfirmationsReadV1 {
            positions: Arc::new(vec![]),
            commitments: vec![shared_run::DirectedCommitmentReadV1 {
                condition_id: ConditionId("old-market".into()),
                token_id: TokenId("up".into()),
                original_order: BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share: Decimal::new(4, 1),
                    size_shares: Decimal::new(1, 2),
                    post_only: false,
                },
                minimum_shares: Some(ExactDecimal("5".into())),
                remaining_target_shares: ExecutionReadValueV1::Known(ExactDecimal("0.01".into())),
            }],
            unattributed_active_orders: false,
            requests: Default::default(),
        };
        assert_eq!(safely_flat(&facts).unwrap(), Some(true));
        if let BuyOrderIntentV1::GtcLimit { size_shares, .. } =
            &mut facts.commitments[0].original_order
        {
            *size_shares = Decimal::from(10);
        }
        facts.unattributed_active_orders = true;
        assert_eq!(
            safely_flat(&facts).unwrap(),
            Some(false),
            "已知原10份单的0.01余量已证明非安全，不必等待无关未知"
        );
    }
}
