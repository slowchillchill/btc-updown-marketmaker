//! 根时间资格唯一归属。公共生命周期状态不是模型资格，隐藏值由调用方持有。
use contracts::*;
use serde::Serialize;
use shared_run::{ActionBeforeReadV2, SharedExecutionV2};
use sim_system::{
    execution::PublicConsumptionStateV1, public_consumption::TrustedGroupDecisionCutoffV1,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HiddenKey {
    pub trajectory_id: String,
    pub account_id: AccountInstanceKeyV1,
    pub condition_id: ConditionId,
}
#[derive(Clone, Debug, Serialize)]
pub enum HiddenOp {
    ResetRootHidden {
        trajectory_id: String,
        account_id: AccountInstanceKeyV1,
    },
    InitializeZero {
        hidden_state_key: HiddenKey,
    },
    Retire {
        hidden_state_key: HiddenKey,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum InputRole {
    ContextOnly,
    DecisionRequired,
}
#[derive(Serialize)]
pub struct AccountInputRow {
    pub hidden_state_key: HiddenKey,
    pub role: InputRole,
    pub slow: Vec<f32>,
    pub fast: Vec<f32>,
    pub rule: [f32; 30],
    #[serde(skip)]
    pub action_context: Option<NativeActionContext>,
}
/// 同帧规则、tick 与精确前缀共用；账户冻结读取仍由 NativeInputGroup 持有。
pub struct NativeActionContext {
    pub snapshot: std::sync::Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
    pub(crate) buy_enabled: bool,
    pub rules: std::sync::Arc<PublicTradingRuleContextV1>,
    pub(crate) ticks: [Option<contracts::economics::EconomicRational>; 2],
    pub(crate) tokens: [Option<TokenId>; 2],
}
impl NativeActionContext {
    pub(crate) fn from_observation(
        observation: &MarketBusinessObservationV1,
        rules: std::sync::Arc<PublicTradingRuleContextV1>,
        snapshot: std::sync::Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
    ) -> Self {
        let ticks = std::array::from_fn(|i| {
            observation
                .assets
                .iter()
                .find(|a| a.asset.outcome.index as usize == i)
                .filter(|a| {
                    matches!(
                        a.tick.quality,
                        MarketDataQuality::Fresh | MarketDataQuality::Stale
                    )
                })
                .and_then(|a| a.tick.value.as_ref())
                .map(crate::exact::decimal)
        });
        let tokens = std::array::from_fn(|i| {
            observation
                .assets
                .iter()
                .find(|a| a.asset.outcome.index as usize == i)
                .map(|a| a.asset.token_id.clone())
        });
        Self {
            snapshot,
            buy_enabled: observation.active.value != Some(false)
                && observation.closed.value != Some(true)
                && observation.accepting_orders.value != Some(false)
                && observation.terminal.value.is_none(),
            rules,
            ticks,
            tokens,
        }
    }
    pub fn prefix_features(&self, state: &crate::prefix::PrefixState) -> [f32; 60] {
        state.encode(&self.ticks)
    }
}
/// 原生完整特征输入；本增量不实现完整 Actor 合法选择链。
#[derive(Serialize)]
pub struct NativeInputGroup {
    pub cutoff: TrustedGroupDecisionCutoffV1,
    pub pre_frame_hidden_ops: Vec<HiddenOp>,
    pub rows: Vec<AccountInputRow>,
    pub post_frame_hidden_ops: Vec<HiddenOp>,
    pub current: Option<ConditionId>,
    pub old: Vec<ConditionId>,
    pub cancel: Vec<Vec<f32>>,
    #[serde(skip)]
    pub action_before: Option<ActionBeforeReadV2>,
}
pub(crate) struct MarketBinding {
    pub market: MarketIdentityV2,
    pub assets: Vec<AssetIdentityV1>,
}
pub struct NativeInputRoot {
    snapshot: std::sync::Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
    trajectory: String,
    account: AccountInstanceKeyV1,
    started_at: i64,
    reset: bool,
    markets: BTreeMap<String, MarketBinding>,
    hidden: BTreeSet<String>,
    histories: BTreeMap<String, crate::market::MarketHistory>,
}
impl NativeInputRoot {
    pub fn new(
        trajectory_id: String,
        account_id: AccountInstanceKeyV1,
        root_started_at_utc_ms: i64,
        snapshot: std::sync::Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
    ) -> Self {
        Self {
            snapshot,
            trajectory: trajectory_id,
            account: account_id,
            started_at: root_started_at_utc_ms,
            reset: false,
            markets: BTreeMap::new(),
            hidden: BTreeSet::new(),
            histories: BTreeMap::new(),
        }
    }
    fn key(&self, c: &str) -> HiddenKey {
        HiddenKey {
            trajectory_id: self.trajectory.clone(),
            account_id: self.account.clone(),
            condition_id: ConditionId(c.into()),
        }
    }
    pub fn consume(
        &mut self,
        shared: &mut SharedExecutionV2,
        driver: &mut PublicConsumptionStateV1,
    ) -> Result<Option<NativeInputGroup>, crate::InputError> {
        // 由原共享组合完成唯一应用；随后重取真实 decision 和公共帧。
        let before = shared.action_before(driver)?;
        let Some(decision) = driver.decision() else {
            return Ok(None);
        };
        let cutoff = decision.cutoff().clone();
        let frames = decision.frames();
        if !frames.is_empty() && before.is_none() {
            return Ok(None);
        }
        let now = cutoff.decision_time().logical_time_utc_ms();
        for f in &frames {
            self.markets
                .entry(f.observation.market.condition_id.0.clone())
                .or_insert_with(|| MarketBinding {
                    market: f.observation.market.clone(),
                    assets: f
                        .observation
                        .assets
                        .iter()
                        .map(|a| a.asset.clone())
                        .collect(),
                });
        }
        let eligible = |m: &MarketBinding| {
            i128::from(self.started_at) <= i128::from(m.market.window.start_at_utc_ms) - 30_000
        };
        let current = self
            .markets
            .values()
            .find(|m| {
                eligible(m)
                    && m.market.window.start_at_utc_ms <= now
                    && now < m.market.window.end_at_utc_ms
            })
            .map(|m| m.market.condition_id.clone());
        // 完整账户投影包括仅本地预留的市场；经济成交表不是市场人口权威。
        let without_frame;
        let ledger = if let Some(read) = &before {
            read.ledger()
        } else {
            without_frame = shared
                .ledger()
                .projection_v2()
                .ok_or(crate::InputError::MissingAssociation)?;
            &without_frame
        };
        let old: Vec<_> = ledger
            .market_projections
            .iter()
            .filter_map(|m| {
                self.markets
                    .get(&m.condition_id.0)
                    .filter(|b| eligible(b) && b.market.window.end_at_utc_ms <= now)
                    .map(|_| m.condition_id.clone())
            })
            .collect();
        let mut pre = Vec::new();
        if !self.reset {
            pre.push(HiddenOp::ResetRootHidden {
                trajectory_id: self.trajectory.clone(),
                account_id: self.account.clone(),
            });
        }
        for (id, m) in &self.markets {
            if eligible(m)
                && i128::from(now) >= i128::from(m.market.window.start_at_utc_ms) - 30_000
                && now < m.market.window.end_at_utc_ms
                && !self.hidden.contains(id)
            {
                pre.push(HiddenOp::InitializeZero {
                    hidden_state_key: self.key(id),
                });
            }
        }
        let mut roles: Vec<_> = frames
            .iter()
            .filter_map(|f| {
                let m = &self.markets[&f.observation.market.condition_id.0];
                if !eligible(m) || now >= m.market.window.end_at_utc_ms {
                    return None;
                }
                if now >= m.market.window.start_at_utc_ms {
                    Some((InputRole::DecisionRequired, m.market.condition_id.clone()))
                } else if i128::from(now) >= i128::from(m.market.window.start_at_utc_ms) - 30_000 {
                    Some((InputRole::ContextOnly, m.market.condition_id.clone()))
                } else {
                    None
                }
            })
            .collect();
        roles.sort_by(|a, b| {
            (a.0 == InputRole::DecisionRequired, &a.1.0)
                .cmp(&(b.0 == InputRole::DecisionRequired, &b.1.0))
        });
        let mut rows = Vec::new();
        let mut cancel = Vec::new();
        if let Some(read) = &before {
            for (role, id) in roles {
                let frame = frames
                    .iter()
                    .find(|f| f.observation.market.condition_id == id)
                    .unwrap();
                let rule = crate::market::rules(frame.observation, &frame.rule_context, now);
                rows.push(AccountInputRow {
                    hidden_state_key: self.key(&id.0),
                    role,
                    fast: Vec::new(),
                    rule,
                    action_context: (role == InputRole::DecisionRequired).then(|| {
                        NativeActionContext::from_observation(
                            frame.observation,
                            frame.rule_context.clone(),
                            self.snapshot.clone(),
                        )
                    }),
                    slow: crate::account::encode(
                        read,
                        current.as_ref(),
                        &old,
                        &self.markets,
                        now,
                        role,
                    )?
                    .to_vec(),
                });
                if role == InputRole::DecisionRequired {
                    cancel =
                        crate::cancel::encode(read, current.as_ref(), &old, &self.markets, now)?;
                }
            }
        }
        for row in &mut rows {
            let id = &row.hidden_state_key.condition_id;
            let frame = frames
                .iter()
                .find(|f| &f.observation.market.condition_id == id)
                .unwrap();
            row.fast = crate::market::encode(
                frame.observation,
                &row.rule,
                now,
                self.histories.entry(id.0.clone()).or_default(),
            )
            .to_vec();
        }
        self.reset = true;
        for op in &pre {
            if let HiddenOp::InitializeZero { hidden_state_key } = op {
                self.hidden.insert(hidden_state_key.condition_id.0.clone());
            }
        }
        let retiring: Vec<_> = self
            .hidden
            .iter()
            .filter(|id| self.markets[*id].market.window.end_at_utc_ms <= now)
            .cloned()
            .collect();
        let mut post = Vec::new();
        for id in retiring {
            self.hidden.remove(&id);
            self.histories.remove(&id);
            post.push(HiddenOp::Retire {
                hidden_state_key: self.key(&id),
            });
        }
        Ok(Some(NativeInputGroup {
            cutoff,
            pre_frame_hidden_ops: pre,
            rows,
            post_frame_hidden_ops: post,
            current,
            old,
            cancel,
            action_before: before,
        }))
    }
}
