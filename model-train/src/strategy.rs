use crate::{domain::*, encoding::*, worker::ModelWorker};
use contracts::{economics::EconomicRational as R, *};
use ledger::*;
use num_bigint::BigUint;
use num_traits::{One, Zero};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use shared_run::*;
use sim_system::{execution::RequestContextV2, platform_rules::SealedProcessRuleSnapshotV3};
use std::{
    cell::RefCell,
    io::{BufWriter, Write},
    sync::Arc,
    time::Instant,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerConfig {
    pub account_key: String,
    pub sampling_seed: u64,
    pub request_namespace: String,
    pub progress_every: u64,
    #[serde(default)]
    pub learning: Option<crate::learning::CollectionConfig>,
}
#[derive(Clone, Debug, Serialize)]
pub struct ModelDiagnostics {
    pub call: u64,
    pub action: String,
    pub joint_log_probability: f64,
    pub heads: u64,
    pub head_trace: Vec<Value>,
    pub forward: Value,
    pub time: Value,
    pub repair_eligible: Vec<[bool; 2]>,
    pub repair_evidence: Vec<Value>,
    pub encoding_seconds: f64,
    pub model_roundtrip_seconds: f64,
    pub exact_domain_and_construction_seconds: f64,
}
pub struct ModelConsumer {
    pub config: ConsumerConfig,
    worker: ModelWorker,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    call: u64,
    entry_scope: backtest::EffectiveEntryMarketScopeV1,
    rpc: RefCell<Vec<Value>>,
    trajectory: Option<BufWriter<std::fs::File>>,
    last_confirmed: Option<Arc<ledger::ConfirmedEconomicsReadV1>>,
    confirmed_revision: u64,
    last_qualified: Vec<shared_run::QualifiedRebateReadV1>,
}
#[derive(Clone)]
struct MarketDomain {
    ticks: [ExactDecimal; 2],
    max_prices: [u16; 2],
    eligible: [bool; 2],
    pair: bool,
}
fn decimal(v: &ExactDecimal) -> Result<Decimal, String> {
    Decimal::from_str_exact(&v.0).map_err(|e| e.to_string())
}
fn original_leg(intent: &ModelIntentV1, ordinal: u16) -> Option<(&ConditionId, &BuyLegIntentV1)> {
    match intent {
        ModelIntentV1::SubmitSingleBuy {
            condition_id,
            order,
        } if ordinal == 0 => Some((condition_id, order)),
        ModelIntentV1::SubmitPairedBuy {
            condition_id,
            orders,
        } => orders.get(usize::from(ordinal)).map(|l| (condition_id, l)),
        _ => None,
    }
}
/// 原请求静态可达性；只使用同市场不可变公开MOS，不预测盘口/成交。
fn credible_remaining(
    expected_condition: &ConditionId,
    condition: &ConditionId,
    order: &BuyOrderIntentV1,
    remaining: &ExecutionReadValueV1<ExactDecimal>,
    minimum: Decimal,
) -> Result<ExecutionReadValueV1<R>, String> {
    if condition != expected_condition {
        return Ok(ExecutionReadValueV1::NotApplicable);
    }
    if matches!(order,BuyOrderIntentV1::GtcLimit{size_shares,..}|BuyOrderIntentV1::GtdLimit{size_shares,..} if *size_shares<minimum)
    {
        return Ok(ExecutionReadValueV1::NotApplicable);
    }
    match remaining {
        ExecutionReadValueV1::Known(q) => {
            Ok(ExecutionReadValueV1::Known(R::from_decimal(decimal(q)?)))
        }
        ExecutionReadValueV1::Unavailable => Ok(ExecutionReadValueV1::Unavailable),
        ExecutionReadValueV1::NotApplicable => Ok(ExecutionReadValueV1::NotApplicable),
    }
}
/// 风险只读：已终局成功份额加原承诺未终局残量，双方可达结果分别判断。
/// 不把经济暂记持仓与包含同一成交的remaining_target_shares再次相加。
pub fn repair_eligibility(
    input: &StrategyInputV1,
    frame: &FrozenMarketReadV1,
) -> Result<([bool; 2], Value), String> {
    let market = &frame.observation;
    let id = &market.market.condition_id;
    let minimum = decimal(&frame.rule_context.limit_min_quantity_shares)?;
    let mut net = R::zero();
    let economic = input
        .action_before
        .economics()
        .markets
        .iter()
        .find(|m| m.condition_id == *id);
    if let Some(e) = economic.filter(|e| e.settlement_state != EconomicSettlementStateV1::Redeemed)
        && let Some(assets) = &e.assets
        && let Some(first) = market
            .assets
            .first()
            .and_then(|a| assets.iter().position(|b| b.token_id == a.asset.token_id))
    {
        // gap[i]为买入i侧应修复的终局差额；相减还原当前两侧净差。
        net = R::from_decimal(decimal(&e.repair_gap_shares[1 - first])?)
            .sub(&R::from_decimal(decimal(&e.repair_gap_shares[first])?));
    }
    let mut remaining = [R::zero(), R::zero()];
    let mut unknown = [false, false];
    let mut commitment_features = Vec::new();
    let mut known_static_rejection_count = 0;
    for c in input.action_before.commitments() {
        let Some((condition, leg)) =
            original_leg(c.origin.intent.intent(), c.origin.batch_item_ordinal)
        else {
            continue;
        };
        if condition != id {
            continue;
        }
        let Some(side) = market
            .assets
            .iter()
            .position(|a| a.asset.token_id == leg.token_id)
        else {
            continue;
        };
        if side >= 2 {
            continue;
        }
        let credible = credible_remaining(
            id,
            condition,
            &leg.order,
            &c.remaining_target_shares,
            minimum,
        )?;
        match &credible {
            ExecutionReadValueV1::Known(q) => {
                remaining[side] = remaining[side].add(q);
            }
            ExecutionReadValueV1::Unavailable => unknown[side] = true,
            ExecutionReadValueV1::NotApplicable => known_static_rejection_count += 1,
        }
        commitment_features.push(json!({"side":side,"remaining_target_shares":c.remaining_target_shares,"order_formed":c.order_identity.is_some(),"order":leg.order}));
    }
    // 即使某些承诺未知，已确认孤腿或已知承诺仍能独立证明可达风险；未知不当零，也不凭未知制造资格。
    let risk0 = net.add(&remaining[0]).is_positive();
    let risk1 = R::zero().sub(&net).add(&remaining[1]).is_positive();
    Ok((
        [risk1, risk0],
        json!({"condition_id":id,"confirmed_net_side0_minus_side1":net.public(),"economic_success_shares":economic.map(|e| market.assets.iter().map(|asset|e.assets.as_ref().and_then(|a|a.iter().position(|b|b.token_id==asset.asset.token_id)).map(|i|&e.success_shares[i])).collect::<Vec<_>>()),"known_static_rejection_count":known_static_rejection_count,"known_remaining_target_shares":[remaining[0].public(),remaining[1].public()],"remaining_has_unavailable":unknown,"commitments":commitment_features}),
    ))
}
pub(crate) fn open_collection_trajectory(
    path: &std::path::Path,
) -> Result<BufWriter<std::fs::File>, String> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map(BufWriter::new)
        .map_err(|e| e.to_string())
}
impl ModelConsumer {
    pub fn new(
        config: ConsumerConfig,
        worker: ModelWorker,
        rules: Arc<SealedProcessRuleSnapshotV3>,
        entry_scope: backtest::EffectiveEntryMarketScopeV1,
    ) -> Result<Self, String> {
        let trajectory = config
            .learning
            .as_ref()
            .map(|c| open_collection_trajectory(&c.trajectory_path))
            .transpose()?;
        Ok(Self {
            config,
            worker,
            rules,
            call: 0,
            entry_scope,
            rpc: RefCell::new(Vec::new()),
            trajectory,
            last_confirmed: None,
            confirmed_revision: 0,
            last_qualified: Vec::new(),
        })
    }
    fn exchange(&self, v: Value, elapsed: &mut f64) -> Result<Value, String> {
        let s = Instant::now();
        let r = self.worker.request(v.clone())?;
        if self.config.learning.is_some() && v["phase"] != "begin" {
            self.rpc.borrow_mut().push(json!({"request":v,"response":{"post_only":r["post_only"],"index":r["index"],"rank":r["rank"]}}));
        }
        *elapsed += s.elapsed().as_secs_f64();
        Ok(r)
    }
    fn base(&self, phase: &str) -> Value {
        json!({"phase":phase,"account":self.config.account_key,"call":self.call})
    }
}
impl StrategyV1 for ModelConsumer {
    type Error = String;
    type Diagnostics = ModelDiagnostics;
    fn active_until_utc_ms(&self) -> Option<i64> {
        self.config.learning.as_ref().map(|c| c.active_end_utc_ms)
    }
    fn requires_learning_economics(&self) -> bool {
        self.config.learning.is_some()
    }
    fn research_diagnostics(&self, d: &Self::Diagnostics) -> Value {
        serde_json::to_value(d).unwrap()
    }
    fn requires_trade_economics_details(&self) -> bool {
        false
    }
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, String> {
        let started = Instant::now();
        self.call += 1;
        self.rpc.borrow_mut().clear();
        let cash = input.action_before.cash_available();
        let mut time = time_projection(input, at);
        if let Some(c) = &self.config.learning {
            time["active_end_utc_ms"] = c.active_end_utc_ms.into();
            time["evidence_end_utc_ms"] = c.evidence_end_utc_ms.into();
            time["until_active_end_ms"] = (c.active_end_utc_ms - at).into();
        }
        let mut domains = Vec::new();
        let mut market_tokens = Vec::new();
        let mut private_markets = Vec::new();
        let mut repair_evidence = Vec::new();
        let mut options = vec![json!({"kind":"NoTrade","market":null,"side":null})];
        for (index, m) in input.markets.iter().enumerate() {
            let o = &m.observation;
            let (eligible, account) = repair_eligibility(input, m)?;
            let mut evidence = account.clone();
            if let Some(object) = evidence.as_object_mut() {
                object.remove("commitments");
            }
            repair_evidence.push(evidence);
            let order_feedback=input.action_before.orders().filter(|r|r.condition_id==Some(&o.market.condition_id)).map(|r|json!({"token_id":r.token_id,"matched_shares":r.matched_shares,"active_unfilled_shares":r.active_unfilled_shares,"accepted":r.accepted.is_some(),"formed":r.formed.is_some(),"latest_lifecycle":r.latest_lifecycle.map(|x|x.record()),"latest_direct_feedback":r.latest_direct_feedback.map(|x|x.record()),"cancellable":r.cancellable})).collect::<Vec<_>>();
            let funds = input.action_before.market_funds(&o.market.condition_id);
            let private = json!({"account":account,"order_feedback":order_feedback,"funds":{"principal":funds.principal.to_string(),"fee":funds.fee.to_string(),"local_principal":funds.local_principal.to_string(),"local_fee":funds.local_fee.to_string(),"platform_pending_principal":funds.platform_pending_principal.to_string(),"platform_pending_fee":funds.platform_pending_fee.to_string(),"pending_count":funds.pending_count}});
            let mut projection = private.clone();
            projection["public"] = market_projection(m, at);
            private_markets.push(private);
            market_tokens.push(tokens(&projection, &self.worker.config.encoding));
            let mut ticks = [ExactDecimal("0".into()), ExactDecimal("0".into())];
            let mut max_prices = [0u16; 2];
            if o.assets.len() == 2 {
                for side in 0..2 {
                    if let Some(tick) = &o.assets[side].tick.value {
                        let t = decimal(tick)?;
                        if t > Decimal::ZERO {
                            ticks[side] = tick.clone();
                            max_prices[side] =
                                u16::try_from(((Decimal::ONE - t) / t).floor().mantissa())
                                    .map_err(|_| "tick超出原SDK六档索引")?;
                        }
                    }
                }
            }
            let start = o.market.window.start_at_utc_ms;
            let publicly_allowed = o.active.value != Some(false)
                && o.closed.value != Some(true)
                && o.accepting_orders.value != Some(false);
            let pair = publicly_allowed && pair_start_allowed(start, &self.entry_scope);
            let d = MarketDomain {
                ticks,
                max_prices,
                eligible,
                pair,
            };
            if max_prices.iter().all(|n| *n > 0) && publicly_allowed {
                for (kind, side) in [
                    ("Pair", None),
                    ("Repair", Some(0usize)),
                    ("Repair", Some(1usize)),
                ] {
                    if (kind == "Pair" && !d.pair) || side.is_some_and(|s| !eligible[s]) {
                        continue;
                    }
                    let indices: Vec<_> = side.map_or_else(|| vec![0, 1], |s| vec![s]);
                    let legs = indices
                        .iter()
                        .map(|s| LimitLeg {
                            price_index: 1,
                            tick: d.ticks[*s].clone(),
                            post_only: true,
                        })
                        .collect::<Vec<_>>();
                    if !affordable_count(&self.rules, cash, &legs)?.is_zero() {
                        options.push(json!({"kind":kind,"market":index,"side":side}));
                    }
                }
            }
            domains.push(d);
        }
        let global_projection = json!({"time":time,"cash_available":cash.to_string(),"unfinished_order_count":input.action_before.orders().count(),"commitment_count":input.action_before.commitments().count()});
        let global = tokens(&global_projection, &self.worker.config.encoding);
        let encoding_seconds = started.elapsed().as_secs_f64();
        let mut roundtrip = 0.0;
        let mut request = self.base("begin");
        request["seed"] = self.config.sampling_seed.into();
        request["global"] = json!(global);
        request["markets"] = json!(market_tokens);
        request["options"] = json!(options);
        let begin = self.exchange(request, &mut roundtrip)?;
        let mut logp = begin["log_probability"]
            .as_f64()
            .ok_or("模型没有返回动作概率")?;
        let mut head_trace = begin["head_trace"]
            .as_array()
            .ok_or("模型缺动作概率路径")?
            .clone();
        let mut heads = begin["heads"].as_u64().ok_or("模型没有返回动作头计数")?;
        let selected = begin["option"].as_u64().ok_or("模型没有返回动作索引")? as usize;
        let option = options.get(selected).ok_or("模型动作索引越界")?;
        let action = option["kind"].as_str().unwrap().to_owned();
        let mut intents = Vec::new();
        if action != "NoTrade" {
            let market = option["market"].as_u64().ok_or("模型缺市场")? as usize;
            let d = &domains[market];
            let original = &input.markets[market].observation;
            let sides: Vec<usize> = if action == "Pair" {
                vec![0, 1]
            } else {
                vec![option["side"].as_u64().ok_or("模型缺修复方向")? as usize]
            };
            let mut selected_legs = Vec::new();
            for (ordinal, side) in sides.iter().enumerate() {
                // 属性先选；价格和数量随后使用该属性的原公共成本。
                let mut supported_posts = Vec::new();
                for post in [false, true] {
                    let mut minimum = selected_legs.clone();
                    minimum.push(LimitLeg {
                        price_index: 1,
                        tick: d.ticks[*side].clone(),
                        post_only: post,
                    });
                    for other in sides.iter().skip(ordinal + 1) {
                        minimum.push(LimitLeg {
                            price_index: 1,
                            tick: d.ticks[*other].clone(),
                            post_only: true,
                        });
                    }
                    if !affordable_count(&self.rules, cash, &minimum)?.is_zero() {
                        supported_posts.push(post);
                    }
                }
                let mut req = self.base("post");
                req["allowed"] = json!(supported_posts);
                req["ordinal"] = ordinal.into();
                req["side"] = (*side).into();
                let post_result = self.exchange(req, &mut roundtrip)?;
                head_trace.extend(
                    post_result["head_trace"]
                        .as_array()
                        .ok_or("模型缺属性概率路径")?
                        .iter()
                        .cloned(),
                );
                let post_only = post_result["post_only"]
                    .as_bool()
                    .ok_or("模型缺post_only")?;
                if !supported_posts.contains(&post_only) {
                    return Err("post_only超出冻结资金域".into());
                }
                logp += post_result["log_probability"]
                    .as_f64()
                    .ok_or("模型缺属性概率")?;
                heads += post_result["heads"].as_u64().ok_or("模型缺属性头计数")?;
                // 所选价格须至少允许0.01份，余腿保留最便宜合法价格；不是平台MOS掩码。
                let mut lower = 1u16;
                let mut upper = d.max_prices[*side];
                while lower < upper {
                    let mid = lower + (upper - lower).div_ceil(2);
                    let mut candidate = selected_legs.clone();
                    candidate.push(LimitLeg {
                        price_index: mid,
                        tick: d.ticks[*side].clone(),
                        post_only,
                    });
                    for other in sides.iter().skip(ordinal + 1) {
                        candidate.push(LimitLeg {
                            price_index: 1,
                            tick: d.ticks[*other].clone(),
                            post_only: true,
                        });
                    }
                    let total = costs(&self.rules, &BigUint::one(), &candidate)?
                        .into_iter()
                        .fold(BigUint::zero(), |s, c| {
                            s + c.principal_micro + c.fee_1e5_units * 10u8
                        });
                    if total <= cash_micro(cash) {
                        lower = mid;
                    } else {
                        upper = mid - 1;
                    }
                }
                let mut req = self.base("price");
                req["maximum"] = lower.into();
                req["ordinal"] = ordinal.into();
                req["side"] = (*side).into();
                let result = self.exchange(req, &mut roundtrip)?;
                head_trace.extend(
                    result["head_trace"]
                        .as_array()
                        .ok_or("模型缺价格概率路径")?
                        .iter()
                        .cloned(),
                );
                let index = u16::try_from(result["index"].as_u64().ok_or("模型缺价格")?)
                    .map_err(|e| e.to_string())?;
                if index == 0 || index > lower {
                    return Err("模型价格超出冻结支持域".into());
                }
                logp += result["log_probability"].as_f64().ok_or("模型缺价格概率")?;
                heads += result["heads"].as_u64().ok_or("模型缺价格头计数")?;
                selected_legs.push(LimitLeg {
                    price_index: index,
                    tick: d.ticks[*side].clone(),
                    post_only,
                });
            }
            let count = affordable_count(&self.rules, cash, &selected_legs)?;
            if count.is_zero() {
                return Err("冻结资金域为空，不能用NoTrade替代已选交易".into());
            }
            let mut req = self.base("quantity");
            req["count"] = count.to_string().into();
            let result = self.exchange(req, &mut roundtrip)?;
            head_trace.extend(
                result["head_trace"]
                    .as_array()
                    .ok_or("模型缺数量概率路径")?
                    .iter()
                    .cloned(),
            );
            let rank: BigUint = result["rank"]
                .as_str()
                .ok_or("模型缺精确份额排名")?
                .parse()
                .map_err(|e| format!("份额排名:{e}"))?;
            if rank >= count {
                return Err("模型份额排名超出冻结域".into());
            }
            let n = rank_to_centishares(&rank)?;
            let size = shares(&n)?;
            logp += result["log_probability"].as_f64().ok_or("模型缺数量概率")?;
            heads += result["heads"].as_u64().ok_or("模型缺数量头计数")?;
            let costs = costs(&self.rules, &n, &selected_legs)?;
            let id = format!(
                "{}:{}:{}",
                self.config.request_namespace, self.config.account_key, self.call
            );
            let orders = sides
                .iter()
                .zip(&selected_legs)
                .map(|(side, l)| {
                    Ok(BuyLegIntentV1 {
                        token_id: original.assets[*side].asset.token_id.clone(),
                        order: BuyOrderIntentV1::GtcLimit {
                            limit_price_pusd_per_share: decimal(&l.tick)?
                                .checked_mul(Decimal::from(l.price_index))
                                .ok_or("价格溢出")?,
                            size_shares: size,
                            post_only: l.post_only,
                        },
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            let intent = if action == "Pair" {
                ModelIntentV1::SubmitPairedBuy {
                    condition_id: original.market.condition_id.clone(),
                    orders: orders.try_into().map_err(|_| "配对腿数")?,
                }
            } else {
                ModelIntentV1::SubmitSingleBuy {
                    condition_id: original.market.condition_id.clone(),
                    order: orders.into_iter().next().unwrap(),
                }
            };
            let reserves = costs
                .into_iter()
                .enumerate()
                .map(|(i, c)| IntentReserveV1 {
                    batch_item_ordinal: i as u16,
                    reserve: ReserveIntentV1 {
                        request_identity: LedgerRequestIdentityV1(format!("{id}:leg-{i}")),
                        condition_id: original.market.condition_id.clone(),
                        requested_principal_pusd: ExactDecimal(scaled_string(
                            &c.principal_micro,
                            6,
                        )),
                        immediate_fee_reserve_pusd: ExactDecimal(scaled_string(
                            &c.fee_1e5_units,
                            5,
                        )),
                    },
                })
                .collect();
            intents.push(ReservedIntentV1 {
                context: RequestContextV2 {
                    global_request_id: id,
                    caller_correlation_id: None,
                },
                intent: Arc::new(intent),
                reserves,
            });
        }
        if let Some(c) = &self.config.learning {
            let economic = input
                .action_before
                .learning_account()
                .ok_or("学习消费者缺公共经济读取")?;
            if self.last_confirmed.as_ref().is_none_or(|old| {
                !Arc::ptr_eq(old, &economic.confirmed) && **old != *economic.confirmed
            }) || self.last_qualified != economic.qualified_rebates
            {
                self.confirmed_revision += 1;
                let update = json!({"record_type":"confirmed_economic_update","revision":self.confirmed_revision,"confirmed":economic.confirmed,"qualified_rebates":economic.qualified_rebates});
                let file = self.trajectory.as_mut().unwrap();
                serde_json::to_writer(&mut *file, &update).map_err(|e| e.to_string())?;
                file.write_all(b"\n").map_err(|e| e.to_string())?;
                self.last_confirmed = Some(economic.confirmed.clone());
                self.last_qualified = economic.qualified_rebates.clone();
            }
            let record = json!({"version":"learning-trajectory-v1","group":c.group_id,"account":self.config.account_key,"call":self.call,"processing_start_utc_ms":at,"market_frame":input.market_frame_timing,"account_cutoff":input.action_before.cutoff(),"market_ids":input.markets.iter().map(|m|m.observation.market.condition_id.clone()).collect::<Vec<_>>(),"global_private":global_projection,"market_private":private_markets,"options":options,"selected_option":selected,"rpc":self.rpc.borrow().clone(),"old_joint_logp":logp,"old_value":begin["value"],"behavior":begin["behavior"],"k_exact":crate::learning::confirmed_k(economic)?.public(),"economic":{"confirmed_revision":self.confirmed_revision,"obligations":economic.obligations,"pending_noe_record_count":economic.pending_noe_record_count},"actual_intents":intents,"special":{"frame_age_ms":time["frame_age_ms"],"eligible_record_threshold_ms":100,"repair_evidence":repair_evidence,"action":action,"numeric_special_reward":false}});
            let file = self.trajectory.as_mut().unwrap();
            serde_json::to_writer(&mut *file, &record).map_err(|e| e.to_string())?;
            file.write_all(b"\n").map_err(|e| e.to_string())?;
            file.flush().map_err(|e| e.to_string())?;
            if c.collection_fault_after_call == Some(self.call) {
                return Err("injected-collection-boundary-fault".into());
            }
        }
        let total = started.elapsed().as_secs_f64();
        let diagnostics = ModelDiagnostics {
            call: self.call,
            action,
            joint_log_probability: logp,
            heads,
            head_trace,
            forward: begin["forward"].clone(),
            time,
            repair_eligible: domains.iter().map(|d| d.eligible).collect(),
            repair_evidence,
            encoding_seconds,
            model_roundtrip_seconds: roundtrip,
            exact_domain_and_construction_seconds: (total - encoding_seconds - roundtrip).max(0.0),
        };
        if self.config.progress_every > 0 && self.call.is_multiple_of(self.config.progress_every) {
            eprintln!(
                "模型账户{} 已处理{}帧，本次{:.4}s",
                self.config.account_key, self.call, total
            );
        }
        Ok(StrategyOutputV1::Continue {
            intents,
            next_evaluation_at_utc_ms: None,
            diagnostics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_request_minimum_not_remaining_quantity_controls_credible_risk() {
        let condition = ConditionId(
            "0x1111111111111111111111111111111111111111111111111111111111111111".into(),
        );
        let other = ConditionId(
            "0x2222222222222222222222222222222222222222222222222222222222222222".into(),
        );
        let order = |size| BuyOrderIntentV1::GtcLimit {
            limit_price_pusd_per_share: Decimal::new(4, 1),
            size_shares: size,
            post_only: false,
        };
        let tail = ExecutionReadValueV1::Known(ExactDecimal("0.01".into()));
        assert_eq!(
            credible_remaining(
                &condition,
                &condition,
                &order(Decimal::new(1, 2)),
                &tail,
                Decimal::from(5)
            )
            .unwrap(),
            ExecutionReadValueV1::NotApplicable
        );
        assert_eq!(
            credible_remaining(
                &condition,
                &condition,
                &order(Decimal::from(10)),
                &tail,
                Decimal::from(5)
            )
            .unwrap(),
            ExecutionReadValueV1::Known(R::from_decimal(Decimal::new(1, 2)))
        );
        assert_eq!(
            credible_remaining(
                &condition,
                &other,
                &order(Decimal::from(10)),
                &tail,
                Decimal::from(5)
            )
            .unwrap(),
            ExecutionReadValueV1::NotApplicable
        );
        assert_eq!(
            credible_remaining(
                &condition,
                &condition,
                &order(Decimal::from(10)),
                &ExecutionReadValueV1::Unavailable,
                Decimal::from(5)
            )
            .unwrap(),
            ExecutionReadValueV1::Unavailable
        );
    }
}

/// 声明输入短窗与UTC允许市场开始集合相交；修复/观察不受新配对门影响。
pub fn pair_start_allowed(start: i64, scope: &backtest::EffectiveEntryMarketScopeV1) -> bool {
    (300_000..=85_800_000).contains(&start.rem_euclid(86_400_000))
        && start >= scope.market_start_inclusive_utc_ms
        && start < scope.market_start_exclusive_utc_ms
}
#[cfg(test)]
mod entry_scope_tests {
    use super::*;
    #[test]
    fn short_window_intersects_utc_without_removing_edges_or_prefetched_next() {
        let s = backtest::EntryMarketScopeV1::default()
            .expand(300_000, 312_000)
            .unwrap();
        assert!(pair_start_allowed(300_000, &s));
        assert!(!pair_start_allowed(600_000, &s));
        let s = backtest::EntryMarketScopeV1::default()
            .expand(0, 86_400_000)
            .unwrap();
        assert!(!pair_start_allowed(0, &s));
        assert!(pair_start_allowed(300_000, &s));
        assert!(pair_start_allowed(85_800_000, &s));
        assert!(!pair_start_allowed(86_100_000, &s));
    }
}
