use crate::quantity::Quantity;
use ::execution_normalization::{
    MemoryLogicalIdentityV1 as Identity, NoeConsumedRecordRefV1 as Ref, NoeConsumptionVisibilityV1,
    NormalizationRecord, SourceNeutralEvidenceKind,
};
use ExecutionReadValueV1::{Known, NotApplicable, Unavailable};
use contracts::{execution_normalization::*, *};
use rust_decimal::Decimal;
use sim_system::execution::MutationRequestReceiptV2;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Debug)]
pub struct FrozenIntentRefV1 {
    context: Arc<ExecutionNormalizationContextV1>,
    id: String,
    intent: Arc<ModelIntentV1>,
}
impl FrozenIntentRefV1 {
    pub fn context(&self) -> &ExecutionNormalizationContextV1 {
        &self.context
    }
    pub fn global_request_id(&self) -> &str {
        &self.id
    }
    pub fn intent(&self) -> &ModelIntentV1 {
        &self.intent
    }
}
pub struct SubmittedLegReadV1<'a> {
    pub intent: &'a FrozenIntentRefV1,
    pub batch_item_ordinal: u16,
}
pub struct ExecutionOrderReadV1<'a> {
    pub order_identity: &'a Identity,
    pub origin: Option<SubmittedLegReadV1<'a>>,
    pub accepted: Option<&'a Ref>,
    pub formed: Option<&'a Ref>,
    pub latest_lifecycle: Option<&'a Ref>,
    pub latest_direct_feedback: Option<&'a Ref>,
    pub matched_shares: ExecutionReadValueV1<ExactDecimal>,
    pub active_unfilled_shares: ExecutionReadValueV1<ExactDecimal>,
    pub cancellable: ExecutionReadValueV1<bool>,
}
#[derive(Clone, Default)]
struct Order {
    origin: Option<(String, u16)>,
    accepted: Option<Ref>,
    formed: Option<Ref>,
    lifecycle: Option<Ref>,
    direct: Option<Ref>,
    trades: BTreeMap<Identity, Ref>,
}
#[derive(Clone)]
struct Submitted {
    origin: FrozenIntentRefV1,
    receipt: Option<MutationRequestReceiptV2>,
    outcomes: BTreeMap<u16, Option<Identity>>,
}
/// 普通已稳定 NOE 读取不能直接传入共享登记；这不是公开生产者接口的来源认证。
/// ```compile_fail
/// fn observe(registry: &mut shared_run::SharedExecutionRegistryV1,
///            view: &execution_normalization::NoeDurableRecordViewV1) {
///     registry.observe(view);
/// }
/// ```
#[derive(Clone)]
pub struct SharedExecutionRegistryV1 {
    context: Arc<ExecutionNormalizationContextV1>,
    intents: BTreeMap<String, Submitted>,
    orders: BTreeMap<Identity, Order>,
    markets: BTreeMap<String, Arc<PublicTradingRuleContextV1>>,
}
impl SharedExecutionRegistryV1 {
    pub fn new(context: Arc<ExecutionNormalizationContextV1>) -> Self {
        Self {
            context,
            intents: BTreeMap::new(),
            orders: BTreeMap::new(),
            markets: BTreeMap::new(),
        }
    }
    pub(crate) fn observe_market(&mut self, rules: &Arc<PublicTradingRuleContextV1>) {
        self.markets
            .entry(rules.market.condition_id.0.clone())
            .or_insert_with(|| rules.clone());
    }
    pub(crate) fn action_snapshot(&self) -> Self {
        let orders: BTreeMap<_, _> = self.orders.iter().filter(|(id, state)| {
            let cancellable = self.order(id).unwrap().cancellable;
            !matches!(cancellable, Known(false))
                || state.trades.values().filter_map(trade)
                    .any(|t| !final_disposition(&t.economic_disposition))
                // 不可撤不代表原关联、累计成交覆盖和承诺已经闭合。
                || state.origin.as_ref()
                    .and_then(|(request, ordinal)| self.intents.get(request)
                        .and_then(|s| leg(s.origin.intent(), *ordinal)))
                    .is_none_or(|(_, leg)| !matches!(self.remaining(id, &leg.order), Known(v) if decimal(&v).is_zero()))
        }).map(|(id, state)| (id.clone(), state.clone())).collect();
        let needed: BTreeSet<_> = self
            .all_commitments()
            .map(|r| r.origin.intent.global_request_id())
            .chain(
                orders
                    .values()
                    .filter_map(|o| o.origin.as_ref().map(|(id, _)| id.as_str())),
            )
            .collect();
        let intents = self
            .intents
            .iter()
            .filter(|(id, _)| needed.contains(id.as_str()))
            .map(|(id, s)| (id.clone(), s.clone()))
            .collect();
        Self {
            context: self.context.clone(),
            intents,
            orders,
            markets: self.markets.clone(),
        }
    }
    pub fn freeze(&mut self, id: String, intent: Arc<ModelIntentV1>) -> FrozenIntentRefV1 {
        self.intents
            .entry(id.clone())
            .or_insert_with(|| Submitted {
                origin: FrozenIntentRefV1 {
                    context: self.context.clone(),
                    id,
                    intent,
                },
                receipt: None,
                outcomes: BTreeMap::new(),
            })
            .origin
            .clone()
    }
    pub fn record_sim_receipt(
        &mut self,
        origin: &FrozenIntentRefV1,
        receipt: &MutationRequestReceiptV2,
    ) {
        self.intents
            .get_mut(origin.global_request_id())
            .expect("本账户先冻结意图")
            .receipt = Some(*receipt);
    }
    pub fn observe(&mut self, record: &Ref) {
        let NormalizationRecord::Evidence { evidence, .. } = record.record() else {
            return;
        };
        match evidence {
            SourceNeutralEvidenceKind::Control { result } => match result {
                SourceNeutralControlResult::Submission { outcome } => {
                    let association = match &record.record().source_reference().record {
                        SimV2SourceRecordReferenceV1::DirectResponse {
                            association:
                                SimV2DirectResponseAssociationV1::SubmissionItem {
                                    global_request_id,
                                    batch_item_ordinal,
                                },
                            ..
                        } => Some((global_request_id.clone(), *batch_item_ordinal)),
                        _ => None,
                    };
                    let order = match outcome {
                        SourceNeutralSubmissionOutcomeV2::Accepted { order, .. } => {
                            Some(order.order_identity.clone())
                        }
                        _ => None,
                    };
                    if let Some((id, ordinal)) = &association
                        && let Some(intent) = self.intents.get_mut(id)
                    {
                        intent.outcomes.insert(*ordinal, order.clone());
                    }
                    if let Some(id) = order {
                        let state = self.orders.entry(id).or_default();
                        state.origin = association;
                        replace_source(&mut state.accepted, record);
                        replace_delivery(&mut state.direct, record);
                    }
                }
                SourceNeutralControlResult::Cancellation { order_identity, .. } => {
                    replace_delivery(
                        &mut self
                            .orders
                            .entry(order_identity.clone())
                            .or_default()
                            .direct,
                        record,
                    );
                }
                SourceNeutralControlResult::OrderQuery {
                    result: SourceNeutralOrderQueryResult::Found { order },
                } => {
                    replace_delivery(
                        &mut self
                            .orders
                            .entry(order.order_identity.clone())
                            .or_default()
                            .direct,
                        record,
                    );
                }
                SourceNeutralControlResult::OrdersQuery {
                    result: SourceNeutralOrdersQueryResult::Page(page),
                } => {
                    for order in &page.items {
                        replace_delivery(
                            &mut self
                                .orders
                                .entry(order.order_identity.clone())
                                .or_default()
                                .direct,
                            record,
                        );
                    }
                }
                _ => {}
            },
            SourceNeutralEvidenceKind::OrderLifecycle { result } => {
                let state = self
                    .orders
                    .entry(result.order_identity.clone())
                    .or_default();
                if matches!(
                    result.event,
                    SourceNeutralOrderLifecycleEventV1::Accepted { .. }
                ) {
                    replace_source(&mut state.formed, record);
                }
                replace_source(&mut state.lifecycle, record);
            }
            SourceNeutralEvidenceKind::TradeLifecycle { result } => {
                let state = self
                    .orders
                    .entry(result.order_identity.clone())
                    .or_default();
                match state.trades.get(&result.trade_identity) {
                    Some(prior) if source_sequence(prior) >= source_sequence(record) => {}
                    _ => {
                        state
                            .trades
                            .insert(result.trade_identity.clone(), record.clone());
                    }
                }
            }
            SourceNeutralEvidenceKind::TradeLifecycleV3 { result } => {
                let result = &result.result;
                let state = self
                    .orders
                    .entry(result.order_identity.clone())
                    .or_default();
                match state.trades.get(&result.trade_identity) {
                    Some(prior) if source_sequence(prior) >= source_sequence(record) => {}
                    _ => {
                        state
                            .trades
                            .insert(result.trade_identity.clone(), record.clone());
                    }
                }
            }
            _ => {}
        }
    }
    pub fn order(&self, id: &Identity) -> Option<ExecutionOrderReadV1<'_>> {
        let (id, state) = self.orders.get_key_value(id)?;
        let origin = state.origin.as_ref().and_then(|(id, ordinal)| {
            self.intents.get(id).map(|s| SubmittedLegReadV1 {
                intent: &s.origin,
                batch_item_ordinal: *ordinal,
            })
        });
        let (matched, active) = quantities(state);
        // 接受记录独立保留；后续 FAK 余款释放不能遮蔽已有的不可撤事实。
        let accepted_matched = matches!(
            state.formed.as_ref().map(Ref::record),
            Some(NormalizationRecord::Evidence {
                evidence: SourceNeutralEvidenceKind::OrderLifecycle {
                    result: SourceNeutralOrderLifecycleResultV1 {
                        event: SourceNeutralOrderLifecycleEventV1::Accepted {
                            state: SourceNeutralOrderStateV1::Matched,
                        },
                        ..
                    },
                },
                ..
            })
        );
        let cancellable = if is_closed(state) || is_quote(state) || accepted_matched {
            Known(false)
        } else {
            match &active {
                Known(q) => Known(decimal(q) > Decimal::ZERO),
                _ => Unavailable,
            }
        };
        Some(ExecutionOrderReadV1 {
            order_identity: id,
            origin,
            accepted: state.accepted.as_ref(),
            formed: state.formed.as_ref(),
            latest_lifecycle: state.lifecycle.as_ref(),
            latest_direct_feedback: state.direct.as_ref(),
            matched_shares: matched,
            active_unfilled_shares: active,
            cancellable,
        })
    }
    pub fn orders(&self) -> impl Iterator<Item = ExecutionOrderReadV1<'_>> {
        self.orders.keys().map(|id| self.order(id).unwrap())
    }
    pub fn order_trades(&self, id: &Identity) -> impl Iterator<Item = &Ref> {
        self.orders
            .get(id)
            .into_iter()
            .flat_map(|o| o.trades.values())
    }
    pub fn commitments(
        &self,
        condition: &ConditionId,
        token: &TokenId,
    ) -> impl Iterator<Item = IntentCommitmentReadV1<'_>> {
        self.all_commitments().filter(move |r| {
            leg(r.origin.intent.intent(), r.origin.batch_item_ordinal)
                .is_some_and(|(c, l)| c == condition && &l.token_id == token)
        })
    }
    pub fn all_commitments(&self) -> impl Iterator<Item = IntentCommitmentReadV1<'_>> {
        self.intents
            .values()
            .filter(|s| s.receipt.is_some())
            .flat_map(move |s| {
                (0..2).filter_map(move |ordinal| {
                    let (_, leg) = leg(s.origin.intent(), ordinal)?;
                    let outcome = s.outcomes.get(&ordinal);
                    let order = outcome.and_then(Option::as_ref);
                    let remaining = match outcome {
                        Some(None) => Known(exact(Decimal::ZERO)),
                        Some(Some(id)) => self.remaining(id, &leg.order),
                        None => target(&leg.order),
                    };
                    if matches!(&remaining,Known(q) if decimal(q).is_zero()) {
                        return None;
                    }
                    Some(IntentCommitmentReadV1 {
                        origin: SubmittedLegReadV1 {
                            intent: &s.origin,
                            batch_item_ordinal: ordinal,
                        },
                        order_identity: order,
                        remaining_target_shares: remaining,
                    })
                })
            })
    }
    pub fn committed_target_shares(
        &self,
        condition: &ConditionId,
        token: &TokenId,
    ) -> ExecutionReadValueV1<ExactDecimal> {
        sum(self
            .commitments(condition, token)
            .map(|r| r.remaining_target_shares))
    }
    fn remaining(
        &self,
        id: &Identity,
        intent: &BuyOrderIntentV1,
    ) -> ExecutionReadValueV1<ExactDecimal> {
        let Some(order) = self.orders.get(id) else {
            return Unavailable;
        };
        let Some(measurement) = measurement(order) else {
            return target(intent);
        };
        let initial = match measurement {
            SourceNeutralSubmissionMeasurementV1::Base {
                matched_base_quantity_shares,
                ..
            } => Quantity::parse(matched_base_quantity_shares),
            SourceNeutralSubmissionMeasurementV1::QuoteBudget {
                actual_matched_base_quantity_shares,
            } => Quantity::parse(actual_matched_base_quantity_shares),
        };
        let mut taker = Quantity::zero();
        for t in order
            .trades
            .values()
            .filter_map(trade)
            .filter(|t| t.liquidity_role == TradeLiquidityRoleV2::Taker)
        {
            taker.add(&Quantity::parse(&t.base_quantity_shares));
        }
        let coverage = taker.equal(&initial);
        if matches!(
            intent,
            BuyOrderIntentV1::FokMarket { .. } | BuyOrderIntentV1::FakMarket { .. }
        ) {
            return if coverage
                && order
                    .trades
                    .values()
                    .filter_map(trade)
                    .all(|t| final_disposition(&t.economic_disposition))
            {
                Known(exact(Decimal::ZERO))
            } else {
                target(intent)
            };
        }
        if !coverage {
            return Unavailable;
        }
        let (_, active) = quantities(order);
        sum(std::iter::once(active).chain(
            order
                .trades
                .values()
                .filter_map(trade)
                .filter(|t| !final_disposition(&t.economic_disposition))
                .map(|t| Known(t.base_quantity_shares.clone())),
        ))
    }
    fn processing_key(&self, order: &ExecutionOrderReadV1<'_>) -> Option<(i64, u64, u16)> {
        let origin = order.origin.as_ref()?;
        let receipt = self
            .intents
            .get(origin.intent.global_request_id())?
            .receipt?;
        let NormalizationRecord::Evidence {
            evidence:
                SourceNeutralEvidenceKind::Control {
                    result:
                        SourceNeutralControlResult::Submission {
                            outcome:
                                SourceNeutralSubmissionOutcomeV2::Accepted {
                                    effective_processing_at_utc_ms,
                                    ..
                                },
                        },
                },
            ..
        } = order.accepted?.record()
        else {
            return None;
        };
        Some((
            *effective_processing_at_utc_ms,
            receipt.input_sequence(),
            origin.batch_item_ordinal,
        ))
    }
    /// 形成顺序使用整集合的完备公开证据；不把尚未收到的形成反馈补造成已知。

    pub fn cancel_objects(&self) -> ExecutionReadValueV1<Vec<CancelObjectReadV1<'_>>> {
        if self.orders().any(|o| matches!(o.cancellable, Unavailable)) {
            return Unavailable;
        }
        let mut eligible: Vec<_> = self
            .orders()
            .filter(|o| matches!(o.cancellable, Known(true)))
            .collect();
        if eligible.iter().any(|o| {
            self.orders
                .get(o.order_identity)
                .and_then(order_condition)
                .is_none_or(|c| !self.markets.contains_key(&c.0))
        }) {
            return Unavailable;
        }
        // SIM 原动作按有效处理时间、首次请求序号和原 ordinal 处理，
        // 与形成反馈源序严格保序。缺回执的原消费者仍可使用完整形成源序。
        let use_processing = eligible.iter().all(|o| self.processing_key(o).is_some());
        if !use_processing && eligible.iter().any(|o| o.formed.is_none()) {
            return Unavailable;
        }
        let formation = |o: &ExecutionOrderReadV1<'_>| {
            if use_processing {
                FormationOrderKey::Processing(self.processing_key(o).unwrap())
            } else {
                FormationOrderKey::Source(source_sequence(o.formed.unwrap()))
            }
        };
        eligible.sort_by(|a, b| {
            let start = |o: &ExecutionOrderReadV1<'_>| {
                self.markets[&order_condition(&self.orders[o.order_identity]).unwrap().0]
                    .market
                    .window
                    .start_at_utc_ms
            };
            start(a)
                .cmp(&start(b))
                .then_with(|| {
                    order_condition(&self.orders[a.order_identity])
                        .unwrap()
                        .0
                        .cmp(&order_condition(&self.orders[b.order_identity]).unwrap().0)
                })
                .then_with(|| formation(a).cmp(&formation(b)))
                .then_with(|| a.order_identity.cmp(b.order_identity))
        });
        let mut objects: Vec<_> = eligible
            .iter()
            .map(|o| CancelObjectReadV1::Single {
                order: self.order(o.order_identity).unwrap(),
            })
            .collect();
        let set: BTreeSet<_> = eligible.iter().map(|o| o.order_identity).collect();
        let mut paired = Vec::new();
        for submitted in self.intents.values() {
            if !matches!(
                submitted.origin.intent(),
                ModelIntentV1::SubmitPairedBuy { .. }
            ) {
                continue;
            }
            if let (Some(Some(a)), Some(Some(b))) =
                (submitted.outcomes.get(&0), submitted.outcomes.get(&1))
                && set.contains(a)
                && set.contains(b)
            {
                let orders = [self.order(a).unwrap(), self.order(b).unwrap()];
                let condition = order_condition(&self.orders[a]).unwrap();
                let mut members = [(formation(&orders[0]), a), (formation(&orders[1]), b)];
                // 排序键按真实成员形成序；返回的两成员数组仍保持原 ordinal 0/1。
                members.sort();
                let key = (
                    self.markets[&condition.0].market.window.start_at_utc_ms,
                    &condition.0,
                    members,
                );
                paired.push((key, &submitted.origin, orders));
            }
        }
        paired.sort_by_key(|(key, _, _)| *key);
        for (_, origin, orders) in paired {
            objects.push(CancelObjectReadV1::Paired { origin, orders });
        }
        let mut markets: BTreeMap<&str, (&ConditionId, Vec<ExecutionOrderReadV1<'_>>)> =
            BTreeMap::new();
        for order in eligible {
            let Some(condition) = self
                .orders
                .get(order.order_identity)
                .and_then(order_condition)
            else {
                return Unavailable;
            };
            markets
                .entry(&condition.0)
                .or_insert_with(|| (condition, Vec::new()))
                .1
                .push(order);
        }
        let mut markets: Vec<_> = markets.into_values().collect();
        markets.sort_by_key(|(condition, _)| {
            (
                self.markets[&condition.0].market.window.start_at_utc_ms,
                &condition.0,
            )
        });
        for (condition_id, orders) in markets {
            objects.push(CancelObjectReadV1::Market {
                condition_id,
                orders,
            });
        }
        Known(objects)
    }
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum FormationOrderKey {
    Processing((i64, u64, u16)),
    Source(u64),
}
pub struct IntentCommitmentReadV1<'a> {
    pub origin: SubmittedLegReadV1<'a>,
    pub order_identity: Option<&'a Identity>,
    pub remaining_target_shares: ExecutionReadValueV1<ExactDecimal>,
}
pub enum CancelObjectReadV1<'a> {
    Single {
        order: ExecutionOrderReadV1<'a>,
    },
    Paired {
        origin: &'a FrozenIntentRefV1,
        orders: [ExecutionOrderReadV1<'a>; 2],
    },
    Market {
        condition_id: &'a ConditionId,
        orders: Vec<ExecutionOrderReadV1<'a>>,
    },
}
fn leg(intent: &ModelIntentV1, ordinal: u16) -> Option<(&ConditionId, &BuyLegIntentV1)> {
    match intent {
        ModelIntentV1::SubmitSingleBuy {
            condition_id,
            order,
        } if ordinal == 0 => Some((condition_id, order)),
        ModelIntentV1::SubmitPairedBuy {
            condition_id,
            orders,
        } => orders.get(ordinal as usize).map(|l| (condition_id, l)),
        _ => None,
    }
}
fn target(order: &BuyOrderIntentV1) -> ExecutionReadValueV1<ExactDecimal> {
    Known(match order {
        BuyOrderIntentV1::GtcLimit { size_shares, .. }
        | BuyOrderIntentV1::GtdLimit { size_shares, .. } => exact(*size_shares),
        BuyOrderIntentV1::FokMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        }
        | BuyOrderIntentV1::FakMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => crate::quantity::original_target(*spend_pusd, *worst_price_pusd_per_share),
    })
}
fn measurement(order: &Order) -> Option<&SourceNeutralSubmissionMeasurementV1> {
    match order.accepted.as_ref()?.record() {
        NormalizationRecord::Evidence {
            evidence:
                SourceNeutralEvidenceKind::Control {
                    result:
                        SourceNeutralControlResult::Submission {
                            outcome: SourceNeutralSubmissionOutcomeV2::Accepted { measurement, .. },
                        },
                },
            ..
        } => Some(measurement),
        _ => None,
    }
}
fn is_quote(o: &Order) -> bool {
    matches!(
        measurement(o),
        Some(SourceNeutralSubmissionMeasurementV1::QuoteBudget { .. })
    )
}
fn is_closed(o: &Order) -> bool {
    matches!(
        o.lifecycle.as_ref().map(Ref::record),
        Some(NormalizationRecord::Evidence {
            evidence: SourceNeutralEvidenceKind::OrderLifecycle {
                result: SourceNeutralOrderLifecycleResultV1 {
                    event: SourceNeutralOrderLifecycleEventV1::CallerCanceled
                        | SourceNeutralOrderLifecycleEventV1::GtdExpired
                        | SourceNeutralOrderLifecycleEventV1::VenueDustTerminal
                        | SourceNeutralOrderLifecycleEventV1::MarketResolvedCanceled { .. },
                    ..
                }
            },
            ..
        })
    )
}
fn quantities(
    o: &Order,
) -> (
    ExecutionReadValueV1<ExactDecimal>,
    ExecutionReadValueV1<ExactDecimal>,
) {
    let Some(base) = measurement(o) else {
        return (
            Unavailable,
            if is_closed(o) {
                Known(exact(Decimal::ZERO))
            } else {
                Unavailable
            },
        );
    };
    match base {
        SourceNeutralSubmissionMeasurementV1::QuoteBudget {
            actual_matched_base_quantity_shares,
        } => (
            Known(actual_matched_base_quantity_shares.clone()),
            NotApplicable,
        ),
        SourceNeutralSubmissionMeasurementV1::Base {
            matched_base_quantity_shares,
            open_base_quantity_shares,
        } => {
            let mut later = Quantity::zero();
            for t in o
                .trades
                .values()
                .filter_map(trade)
                .filter(|t| t.liquidity_role == TradeLiquidityRoleV2::MakerInferred)
            {
                later.add(&Quantity::parse(&t.base_quantity_shares));
            }
            let mut matched = Quantity::parse(matched_base_quantity_shares);
            matched.add(&later);
            let mut active = Quantity::parse(open_base_quantity_shares);
            active.subtract(&later);
            let matched = Known(matched.exact());
            let active = if is_closed(o) {
                Known(exact(Decimal::ZERO))
            } else if active.is_negative() {
                Unavailable
            } else {
                Known(active.exact())
            };
            (matched, active)
        }
    }
}
fn trade(r: &Ref) -> Option<&SourceNeutralTradeLifecycleResultV2<Identity>> {
    match r.record() {
        NormalizationRecord::Evidence {
            evidence: SourceNeutralEvidenceKind::TradeLifecycle { result },
            ..
        } => Some(result),
        NormalizationRecord::Evidence {
            evidence: SourceNeutralEvidenceKind::TradeLifecycleV3 { result },
            ..
        } => Some(&result.result),
        _ => None,
    }
}
fn order_condition(o: &Order) -> Option<&ConditionId> {
    if let Some(r) = &o.formed
        && let NormalizationRecord::Evidence {
            evidence: SourceNeutralEvidenceKind::OrderLifecycle { result },
            ..
        } = r.record()
    {
        return Some(&result.condition_id);
    };
    match o.accepted.as_ref()?.record() {
        NormalizationRecord::Evidence {
            evidence:
                SourceNeutralEvidenceKind::Control {
                    result:
                        SourceNeutralControlResult::Submission {
                            outcome: SourceNeutralSubmissionOutcomeV2::Accepted { order, .. },
                        },
                },
            ..
        } => Some(&order.condition_id),
        _ => None,
    }
}
fn final_disposition(d: &SourceNeutralTradeEconomicDispositionV1) -> bool {
    matches!(
        d,
        SourceNeutralTradeEconomicDispositionV1::FinalSuccess
            | SourceNeutralTradeEconomicDispositionV1::FinalFailureUnapplied
            | SourceNeutralTradeEconomicDispositionV1::FinalFailureCompensated
    )
}
fn source_sequence(r: &Ref) -> u64 {
    match r.record().source_reference().record {
        SimV2SourceRecordReferenceV1::DirectResponse {
            private_feedback_sequence,
            ..
        }
        | SimV2SourceRecordReferenceV1::OrderLifecycle {
            private_feedback_sequence,
            ..
        }
        | SimV2SourceRecordReferenceV1::TradeLifecycle {
            private_feedback_sequence,
            ..
        }
        | SimV2SourceRecordReferenceV1::SettlementLifecycle {
            private_feedback_sequence,
            ..
        } => private_feedback_sequence,
        _ => 0,
    }
}
fn replace_source(slot: &mut Option<Ref>, r: &Ref) {
    if slot
        .as_ref()
        .is_none_or(|p| source_sequence(p) < source_sequence(r))
    {
        *slot = Some(r.clone());
    }
}
fn replace_delivery(slot: &mut Option<Ref>, r: &Ref) {
    let seq = |r: &Ref| match r.visibility() {
        NoeConsumptionVisibilityV1::Initialization
        | NoeConsumptionVisibilityV1::ControlAdmission => 0,
        NoeConsumptionVisibilityV1::Feedback {
            delivery_sequence, ..
        } => *delivery_sequence,
    };
    if slot.as_ref().is_none_or(|p| seq(p) < seq(r)) {
        *slot = Some(r.clone());
    }
}
fn decimal(v: &ExactDecimal) -> Decimal {
    v.0.parse().expect("NOE规范化精确数值")
}
fn exact(v: Decimal) -> ExactDecimal {
    ExactDecimal(v.normalize().to_string())
}
fn sum(
    values: impl Iterator<Item = ExecutionReadValueV1<ExactDecimal>>,
) -> ExecutionReadValueV1<ExactDecimal> {
    let mut total = crate::quantity::Quantity::zero();
    for value in values {
        let Known(v) = value else { return Unavailable };
        total.add(&crate::quantity::Quantity::parse(&v));
    }
    Known(total.exact())
}
