use crate::quantity::Quantity;
use ::execution_normalization::{
    MemoryLogicalIdentityV1 as Identity, NoeConsumedRecordRef as Ref, NoeConsumptionVisibilityV1,
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
    pub(crate) fn intent_arc(&self) -> Arc<ModelIntentV1> {
        Arc::clone(&self.intent)
    }
}
#[derive(Clone)]
pub struct SubmittedLegReadV1<'a> {
    pub intent: &'a FrozenIntentRefV1,
    pub batch_item_ordinal: u16,
}
#[derive(Clone)]
pub struct ExecutionOrderRead<'a, R> {
    pub order_identity: &'a Identity,
    pub condition_id: Option<&'a ConditionId>,
    pub token_id: Option<&'a TokenId>,
    pub base_quantity_applicability: Option<bool>,
    pub latest_cancel_feedback: Option<&'a Ref<R>>,
    pub origin: Option<SubmittedLegReadV1<'a>>,
    pub accepted: Option<&'a Ref<R>>,
    pub formed: Option<&'a Ref<R>>,
    pub latest_lifecycle: Option<&'a Ref<R>>,
    pub latest_direct_feedback: Option<&'a Ref<R>>,
    pub matched_shares: ExecutionReadValueV1<ExactDecimal>,
    pub active_unfilled_shares: ExecutionReadValueV1<ExactDecimal>,
    pub cancellable: ExecutionReadValueV1<bool>,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Order<R> {
    origin: Option<(String, u16)>,
    accepted: Option<Ref<R>>,
    formed: Option<Ref<R>>,
    lifecycle: Option<Ref<R>>,
    direct: Option<Ref<R>>,
    known_query: Option<Ref<R>>,
    canceled: Option<Ref<R>>,
    cancellation: Option<Ref<R>>,
    trades: BTreeMap<Identity, Ref<R>>,
}
impl<R> Default for Order<R> {
    fn default() -> Self {
        Self {
            origin: None,
            accepted: None,
            formed: None,
            lifecycle: None,
            direct: None,
            known_query: None,
            canceled: None,
            cancellation: None,
            trades: BTreeMap::new(),
        }
    }
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Submitted<R> {
    origin: FrozenIntentRefV1,
    receipt: Option<MutationRequestReceiptV2>,
    completed: Option<Ref<R>>,
    cancel_targets: Vec<Ref<R>>,
    outcomes: BTreeMap<u16, Option<Identity>>,
}
/// 原 owner 的活动身份集合；不保存第二份订单或成交事实。
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct ActionPopulation {
    orders: BTreeSet<Identity>,
    commitments: BTreeSet<String>,
    cancellations: BTreeSet<String>,
}
/// 普通已稳定 NOE 读取不能直接传入共享登记；这不是公开生产者接口的来源认证。
/// ```compile_fail
/// fn observe(registry: &mut shared_run::SharedExecutionRegistryV1,
///            view: &execution_normalization::NoeDurableRecordViewV1) {
///     registry.observe(view);
/// }
/// ```
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SharedExecutionRegistry<R> {
    context: Arc<ExecutionNormalizationContextV1>,
    intents: BTreeMap<String, Submitted<R>>,
    orders: BTreeMap<Identity, Order<R>>,
    markets: BTreeMap<String, Arc<PublicTradingRuleContextV1>>,
    cancel_locations: BTreeMap<Identity, Ref<R>>,
    #[serde(default)]
    action_population: Option<ActionPopulation>,
}
impl<R: Clone + std::fmt::Debug + Eq> SharedExecutionRegistry<R> {
    pub fn new(context: Arc<ExecutionNormalizationContextV1>) -> Self {
        Self {
            context,
            intents: BTreeMap::new(),
            orders: BTreeMap::new(),
            markets: BTreeMap::new(),
            cancel_locations: BTreeMap::new(),
            action_population: Some(ActionPopulation::default()),
        }
    }
    pub(crate) fn context(&self) -> &ExecutionNormalizationContextV1 {
        &self.context
    }
    pub(crate) fn observed_market_count(&self) -> usize {
        self.markets.len()
    }
    pub(crate) fn observe_market(&mut self, rules: &Arc<PublicTradingRuleContextV1>) -> bool {
        if let std::collections::btree_map::Entry::Vacant(entry) =
            self.markets.entry(rules.market.condition_id.0.clone())
        {
            entry.insert(rules.clone());
            true
        } else {
            false
        }
    }
    pub(crate) fn action_snapshot(&self) -> Self {
        let mut orders: BTreeMap<_, _> = self
            .unfinished_orders()
            .map(|o| {
                (
                    o.order_identity.clone(),
                    self.orders[o.order_identity].clone(),
                )
            })
            .collect();
        let mut needed: BTreeSet<_> = self
            .all_commitments()
            .map(|r| r.origin.intent.global_request_id())
            .chain(
                orders
                    .values()
                    .filter_map(|o| o.origin.as_ref().map(|(id, _)| id.as_str())),
            )
            .map(str::to_owned)
            .collect();
        needed.extend(
            self.cancellation_intents()
                .filter(|r| {
                    r.receipt.is_some()
                        && r.completed.is_none()
                        && cancellation_scope(r.origin.intent()).is_some()
                })
                .map(|r| r.origin.global_request_id().to_owned()),
        );
        if let Some((request, _)) = self.latest_cancel_request(None) {
            needed.insert(request.origin.global_request_id().to_owned());
        }
        if let Known(objects) = self.cancel_objects() {
            for object in objects {
                if let Some((request, _)) =
                    self.latest_cancel_request(Some(&object.request_scope()))
                {
                    needed.insert(request.origin.global_request_id().to_owned());
                }
            }
        }
        // 保留请求的 outcomes 仍读取这些原订单；闭合腿的零承诺证据不能随观察人口裁掉。
        for id in &needed {
            for identity in self.intents[id].outcomes.values().flatten() {
                if let Some(state) = self.orders.get(identity) {
                    orders
                        .entry(identity.clone())
                        .or_insert_with(|| state.clone());
                }
            }
        }
        let mut cancel_locations = BTreeMap::new();
        // 原响应过滤需要其目标的市场证据；只保留仍被选中响应或在途请求引用的订单。
        for id in &needed {
            let request = &self.intents[id];
            let mut referenced = BTreeSet::new();
            if let Some(CancellationScopeV1::Orders(ids)) =
                cancellation_scope(request.origin.intent())
            {
                referenced.extend(ids.into_iter().map(|id| self.platform_order_identity(id)));
            }
            for target in &request.cancel_targets {
                if let NormalizationRecord::Evidence {
                    evidence:
                        SourceNeutralEvidenceKind::Control {
                            result: SourceNeutralControlResult::Cancellation { order_identity, .. },
                        },
                    ..
                } = target.record()
                {
                    referenced.insert(order_identity.clone());
                }
            }
            for identity in referenced {
                if !orders.contains_key(&identity) {
                    if let Some(state) = self.orders.get(&identity)
                        && let Some(record) = location_records(state)
                            .find(|r| record_location(r, &identity).is_some())
                    {
                        cancel_locations
                            .entry(identity.clone())
                            .or_insert_with(|| record.clone());
                    }
                    if let Some(record) = self.cancel_locations.get(&identity) {
                        cancel_locations
                            .entry(identity)
                            .or_insert_with(|| record.clone());
                    }
                }
            }
        }
        let intents = needed
            .into_iter()
            .filter_map(|id| self.intents.get(&id).map(|s| (id, s.clone())))
            .collect();
        Self {
            context: self.context.clone(),
            intents,
            orders,
            markets: self.markets.clone(),
            cancel_locations,
            action_population: None, // 小型动作快照只包含本次所需原事实。
        }
    }
    /// 原请求身份已经绑定的不可变意图；供批次边界保持身份与原值一致。
    pub fn frozen_intent(&self, id: &str) -> Option<&FrozenIntentRefV1> {
        self.intents.get(id).map(|s| &s.origin)
    }
    /// 原SIM接收回执或原NOE提交响应证明该全局请求已经接纳；不复制回执状态。
    pub(crate) fn has_sim_submission_evidence(&self, id: &str) -> bool {
        self.intents
            .get(id)
            .is_some_and(|s| s.receipt.is_some() || !s.outcomes.is_empty())
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
                completed: None,
                cancel_targets: Vec::new(),
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
        self.refresh_request(origin.global_request_id());
    }
    pub fn observe(&mut self, record: &Ref<R>) {
        let NormalizationRecord::Evidence { evidence, .. } = record.record() else {
            return;
        };
        let touched: Vec<Identity> = match evidence {
            SourceNeutralEvidenceKind::Control { result } => match result {
                SourceNeutralControlResult::Submission {
                    outcome: SourceNeutralSubmissionOutcomeV2::Accepted { order, .. },
                } => vec![order.order_identity.clone()],
                SourceNeutralControlResult::Cancellation { order_identity, .. } => {
                    vec![order_identity.clone()]
                }
                SourceNeutralControlResult::OrderQuery {
                    result: SourceNeutralOrderQueryResult::Found { order },
                } => vec![order.order_identity.clone()],
                SourceNeutralControlResult::OrdersQuery {
                    result: SourceNeutralOrdersQueryResult::Page(page),
                } => page
                    .items
                    .iter()
                    .map(|o| o.order_identity.clone())
                    .collect(),
                _ => vec![],
            },
            SourceNeutralEvidenceKind::OrderLifecycle { result } => {
                vec![result.order_identity.clone()]
            }
            SourceNeutralEvidenceKind::TradeLifecycle { result } => {
                vec![result.order_identity.clone()]
            }
            SourceNeutralEvidenceKind::TradeLifecycleV3 { result } => {
                vec![result.result.order_identity.clone()]
            }
            _ => vec![],
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
                SourceNeutralControlResult::Cancellation {
                    order_identity,
                    outcome,
                } => {
                    let state = self.orders.entry(order_identity.clone()).or_default();
                    replace_delivery(&mut state.direct, record);
                    replace_delivery(&mut state.cancellation, record);
                    if matches!(outcome, SourceNeutralCancellationOutcomeV1::Canceled) {
                        replace_source(&mut state.canceled, record);
                    }
                    if let Some(id) = cancellation_request_id(record)
                        && let Some(request) = self.intents.get_mut(id)
                        && !request
                            .cancel_targets
                            .iter()
                            .any(|r| r.position() == record.position())
                    {
                        request.cancel_targets.push(record.clone());
                    }
                }
                SourceNeutralControlResult::CancellationRequestCompleted { .. } => {
                    if let Some(id) = cancellation_request_id(record)
                        && let Some(request) = self.intents.get_mut(id)
                    {
                        replace_delivery(&mut request.completed, record);
                    }
                }
                SourceNeutralControlResult::OrderQuery {
                    result: SourceNeutralOrderQueryResult::Found { order },
                } => {
                    let state = self.orders.entry(order.order_identity.clone()).or_default();
                    replace_delivery(&mut state.direct, record);
                    if !query_is_terminal(state, &order.order_identity) {
                        replace_delivery(&mut state.known_query, record);
                    }
                }
                SourceNeutralControlResult::OrdersQuery {
                    result: SourceNeutralOrdersQueryResult::Page(page),
                } => {
                    for order in &page.items {
                        let state = self.orders.entry(order.order_identity.clone()).or_default();
                        replace_delivery(&mut state.direct, record);
                        if !query_is_terminal(state, &order.order_identity) {
                            replace_delivery(&mut state.known_query, record);
                        }
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
        for identity in touched {
            self.refresh_order(&identity);
        }
        if let SimV2SourceRecordReferenceV1::DirectResponse {
            association:
                SimV2DirectResponseAssociationV1::SubmissionItem {
                    global_request_id, ..
                },
            ..
        } = &record.record().source_reference().record
        {
            self.refresh_request(global_request_id);
        }
        if let Some(id) = cancellation_request_id(record) {
            self.refresh_request(&id);
        }
    }
    fn unfinished(&self, id: &Identity, state: &Order<R>) -> bool {
        !matches!(self.order_from(id, state).cancellable, Known(false))
            || state.trades.values().filter_map(trade)
                .any(|t| !final_disposition(&t.economic_disposition))
            // 不可撤不代表原关联、累计成交覆盖和承诺已经闭合。
            || state.origin.as_ref()
                .and_then(|(request, ordinal)| self.intents.get(request)
                    .and_then(|s| leg(s.origin.intent(), *ordinal)))
                .is_none_or(|(_, leg)| !matches!(self.remaining(id, &leg.order), Known(v) if decimal(&v).is_zero()))
    }
    pub(crate) fn unfinished_orders(&self) -> impl Iterator<Item = ExecutionOrderRead<'_, R>> {
        self.active_order_entries()
            .map(|(id, state)| self.order_from(id, state))
    }
    pub(crate) fn contains_unfinished_order(&self, id: &Identity) -> bool {
        self.orders
            .get(id)
            .is_some_and(|state| self.unfinished(id, state))
    }
    pub fn order(&self, id: &Identity) -> Option<ExecutionOrderRead<'_, R>> {
        let (id, state) = self.orders.get_key_value(id)?;
        Some(self.order_from(id, state))
    }
    fn order_from<'a>(
        &'a self,
        id: &'a Identity,
        state: &'a Order<R>,
    ) -> ExecutionOrderRead<'a, R> {
        let origin = state.origin.as_ref().and_then(|(id, ordinal)| {
            self.intents.get(id).map(|s| SubmittedLegReadV1 {
                intent: &s.origin,
                batch_item_ordinal: *ordinal,
            })
        });
        let (matched, mut active) = quantities(state, id);
        let base_quantity_applicability = measurement(state)
            .map(|m| matches!(m, SourceNeutralSubmissionMeasurementV1::Base { .. }))
            .or_else(|| query_base_applicability(state, id))
            .or_else(|| {
                origin
                    .as_ref()
                    .and_then(|o| leg(o.intent.intent(), o.batch_item_ordinal))
                    .map(|(_, leg)| {
                        matches!(
                            leg.order,
                            BuyOrderIntentV1::GtcLimit { .. } | BuyOrderIntentV1::GtdLimit { .. }
                        )
                    })
            });
        if base_quantity_applicability == Some(false) {
            active = NotApplicable;
        } else if base_quantity_applicability == Some(true) && is_closed(state, id) {
            active = Known(exact(Decimal::ZERO));
        }

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
        let cancellable = if is_closed(state, id) || is_quote(state) || accepted_matched {
            Known(false)
        } else {
            match &active {
                Known(q) => Known(decimal(q) > Decimal::ZERO),
                _ => Unavailable,
            }
        };
        let location = order_location(state, id);
        ExecutionOrderRead {
            order_identity: id,
            condition_id: location.map(|x| x.0),
            token_id: location.map(|x| x.1),
            base_quantity_applicability,
            latest_cancel_feedback: state.cancellation.as_ref(),
            origin,
            accepted: state.accepted.as_ref(),
            formed: state.formed.as_ref(),
            latest_lifecycle: state.lifecycle.as_ref(),
            latest_direct_feedback: state.direct.as_ref(),
            matched_shares: matched,
            active_unfilled_shares: active,
            cancellable,
        }
    }
    pub fn orders(&self) -> impl Iterator<Item = ExecutionOrderRead<'_, R>> {
        self.orders
            .iter()
            .map(|(id, state)| self.order_from(id, state))
    }
    pub fn order_trades(&self, id: &Identity) -> impl Iterator<Item = &Ref<R>> {
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
        self.commitment_intents()
            .flat_map(|s| self.request_commitments(s))
    }
    fn request_commitments<'a>(
        &'a self,
        s: &'a Submitted<R>,
    ) -> impl Iterator<Item = IntentCommitmentReadV1<'a>> {
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
    }
    fn active_order_entries(&self) -> Box<dyn Iterator<Item = (&Identity, &Order<R>)> + '_> {
        if let Some(population) = &self.action_population {
            Box::new(
                population
                    .orders
                    .iter()
                    .filter_map(|id| self.orders.get_key_value(id)),
            )
        } else {
            Box::new(
                self.orders
                    .iter()
                    .filter(|(id, state)| self.unfinished(id, state)),
            )
        }
    }
    fn commitment_intents(&self) -> Box<dyn Iterator<Item = &Submitted<R>> + '_> {
        if let Some(population) = &self.action_population {
            Box::new(
                population
                    .commitments
                    .iter()
                    .filter_map(|id| self.intents.get(id)),
            )
        } else {
            Box::new(self.intents.values().filter(|s| s.receipt.is_some()))
        }
    }
    fn cancellation_intents(&self) -> Box<dyn Iterator<Item = &Submitted<R>> + '_> {
        if let Some(population) = &self.action_population {
            Box::new(
                population
                    .cancellations
                    .iter()
                    .filter_map(|id| self.intents.get(id)),
            )
        } else {
            Box::new(
                self.intents
                    .values()
                    .filter(|s| cancellation_scope(s.origin.intent()).is_some()),
            )
        }
    }
    fn refresh_order(&mut self, identity: &Identity) {
        let Some(state) = self.orders.get(identity) else {
            return;
        };
        let active = self.unfinished(identity, state);
        let origin = state.origin.as_ref().map(|(id, _)| id.clone());
        if let Some(population) = &mut self.action_population {
            if active {
                population.orders.insert(identity.clone());
            } else {
                population.orders.remove(identity);
            }
        }
        if let Some(id) = origin {
            self.refresh_request(&id);
        }
    }
    fn refresh_request(&mut self, id: &str) {
        let Some(request) = self.intents.get(id) else {
            return;
        };
        let commitment =
            request.receipt.is_some() && self.request_commitments(request).next().is_some();
        let cancel = cancellation_scope(request.origin.intent()).is_some();
        if let Some(population) = &mut self.action_population {
            if commitment {
                population.commitments.insert(id.to_owned());
            } else {
                population.commitments.remove(id);
            }
            if cancel {
                population.cancellations.insert(id.to_owned());
            }
        }
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
        let (_, active) = quantities(order, id);
        sum(std::iter::once(active).chain(
            order
                .trades
                .values()
                .filter_map(trade)
                .filter(|t| !final_disposition(&t.economic_disposition))
                .map(|t| Known(t.base_quantity_shares.clone())),
        ))
    }
    fn processing_key(&self, order: &ExecutionOrderRead<'_, R>) -> Option<(i64, u64, u16)> {
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

    pub fn cancel_objects(&self) -> ExecutionReadValueV1<Vec<CancelObjectRead<'_, R>>> {
        self.cancel_objects_in(self.markets.values().map(|m| &m.market.condition_id))
    }
    pub fn cancel_objects_in<'a>(
        &'a self,
        conditions: impl IntoIterator<Item = &'a ConditionId>,
    ) -> ExecutionReadValueV1<Vec<CancelObjectRead<'a, R>>> {
        let eligible: BTreeMap<_, _> = self
            .orders
            .iter()
            .filter_map(|(id, state)| {
                if !(state.accepted.is_some()
                    || state.formed.is_some()
                    || state.lifecycle.is_some()
                    || state.known_query.is_some()
                    || !state.trades.is_empty()
                    || state.canceled.is_some())
                {
                    return None;
                }
                let order = self.order_from(id, state);
                (!matches!(order.cancellable, Known(false))).then_some((id, order))
            })
            .collect();
        let mut groups: BTreeMap<Option<(i64, &str)>, Vec<&ExecutionOrderRead<'_, R>>> =
            BTreeMap::new();
        for order in eligible.values() {
            let key = order
                .condition_id
                .and_then(|condition| self.markets.get(&condition.0))
                .map(|market| {
                    (
                        market.market.window.start_at_utc_ms,
                        market.market.condition_id.0.as_str(),
                    )
                });
            groups.entry(key).or_default().push(order);
        }
        let unknown = groups.remove(&None).unwrap_or_default();
        let mut ordered = Vec::new();
        for members in groups.into_values().chain(std::iter::once(unknown)) {
            ordered.extend(self.formation_order(&members));
        }
        let rank: BTreeMap<_, _> = ordered
            .iter()
            .enumerate()
            .map(|(rank, id)| (*id, rank))
            .collect();
        let mut objects: Vec<_> = ordered
            .iter()
            .map(|id| CancelObjectRead::Single {
                order: eligible[*id].clone(),
            })
            .collect();
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
                && let (Some(&a_rank), Some(&b_rank)) = (rank.get(a), rank.get(b))
            {
                let mut key = [a_rank, b_rank];
                key.sort();
                paired.push((
                    key,
                    &submitted.origin,
                    [eligible[a].clone(), eligible[b].clone()],
                ));
            }
        }
        paired.sort_by_key(|(key, _, _)| *key);
        objects.extend(
            paired
                .into_iter()
                .map(|(_, origin, orders)| CancelObjectRead::Paired { origin, orders }),
        );
        let mut markets: Vec<_> = conditions
            .into_iter()
            .filter_map(|c| self.markets.get(&c.0))
            .collect();
        markets.sort_by_key(|m| (m.market.window.start_at_utc_ms, &m.market.condition_id.0));
        markets.dedup_by_key(|m| &m.market.condition_id.0);
        for market in markets {
            let condition_id = &market.market.condition_id;
            let orders = ordered
                .iter()
                .map(|id| &eligible[*id])
                .filter(|o| o.condition_id == Some(condition_id))
                .cloned()
                .collect();
            objects.push(CancelObjectRead::Market {
                condition_id,
                orders,
            });
        }
        Known(objects)
    }
    fn formation_order<'a>(&self, orders: &[&ExecutionOrderRead<'a, R>]) -> Vec<&'a Identity> {
        let mut processing: Vec<_> = orders
            .iter()
            .filter_map(|o| self.processing_key(o).map(|key| (key, o.order_identity)))
            .collect();
        let mut source: Vec<_> = orders
            .iter()
            .filter_map(|o| o.formed.map(|r| (source_sequence(r), o.order_identity)))
            .collect();
        processing.sort();
        source.sort();
        let evidenced: BTreeSet<_> = processing
            .iter()
            .map(|(_, id)| *id)
            .chain(source.iter().map(|(_, id)| *id))
            .collect();
        let mut chains = Vec::new();
        if processing.len() == evidenced.len() {
            chains.push(processing.into_iter().map(|(_, id)| id).collect::<Vec<_>>());
        } else if source.len() == evidenced.len() {
            chains.push(source.into_iter().map(|(_, id)| id).collect::<Vec<_>>());
        } else {
            chains.push(processing.into_iter().map(|(_, id)| id).collect::<Vec<_>>());
            chains.push(source.into_iter().map(|(_, id)| id).collect::<Vec<_>>());
        }
        let mut predecessors: BTreeMap<_, usize> =
            orders.iter().map(|o| (o.order_identity, 0)).collect();
        let mut edges: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
        for chain in chains {
            for pair in chain.windows(2) {
                if edges.entry(pair[0]).or_default().insert(pair[1]) {
                    *predecessors.get_mut(pair[1]).unwrap() += 1;
                }
            }
        }
        let mut ready: BTreeSet<_> = predecessors
            .iter()
            .filter_map(|(id, n)| (*n == 0).then_some(*id))
            .collect();
        let mut ordered = Vec::with_capacity(orders.len());
        while let Some(id) = ready.pop_first() {
            ordered.push(id);
            for next in edges.get(id).into_iter().flatten() {
                let n = predecessors.get_mut(next).unwrap();
                *n -= 1;
                if *n == 0 {
                    ready.insert(*next);
                }
            }
        }
        ordered
    }
}
pub struct IntentCommitmentReadV1<'a> {
    pub origin: SubmittedLegReadV1<'a>,
    pub order_identity: Option<&'a Identity>,
    pub remaining_target_shares: ExecutionReadValueV1<ExactDecimal>,
}
pub enum CancelObjectRead<'a, R> {
    Single {
        order: ExecutionOrderRead<'a, R>,
    },
    Paired {
        origin: &'a FrozenIntentRefV1,
        orders: [ExecutionOrderRead<'a, R>; 2],
    },
    Market {
        condition_id: &'a ConditionId,
        orders: Vec<ExecutionOrderRead<'a, R>>,
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
fn measurement<R: Clone + std::fmt::Debug + Eq>(
    order: &Order<R>,
) -> Option<&SourceNeutralSubmissionMeasurementV1> {
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
fn is_quote<R: Clone + std::fmt::Debug + Eq>(o: &Order<R>) -> bool {
    matches!(
        measurement(o),
        Some(SourceNeutralSubmissionMeasurementV1::QuoteBudget { .. })
    )
}
fn is_closed<R: Clone + std::fmt::Debug + Eq>(o: &Order<R>, id: &Identity) -> bool {
    query_is_terminal(o, id)
        || o.canceled.is_some()
        || matches!(
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
fn quantities<R: Clone + std::fmt::Debug + Eq>(
    o: &Order<R>,
    id: &Identity,
) -> (
    ExecutionReadValueV1<ExactDecimal>,
    ExecutionReadValueV1<ExactDecimal>,
) {
    let Some(base) = measurement(o) else {
        return (Unavailable, Unavailable);
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
            let active = if is_closed(o, id) {
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
fn trade<R: Clone + std::fmt::Debug + Eq>(
    r: &Ref<R>,
) -> Option<&SourceNeutralTradeLifecycleResultV2<Identity>> {
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
fn location_records<R>(o: &Order<R>) -> impl Iterator<Item = &Ref<R>> {
    o.formed
        .iter()
        .chain(o.lifecycle.iter())
        .chain(o.accepted.iter())
        .chain(o.trades.values())
        .chain(o.known_query.iter())
}
fn order_location<'a, R: Clone + std::fmt::Debug + Eq>(
    o: &'a Order<R>,
    id: &Identity,
) -> Option<(&'a ConditionId, &'a TokenId)> {
    location_records(o).find_map(|r| record_location(r, id))
}
fn record_location<'a, R: Clone + std::fmt::Debug + Eq>(
    r: &'a Ref<R>,
    id: &Identity,
) -> Option<(&'a ConditionId, &'a TokenId)> {
    let NormalizationRecord::Evidence { evidence, .. } = r.record() else {
        return None;
    };
    match evidence {
        SourceNeutralEvidenceKind::OrderLifecycle { result } => {
            Some((&result.condition_id, &result.token_id))
        }
        SourceNeutralEvidenceKind::TradeLifecycle { result } => {
            Some((&result.condition_id, &result.token_id))
        }
        SourceNeutralEvidenceKind::TradeLifecycleV3 { result } => {
            Some((&result.result.condition_id, &result.result.token_id))
        }
        SourceNeutralEvidenceKind::Control {
            result:
                SourceNeutralControlResult::Submission {
                    outcome: SourceNeutralSubmissionOutcomeV2::Accepted { order, .. },
                },
        } => Some((&order.condition_id, &order.token_id)),
        SourceNeutralEvidenceKind::Control {
            result:
                SourceNeutralControlResult::OrderQuery {
                    result: SourceNeutralOrderQueryResult::Found { order },
                },
        } => Some((&order.condition_id, &order.token_id)),
        SourceNeutralEvidenceKind::Control {
            result:
                SourceNeutralControlResult::OrdersQuery {
                    result: SourceNeutralOrdersQueryResult::Page(page),
                },
        } => page
            .items
            .iter()
            .find(|o| &o.order_identity == id)
            .map(|o| (&o.condition_id, &o.token_id)),
        _ => None,
    }
}
fn cancellation_request_id<R: Clone + std::fmt::Debug + Eq>(record: &Ref<R>) -> Option<&str> {
    match &record.record().source_reference().record {
        SimV2SourceRecordReferenceV1::DirectResponse {
            association:
                SimV2DirectResponseAssociationV1::Request { global_request_id }
                | SimV2DirectResponseAssociationV1::CancellationTarget {
                    global_request_id, ..
                },
            ..
        } => Some(global_request_id),
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
fn source_sequence<R: Clone + std::fmt::Debug + Eq>(r: &Ref<R>) -> u64 {
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
fn replace_source<R: Clone + std::fmt::Debug + Eq>(slot: &mut Option<Ref<R>>, r: &Ref<R>) {
    if slot
        .as_ref()
        .is_none_or(|p| source_sequence(p) < source_sequence(r))
    {
        *slot = Some(r.clone());
    }
}
fn replace_delivery<R: Clone + std::fmt::Debug + Eq>(slot: &mut Option<Ref<R>>, r: &Ref<R>) {
    if slot
        .as_ref()
        .is_none_or(|p| delivery_key(p) < delivery_key(r))
    {
        *slot = Some(r.clone());
    }
}
fn delivery_key<R: Clone + std::fmt::Debug + Eq>(r: &Ref<R>) -> (u64, u64) {
    let sequence = match r.visibility() {
        NoeConsumptionVisibilityV1::Feedback {
            delivery_sequence, ..
        } => *delivery_sequence,
        _ => 0,
    };
    (sequence, r.position().record_ordinal)
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

pub type SharedExecutionRegistryV1 = SharedExecutionRegistry<NoCancellationRequestCompletionV0>;
pub type SharedExecutionRegistryV2 = SharedExecutionRegistry<CancellationRequestCompletedV1>;
pub type ExecutionOrderReadV1<'a> = ExecutionOrderRead<'a, NoCancellationRequestCompletionV0>;
pub type ExecutionOrderReadV2<'a> = ExecutionOrderRead<'a, CancellationRequestCompletedV1>;
pub type CancelObjectReadV1<'a> = CancelObjectRead<'a, NoCancellationRequestCompletionV0>;
pub type CancelObjectReadV2<'a> = CancelObjectRead<'a, CancellationRequestCompletedV1>;

/// 请求域来自冻结意图；市场域仅由调用方同组 c/O 绑定提供。
#[derive(Clone)]
pub enum CancellationScopeV1<'a> {
    Orders(Vec<&'a str>),
    Market(&'a ConditionId),
}
impl<'a, R> CancelObjectRead<'a, R> {
    pub fn request_scope(&self) -> CancellationScopeV1<'a> {
        match self {
            Self::Single { order } => {
                CancellationScopeV1::Orders(vec![platform_id(order.order_identity)])
            }
            Self::Paired { orders, .. } => CancellationScopeV1::Orders(
                orders
                    .iter()
                    .map(|o| platform_id(o.order_identity))
                    .collect(),
            ),
            Self::Market { condition_id, .. } => CancellationScopeV1::Market(condition_id),
        }
    }
}
fn platform_id(id: &Identity) -> &str {
    match &id.key {
        ::execution_normalization::MemoryIdentityKeyV1::Entity(
            ::execution_normalization::SourceNeutralEntityKeyV1::Order { source_order_id },
        ) => source_order_id,
        _ => unreachable!("订单读取来自原订单身份"),
    }
}
fn cancellation_scope(intent: &ModelIntentV1) -> Option<CancellationScopeV1<'_>> {
    Some(match intent {
        ModelIntentV1::CancelOrder { platform_order_id } => {
            CancellationScopeV1::Orders(vec![platform_order_id])
        }
        ModelIntentV1::CancelOrders { platform_order_ids } => {
            CancellationScopeV1::Orders(platform_order_ids.iter().map(String::as_str).collect())
        }
        ModelIntentV1::CancelMarketOrders { condition_id } => {
            CancellationScopeV1::Market(condition_id)
        }
        _ => return None,
    })
}
impl<R: Clone + std::fmt::Debug + Eq> SharedExecutionRegistry<R> {
    fn platform_order_identity(&self, id: &str) -> Identity {
        Identity {
            account: Arc::new(self.context.account_identity.clone()),
            key: ::execution_normalization::MemoryIdentityKeyV1::Entity(
                ::execution_normalization::SourceNeutralEntityKeyV1::Order {
                    source_order_id: id.to_owned(),
                },
            ),
        }
    }
    fn condition_for_platform_id(&self, id: &str) -> Option<&ConditionId> {
        let identity = self.platform_order_identity(id);
        self.orders
            .get_key_value(&identity)
            .and_then(|(identity, state)| order_location(state, identity).map(|(c, _)| c))
            .or_else(|| {
                self.cancel_locations
                    .get_key_value(&identity)
                    .and_then(|(identity, r)| record_location(r, identity).map(|(c, _)| c))
            })
    }
    fn relation(&self, a: &CancellationScopeV1<'_>, b: &CancellationScopeV1<'_>) -> Option<bool> {
        match (a, b) {
            (CancellationScopeV1::Orders(a), CancellationScopeV1::Orders(b)) => {
                Some(a.iter().any(|id| b.contains(id)))
            }
            (CancellationScopeV1::Market(a), CancellationScopeV1::Market(b)) => Some(a == b),
            (CancellationScopeV1::Orders(ids), CancellationScopeV1::Market(condition))
            | (CancellationScopeV1::Market(condition), CancellationScopeV1::Orders(ids)) => {
                let mut unknown = false;
                for id in ids {
                    match self.condition_for_platform_id(id) {
                        Some(c) if c == *condition => return Some(true),
                        Some(_) => {}
                        None => unknown = true,
                    }
                }
                (!unknown).then_some(false)
            }
        }
    }
    fn latest_cancel_request(
        &self,
        scope: Option<&CancellationScopeV1<'_>>,
    ) -> Option<(&Submitted<R>, bool)> {
        self.cancellation_intents()
            .filter_map(|request| {
                let request_scope = cancellation_scope(request.origin.intent())?;
                let complete = request.completed.as_ref()?;
                let relation = scope.map_or(Some(true), |s| self.relation(s, &request_scope));
                (relation != Some(false)).then_some((
                    request,
                    relation.is_some(),
                    delivery_key(complete),
                ))
            })
            .max_by_key(|(_, _, key)| *key)
            .map(|(request, known, _)| (request, known))
    }
}

pub struct CancellationRequestsReadV1<'a> {
    pub pending_count: ExecutionReadValueV1<u64>,
    pub response_counts: [ExecutionReadValueV1<u64>; 3],
    /// None 为从未收到；Unavailable 为最新可能相关响应无法确定。
    pub latest_response: ExecutionReadValueV1<Option<&'a Ref<CancellationRequestCompletedV1>>>,
}
/// 从原请求及完成事实只读累计。有SIM回执的请求与无回执注册分列；回执不证明平台接受。
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ExecutionRequestTotalsReadV1 {
    pub registered_requests_without_receipt: u64,
    pub receipted_submission_requests: u64,
    pub receipted_submission_requested_legs: u64,
    pub receipted_cancellation_requests: u64,
    pub completed_receipted_cancellation_requests: u64,
    pub receipted_cancellation_response_counts: [u64; 3],
}
impl SharedExecutionRegistryV2 {
    pub fn cumulative_request_totals(&self) -> ExecutionRequestTotalsReadV1 {
        let mut totals = ExecutionRequestTotalsReadV1::default();
        for request in self.intents.values() {
            if request.receipt.is_none() {
                totals.registered_requests_without_receipt += 1;
                continue;
            }
            match request.origin.intent() {
                ModelIntentV1::SubmitSingleBuy { .. } => {
                    totals.receipted_submission_requests += 1;
                    totals.receipted_submission_requested_legs += 1;
                }
                ModelIntentV1::SubmitPairedBuy { .. } => {
                    totals.receipted_submission_requests += 1;
                    totals.receipted_submission_requested_legs += 2;
                }
                _ => {
                    totals.receipted_cancellation_requests += 1;
                    if let Some(complete) = &request.completed {
                        if let NormalizationRecord::Evidence {
                            evidence:
                                SourceNeutralEvidenceKind::Control {
                                    result:
                                        SourceNeutralControlResult::CancellationRequestCompleted {
                                            result,
                                        },
                                },
                            ..
                        } = complete.record()
                        {
                            totals.completed_receipted_cancellation_requests += 1;
                            totals.receipted_cancellation_response_counts[0] +=
                                result.canceled_count;
                            totals.receipted_cancellation_response_counts[1] +=
                                result.not_canceled_no_effect_count;
                            totals.receipted_cancellation_response_counts[2] +=
                                result.evidence_unknown_no_effect_count;
                        }
                    }
                }
            }
        }
        totals
    }
    /// 只读相关在途请求数量，不枚举历史完成响应。
    pub fn pending_cancellation_count(
        &self,
        scope: Option<&CancellationScopeV1<'_>>,
    ) -> ExecutionReadValueV1<u64> {
        let mut pending = 0;
        let mut pending_known = true;
        for request in self
            .cancellation_intents()
            .filter(|r| r.receipt.is_some() && r.completed.is_none())
        {
            let Some(request_scope) = cancellation_scope(request.origin.intent()) else {
                continue;
            };
            match scope.map_or(Some(true), |s| self.relation(s, &request_scope)) {
                Some(true) => pending += 1,
                Some(false) => {}
                None => pending_known = false,
            }
        }
        if pending_known {
            Known(pending)
        } else {
            Unavailable
        }
    }
    pub fn cancellation_requests(
        &self,
        scope: Option<&CancellationScopeV1<'_>>,
    ) -> CancellationRequestsReadV1<'_> {
        let mut read = CancellationRequestsReadV1 {
            pending_count: self.pending_cancellation_count(scope),
            response_counts: [NotApplicable, NotApplicable, NotApplicable],
            latest_response: Known(None),
        };
        let Some((request, known)) = self.latest_cancel_request(scope) else {
            return read;
        };
        if !known {
            read.response_counts = [Unavailable, Unavailable, Unavailable];
            read.latest_response = Unavailable;
            return read;
        }
        let complete = request.completed.as_ref().unwrap();
        read.latest_response = Known(Some(complete));
        if let Some(scope) = scope {
            let mut counts = [0; 3];
            let mut known = [true; 3];
            for target in &request.cancel_targets {
                if let NormalizationRecord::Evidence {
                    evidence:
                        SourceNeutralEvidenceKind::Control {
                            result:
                                SourceNeutralControlResult::Cancellation {
                                    order_identity,
                                    outcome,
                                },
                        },
                    ..
                } = target.record()
                {
                    let index = match outcome {
                        SourceNeutralCancellationOutcomeV1::Canceled => 0,
                        SourceNeutralCancellationOutcomeV1::NotCanceledNoEffect => 1,
                        SourceNeutralCancellationOutcomeV1::EvidenceUnknownNoEffect => 2,
                    };
                    match self.relation(
                        scope,
                        &CancellationScopeV1::Orders(vec![platform_id(order_identity)]),
                    ) {
                        Some(true) => counts[index] += 1,
                        Some(false) => {}
                        None => known[index] = false,
                    }
                }
            }
            read.response_counts = std::array::from_fn(|i| {
                if known[i] {
                    Known(counts[i])
                } else {
                    Unavailable
                }
            });
        } else if let NormalizationRecord::Evidence {
            evidence:
                SourceNeutralEvidenceKind::Control {
                    result: SourceNeutralControlResult::CancellationRequestCompleted { result },
                },
            ..
        } = complete.record()
        {
            read.response_counts = [
                Known(result.canceled_count),
                Known(result.not_canceled_no_effect_count),
                Known(result.evidence_unknown_no_effect_count),
            ];
        }
        read
    }
}

// 已到终态是单调关闭证据；后到旧 Open 只更新 direct，不覆盖这份原查询引用。
fn query_is_terminal<R: Clone + std::fmt::Debug + Eq>(order: &Order<R>, id: &Identity) -> bool {
    query_snapshot(order, id).is_some_and(|snapshot| {
        matches!(
            snapshot.state,
            SourceNeutralOrderStateV1::Matched
                | SourceNeutralOrderStateV1::VenueDustTerminal
                | SourceNeutralOrderStateV1::MarketResolvedCanceled
                | SourceNeutralOrderStateV1::Canceled
                | SourceNeutralOrderStateV1::Expired
        )
    })
}
fn query_base_applicability<R: Clone + std::fmt::Debug + Eq>(
    order: &Order<R>,
    id: &Identity,
) -> Option<bool> {
    Some(matches!(
        query_snapshot(order, id)?.quantities,
        SourceNeutralOrderQuantityEvidenceV1::Base { .. }
    ))
}
fn query_snapshot<'a, R: Clone + std::fmt::Debug + Eq>(
    order: &'a Order<R>,
    id: &Identity,
) -> Option<&'a SourceNeutralOrderSnapshotV2<Identity>> {
    let NormalizationRecord::Evidence {
        evidence: SourceNeutralEvidenceKind::Control { result },
        ..
    } = order.known_query.as_ref()?.record()
    else {
        return None;
    };
    let snapshot = match result {
        SourceNeutralControlResult::OrderQuery {
            result: SourceNeutralOrderQueryResult::Found { order },
        } => order,
        SourceNeutralControlResult::OrdersQuery {
            result: SourceNeutralOrdersQueryResult::Page(page),
        } => page.items.iter().find(|o| &o.order_identity == id)?,
        _ => return None,
    };
    Some(snapshot)
}
