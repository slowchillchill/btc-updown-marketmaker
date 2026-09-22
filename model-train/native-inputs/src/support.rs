//! #6 C/F.4/H.2：唯一精确完成性与前缀推进；模型分数不参与支持集。
use crate::{InputError, NativeActionContext, NativeInputGroup, exact::*, prefix::*};
use contracts::ModelIntentV1;
use contracts::{ExactDecimal, ExecutionReadValueV1, economics::EconomicRational as R};
use num_bigint::{BigInt, BigUint};
use num_integer::Integer;
use num_traits::{One, ToPrimitive, Zero};
use std::sync::Arc;

pub fn encoding_limit() -> BigUint {
    ((BigUint::one() << 96) - BigUint::one()) / 10_000u32
}
fn floor_positive(r: &R) -> BigUint {
    (r.numerator() / r.denominator())
        .to_biguint()
        .unwrap_or_default()
}
fn index(leg: Leg) -> usize {
    usize::from(leg == Leg::B)
}
fn leg(i: usize) -> Leg {
    if i == 0 { Leg::A } else { Leg::B }
}
fn variants() -> [Variant; 4] {
    [
        Variant::GtcLimit,
        Variant::GtdLimit,
        Variant::FokMarket,
        Variant::FakMarket,
    ]
}
fn families() -> [Family; 6] {
    [
        Family::NoTrade,
        Family::SubmitSingleBuy,
        Family::SubmitPairedBuy,
        Family::CancelOrder,
        Family::CancelOrders,
        Family::CancelMarketOrders,
    ]
}
fn lifetimes() -> [Lifetime; 3] {
    [
        Lifetime::Seconds180,
        Lifetime::Seconds240,
        Lifetime::Seconds300,
    ]
}

/// 只持有原组，金额和身份不传给 Python 重建；选择路径用于原条件概率重算。
pub struct NativeActor {
    pub group: Arc<NativeInputGroup>,
    pub row: usize,
    pub state: PrefixState,
    pub path: Vec<usize>,
    pub(crate) done: bool,
    pub(crate) cancel_index: Option<usize>,
}
impl NativeActor {
    pub fn new(group: Arc<NativeInputGroup>, row: usize) -> Result<Self, InputError> {
        group.rows[row]
            .action_context
            .as_ref()
            .ok_or(InputError::MissingAssociation)?;
        Ok(Self {
            group,
            row,
            state: PrefixState::default(),
            path: vec![],
            done: false,
            cancel_index: None,
        })
    }
    pub fn intent(&self) -> Result<Option<contracts::ModelIntentV1>, InputError> {
        if !self.done {
            return Err(InputError::UnsupportedChoice);
        }
        match self.state.family.unwrap() {
            Family::NoTrade => Ok(None),
            Family::SubmitSingleBuy | Family::SubmitPairedBuy => {
                self.group.buy_intent(self.row, &self.state).map(Some)
            }
            _ => self
                .group
                .cancel_index_intent(self.cancel_index.unwrap())
                .map(Some),
        }
    }
    pub async fn execute(
        &self,
        shared: &mut shared_run::SharedExecutionV2,
        decision: &mut sim_system::execution::PublicGroupDecisionV1<'_>,
        context: sim_system::execution::RequestContextV2,
    ) -> Result<
        Option<sim_system::execution::MutationRequestReceiptV2>,
        crate::action::ActionError<std::convert::Infallible>,
    > {
        if !self.done {
            return Err(crate::action::ActionError::Input(
                InputError::UnsupportedChoice,
            ));
        }
        let selected = match self.state.family.unwrap() {
            Family::NoTrade => crate::SelectedAction::NoTrade,
            Family::SubmitSingleBuy | Family::SubmitPairedBuy => {
                crate::SelectedAction::Buy(&self.state)
            }
            _ => crate::SelectedAction::CancelIndex(self.cancel_index.unwrap()),
        };
        self.group
            .execute_selected(self.row, selected, shared, decision, context, |_, _, _| {
                unreachable!("已选原对象索引")
            })
            .await
    }
    pub fn head_index(&self) -> usize {
        match self.state.stage {
            Stage::Family => 0,
            Stage::QuantityBranch => 1,
            Stage::Variant(_) => 2,
            Stage::PostOnly(_) => 4,
            Stage::Expiry(_) => 5,
            Stage::Price(_) => 3,
            Stage::CancelObject => 7,
        }
    }
    pub fn is_complete(&self) -> bool {
        self.done
    }
    pub fn prefix_features(&self) -> [f32; 60] {
        self.context().prefix_features(&self.state)
    }
    fn context(&self) -> &NativeActionContext {
        self.group.rows[self.row].action_context.as_ref().unwrap()
    }
    fn cash_micro(&self) -> BigUint {
        let c = &self.group.action_before.as_ref().unwrap().economics().cash;
        floor_positive(
            &decimal(&c.actual_available_pusd)
                .min(decimal(&c.maker_available_pusd))
                .mul(&integer(1_000_000)),
        )
    }
    fn repair(&self) -> Result<Option<(Leg, BigUint)>, InputError> {
        let read = self
            .group
            .action_before
            .as_ref()
            .ok_or(InputError::MissingAssociation)?;
        let condition = &self.group.rows[self.row].hidden_state_key.condition_id;
        let Some(market) = read
            .economics()
            .markets
            .iter()
            .find(|m| &m.condition_id == condition)
        else {
            return Ok(None);
        };
        let assets = market
            .assets
            .as_ref()
            .ok_or(InputError::MissingAssociation)?;
        for side in 0..2 {
            let at = assets
                .iter()
                .position(|a| a.outcome.index == side as u32)
                .ok_or(InputError::MissingAssociation)?;
            let gap = decimal(&market.repair_gap_shares[at]);
            if !gap.is_positive() {
                continue;
            }
            let mut committed = R::zero();
            for c in read.commitments() {
                let (market, order) = crate::values::leg(&c.origin)?;
                if market != condition || order.token_id != assets[at].token_id {
                    continue;
                }
                let ExecutionReadValueV1::Known(value) = c.remaining_target_shares else {
                    return Err(InputError::MissingAssociation);
                };
                committed = committed.add(&decimal(&value));
            }
            let cap = floor_positive(&gap.sub(&committed).mul(&integer(100)));
            if !cap.is_zero() {
                return Ok(Some((leg(side), cap)));
            }
        }
        Ok(None)
    }
    fn sides(&self, state: &PrefixState) -> Vec<usize> {
        if state.family == Some(Family::SubmitPairedBuy) {
            vec![0, 1]
        } else {
            state.single_leg.map(index).into_iter().collect()
        }
    }
    fn tick_units(&self, i: usize) -> Result<u32, InputError> {
        let t = self.context().ticks[i]
            .as_ref()
            .ok_or(InputError::MissingAssociation)?;
        Ok(floor_positive(&t.mul(&integer(10_000)))
            .to_u32()
            .expect("六档合法tick"))
    }
    fn tick(&self, i: usize) -> Result<ExactDecimal, InputError> {
        Ok(ExactDecimal(
            rust_decimal::Decimal::new(i64::from(self.tick_units(i)?), 4)
                .normalize()
                .to_string(),
        ))
    }
    fn max_price(&self, i: usize) -> Result<u16, InputError> {
        Ok((10_000 / self.tick_units(i)? - 1).min(9999) as u16)
    }
    fn buy_enabled(&self) -> bool {
        self.context().buy_enabled
    }
    pub fn root_upper(&self, family: Family) -> Result<BigUint, InputError> {
        if !self.buy_enabled() {
            return Ok(BigUint::zero());
        }
        let (sides, cap) = if family == Family::SubmitSingleBuy {
            let Some((side, cap)) = self.repair()? else {
                return Ok(BigUint::zero());
            };
            (vec![index(side)], Some(cap))
        } else {
            (vec![0, 1], None)
        };
        let tick_sum = sides
            .iter()
            .try_fold(0u32, |a, &i| Ok::<_, InputError>(a + self.tick_units(i)?))?;
        // P_micro=n*(k*tick*10000)，原现金root不能被N_enc裁剪。
        let upper = self.cash_micro() / tick_sum;
        Ok(cap.map_or(upper.clone(), |cap| upper.min(cap)))
    }
    pub fn has_completion(
        &self,
        lo: &BigUint,
        hi: &BigUint,
        state: &PrefixState,
    ) -> Result<bool, InputError> {
        if !self.buy_enabled() || lo > hi {
            return Ok(false);
        }
        let upper = self
            .root_upper(state.family.unwrap())?
            .min(hi.clone())
            .min(encoding_limit());
        let lower = lo.clone().max(BigUint::one());
        if lower > upper {
            return Ok(false);
        }
        if state.legs.iter().all(|s| s.variant.is_none()) && upper >= BigUint::from(500u32) {
            return Ok(true);
        }
        // 数量选择早于全部属性；小域最多499，不能用端点或单调性代替。
        let mut n = lower;
        let small_upper = upper.min(BigUint::from(499u32));
        while n <= small_upper {
            if self.fixed_completion(&n, state)? {
                return Ok(true);
            }
            n += 1u32;
        }
        Ok(false)
    }
    fn cost(
        &self,
        n: &BigUint,
        i: usize,
        k: u16,
        variant: Variant,
        post_only: bool,
    ) -> Result<(BigUint, BigUint), InputError> {
        let principal = n * BigUint::from(u32::from(k) * self.tick_units(i)?);
        let fee = if matches!(variant, Variant::GtcLimit | Variant::GtdLimit) {
            self.context()
                .snapshot
                .limit_buy_cost_v1(n, k, &self.tick(i)?, post_only)
                .map_err(InputError::Fee)?
                .fee_1e5_units
        } else {
            BigUint::zero()
        };
        Ok((principal, fee))
    }
    fn min_cost(
        &self,
        n: &BigUint,
        i: usize,
        s: &LegSelection,
    ) -> Result<Option<BigUint>, InputError> {
        let mut best = None;
        for v in variants()
            .into_iter()
            .filter(|v| s.variant.is_none_or(|selected| selected == *v))
        {
            if matches!(v, Variant::GtcLimit | Variant::GtdLimit) && n < &BigUint::from(500u32) {
                continue;
            }
            let min_k = if matches!(v, Variant::FokMarket | Variant::FakMarket) {
                let m = floor_positive(
                    &decimal(&self.context().rules.market_buy_min_spend_pusd)
                        .mul(&integer(1_000_000)),
                );
                m.div_ceil(&(n * self.tick_units(i)?)).max(BigUint::one())
            } else {
                BigUint::one()
            };
            let k = s.price_index.as_ref().unwrap_or(&min_k);
            if k < &min_k || k > &BigUint::from(self.max_price(i)?) {
                continue;
            }
            // 尚未选择post-only时true是该属性集合的零费最小成本见证。
            let (p, f) = self.cost(n, i, k.to_u16().unwrap(), v, s.post_only.unwrap_or(true))?;
            let cost = p + f * 10u32;
            best = Some(best.map_or(cost.clone(), |old: BigUint| old.min(cost)));
        }
        Ok(best)
    }
    pub fn fixed_completion(&self, n: &BigUint, state: &PrefixState) -> Result<bool, InputError> {
        if n.is_zero() || n > &encoding_limit() || n > &self.root_upper(state.family.unwrap())? {
            return Ok(false);
        }
        let mut total = BigUint::zero();
        for i in self.sides(state) {
            let Some(cost) = self.min_cost(n, i, &state.legs[i])? else {
                return Ok(false);
            };
            total += cost;
        }
        Ok(total <= self.cash_micro())
    }
    fn cancel_matches(family: Family, intent: &ModelIntentV1) -> bool {
        matches!(
            (family, intent),
            (Family::CancelOrder, ModelIntentV1::CancelOrder { .. })
                | (Family::CancelOrders, ModelIntentV1::CancelOrders { .. })
                | (
                    Family::CancelMarketOrders,
                    ModelIntentV1::CancelMarketOrders { .. }
                )
        )
    }
    fn choice_count(&self) -> usize {
        if self.done {
            return 0;
        }
        match self.state.stage {
            Stage::Family => 6,
            Stage::QuantityBranch | Stage::PostOnly(_) => 2,
            Stage::Variant(_) => 4,
            Stage::Expiry(_) => 3,
            Stage::Price(_) => 9999,
            Stage::CancelObject => self.group.cancel_intents.len(),
        }
    }
    fn supports_choice(&self, choice: usize) -> Result<bool, InputError> {
        if choice >= self.choice_count() {
            return Ok(false);
        }
        match self.state.stage {
            Stage::Family => {
                let family = families()[choice];
                match family {
                    Family::NoTrade => Ok(true),
                    Family::SubmitSingleBuy | Family::SubmitPairedBuy => {
                        let state = PrefixState {
                            family: Some(family),
                            single_leg: self.repair()?.map(|x| x.0),
                            ..Default::default()
                        };
                        self.has_completion(&BigUint::one(), &self.root_upper(family)?, &state)
                    }
                    _ => Ok(self
                        .group
                        .cancel_intents
                        .iter()
                        .any(|o| Self::cancel_matches(family, o))),
                }
            }
            Stage::QuantityBranch => {
                let q = self.state.interval.as_ref().unwrap();
                let mid = (&q.lo + &q.hi) / 2u32;
                if choice == 0 {
                    self.has_completion(&q.lo, &mid, &self.state)
                } else {
                    self.has_completion(&(mid + 1u32), &q.hi, &self.state)
                }
            }
            Stage::CancelObject => Ok(Self::cancel_matches(
                self.state.family.unwrap(),
                &self.group.cancel_intents[choice],
            )),
            _ => {
                let mut state = self.state.clone();
                Self::set_parameter(&mut state, choice);
                self.fixed_completion(state.quantity_hundredths.as_ref().unwrap(), &state)
            }
        }
    }
    pub fn support(&self) -> Result<Vec<bool>, InputError> {
        if !self.done && self.state.stage == Stage::Family {
            let mut mask = vec![
                true,
                self.supports_choice(1)?,
                self.supports_choice(2)?,
                false,
                false,
                false,
            ];
            for intent in &self.group.cancel_intents {
                let i = match intent {
                    ModelIntentV1::CancelOrder { .. } => 3,
                    ModelIntentV1::CancelOrders { .. } => 4,
                    ModelIntentV1::CancelMarketOrders { .. } => 5,
                    _ => unreachable!("冻结撤单人口只包含撤单意图"),
                };
                mask[i] = true;
            }
            return Ok(mask);
        }
        (0..self.choice_count())
            .map(|choice| self.supports_choice(choice))
            .collect()
    }
    fn set_parameter(s: &mut PrefixState, k: usize) {
        match s.stage {
            Stage::Variant(l) => s.legs[index(l)].variant = Some(variants()[k]),
            Stage::PostOnly(l) => s.legs[index(l)].post_only = Some(k == 1),
            Stage::Expiry(l) => s.legs[index(l)].lifetime = Some(lifetimes()[k]),
            Stage::Price(l) => s.legs[index(l)].price_index = Some(BigUint::from(k + 1)),
            _ => unreachable!(),
        }
    }
    fn next_attributes(&mut self, i: usize) {
        let sides = self.sides(&self.state);
        if let Some(next) = sides.iter().copied().find(|v| *v > i) {
            self.state.stage = Stage::Variant(leg(next));
        } else {
            self.state.stage = Stage::Price(leg(sides[0]));
        }
    }
    pub fn choose(&mut self, choice: usize) -> Result<(), InputError> {
        if !self.supports_choice(choice)? {
            return Err(InputError::UnsupportedChoice);
        }
        self.path.push(choice);
        match self.state.stage {
            Stage::Family => {
                let f = families()[choice];
                self.state.family = Some(f);
                match f {
                    Family::NoTrade => self.done = true,
                    Family::SubmitSingleBuy | Family::SubmitPairedBuy => {
                        self.state.single_leg = if f == Family::SubmitSingleBuy {
                            self.repair()?.map(|x| x.0)
                        } else {
                            None
                        };
                        let upper = self.root_upper(f)?;
                        self.state.interval = Some(QuantityInterval {
                            lo: BigUint::one(),
                            hi: upper.clone(),
                            upper,
                        });
                        self.state.stage = Stage::QuantityBranch;
                        self.close_quantity();
                    }
                    _ => self.state.stage = Stage::CancelObject,
                }
            }
            Stage::QuantityBranch => {
                let q = self.state.interval.as_mut().unwrap();
                let mid = (&q.lo + &q.hi) / 2u32;
                if choice == 0 {
                    q.hi = mid;
                } else {
                    q.lo = mid + 1u32;
                }
                self.close_quantity();
            }
            Stage::Variant(l) => {
                Self::set_parameter(&mut self.state, choice);
                if matches!(
                    self.state.legs[index(l)].variant,
                    Some(Variant::GtcLimit | Variant::GtdLimit)
                ) {
                    self.state.stage = Stage::PostOnly(l)
                } else {
                    self.next_attributes(index(l))
                }
            }
            Stage::PostOnly(l) => {
                Self::set_parameter(&mut self.state, choice);
                if self.state.legs[index(l)].variant == Some(Variant::GtdLimit) {
                    self.state.stage = Stage::Expiry(l)
                } else {
                    self.next_attributes(index(l))
                }
            }
            Stage::Expiry(l) => {
                Self::set_parameter(&mut self.state, choice);
                self.next_attributes(index(l));
            }
            Stage::Price(l) => {
                Self::set_parameter(&mut self.state, choice);
                if l == Leg::A && self.state.family == Some(Family::SubmitPairedBuy) {
                    self.state.stage = Stage::Price(Leg::B)
                } else {
                    self.done = true
                }
            }
            Stage::CancelObject => {
                self.cancel_index = Some(choice);
                self.done = true;
            }
        }
        Ok(())
    }
    fn close_quantity(&mut self) {
        let q = self.state.interval.as_ref().unwrap();
        if q.lo == q.hi {
            self.state.select_quantity(q.lo.clone());
            self.state.stage = Stage::Variant(leg(self.sides(&self.state)[0]));
        }
    }
    pub fn price_fee_features(&self) -> Result<Vec<f32>, InputError> {
        let mut out = vec![0.; 9999];
        let Stage::Price(l) = self.state.stage else {
            return Ok(out);
        };
        let i = index(l);
        if !matches!(
            self.state.legs[i].variant,
            Some(Variant::FokMarket | Variant::FakMarket)
        ) {
            return Ok(out);
        }
        let n = self.state.quantity_hundredths.as_ref().unwrap();
        let minimum = floor_positive(
            &decimal(&self.context().rules.market_buy_min_spend_pusd).mul(&integer(1_000_000)),
        );
        for k in 1..=self.max_price(i)? {
            if n * (u32::from(k) * self.tick_units(i)?) < minimum {
                continue;
            }
            let fee = self
                .context()
                .snapshot
                .market_buy_fee_estimate_v1(n, k, &self.tick(i)?)
                .map_err(InputError::Fee)?
                .fee_1e5_units;
            out[usize::from(k) - 1] = signed_log_f32(&ratio(BigInt::from(fee), 100_000));
        }
        Ok(out)
    }
}
