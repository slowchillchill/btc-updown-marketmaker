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
fn variants() -> [Variant; 2] {
    [Variant::GtcLimit, Variant::FokMarket]
}
/// 固定分段号，不对非空段重新编号。
pub fn quantity_segment(q: &QuantityInterval, j: usize) -> Option<(BigUint, BigUint)> {
    if j >= 16 {
        return None;
    }
    let m = &q.hi - &q.lo + 1u32;
    let lo = &q.lo + &m * j / 16usize;
    let end = &q.lo + &m * (j + 1) / 16usize;
    (lo < end).then(|| (lo, end - 1u32))
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

#[cfg(test)]
pub(crate) fn gpu_intent_reference(intent:&ModelIntentV1)->serde_json::Value {
    use contracts::{BuyLegIntentV1,BuyOrderIntentV1};
    fn order(leg:&BuyLegIntentV1)->serde_json::Value {
        let value=match &leg.order {
            BuyOrderIntentV1::GtcLimit {size_shares,limit_price_pusd_per_share,post_only}=>serde_json::json!({
                "GtcLimit":{"size_shares":size_shares.to_string(),"limit_price_pusd_per_share":limit_price_pusd_per_share.to_string(),"post_only":post_only}}),
            BuyOrderIntentV1::FokMarket {spend_pusd,worst_price_pusd_per_share}=>serde_json::json!({
                "FokMarket":{"spend_pusd":spend_pusd.to_string(),"worst_price_pusd_per_share":worst_price_pusd_per_share.to_string()}}),
            _=>unreachable!("原NativeActor只形成GTC/FOK"),
        };
        serde_json::json!({"token_id":leg.token_id,"order":value})
    }
    match intent {
        ModelIntentV1::SubmitSingleBuy {condition_id,order:leg}=>serde_json::json!({"SubmitSingleBuy":{"condition_id":condition_id,"order":order(leg)}}),
        ModelIntentV1::SubmitPairedBuy {condition_id,orders}=>serde_json::json!({"SubmitPairedBuy":{"condition_id":condition_id,"orders":[order(&orders[0]),order(&orders[1])]}}),
        ModelIntentV1::CancelOrder {platform_order_id}=>serde_json::json!({"CancelOrder":{"platform_order_id":platform_order_id}}),
        ModelIntentV1::CancelOrders {platform_order_ids}=>serde_json::json!({"CancelOrders":{"platform_order_ids":platform_order_ids}}),
        ModelIntentV1::CancelMarketOrders {condition_id}=>serde_json::json!({"CancelMarketOrders":{"condition_id":condition_id}}),
    }
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
        let context = group.rows[row]
            .action_context
            .as_ref()
            .ok_or(InputError::MissingAssociation)?;
        context.validate_ticks()?;
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
            Stage::Price(_) => 3,
            Stage::CancelObject => 5,
        }
    }
    pub fn is_complete(&self) -> bool {
        self.done
    }
    #[cfg(test)]
    pub(crate) fn gpu_context_reference(&self) -> serde_json::Value {
        let repair = self.repair().unwrap();
        let read = self.group.action_before.as_ref().unwrap();
        let cash = &read.economics().cash;
        let gaps: Vec<_> = (0..2).map(|side| {
            read.economics().markets.iter()
                .find(|market| market.condition_id == self.group.rows[self.row].hidden_state_key.condition_id)
                .map(|market| {
                    let at = market.assets.as_ref().unwrap().iter()
                        .position(|asset| asset.outcome.index == side).unwrap();
                    market.repair_gap_shares[at].clone()
                }).unwrap_or_else(|| ExactDecimal("0".into()))
        }).collect();
        serde_json::json!({
            "actual_available_pusd": cash.actual_available_pusd,
            "maker_available_pusd": cash.maker_available_pusd,
            "cash_micro": self.cash_micro().to_string(),
            "ticks": [self.tick(0).unwrap(),self.tick(1).unwrap()],
            "tokens": self.context().tokens,
            "buy_enabled": self.buy_enabled(),
            "single_side": repair.as_ref().map(|(side,_)| index(*side)),
            "cap_centishares": repair.map(|(_,cap)|cap.to_string()),
            "repair_gap_shares": gaps,
            "rules": self.context().rules,
            "immediate_limit_min_principal_pusd": self.context().rules.immediate_limit_min_principal_pusd,
            "cancel_intents": self.group.cancel_intents.iter().map(gpu_intent_reference).collect::<Vec<_>>(),
        })
    }
    pub fn prefix_features(&self) -> [f32; 36] {
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
            .expect("两档合法 tick"))
    }
    fn tick(&self, i: usize) -> Result<ExactDecimal, InputError> {
        Ok(ExactDecimal(
            rust_decimal::Decimal::new(i64::from(self.tick_units(i)?), 4)
                .normalize()
                .to_string(),
        ))
    }
    fn max_price(&self, i: usize) -> Result<u16, InputError> {
        Ok((10_000 / self.tick_units(i)? - 1).min(999) as u16)
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
        // 数量选择早于属性：上界已满足本金、编码和单腿cap，
        // 逐腿最低 tick 本金小于 1 pUSD 时普通 GTC 无即时费用；
        // 达门槛时 FOK 提供本金续项，两腿允许混合类型。
        if state.legs.iter().all(|s| s.variant.is_none()) {
            return Ok(true);
        }
        // 固定属性查询仍逐数量检查，不能把市价支出的取整当作单调。
        let mut n = lower;
        while n <= upper {
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
            let (p, f) = self.cost(n, i, k.to_u16().unwrap(), v, false)?;
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
            Stage::QuantityBranch => 16,
            Stage::Variant(_) => 2,
            Stage::Price(_) => 999,
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
                match quantity_segment(q, choice) {
                    Some((lo, hi)) => self.has_completion(&lo, &hi, &self.state),
                    None => Ok(false),
                }
            }
            Stage::Price(_) => Ok(self
                .price_range()?
                .is_some_and(|(lo, hi)| usize::from(lo - 1) <= choice && choice < usize::from(hi))),
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
        if !self.done && matches!(self.state.stage, Stage::Price(_)) {
            let mut mask = vec![false; 999];
            if let Some((lo, hi)) = self.price_range()? {
                mask[usize::from(lo - 1)..usize::from(hi)].fill(true);
            }
            return Ok(mask);
        }
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
            Stage::Variant(l) => {
                s.legs[index(l)].variant = Some(variants()[k]);
                s.legs[index(l)].post_only = Some(false);
            }
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
                let (lo, hi) = quantity_segment(q, choice).unwrap();
                q.lo = lo;
                q.hi = hi;
                self.close_quantity();
            }
            Stage::Variant(l) => {
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
    /// 当前腿的连续合法价格区间；精确费用二分，不逐档重跑完成性。
    pub fn price_range(&self) -> Result<Option<(u16, u16)>, InputError> {
        let Stage::Price(l) = self.state.stage else {
            return Ok(None);
        };
        let i = index(l);
        let n = self.state.quantity_hundredths.as_ref().unwrap();
        let mut remaining = self.cash_micro();
        for other in self.sides(&self.state).into_iter().filter(|&j| j != i) {
            let Some(cost) = self.min_cost(n, other, &self.state.legs[other])? else {
                return Ok(None);
            };
            if cost > remaining {
                return Ok(None);
            }
            remaining -= cost;
        }
        let variant = self.state.legs[i].variant.unwrap();
        let unit = n * self.tick_units(i)?;
        let max = BigUint::from(self.max_price(i)?)
            .min(&remaining / &unit)
            .to_u16()
            .unwrap();
        if variant == Variant::FokMarket {
            let minimum = floor_positive(
                &decimal(&self.context().rules.market_buy_min_spend_pusd).mul(&integer(1_000_000)),
            );
            let min = minimum.div_ceil(&unit).max(BigUint::one());
            return Ok((min <= BigUint::from(max)).then(|| (min.to_u16().unwrap(), max)));
        }
        let mut lo = 0u16;
        let mut hi = max;
        while lo < hi {
            let k = lo + (hi - lo).div_ceil(2);
            let (p, fee) = self.cost(n, i, k, Variant::GtcLimit, false)?;
            if p + fee * 10u32 <= remaining {
                lo = k;
            } else {
                hi = k - 1;
            }
        }
        Ok((lo > 0).then_some((1, lo)))
    }
    pub fn price_indices(&self) -> Result<Vec<i64>, InputError> {
        use std::sync::OnceLock;
        static CENT: OnceLock<Vec<i64>> = OnceLock::new();
        static MILLI: OnceLock<Vec<i64>> = OnceLock::new();
        let Some((lo, hi)) = self.price_range()? else {
            return Ok(Vec::new());
        };
        let Stage::Price(l) = self.state.stage else {
            unreachable!()
        };
        let grid = if self.tick_units(index(l))? == 100 {
            CENT.get_or_init(|| (0..99).collect())
        } else {
            MILLI.get_or_init(|| (0..999).collect())
        };
        Ok(grid[usize::from(lo - 1)..usize::from(hi)].to_vec())
    }
    pub fn price_fee_features(&self) -> Result<Vec<f32>, InputError> {
        let indices = self.price_indices()?;
        let mut out = vec![0.; indices.len()];
        let Stage::Price(l) = self.state.stage else {
            return Ok(out);
        };
        let i = index(l);
        if self.state.legs[i].variant != Some(Variant::FokMarket) {
            return Ok(out);
        }
        let n = self.state.quantity_hundredths.as_ref().unwrap();
        for (&coordinate, value) in indices.iter().zip(&mut out) {
            let fee = self
                .context()
                .snapshot
                .market_buy_fee_estimate_v1(n, (coordinate + 1) as u16, &self.tick(i)?)
                .map_err(InputError::Fee)?
                .fee_1e5_units;
            *value = signed_log_f32(&ratio(BigInt::from(fee), 100_000));
        }
        Ok(out)
    }
}
