//! 公共确认主营分量与唯一本金；只投影原成本池，不引入训练目标或第二账本。
use super::*;
use crate::identity::LedgerIdentity;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfirmedComponentKindV1 {
    PairedGross,
    MakerRebate,
    TakerFee,
    EffectiveTakerRebate,
    OtherCost,
    OrphanLoss,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfirmedComponentReadV1 {
    /// 市场或返佣日的稳定经济聚合身份；后继同身份金额替换，重复读取不是新确认。
    pub identity: String,
    pub kind: ConfirmedComponentKindV1,
    pub amount_pusd: ExactRationalV1,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfirmedEconomicsReadV1 {
    pub version: String,
    pub components: Vec<ConfirmedComponentReadV1>,
    /// 未判定孤腿原成本，加已应用但未终局的原成交本金。配对/确认债权/费用不在此项。
    pub unresolved_position_principal_pusd: ExactRationalV1,
    /// 负成本尚无合法金额或上界，标签消费者必须保留待定。
    pub unknown_negative_cost_identities: Vec<String>,
    /// 缺成交详情但已由原生命周期精确负金额覆盖；原身份保留供追溯。
    #[serde(default)]
    pub covered_unapportioned_negative_cost_identities: Vec<String>,
}
impl LedgerEconomicReadV2<'_> {
    pub fn confirmed_economics_v1(&self) -> std::sync::Arc<ConfirmedEconomicsReadV1> {
        self.state
            .read_index
            .as_ref()
            .expect("已恢复原经济索引")
            .confirmed
            .get_or_init(|| std::sync::Arc::new(self.state.confirmed_components(self.lifecycle)))
            .clone()
    }
}
impl EconomicsState {
    fn confirmed_components(
        &self,
        lifecycle: &crate::lifecycle::LifecycleStateV1,
    ) -> ConfirmedEconomicsReadV1 {
        let mut items = Vec::new();
        let mut principal = R::zero();
        let mut unknown = Vec::new();
        let mut covered = Vec::new();
        let mut extra_fees = BTreeMap::<String, R>::new();
        for (id, (applied, _)) in &self.unapportioned_trades {
            if !applied {
                continue;
            }
            let key = id.ledger_key();
            let mut found = false;
            for (_, fill) in lifecycle
                .applied_fills
                .range((key.clone(), 0)..=(key, u32::MAX))
                .filter(|(_, f)| f.active)
            {
                found = true;
                principal = principal.add(&R::from_decimal(fill.cost_basis_pusd));
                let condition = fill
                    .condition_id
                    .as_ref()
                    .map(|c| c.0.clone())
                    .unwrap_or_else(|| format!("unattributed:{id:?}"));
                let fee = extra_fees.entry(condition).or_insert_with(R::zero);
                *fee = fee.add(&R::from_decimal(fill.fee_pusd));
            }
            if found {
                covered.push(format!("unapportioned:{id:?}"));
            } else {
                unknown.push(format!("unapportioned:{id:?}"));
            }
        }
        for (id, m) in &self.markets {
            let paired = R::from_decimal(m.paired)
                .sub(&m.paired_cost[0])
                .sub(&m.paired_cost[1]);
            let mut orphan_loss = R::zero();
            if m.resolution.is_some() {
                for i in 0..2 {
                    let payoff = R::from_decimal(m.pools[i].quantity)
                        .mul(&R::from_decimal(m.payout(i).expect("原判定已验证")));
                    orphan_loss = orphan_loss.add(&m.pools[i].cost.sub(&payoff).max(R::zero()));
                }
            } else {
                principal = principal.add(&m.pools[0].cost).add(&m.pools[1].cost);
            }
            if let Some(t) = self.read_index.as_ref().and_then(|i| i.markets.get(id)) {
                principal = principal.add(&t.pending_principal);
            }
            for (suffix, kind, amount) in [
                ("paired", ConfirmedComponentKindV1::PairedGross, paired),
                (
                    "taker-fee",
                    ConfirmedComponentKindV1::TakerFee,
                    R::from_decimal(m.executed_fees[1])
                        .add(&extra_fees.remove(id).unwrap_or_else(R::zero)),
                ),
                (
                    "other-cost",
                    ConfirmedComponentKindV1::OtherCost,
                    R::from_decimal(m.settlement_recognized),
                ),
                (
                    "orphan-loss",
                    ConfirmedComponentKindV1::OrphanLoss,
                    orphan_loss,
                ),
            ] {
                items.push(ConfirmedComponentReadV1 {
                    identity: format!("market:{id}:{suffix}"),
                    kind,
                    amount_pusd: amount.public(),
                });
            }
        }
        for ((kind, day), d) in &self.days {
            // 未付+已付保持同一累计合格权益；付款不是新收益，锁定余数/低额不授信用。
            let amounts = d.amounts(None);
            items.push(ConfirmedComponentReadV1 {
                identity: format!("rebate:{kind:?}:{day}"),
                kind: if *kind == RebateKindV1::Maker {
                    ConfirmedComponentKindV1::MakerRebate
                } else {
                    ConfirmedComponentKindV1::EffectiveTakerRebate
                },
                amount_pusd: amounts[2].add(&amounts[3]).public(),
            });
        }
        for (condition, amount) in extra_fees {
            items.push(ConfirmedComponentReadV1 {
                identity: format!("market:{condition}:taker-fee"),
                kind: ConfirmedComponentKindV1::TakerFee,
                amount_pusd: amount.public(),
            });
        }
        ConfirmedEconomicsReadV1 {
            version: "confirmed_economics_v1".into(),
            components: items,
            unresolved_position_principal_pusd: principal.public(),
            unknown_negative_cost_identities: unknown,
            covered_unapportioned_negative_cost_identities: covered,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::LedgerIdentity;
    #[test]
    fn cumulative_pair_is_gross_and_corrections_can_be_negative() {
        let mut s = EconomicsState::default();
        let m = s.markets.entry("condition".into()).or_default();
        m.paired = Decimal::from(10);
        m.paired_cost = [
            R::from_decimal(Decimal::from(3)),
            R::from_decimal(Decimal::from(4)),
        ];
        m.executed_fees = [Decimal::ZERO, Decimal::ONE];
        m.pools[0].cost = R::from_decimal(Decimal::from(2));
        let a = s.confirmed_components(&crate::lifecycle::LifecycleStateV1::default());
        assert_eq!(
            a.components[0].amount_pusd,
            R::from_decimal(Decimal::from(3)).public()
        );
        assert_eq!(
            a.unresolved_position_principal_pusd,
            R::from_decimal(Decimal::from(2)).public()
        );
        assert_eq!(
            a,
            s.confirmed_components(&crate::lifecycle::LifecycleStateV1::default()),
            "重复读取不创造增量"
        );
        s.markets.get_mut("condition").unwrap().paired_cost[0] = R::from_decimal(Decimal::from(5));
        let b = s.confirmed_components(&crate::lifecycle::LifecycleStateV1::default());
        assert_eq!(a.components[0].identity, b.components[0].identity);
        assert_eq!(
            R::try_from(&b.components[0].amount_pusd)
                .unwrap()
                .sub(&R::try_from(&a.components[0].amount_pusd).unwrap()),
            R::from_decimal(Decimal::from(-2))
        );
    }
}
