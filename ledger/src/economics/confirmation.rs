//! 原合法终局量及实际赎回的定向读取；不参与现金或成本池归约。
use super::*;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalSuccessAnchorV1 {
    /// 原市场资产顺序；反转方向时净量和成交符号同时反转，绝对风险不变。
    pub assets: [AssetIdentityV1; 2],
    pub net_before_first_success_shares: ExactRationalV1,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfirmedPositionReadV1 {
    pub condition_id: ConditionId,
    pub assets: Option<[AssetIdentityV1; 2]>,
    pub net_first_asset_shares: ExecutionReadValueV1<ExactRationalV1>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeConfirmationReadV1 {
    pub trade_identity: Identity,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub first_success_seen: Option<bool>,
    pub first_success_anchor: Option<FinalSuccessAnchorV1>,
    /// 官方当前有效终局量；未终局或补偿为零，不读经济池旗标。
    pub current_final_success_shares: ExactDecimal,
}
impl EconomicsState {
    pub(super) fn confirmation_anchor(&self, condition: &str) -> Option<FinalSuccessAnchorV1> {
        let market = self.markets.get(condition)?;
        let assets = market.assets.clone()?;
        let totals = self.read_index.as_ref()?.markets.get(condition);
        let quantity = |i: usize| {
            let confirmed = totals
                .and_then(|t| t.final_success.get(&assets[i].token_id.0))
                .copied()
                .unwrap_or_default();
            let (pair, orphan) = market
                .resolution
                .as_ref()
                .map(|r| (r.redeemed_pair, r.redeemed_orphan[i]))
                .unwrap_or_default();
            R::from_decimal(confirmed)
                .sub(&R::from_decimal(pair))
                .sub(&R::from_decimal(orphan))
        };
        Some(FinalSuccessAnchorV1 {
            assets: assets.clone(),
            net_before_first_success_shares: quantity(0).sub(&quantity(1)).public(),
        })
    }
    fn confirmed_positions(&self) -> Vec<ConfirmedPositionReadV1> {
        self.markets
            .iter()
            .map(|(id, market)| {
                let net = match self.confirmation_anchor(id) {
                    Some(anchor) => {
                        ExecutionReadValueV1::Known(anchor.net_before_first_success_shares)
                    }
                    None if self
                        .read_index
                        .as_ref()
                        .and_then(|idx| idx.markets.get(id))
                        .is_none_or(|t| t.final_success.values().all(Decimal::is_zero)) =>
                    {
                        ExecutionReadValueV1::Known(R::zero().public())
                    }
                    None => ExecutionReadValueV1::Unavailable,
                };
                ConfirmedPositionReadV1 {
                    condition_id: ConditionId(id.clone()),
                    assets: market.assets.clone(),
                    net_first_asset_shares: net,
                }
            })
            .collect()
    }
}
impl LedgerNecessaryReadV2<'_> {
    /// 同一经济前缀共享一次投影；未变读取不重扫全账户成交。
    pub fn confirmed_positions_v1(&self) -> Arc<Vec<ConfirmedPositionReadV1>> {
        let state = &self.writer.economics;
        match &state.read_index {
            Some(index) => index
                .positions
                .get_or_init(|| Arc::new(state.confirmed_positions()))
                .clone(),
            None => Arc::new(state.confirmed_positions()),
        }
    }
    /// 只读取消费者显式保留的原成交身份，包括已闭合的后继修正。
    pub fn trade_confirmation_v1(&self, id: &Identity) -> Option<Arc<TradeConfirmationReadV1>> {
        let trade = self.writer.economics.trades.get(id)?;
        Some(
            trade
                .confirmation_read_cache
                .get_or_init(|| {
                    Arc::new(TradeConfirmationReadV1 {
                        trade_identity: id.clone(),
                        condition_id: trade.fact.condition_id.clone(),
                        token_id: trade.fact.token_id.clone(),
                        first_success_seen: trade.first_success_seen,
                        first_success_anchor: trade.first_success_anchor.clone(),
                        current_final_success_shares: if trade.fact.economic_disposition
                            == SourceNeutralTradeEconomicDispositionV1::FinalSuccess
                        {
                            trade.fact.base_quantity_shares.clone()
                        } else {
                            exact(Decimal::ZERO)
                        },
                    })
                })
                .clone(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeConfirmationChangeReadV1 {
    pub ledger_record_sequence: u64,
    pub ledger_entry_sequence: u64,
    pub source: execution_normalization::NoeConsumedRecordReferenceV1,
    pub position: execution_normalization::NoeRecordPositionV1,
    pub trade: TradeConfirmationReadV1,
}
/// 按原提交段流式选择必要成交变化；不重跑账户，不创建历史事件账本。
/// 只为显式消费者在完整采集后读取，普通账户/100ms热路径不调用。
pub fn visit_trade_confirmation_changes_v1(
    directory: &std::path::Path,
    identities: &std::collections::BTreeSet<Identity>,
    mut visit: impl FnMut(TradeConfirmationChangeReadV1) -> Result<(), crate::LedgerDurableErrorV1>,
) -> Result<(), crate::LedgerDurableErrorV1> {
    use crate::durable::{DurablePayloadV1, LedgerDurableErrorV1};
    if identities.is_empty() {
        return Ok(());
    }
    let mut previous = BTreeMap::new();
    let mut positions = execution_normalization::NoeRecoveryPositionReader::default();
    crate::durable::parquet::visit_committed(directory, |seq, payload| {
        let DurablePayloadV1::Transaction(effect) = payload else {
            return Ok(());
        };
        let ledger_entry_sequence = effect.ledger_entry_seq();
        let Some(patch) = effect.economics else {
            return Ok(());
        };
        for (id, trade) in patch.trades {
            if !identities.contains(&id) {
                continue;
            }
            let current = TradeConfirmationReadV1 {
                trade_identity: id.clone(),
                condition_id: trade.fact.condition_id.clone(),
                token_id: trade.fact.token_id.clone(),
                first_success_seen: trade.first_success_seen,
                first_success_anchor: trade.first_success_anchor.clone(),
                current_final_success_shares: if trade.fact.economic_disposition
                    == SourceNeutralTradeEconomicDispositionV1::FinalSuccess
                {
                    trade.fact.base_quantity_shares.clone()
                } else {
                    exact(Decimal::ZERO)
                },
            };
            if previous.get(&id) == Some(&current) {
                continue;
            }
            previous.insert(id, current.clone());
            if current.first_success_seen != Some(true) {
                continue;
            }
            let super::LedgerTransactionSourceV1::NoeConsumed(source) = &effect.transaction.source
            else {
                return Err(LedgerDurableErrorV1::InvalidStructure(
                    "专项成交变化缺原NOE持久引用".into(),
                ));
            };
            let position = positions
                .read(source.result())
                .map_err(|e| LedgerDurableErrorV1::InvalidStructure(e.to_string()))?;
            visit(TradeConfirmationChangeReadV1 {
                ledger_record_sequence: seq,
                ledger_entry_sequence,
                source: source.clone(),
                position,
                trade: current,
            })?;
        }
        Ok(())
    })?;
    Ok(())
}
