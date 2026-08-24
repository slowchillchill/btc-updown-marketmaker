use std::collections::BTreeMap;

use contracts::{ConditionId, TokenId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::reservation::LedgerRequestIdentityV1;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LedgerSubmissionItemIdentityV1 {
    pub global_request_id: String,
    pub batch_item_ordinal: u16,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct LifecycleTotalsV1 {
    pub(crate) platform_pending_principal_pusd: Decimal,
    pub(crate) platform_pending_immediate_fee_pusd: Decimal,
    pub(crate) platform_pending_economic_count: u64,
    pub(crate) open_order_count: u64,
    pub(crate) pending_fill_count: u64,
    pub(crate) position_cost_basis_pusd: Decimal,
    pub(crate) fees_pusd: Decimal,
    pub(crate) realized_pnl_pusd: Decimal,
    pub(crate) cumulative_accepted_principal_pusd: Decimal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct OutcomeCostBasisV1 {
    pub(crate) token_id: TokenId,
    pub(crate) cost_basis_pusd: Decimal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MarketLifecycleStateV1 {
    pub(crate) condition_id: ConditionId,
    pub(crate) totals: LifecycleTotalsV1,
    pub(crate) outcome_cost_basis_pusd: BTreeMap<String, OutcomeCostBasisV1>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct OrderLifecycleIndexV1 {
    pub(crate) condition_id: Option<ConditionId>,
    pub(crate) request_identity: Option<LedgerRequestIdentityV1>,
    pub(crate) economic_applied: bool,
    pub(crate) accepted_principal_counted: bool,
    pub(crate) open: bool,
    pub(crate) last_ledger_entry_seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TradeLifecycleIndexV1 {
    pub(crate) condition_id: ConditionId,
    pub(crate) pending: bool,
    pub(crate) _last_ledger_entry_seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct AppliedFillIndexV1 {
    pub(crate) condition_id: Option<ConditionId>,
    pub(crate) token_id: Option<TokenId>,
    pub(crate) cost_basis_pusd: Decimal,
    pub(crate) fee_pusd: Decimal,
    pub(crate) active: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct LifecycleStateV1 {
    #[serde(with = "submission_requests_serde")]
    pub(crate) submission_requests:
        BTreeMap<LedgerSubmissionItemIdentityV1, LedgerRequestIdentityV1>,
    pub(crate) query_requests: BTreeMap<String, LedgerRequestIdentityV1>,
    pub(crate) orders: BTreeMap<String, OrderLifecycleIndexV1>,
    pub(crate) trades: BTreeMap<String, TradeLifecycleIndexV1>,
    #[serde(with = "applied_fills_serde")]
    pub(crate) applied_fills: BTreeMap<(String, u32), AppliedFillIndexV1>,
    pub(crate) totals: LifecycleTotalsV1,
    pub(crate) outcome_cost_basis_pusd: BTreeMap<String, OutcomeCostBasisV1>,
    pub(crate) markets: BTreeMap<String, MarketLifecycleStateV1>,
}

mod submission_requests_serde {
    use super::*;

    pub(super) fn serialize<S>(
        value: &BTreeMap<LedgerSubmissionItemIdentityV1, LedgerRequestIdentityV1>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        value.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<LedgerSubmissionItemIdentityV1, LedgerRequestIdentityV1>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Vec::<(LedgerSubmissionItemIdentityV1, LedgerRequestIdentityV1)>::deserialize(deserializer)
            .map(IntoIterator::into_iter)
            .map(Iterator::collect)
    }
}

mod applied_fills_serde {
    use super::*;

    pub(super) fn serialize<S>(
        value: &BTreeMap<(String, u32), AppliedFillIndexV1>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        value.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<(String, u32), AppliedFillIndexV1>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Vec::<((String, u32), AppliedFillIndexV1)>::deserialize(deserializer)
            .map(IntoIterator::into_iter)
            .map(Iterator::collect)
    }
}

impl LifecycleStateV1 {
    pub(crate) fn market_mut(&mut self, condition_id: &ConditionId) -> &mut MarketLifecycleStateV1 {
        self.markets
            .entry(condition_id.0.clone())
            .or_insert_with(|| MarketLifecycleStateV1 {
                condition_id: condition_id.clone(),
                totals: LifecycleTotalsV1::default(),
                outcome_cost_basis_pusd: BTreeMap::new(),
            })
    }
}
