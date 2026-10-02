use super::*;

/// 账户接续仅包含策略记忆，不能代替公共账户/订单恢复。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyContinuityV1 {
    pub confirmed_high_water_pusd: ExactRationalV1,
    pub terminating: bool,
    pub repair_market_ends: BTreeMap<String, i64>,
    pub next_request_sequence: u64,
    pub unresolved_profit_allocations: BTreeMap<String, ProfitAllocationV1>,
}
impl StrategyContinuityV1 {
    pub fn new_account() -> Self {
        Self {
            confirmed_high_water_pusd: r(Decimal::from(2000)).public(),
            terminating: false,
            repair_market_ends: BTreeMap::new(),
            next_request_sequence: 0,
            unresolved_profit_allocations: BTreeMap::new(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UrgencyStateV1 {
    pub trading_remaining_ms: i64,
    pub window_remaining_ms: i64,
    pub settlement_window_start_utc_ms: Option<i64>,
    pub standardized_distance: Option<f64>,
    pub uncertainty_usd: Option<f64>,
    pub information_age_ms: Option<u64>,
    pub binance_innovation_usd: Option<f64>,
    pub orphan_cost_pusd: ExactRationalV1,
    pub free_cash_pusd: ExactDecimal,
    pub unresolved_commitment_count: usize,
    pub repair_ask_depth_shares: Option<ExactDecimal>,
    pub maker_path_ms: u64,
    pub active_path_ms: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MarketChoiceV1 {
    Maintain,
    NewPair,
    MakerRepair,
    ActiveRepair,
    CancelBeforeActiveRepair,
    Wait,
    RetainOrphan,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ActionUnavailableV1 {
    MissingBookOrTick,
    UnknownCostOrCommitment,
    InsufficientCashOrInvestment,
    InventoryCapacity,
    CostLine,
    AmountOverfillBound,
    ActionPathEndsAfterWindow,
    EstimateRequired,
    NoLegalMakerPrice,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RepairComparisonV1 {
    pub paired_net_per_share: ExactRationalV1,
    pub premium_per_share: Option<ExactRationalV1>,
    pub maker_value_per_share: Option<f64>,
    pub active_value_per_share: Option<f64>,
    pub wait_value_per_share: Option<f64>,
    pub retain_value_per_share: f64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketDecisionDiagnosticV1 {
    pub condition_id: ConditionId,
    pub probability: ProbabilityEstimateV1,
    pub urgency: UrgencyStateV1,
    pub choice: MarketChoiceV1,
    pub unavailable: Vec<ActionUnavailableV1>,
    pub comparison: Option<RepairComparisonV1>,
    pub maximum_repair_price: Option<ExactDecimal>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfitAllocationV1 {
    pub global_request_id: String,
    pub reserve_identity: LedgerRequestIdentityV1,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub principal_bound_pusd: ExactRationalV1,
    pub fee_bound_pusd: ExactRationalV1,
    pub original_shares: Option<ExactDecimal>,
    pub incremental_loss_per_share: ExactRationalV1,
    pub allocated_profit_pusd: ExactRationalV1,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ComplementaryPairDiagnosticV1 {
    pub strategy_id: String,
    pub rule_version: String,
    pub risk_equity_pusd: ExecutionReadValueV1<ExactRationalV1>,
    pub high_water_pusd: ExactRationalV1,
    pub terminating: bool,
    pub qualified_net_pnl_pusd: ExactRationalV1,
    pub available_profit_pusd: ExactRationalV1,
    pub markets: Vec<MarketDecisionDiagnosticV1>,
    pub profit_allocations: Vec<ProfitAllocationV1>,
}
