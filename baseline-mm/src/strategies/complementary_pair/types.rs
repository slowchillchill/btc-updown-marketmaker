use super::*;

/// 账户接续仅包含策略记忆，不能代替公共账户/订单恢复。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyContinuityV2 {
    pub rule_version: String,
    pub confirmed_high_water_pusd: ExactRationalV1,
    pub terminating: bool,
    pub repair_market_ends: BTreeMap<String, i64>,
    pub next_request_sequence: u64,
    pub unresolved_profit_allocations: BTreeMap<String, ProfitAllocationV2>,
}
impl StrategyContinuityV2 {
    pub fn new_account() -> Self {
        Self {
            rule_version: RULE_VERSION.into(),
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
    /// 仅阻断项，保持原动作许可语义。
    pub source_health: Vec<SourceHealthIssueV1>,
    pub source_health_states: Vec<SourceHealthStatusV1>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceHealthFailureV1 {
    UnknownBusinessProgress,
    ReceiveSilence,
    EventDelay,
    UnknownEventTime,
    TimeInconsistent,
    SynchronizationRequired,
    Unavailable,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceHealthIssueV1 {
    pub scope: SourceStreamScopeV1,
    pub failure: SourceHealthFailureV1,
}
/// 与冻结请求关联的最小来源依赖；不保存另一份行情。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionHealthDependencyV1 {
    pub global_request_id: String,
    pub condition_id: ConditionId,
    pub sources: Vec<SourceStreamScopeV1>,
    pub repair_token: Option<TokenId>,
    pub independent_held_token: Option<TokenId>,
    pub business: ActionBusinessDependencyV1,
}
/// 只冻结本市场支撑价量/成本/容量的标量；变化时重算，不存第二份账本。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionBusinessDependencyV1 {
    pub orphan_shares: [ExactDecimal; 2],
    pub orphan_principal: ExactRationalV1,
    pub orphan_fee: Option<ExactRationalV1>,
    pub success_shares: [ExactDecimal; 2],
    pub success_principal: [ExactRationalV1; 2],
    pub actual_fees: [ExactDecimal; 2],
    pub commitments: Vec<ActionCommitmentDependencyV1>,
    pub unknown_order_ids: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionCommitmentDependencyV1 {
    pub request_id: String,
    pub leg_ordinal: u16,
    pub remaining_shares: ExecutionReadValueV1<ExactDecimal>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfitAllocationV2 {
    pub global_request_id: String,
    pub reserve_identity: LedgerRequestIdentityV1,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub estimated_principal_pusd: ExactRationalV1,
    pub estimated_fee_pusd: ExactRationalV1,
    /// 只冻结决策场景，不能当作已成交数量。
    pub estimated_shares: ExactRationalV1,
    /// 原截止可配对孤腿场景；不能由后续别单池变化重估释放。
    pub original_orphan_shares: ExactRationalV1,
    pub original_orphan_principal_per_share: ExactRationalV1,
    #[serde(with = "sim_system::public_consumption::checkpoint::cutoff")]
    pub estimate_cutoff: sim_system::public_consumption::TrustedGroupDecisionCutoffV1,
    /// 首次分配的利润资源，不是当前预计损失或风险上界。
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
    pub profit_allocations: Vec<ProfitAllocationV2>,
    pub action_health_dependencies: Vec<ActionHealthDependencyV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceHealthStateV1 {
    Normal,
    Elevated,
    Exceeded(SourceHealthFailureV1),
    Unavailable(SourceHealthFailureV1),
}
impl SourceHealthStateV1 {
    pub fn failure(&self) -> Option<SourceHealthFailureV1> {
        match self {
            Self::Exceeded(f) | Self::Unavailable(f) => Some(f.clone()),
            _ => None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceHealthStatusV1 {
    pub scope: SourceStreamScopeV1,
    pub state: SourceHealthStateV1,
}

/// 同一日原策略实例的完整私有记忆，不包含公共账户投影。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComplementaryPairCheckpointV2 {
    pub(super) namespace: String,
    pub(super) continuity: StrategyContinuityV2,
    pub(super) signals: BTreeMap<String, SignalMemory>,
}
