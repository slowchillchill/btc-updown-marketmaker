use super::*;

impl ComplementaryPairV1 {
    pub fn new(
        config: ComplementaryPairConfigV1,
        rules: Arc<SealedProcessRuleSnapshotV3>,
        request_namespace: String,
        continuity: StrategyContinuityV1,
    ) -> Result<Self, StrategyErrorV1> {
        // 类型/算法的定义域检查，不把研究候选数值升级为额外交易资格。
        if request_namespace.is_empty() || request_namespace.trim() != request_namespace {
            return Err(StrategyErrorV1::Configuration(
                "请求命名空间为空或含首尾空白".into(),
            ));
        }
        let decimals = [
            config.quote.base_pair_shares,
            config.quote.base_half_spread,
            config.quote.inventory_shift_per_pusd,
            config.quote.uncertainty_spread_weight,
            config.quote.signal_spread_weight,
            config.repair.base_loss_allowance_per_share,
            config.repair.time_loss_allowance_per_share,
            config.repair.inventory_loss_allowance_per_share,
            config.repair.maximum_overfill_shares,
            config.repair.maximum_overfill_pusd,
            config.repair.wait_price_improvement_per_share,
            config.repair.residual_risk_weight,
            config.risk.per_market_orphan_limit_pusd,
            config.risk.account_orphan_limit_pusd,
        ];
        if decimals.iter().any(|v| *v < Decimal::ZERO) {
            return Err(StrategyErrorV1::Configuration(
                "份额、成本容忍与风险金额必须非负".into(),
            ));
        }
        let p = &config.probability;
        if [
            p.initial_sigma_usd_per_sqrt_second,
            p.estimation_error_usd,
            p.binance_innovation_weight,
            p.binance_pressure_weight_usd,
            p.binance_trade_pressure_weight_usd,
        ]
        .iter()
        .any(|v| !v.is_finite())
            || p.initial_sigma_usd_per_sqrt_second < 0.0
            || p.estimation_error_usd < 0.0
            || !config.repair.maker_fill_weight.is_finite()
            || !(0.0..=1.0).contains(&config.repair.maker_fill_weight)
            || config
                .quote
                .joint_fill_scenario_weights
                .iter()
                .any(|v| !v.is_finite() || *v < 0.0)
            || !config
                .quote
                .joint_fill_scenario_weights
                .iter()
                .sum::<f64>()
                .is_finite()
            || config.quote.joint_fill_scenario_weights.iter().sum::<f64>() <= 0.0
        {
            return Err(StrategyErrorV1::Configuration(
                "统计系数不在其数学定义域".into(),
            ));
        }
        rational(&continuity.confirmed_high_water_pusd)?;
        Ok(Self {
            config,
            rules,
            namespace: request_namespace,
            next_request: continuity.next_request_sequence,
            profit_pending: continuity.unresolved_profit_allocations.clone(),
            continuity,
            signals: BTreeMap::new(),
        })
    }
    pub fn config(&self) -> &ComplementaryPairConfigV1 {
        &self.config
    }
    pub fn continuity(&self) -> StrategyContinuityV1 {
        let mut state = self.continuity.clone();
        state.next_request_sequence = self.next_request;
        state.unresolved_profit_allocations = self.profit_pending.clone();
        state
    }
    pub(super) fn id(&mut self) -> Result<String, StrategyErrorV1> {
        let n = self.next_request;
        self.next_request = self
            .next_request
            .checked_add(1)
            .ok_or(StrategyErrorV1::Arithmetic("请求序号溢出"))?;
        Ok(format!("{}:{n}", self.namespace))
    }
    pub(super) fn cancel(&mut self, ids: Vec<String>) -> Result<ReservedIntentV1, StrategyErrorV1> {
        Ok(ReservedIntentV1 {
            context: RequestContextV2 {
                global_request_id: self.id()?,
                caller_correlation_id: None,
            },
            intent: Arc::new(ModelIntentV1::CancelOrders {
                platform_order_ids: ids,
            }),
            reserves: vec![],
        })
    }
}
