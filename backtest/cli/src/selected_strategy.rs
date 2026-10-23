//! CLI 在明确参数类型处选择正式策略；业务规则留在 baseline-mm。
use baseline_mm::*;
use contracts::*;
use serde::{Deserialize, Serialize};
use shared_run::*;
use sim_system::public_consumption::PublicObservationFrameV1;
#[derive(Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(super) enum Diagnostics {
    Legacy(ComplementaryPairDiagnosticV1),
    Inventory(ContinuousInventoryDiagnosticV1),
}
pub(super) enum SelectedStrategy {
    Legacy(ComplementaryPairV1),
    Inventory(ContinuousInventoryV1),
}
impl SelectedStrategy {
    pub fn with_entry_market_range(self, range: Option<EntryMarketRangeV1>) -> Self {
        match self {
            Self::Legacy(s) => Self::Legacy(s.with_entry_market_range(range)),
            Self::Inventory(s) => Self::Inventory(s.with_entry_market_range(range)),
        }
    }
}
fn legacy(
    c: &StrategyCalculationV1<Diagnostics>,
) -> StrategyCalculationV1<ComplementaryPairDiagnosticV1> {
    map(c, |d| match d {
        Diagnostics::Legacy(d) => d.clone(),
        _ => unreachable!("正式选择一致"),
    })
}
fn inventory(
    c: &StrategyCalculationV1<Diagnostics>,
) -> StrategyCalculationV1<ContinuousInventoryDiagnosticV1> {
    map(c, |d| match d {
        Diagnostics::Inventory(d) => d.clone(),
        _ => unreachable!("正式选择一致"),
    })
}
fn map<A, B>(c: &StrategyCalculationV1<A>, f: impl Fn(&A) -> B) -> StrategyCalculationV1<B> {
    StrategyCalculationV1 {
        input_cutoff: c.input_cutoff.clone(),
        market_frame_timing: c.market_frame_timing.clone(),
        calculation_started_at_utc_ms: c.calculation_started_at_utc_ms,
        output: match &c.output {
            StrategyOutputV1::Continue {
                intents,
                next_evaluation_at_utc_ms,
                diagnostics,
            } => StrategyOutputV1::Continue {
                intents: intents.clone(),
                next_evaluation_at_utc_ms: *next_evaluation_at_utc_ms,
                diagnostics: f(diagnostics),
            },
            StrategyOutputV1::Exit { diagnostics } => StrategyOutputV1::Exit {
                diagnostics: f(diagnostics),
            },
        },
    }
}
fn output<A>(
    o: StrategyOutputV1<A>,
    f: impl FnOnce(A) -> Diagnostics,
) -> StrategyOutputV1<Diagnostics> {
    match o {
        StrategyOutputV1::Continue {
            intents,
            next_evaluation_at_utc_ms,
            diagnostics,
        } => StrategyOutputV1::Continue {
            intents,
            next_evaluation_at_utc_ms,
            diagnostics: f(diagnostics),
        },
        StrategyOutputV1::Exit { diagnostics } => StrategyOutputV1::Exit {
            diagnostics: f(diagnostics),
        },
    }
}
impl StrategyV1 for SelectedStrategy {
    type Error = StrategyErrorV1;
    type Diagnostics = Diagnostics;
    fn uses_borrowed_input(&self) -> bool {
        match self {
            Self::Legacy(s) => s.uses_borrowed_input(),
            Self::Inventory(s) => s.uses_borrowed_input(),
        }
    }
    fn selects_relevant_markets(&self) -> bool {
        match self {
            Self::Legacy(s) => s.selects_relevant_markets(),
            Self::Inventory(s) => s.selects_relevant_markets(),
        }
    }
    fn requires_trade_economics_details(&self) -> bool {
        match self {
            Self::Legacy(s) => s.requires_trade_economics_details(),
            Self::Inventory(s) => s.requires_trade_economics_details(),
        }
    }
    fn requires_source_progress(&self) -> bool {
        match self {
            Self::Legacy(s) => s.requires_source_progress(),
            Self::Inventory(s) => s.requires_source_progress(),
        }
    }
    fn uses_source_health(&self) -> bool {
        match self {
            Self::Legacy(s) => s.uses_source_health(),
            Self::Inventory(s) => s.uses_source_health(),
        }
    }

    fn observation_requirements(&self) -> PublicObservationRequirementsV1 {
        match self {
            Self::Legacy(s) => s.observation_requirements(),
            Self::Inventory(s) => s.observation_requirements(),
        }
    }
    fn historical_market_requirements(&self) -> Vec<ConditionId> {
        match self {
            Self::Legacy(s) => s.historical_market_requirements(),
            Self::Inventory(s) => s.historical_market_requirements(),
        }
    }
    fn calculation_market_requirements(
        &self,
        c: &StrategyCalculationV1<Diagnostics>,
    ) -> Vec<ConditionId> {
        match self {
            Self::Legacy(s) => s.calculation_market_requirements(&legacy(c)),
            Self::Inventory(s) => s.calculation_market_requirements(&inventory(c)),
        }
    }
    fn next_health_evaluation_at(
        &self,
        frames: &[PublicObservationFrameV1<'_>],
        c: Option<&StrategyCalculationV1<Diagnostics>>,
        at: i64,
    ) -> Option<i64> {
        match self {
            Self::Legacy(s) => s.next_health_evaluation_at(frames, c.map(legacy).as_ref(), at),
            Self::Inventory(s) => {
                s.next_health_evaluation_at(frames, c.map(inventory).as_ref(), at)
            }
        }
    }
    fn decide(
        &mut self,
        i: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1<Diagnostics>, Self::Error> {
        match self {
            Self::Legacy(s) => s.decide(i, at).map(|o| output(o, Diagnostics::Legacy)),
            Self::Inventory(s) => s.decide(i, at).map(|o| output(o, Diagnostics::Inventory)),
        }
    }
    fn decide_borrowed(
        &mut self,
        i: &BorrowedStrategyInputV1<'_>,
        at: i64,
    ) -> Result<StrategyOutputV1<Diagnostics>, Self::Error> {
        match self {
            Self::Legacy(s) => s
                .decide_borrowed(i, at)
                .map(|o| output(o, Diagnostics::Legacy)),
            Self::Inventory(s) => s
                .decide_borrowed(i, at)
                .map(|o| output(o, Diagnostics::Inventory)),
        }
    }
    fn qualify_intent(
        &mut self,
        i: &StrategyCurrentReadV1<'_>,
        c: &StrategyCalculationV1<Diagnostics>,
        n: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        match self {
            Self::Legacy(s) => s.qualify_intent(i, &legacy(c), n, at),
            Self::Inventory(s) => s.qualify_intent(i, &inventory(c), n, at),
        }
    }
    fn qualify_intent_borrowed(
        &mut self,
        i: &BorrowedStrategyCurrentReadV1<'_>,
        c: &StrategyCalculationV1<Diagnostics>,
        n: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        match self {
            Self::Legacy(s) => s.qualify_intent_borrowed(i, &legacy(c), n, at),
            Self::Inventory(s) => s.qualify_intent_borrowed(i, &inventory(c), n, at),
        }
    }
    fn qualify_intent_public(
        &self,
        g: &ClosedBusinessGroupV1,
        c: &StrategyCalculationV1<Diagnostics>,
        n: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        match self {
            Self::Legacy(s) => s.qualify_intent_public(g, &legacy(c), n, at),
            Self::Inventory(s) => s.qualify_intent_public(g, &inventory(c), n, at),
        }
    }
    fn invalidated(&mut self, c: &StrategyCalculationV1<Diagnostics>, r: &InvalidatedBatchV1) {
        match self {
            Self::Legacy(s) => s.invalidated(&legacy(c), r),
            Self::Inventory(s) => s.invalidated(&inventory(c), r),
        }
    }
    fn dispatched(
        &mut self,
        c: &StrategyCalculationV1<Diagnostics>,
        r: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
    ) {
        match self {
            Self::Legacy(s) => s.dispatched(&legacy(c), r),
            Self::Inventory(s) => s.dispatched(&inventory(c), r),
        }
    }
    fn research_diagnostics(&self, d: &Diagnostics) -> serde_json::Value {
        match (self, d) {
            (Self::Legacy(s), Diagnostics::Legacy(d)) => s.research_diagnostics(d),
            (Self::Inventory(s), Diagnostics::Inventory(d)) => s.research_diagnostics(d),
            _ => unreachable!("正式选择一致"),
        }
    }
    fn research_compact_diagnostics(&self, d: &Diagnostics) -> serde_json::Value {
        match (self, d) {
            (Self::Legacy(s), Diagnostics::Legacy(d)) => s.research_compact_diagnostics(d),
            (Self::Inventory(s), Diagnostics::Inventory(d)) => s.research_compact_diagnostics(d),
            _ => unreachable!("正式选择一致"),
        }
    }
    fn research_evaluated_markets(&self, d: &Diagnostics) -> Vec<ConditionId> {
        match (self, d) {
            (Self::Legacy(s), Diagnostics::Legacy(d)) => s.research_evaluated_markets(d),
            (Self::Inventory(s), Diagnostics::Inventory(d)) => s.research_evaluated_markets(d),
            _ => unreachable!("正式选择一致"),
        }
    }
    fn research_request_markets(&self, d: &Diagnostics) -> Vec<ConditionId> {
        match (self, d) {
            (Self::Legacy(s), Diagnostics::Legacy(d)) => s.research_request_markets(d),
            (Self::Inventory(s), Diagnostics::Inventory(d)) => s.research_request_markets(d),
            _ => unreachable!("正式选择一致"),
        }
    }
}
impl CheckpointStrategyV1 for SelectedStrategy {
    fn write_strategy_checkpoint(
        &self,
        w: &mut dyn std::io::Write,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Legacy(s) => s.write_strategy_checkpoint(w),
            Self::Inventory(s) => s.write_strategy_checkpoint(w),
        }
    }
    fn restore_strategy_checkpoint(
        &mut self,
        r: &mut dyn std::io::Read,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Legacy(s) => s.restore_strategy_checkpoint(r),
            Self::Inventory(s) => s.restore_strategy_checkpoint(r),
        }
    }
}
