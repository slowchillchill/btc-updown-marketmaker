//! PAPER正式组合根；策略政策属于baseline-mm，公共装配属于shared-run。
use baseline_mm::strategies::continuous_inventory::{
    ContinuousInventoryConfigV1, ContinuousInventoryContinuityV1, ContinuousInventoryV1,
};
use serde::{Deserialize, Serialize};
use shared_run::{paper::PaperError, paper_strategy::*};
use sim_system::platform_rules::SealedProcessRuleSnapshotV3;
use std::{future::Future, num::NonZeroU64, sync::Arc};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PaperTradeConfigV1 {
    pub runtime: PaperStrategyConfigV1,
    pub strategy: ContinuousInventoryConfigV1,
    pub request_namespace: String,
    pub progress_every_ms: NonZeroU64,
}
/// binary及受控组合验收复用同一入口；不创建第二账户或模型宿主。
pub async fn run_paper_trade_v1(
    config: &PaperTradeConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    stop: impl Future<Output = ()>,
    progress: impl FnMut(PaperStrategyProgressV1<'_>),
) -> Result<PaperStrategyReportV1, PaperError> {
    run_paper_trade_inner_v1(config, rules, stop, progress, None).await
}

/// 正式程序固定的Exit政策：普通process::exit(0)，先于报告和排空。
/// 退出码0仅说明策略要求结束，不代表输入自然结束或经济事实完整。
pub async fn run_paper_trade_process_v1(
    config: &PaperTradeConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    stop: impl Future<Output = ()>,
    progress: impl FnMut(PaperStrategyProgressV1<'_>),
) -> Result<PaperStrategyReportV1, PaperError> {
    run_paper_trade_inner_v1(
        config,
        rules,
        stop,
        progress,
        Some(|| std::process::exit(0)),
    )
    .await
}

async fn run_paper_trade_inner_v1(
    config: &PaperTradeConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    stop: impl Future<Output = ()>,
    progress: impl FnMut(PaperStrategyProgressV1<'_>),
    process_exit: Option<fn()>,
) -> Result<PaperStrategyReportV1, PaperError> {
    let strategy = ContinuousInventoryV1::new(
        config.strategy.clone(),
        rules.clone(),
        config.request_namespace.clone(),
        ContinuousInventoryContinuityV1::new_account(config.strategy.initial_cash_pusd),
    )?;
    match process_exit {
        Some(exit) => {
            run_paper_strategy_with_exit_v1(&config.runtime, rules, strategy, stop, progress, exit)
                .await
        }
        None => {
            run_paper_strategy_with_progress_v1(&config.runtime, rules, strategy, stop, progress)
                .await
        }
    }
}
