//! 策略输入装配分项墙钟；stage-timing下每种路径每64次调用采样一次。
//! 子阶段嵌套在backtest的strategy_input_assembly内，不能与父阶段相加。
macro_rules! measure_input_phase {
    ($stage:ident, $body:expr) => {{
        #[cfg(feature = "stage-timing")]
        let _phase =
            crate::input_measurement::PhaseTimer::new(crate::input_measurement::Stage::$stage);
        $body
    }};
}
#[cfg(feature = "stage-timing")]
mod enabled {
    use std::{cell::RefCell, collections::BTreeMap, time::Instant};
    const STRIDE: u64 = 64;
    #[derive(Clone, Copy)]
    pub(crate) enum Mode {
        CurrentPrefix = 0,
        ReferenceFrames = 1,
    }
    #[derive(Clone, Copy)]
    #[repr(usize)]
    pub(crate) enum Stage {
        ApplyClosed,
        CurrentStatisticsFrames,
        RegisterPublicRules,
        FreezeOrderScope,
        FreezeRegistry,
        ActionOrderScope,
        ObligationTrades,
        RequestTradeIdentities,
        ActionFunds,
        LedgerEconomicRead,
        RequestTradeEconomics,
        AccountEconomics,
        LedgerProjection,
        LearningAccount,
        TradeDetails,
    }
    const NAMES: [&str; 15] = [
        "apply_closed",
        "current_statistics_frames",
        "register_public_rules",
        "freeze_order_scope",
        "freeze_registry",
        "action_order_scope",
        "obligation_trades",
        "request_trade_identities",
        "action_funds",
        "ledger_economic_read",
        "request_trade_economics",
        "account_economics",
        "ledger_projection",
        "learning_account",
        "trade_details",
    ];
    #[derive(Clone, Default, serde::Serialize)]
    pub struct Sample {
        pub count: u64,
        pub total_ns: u128,
        pub max_ns: u128,
    }
    #[derive(Clone, Default)]
    struct Measurements {
        calls: u64,
        sampled_calls: u64,
        samples: [Sample; 15],
    }
    struct State {
        active: Option<Mode>,
        modes: [Measurements; 2],
    }
    thread_local! {static STATE:RefCell<State>=RefCell::new(State{active:None,modes:std::array::from_fn(|_|Measurements::default())});}
    pub(crate) struct AssemblyScope {
        previous: Option<Mode>,
    }
    impl AssemblyScope {
        pub(crate) fn new(mode: Mode) -> Self {
            STATE.with(|state| {
                let mut state = state.borrow_mut();
                let previous = state.active;
                let measurements = &mut state.modes[mode as usize];
                let sample = measurements.calls.is_multiple_of(STRIDE);
                measurements.calls += 1;
                if sample {
                    measurements.sampled_calls += 1;
                }
                state.active = sample.then_some(mode);
                Self { previous }
            })
        }
    }
    impl Drop for AssemblyScope {
        fn drop(&mut self) {
            STATE.with(|state| state.borrow_mut().active = self.previous);
        }
    }
    pub(crate) struct PhaseTimer {
        sample: Option<(Mode, Stage, Instant)>,
    }
    impl PhaseTimer {
        pub(crate) fn new(stage: Stage) -> Self {
            Self {
                sample: STATE.with(|state| {
                    state
                        .borrow()
                        .active
                        .map(|mode| (mode, stage, Instant::now()))
                }),
            }
        }
    }
    impl Drop for PhaseTimer {
        fn drop(&mut self) {
            if let Some((mode, stage, started)) = &self.sample {
                let ns = started.elapsed().as_nanos();
                STATE.with(|state| {
                    let mut state = state.borrow_mut();
                    let sample = &mut state.modes[*mode as usize].samples[*stage as usize];
                    sample.count += 1;
                    sample.total_ns += ns;
                    sample.max_ns = sample.max_ns.max(ns);
                });
            }
        }
    }
    #[derive(Clone, serde::Serialize)]
    pub struct InputAssemblyModeWorkV1 {
        pub calls: u64,
        pub sampled_calls: u64,
        pub stages: BTreeMap<&'static str, Sample>,
    }
    #[derive(Clone, serde::Serialize)]
    pub struct InputAssemblyWorkV1 {
        pub sample_stride: u64,
        pub modes: BTreeMap<&'static str, InputAssemblyModeWorkV1>,
    }
    pub fn strategy_input_assembly_work_v1() -> InputAssemblyWorkV1 {
        STATE.with(|state| {
            let state = state.borrow();
            InputAssemblyWorkV1 {
                sample_stride: STRIDE,
                modes: ["current_prefix", "reference_frames"]
                    .into_iter()
                    .zip(&state.modes)
                    .map(|(name, measurements)| {
                        (
                            name,
                            InputAssemblyModeWorkV1 {
                                calls: measurements.calls,
                                sampled_calls: measurements.sampled_calls,
                                stages: NAMES
                                    .into_iter()
                                    .zip(measurements.samples.iter().cloned())
                                    .filter(|(_, s)| s.count > 0)
                                    .collect(),
                            },
                        )
                    })
                    .collect(),
            }
        })
    }
}
#[cfg(feature = "stage-timing")]
pub(crate) use enabled::{AssemblyScope, Mode, PhaseTimer, Stage};
#[cfg(feature = "stage-timing")]
pub use enabled::{InputAssemblyModeWorkV1, InputAssemblyWorkV1, strategy_input_assembly_work_v1};
