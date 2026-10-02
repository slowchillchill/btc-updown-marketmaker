use crate::{MaterialError, *};
use contracts::{execution_normalization::*, *};
use ledger::{AccountAdmissionPolicyV2, LedgerProjectionV2, LedgerWriterV1};
use serde::{Deserialize, Serialize};
use shared_run::*;
use sim_system::{
    execution::*, finality::TradeFinalityPolicyV2,
    latency_profile::load_embedded_proxy_latency_profile, platform_rules::*, public_consumption::*,
    scenario::*,
};
use std::{path::PathBuf, sync::Arc};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyDescriptorV1 {
    pub strategy_id: String,
    pub strategy_rule_version: String,
    pub effective_parameters: serde_json::Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoricalSessionV1 {
    pub run_id: String,
    pub session_id: String,
    pub account_id: String,
    pub caller_instance_id: String,
    pub creation_request_id: String,
    pub response_bridge_binding_id: String,
    pub initial_account_state: InitialAccountStateV2,
    pub session_seed: Sha256Hex,
}
impl HistoricalSessionV1 {
    fn spec(&self) -> SessionSpecV3 {
        SessionSpecV3 {
            run_id: self.run_id.clone(),
            session_id: self.session_id.clone(),
            account_id: self.account_id.clone(),
            caller_role: SimCallerRoleV1::HistoricalBacktest,
            caller_instance_id: self.caller_instance_id.clone(),
            creation_request_id: self.creation_request_id.clone(),
            response_bridge_binding_id: self.response_bridge_binding_id.clone(),
            initial_account_state: self.initial_account_state.clone(),
            session_seed: self.session_seed.clone(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoricalRunConfigV1 {
    pub preparation: HistoricalPreparationConfigV1,
    pub timing: HistoricalTimingV1,
    pub session: HistoricalSessionV1,
    pub strategy: StrategyDescriptorV1,
    pub implementation_identity: ImplementationIdentityV1,
    pub finality_policy_path: PathBuf,
    pub sim_log: PathBuf,
    pub noe_root: PathBuf,
    pub ledger_root: PathBuf,
    pub metadata_path: PathBuf,
    pub result_path: PathBuf,
    pub activation_id: String,
    pub consumption_sequence_ref: String,
    pub noe_implementation_identity: String,
    pub admission_policy: AccountAdmissionPolicyV2,
    pub max_group_bytes: usize,
    pub finite_in_flight_groups: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum HistoricalEndReasonV1 {
    RangeEnd,
    StrategyExit,
    Failure,
}
#[derive(Clone, Debug, Serialize)]
pub struct CalculationTimingRecordV1 {
    pub observation_at_utc_ms: i64,
    pub calculation_started_at_utc_ms: i64,
    pub calculation_completed_at_utc_ms: i64,
    pub scheduled_send_at_utc_ms: i64,
    pub actual_sent_at_utc_ms: Option<i64>,
}
/// 经济读取与未闭合对象来自同一公共状态。Exit/失败的内存读取不声称已经落盘。
#[derive(Debug, Serialize)]
pub struct HistoricalRunReportV1 {
    pub end_reason: HistoricalEndReasonV1,
    pub last_observed_at_utc_ms: Option<i64>,
    pub recording_drained: bool,
    pub durable_noe_prefix: u64,
    pub ledger_consumed_memory: Option<LedgerProjectionV2>,
    pub conservative_nav_pusd: Option<ExecutionReadValueV1<ExactRationalV1>>,
    pub worst_case_maker_nav_pusd: Option<ExactRationalV1>,
    pub unclosed_commitments: usize,
    pub pending_noe_records: usize,
    pub pending_calculation: bool,
    pub next_required_at_utc_ms: Option<i64>,
    pub economics_version: String,
    pub last_dispatched_calculation_timing: Option<CalculationTimingRecordV1>,
    pub pending_calculation_timing: Option<CalculationTimingRecordV1>,
    pub calculation_count: usize,
}
#[derive(Debug)]
pub enum HistoricalFailureCauseV1<E> {
    Material(MaterialError),
    Sim(SimRunActorErrorV2),
    Ledger(ledger::LedgerErrorV1),
    Settlement(SharedSettlementErrorV1),
    Strategy(E),
    Dispatch(BatchExecutionErrorV1),
}
#[derive(Debug)]
pub struct HistoricalRunFailureV1<E> {
    pub cause: HistoricalFailureCauseV1<E>,
    pub report: Option<HistoricalRunReportV1>,
}
struct Pending<D> {
    calculation: CompletedStrategyCalculationV1<D>,
    completed_at: i64,
    send_at: i64,
    record: CalculationTimingRecordV1,
}
fn assemble(
    config: &HistoricalRunConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    input_identity: Sha256Hex,
) -> Result<
    (
        PublicSimExecutionV1,
        SharedExecutionV2,
        MemoryNoeObservation<MeasuredCancellationEvidence>,
    ),
    MaterialError,
> {
    let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&std::fs::read(
        &config.finality_policy_path,
    )?)?);
    let scenario = Arc::new(SealedPlatformScenarioV1::decode_and_seal(
        &serde_json::to_vec(&SimulationScenarioArtifactV1 {
            schema_version: PLATFORM_SCENARIO_SCHEMA_V1.into(),
            markets: vec![],
        })?,
    )?);
    // SIM 自身原经验时延不替换为宿主计算/交付延迟。
    let latency = load_embedded_proxy_latency_profile()?;
    let opened = RunOpenedV2 {
        projection_version: BusinessProjectionVersionV1::V4,
        run_id: config.session.run_id.clone(),
        durability_role: RunDurabilityRoleV2::FiniteHistorical,
        platform_rule_snapshot_file_sha256: rules.file_sha256().clone(),
        trade_finality_policy_file_sha256: finality.policy_id().clone(),
        platform_scenario_file_sha256: scenario.file_sha256().clone(),
        proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
        implementation_identity: config.implementation_identity.clone(),
        market_event_source_identity_sha256: Some(input_identity.clone()),
    };
    let reference = |position| DurableSourceReferenceV1 {
        source: ImmutableSourceReferenceV1::Location(format!(
            "{}#input={position}",
            config.sim_log.display()
        )),
    };
    let port = MemoryNoeProjectionPortV4::new(
        &config.noe_root,
        opened.clone(),
        reference(0),
        reference(1),
        config.session.creation_request_id.clone(),
        config.noe_implementation_identity.clone(),
    );
    let observation = port.observation();
    let public = PublicRunOpenedV1 {
        projection_version: opened.projection_version,
        run_id: opened.run_id,
        platform_rule_snapshot_file_sha256: opened.platform_rule_snapshot_file_sha256,
        trade_finality_policy_file_sha256: opened.trade_finality_policy_file_sha256,
        platform_scenario_file_sha256: opened.platform_scenario_file_sha256,
        proxy_latency_profile_file_sha256: opened.proxy_latency_profile_file_sha256,
        implementation_identity: opened.implementation_identity,
        market_event_source_identity_sha256: opened.market_event_source_identity_sha256,
    };
    let mut execution = PublicSimExecutionV1::create_new_historical(
        &config.sim_log,
        public,
        rules,
        finality,
        scenario,
        latency,
        Box::new(port),
        config.max_group_bytes,
        config.finite_in_flight_groups,
    )?;
    loop {
        match execution.create_session(config.session.spec()) {
            Err(SimRunActorErrorV2::RecordingBackpressured) => {
                execution.wait_for_recording_capacity()?
            }
            result => {
                result?;
                break;
            }
        }
    }
    let mut ledger = LedgerWriterV1::new_with_admission_v2(config.admission_policy);
    execution.bind_ledger_recording_v2(&mut ledger, &config.ledger_root)?;
    let shared = SharedExecutionV2::from_opening(ledger, observation.clone())?
        .ok_or("原开户未产生公共事实")?;
    loop {
        match execution.activate_session(config.activation_id.clone()) {
            Err(SimRunActorErrorV2::RecordingBackpressured) => {
                execution.wait_for_recording_capacity()?
            }
            result => {
                result?;
                break;
            }
        }
    }
    Ok((execution, shared, observation))
}
fn report<I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>>(
    driver: &FinitePublicConsumptionV1<I>,
    shared: &SharedExecutionV2,
    durable: u64,
    reason: HistoricalEndReasonV1,
    timing: Option<CalculationTimingRecordV1>,
    pending_timing: Option<CalculationTimingRecordV1>,
    count: usize,
) -> HistoricalRunReportV1 {
    let frames = driver.frames_at_observation();
    let economics = shared.ledger().economic_read().map(|read| {
        AccountEconomicsReadV2::from_ledger_and_observations(
            &read,
            frames.iter().map(|frame| frame.observation),
        )
    });
    HistoricalRunReportV1 {
        end_reason: reason,
        last_observed_at_utc_ms: driver
            .observation_cutoff()
            .map(|cutoff| cutoff.decision_time().logical_time_utc_ms()),
        recording_drained: false,
        durable_noe_prefix: durable,
        ledger_consumed_memory: shared.ledger().projection_v2(),
        conservative_nav_pusd: economics
            .as_ref()
            .map(|read| read.conservative_nav_pusd.clone()),
        worst_case_maker_nav_pusd: economics.map(|read| read.worst_case_maker_nav_pusd),
        unclosed_commitments: shared.registry().all_commitments().count(),
        pending_noe_records: shared.pending_noe_record_count(),
        pending_calculation: pending_timing.is_some(),
        next_required_at_utc_ms: driver
            .next_required_logical_drive()
            .map(|next| next.required_at_utc_ms),
        economics_version: ACCOUNT_ECONOMICS_VERSION.into(),
        last_dispatched_calculation_timing: timing,
        pending_calculation_timing: pending_timing,
        calculation_count: count,
    }
}
/// 一个账户、一个共享执行与一个策略贯穿整个显式历史范围。策略错误与原分派错误原样交回。
pub async fn run_historical_v1<S: StrategyV1>(
    config: &HistoricalRunConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    strategy: &mut S,
) -> Result<HistoricalRunReportV1, HistoricalRunFailureV1<S::Error>> {
    let prepared =
        prepare_historical_run_v1(&config.preparation).map_err(|cause| HistoricalRunFailureV1 {
            cause: HistoricalFailureCauseV1::Material(cause),
            report: None,
        })?;
    let metadata = serde_json::json!({"config": config, "source_manifests": prepared.source_manifests, "input_set_identity_sha256": prepared.input_set_identity_sha256, "public_prefix": prepared.hidden.prefix, "rules_file_sha256": rules.file_sha256(), "caller_role": SimCallerRoleV1::HistoricalBacktest, "recording_role": RunDurabilityRoleV2::FiniteHistorical, "economics_version": ACCOUNT_ECONOMICS_VERSION});
    let start = || -> Result<_, MaterialError> {
        serde_json::to_writer(std::fs::File::create(&config.metadata_path)?, &metadata)?;
        let (execution, shared, observation) =
            assemble(config, rules, prepared.input_set_identity_sha256.clone())?;
        let source = RecordedPublicGroupSourceV1::open(
            &prepared.public_root,
            config.preparation.read_page_rows,
            config.preparation.max_read_batch_bytes,
            crate::timing::DeliveryTimes {
                delays: config.timing.public_delivery_delay_ms.clone(),
                last: None,
            },
        )?;
        let hidden = RecordedHiddenBucketSourceV1::open(
            &prepared.hidden,
            &prepared.public_root,
            config.preparation.read_page_rows,
            config.preparation.max_read_batch_bytes,
        )?;
        Ok((
            FinitePublicConsumptionV1::new(
                execution,
                source,
                config.consumption_sequence_ref.clone(),
            ),
            shared,
            observation,
            hidden,
        ))
    };
    let (mut driver, mut shared, observation, mut hidden) =
        start().map_err(|cause| HistoricalRunFailureV1 {
            cause: HistoricalFailureCauseV1::Material(cause),
            report: None,
        })?;
    let mut pending: Option<Pending<S::Diagnostics>> = None;
    let mut start_at = None;
    let mut dirty = false;
    let mut calculation_index = 0;
    let mut timing = None;
    let mut evaluation_at = None;
    let mut coordinator = SettlementCoordinatorV1::default();
    let end = config.preparation.scope.received_end_utc_ms;
    let running = async {
        let mut next_hidden = hidden
            .next_bucket()
            .map_err(HistoricalFailureCauseV1::Material)?;
        loop {
            let public_at = driver
                .next_group_at()
                .map_err(HistoricalFailureCauseV1::Sim)?;
            let hidden_at = next_hidden
                .as_ref()
                .map(|bucket| bucket.reference().bucket_time_utc_ms);
            let required = driver
                .next_required_logical_drive()
                .map(|next| next.required_at_utc_ms);
            let pending_at = pending.as_ref().map(|p| {
                if matches!(
                    p.calculation.calculation.output,
                    StrategyOutputV1::Exit { .. }
                ) {
                    p.completed_at
                } else {
                    p.send_at
                }
            });
            let at = [
                public_at,
                hidden_at,
                required,
                start_at.filter(|_| pending.is_none()),
                evaluation_at.filter(|_| pending.is_none()),
                pending_at,
            ]
            .into_iter()
            .flatten()
            .filter(|at| *at < end)
            .min();
            let Some(at) = at else {
                break;
            };
            if pending_at == Some(at)
                && pending.as_ref().is_some_and(|p| {
                    matches!(
                        p.calculation.calculation.output,
                        StrategyOutputV1::Exit { .. }
                    )
                })
            {
                return Ok(HistoricalEndReasonV1::StrategyExit);
            }
            if evaluation_at == Some(at) && pending.is_none() {
                evaluation_at = None;
                dirty = true;
            }
            if required == Some(at) {
                dirty = true;
            }
            // 更早原义务先推进；本毫秒仍开放，全部真实公开组保持原序。
            driver
                .wait_for_recording_capacity()
                .map_err(HistoricalFailureCauseV1::Sim)?;
            driver
                .advance_before(at)
                .map_err(HistoricalFailureCauseV1::Sim)?;
            while driver
                .next_group_at()
                .map_err(HistoricalFailureCauseV1::Sim)?
                == Some(at)
            {
                if let Some(group) = driver
                    .next_prepared_group()
                    .map_err(HistoricalFailureCauseV1::Sim)?
                {
                    for context in &group.pm_contexts {
                        if !driver.has_enrolled_market(&context.market.condition_id) {
                            driver
                                .wait_for_recording_capacity()
                                .map_err(HistoricalFailureCauseV1::Sim)?;
                            driver
                                .enroll_market(MarketEnrollmentRequestV1 {
                                    enrollment_request_id: context.market.condition_id.0.clone(),
                                    market: prepared.markets[&context.market.condition_id.0]
                                        .clone(),
                                })
                                .map_err(HistoricalFailureCauseV1::Sim)?;
                        }
                    }
                }
                driver
                    .wait_for_recording_capacity()
                    .map_err(HistoricalFailureCauseV1::Sim)?;
                driver.next_group().map_err(HistoricalFailureCauseV1::Sim)?;
                dirty = true;
            }
            // 已冻结发送先于本毫秒完整隐藏桶闭合，允许原 SIM 请求/行情竞速。
            if pending_at == Some(at) {
                let p = pending.take().expect("待完成计算");
                if matches!(
                    p.calculation.calculation.output,
                    StrategyOutputV1::Exit { .. }
                ) {
                    return Ok(HistoricalEndReasonV1::StrategyExit);
                }
                driver
                    .wait_for_recording_capacity()
                    .map_err(HistoricalFailureCauseV1::Sim)?;
                let mut decision = driver
                    .send_decision_at(p.calculation.calculation.input_cutoff.clone(), at)
                    .map_err(HistoricalFailureCauseV1::Sim)?;
                let dispatched = shared
                    .dispatch_strategy(&mut decision, strategy, p.calculation)
                    .await
                    .map_err(HistoricalFailureCauseV1::Dispatch)?;
                if let StrategyDispatchV1::Continue {
                    calculation,
                    sent_at_utc_ms,
                    ..
                } = dispatched
                {
                    let mut record = p.record;
                    record.actual_sent_at_utc_ms = Some(sent_at_utc_ms);
                    timing = Some(record);
                    if let StrategyOutputV1::Continue {
                        next_evaluation_at_utc_ms,
                        ..
                    } = calculation.calculation.output
                    {
                        evaluation_at = next_evaluation_at_utc_ms.map(|next| next.max(at));
                    }
                }
            }
            if hidden_at == Some(at) {
                let bucket = next_hidden.take().expect("真实隐藏桶");
                for record in bucket.records() {
                    let market = match record {
                        HiddenExecutionRecordV1::Step { market, .. } => Some(market),
                        HiddenExecutionRecordV1::MarketFact(fact) => Some(&fact.market),
                        HiddenExecutionRecordV1::Reference(_) => None,
                    };
                    if let Some(market) = market
                        && !driver.has_enrolled_market(&market.condition_id)
                    {
                        driver
                            .wait_for_recording_capacity()
                            .map_err(HistoricalFailureCauseV1::Sim)?;
                        driver
                            .enroll_market(MarketEnrollmentRequestV1 {
                                enrollment_request_id: market.condition_id.0.clone(),
                                market: prepared.markets[&market.condition_id.0].clone(),
                            })
                            .map_err(HistoricalFailureCauseV1::Sim)?;
                    }
                }
                driver
                    .wait_for_recording_capacity()
                    .map_err(HistoricalFailureCauseV1::Sim)?;
                driver
                    .hidden_bucket(bucket)
                    .map_err(HistoricalFailureCauseV1::Sim)?;
                next_hidden = hidden
                    .next_bucket()
                    .map_err(HistoricalFailureCauseV1::Material)?;
            }
            driver
                .wait_for_recording_capacity()
                .map_err(HistoricalFailureCauseV1::Sim)?;
            driver
                .observe_at(at)
                .map_err(HistoricalFailureCauseV1::Sim)?;
            shared
                .advance_settlement(&mut coordinator, &mut driver)
                .map_err(HistoricalFailureCauseV1::Settlement)?;
            if pending.is_none() && dirty && start_at.is_none() {
                start_at = Some(
                    crate::timing::after(
                        at,
                        config
                            .timing
                            .calculation_start_delay_ms
                            .at(calculation_index)
                            .map_err(HistoricalFailureCauseV1::Sim)?,
                    )
                    .map_err(HistoricalFailureCauseV1::Sim)?,
                );
            }
            if pending.is_none() && start_at == Some(at) {
                start_at = None;
                if let Some(input) = shared
                    .strategy_input(&mut driver)
                    .map_err(HistoricalFailureCauseV1::Ledger)?
                {
                    let calculation = input
                        .calculate(strategy, at)
                        .map_err(HistoricalFailureCauseV1::Strategy)?;
                    let observation_at = calculation
                        .input_cutoff
                        .decision_time()
                        .logical_time_utc_ms();
                    let completed_at = crate::timing::after(
                        at,
                        config
                            .timing
                            .calculation_duration_ms
                            .at(calculation_index)
                            .map_err(HistoricalFailureCauseV1::Sim)?,
                    )
                    .map_err(HistoricalFailureCauseV1::Sim)?;
                    let send_at = crate::timing::after(
                        completed_at,
                        config
                            .timing
                            .dispatch_delay_ms
                            .at(calculation_index)
                            .map_err(HistoricalFailureCauseV1::Sim)?,
                    )
                    .map_err(HistoricalFailureCauseV1::Sim)?;
                    pending = Some(Pending {
                        calculation: calculation.complete(completed_at),
                        completed_at,
                        send_at,
                        record: CalculationTimingRecordV1 {
                            observation_at_utc_ms: observation_at,
                            calculation_started_at_utc_ms: at,
                            calculation_completed_at_utc_ms: completed_at,
                            scheduled_send_at_utc_ms: send_at,
                            actual_sent_at_utc_ms: None,
                        },
                    });
                    calculation_index += 1;
                    dirty = false;
                }
            }
            // 严格更早的原义务由下一事件证明；在本毫秒无剩余事件时推进闭合。
            let immediate_pending = pending.as_ref().is_some_and(|p| {
                p.send_at == at
                    || (matches!(
                        p.calculation.calculation.output,
                        StrategyOutputV1::Exit { .. }
                    ) && p.completed_at == at)
            });
            let immediate_start =
                pending.is_none() && (start_at == Some(at) || evaluation_at == Some(at));
            if required == Some(at) && !immediate_pending && !immediate_start {
                driver
                    .advance_before(at.checked_add(1).ok_or_else(|| {
                        HistoricalFailureCauseV1::Sim(SimRunActorErrorV2::InvalidBusinessRequest(
                            "历史时序溢出",
                        ))
                    })?)
                    .map_err(HistoricalFailureCauseV1::Sim)?;
            }
        }
        driver
            .finish_input_at(end.checked_sub(1).ok_or_else(|| {
                HistoricalFailureCauseV1::Sim(SimRunActorErrorV2::InvalidBusinessRequest(
                    "历史范围结束时刻溢出",
                ))
            })?)
            .map_err(HistoricalFailureCauseV1::Sim)?;
        shared
            .advance_settlement(&mut coordinator, &mut driver)
            .map_err(HistoricalFailureCauseV1::Settlement)?;
        Ok(HistoricalEndReasonV1::RangeEnd)
    }
    .await;
    match running {
        Ok(reason) => {
            let normal = reason == HistoricalEndReasonV1::RangeEnd;
            let mut result = report(
                &driver,
                &shared,
                observation.durable_through(),
                reason,
                timing,
                pending.as_ref().map(|p| p.record.clone()),
                calculation_index,
            );
            if normal {
                driver
                    .stop_and_drain()
                    .map_err(|cause| HistoricalRunFailureV1 {
                        cause: HistoricalFailureCauseV1::Sim(cause),
                        report: None,
                    })?;
                result.recording_drained = true;
                result.durable_noe_prefix = observation.durable_through();
                serde_json::to_writer(
                    std::fs::File::create(&config.result_path).map_err(|cause| {
                        HistoricalRunFailureV1 {
                            cause: HistoricalFailureCauseV1::Material(Box::new(cause)),
                            report: None,
                        }
                    })?,
                    &result,
                )
                .map_err(|cause| HistoricalRunFailureV1 {
                    cause: HistoricalFailureCauseV1::Material(Box::new(cause)),
                    report: None,
                })?;
            }
            Ok(result)
        }
        Err(cause) => Err(HistoricalRunFailureV1 {
            cause,
            report: Some(report(
                &driver,
                &shared,
                observation.durable_through(),
                HistoricalEndReasonV1::Failure,
                timing,
                pending.as_ref().map(|p| p.record.clone()),
                calculation_index,
            )),
        }),
    }
}
