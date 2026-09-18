//! PAPER 唯一装配和串行消费入口。仅等待 DPT 下一输入时服务原执行时钟。
use crate::SharedExecutionV1;
use ::execution_normalization::{
    MemoryIdentityKeyV1, NormalizationRecord, SourceNeutralEntityKeyV1, SourceNeutralEvidenceKind,
};
use contracts::{execution_normalization::*, *};
use data_pipeline_training::public_online::*;
use ledger::{AccountAdmissionPolicyV1, LedgerProjectionV2, LedgerWriterV1};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sim_system::{
    execution::*, finality::TradeFinalityPolicyV2,
    latency_profile::load_embedded_proxy_latency_profile, platform_rules::*, public_consumption::*,
    scenario::*,
};
use std::{future::Future, path::PathBuf, sync::Arc};

pub type PaperError = Box<dyn std::error::Error + Send + Sync>;

/// 所有值由调用方明确提供；不包含生产资源默认值或可替换的 HTTP 端点。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PaperConfigV1 {
    pub socket_path: PathBuf,
    pub dpt_root: PathBuf,
    pub sim_log: PathBuf,
    pub noe_root: PathBuf,
    pub ledger_root: PathBuf,
    pub public_online: PublicOnlineConfigV1,
    pub reference_market: ReferenceMarketSpecV1,
    pub rule_snapshot_path: PathBuf,
    pub implementation_identity: ImplementationIdentityV1,
    pub finality_policy_path: PathBuf,
    pub session: PaperSessionV1,
    pub activation_id: String,
    pub consumption_sequence_ref: String,
    pub noe_implementation_identity: String,
    pub admission_policy: AccountAdmissionPolicyV1,
    pub max_group_bytes: usize,
    pub finite_in_flight_groups: usize,
    pub script: PaperScriptConfigV1,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PaperSessionV1 {
    pub run_id: String,
    pub session_id: String,
    pub account_id: String,
    pub caller_instance_id: String,
    pub creation_request_id: String,
    pub response_bridge_binding_id: String,
    pub initial_account_state: InitialAccountStateV2,
    pub session_seed: Sha256Hex,
}
impl PaperSessionV1 {
    fn spec(&self) -> SessionSpecV3 {
        SessionSpecV3 {
            run_id: self.run_id.clone(),
            session_id: self.session_id.clone(),
            account_id: self.account_id.clone(),
            caller_role: SimCallerRoleV1::Paper,
            caller_instance_id: self.caller_instance_id.clone(),
            creation_request_id: self.creation_request_id.clone(),
            response_bridge_binding_id: self.response_bridge_binding_id.clone(),
            initial_account_state: self.initial_account_state.clone(),
            session_seed: self.session_seed.clone(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PaperScriptConfigV1 {
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub limit_price_pusd_per_share: Decimal,
    pub size_shares: Decimal,
    pub post_only: bool,
    pub submit_context: RequestContextV2,
    pub cancel_context: RequestContextV2,
}
impl PaperConfigV1 {
    pub fn snapshot_request(&self) -> CurrentProcessSnapshotRequestV1 {
        CurrentProcessSnapshotRequestV1 {
            reference_market: self.reference_market.clone(),
            target_path: self.rule_snapshot_path.clone(),
            implementation_identity: self.implementation_identity.clone(),
        }
    }
}
#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
pub enum PaperScriptStepV1 {
    #[default]
    AwaitingNoTrade,
    AwaitingTargetGroup,
    AwaitingCancellableOrder,
    AwaitingCancellationFeedback,
    CancellationFeedback(SourceNeutralCancellationOutcomeV1),
}
#[derive(Debug, Serialize)]
pub struct PaperReportV1 {
    pub dpt: PublicOnlineOutcomeV1,
    pub sim_noe_ledger_recording_drained: bool,
    pub ledger_consumed: Option<LedgerProjectionV2>,
    pub script: PaperScriptStepV1,
    /// 没有外部生产者退出证据；DPT End 也不等于进程已退出。
    pub collector_exit: Option<i32>,
}
#[derive(Debug, Serialize)]
pub struct PaperFailureV1 {
    pub reason: String,
    pub dpt: Option<PublicOnlineOutcomeV1>,
    pub measurements: PublicOnlineMeasurementsV1,
    pub last_received_transport_sequence: u64,
    pub ledger_consumed: Option<LedgerProjectionV2>,
    pub script: PaperScriptStepV1,
}
impl std::fmt::Display for PaperFailureV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(self).map_err(|_| std::fmt::Error)?
        )
    }
}
impl std::error::Error for PaperFailureV1 {}

fn assemble(
    config: &PaperConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
) -> Result<(ContinuousPublicConsumptionV1, SharedExecutionV1), PaperError> {
    let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&std::fs::read(
        &config.finality_policy_path,
    )?)?);
    let empty = PlatformScenarioV1::try_new(vec![])?;
    let artifact = SimulationScenarioArtifactV1 {
        schema_version: PLATFORM_SCENARIO_SCHEMA_V1.into(),
        scenario_sha256: empty.scenario_sha256().clone(),
        market_set_sha256: empty.market_set_sha256().clone(),
        markets: vec![],
    };
    let scenario = Arc::new(SealedPlatformScenarioV1::decode_and_seal(
        &serde_json::to_vec(&artifact)?,
    )?);
    let latency = load_embedded_proxy_latency_profile()?;
    let opened = RunOpenedV2 {
        projection_version: BusinessProjectionVersionV1::V3,
        run_id: config.session.run_id.clone(),
        durability_role: RunDurabilityRoleV2::CleanRun,
        platform_rule_snapshot_file_sha256: rules.file_sha256().clone(),
        trade_finality_policy_file_sha256: finality.policy_id().clone(),
        platform_scenario_file_sha256: scenario.file_sha256().clone(),
        proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
        implementation_identity: config.implementation_identity.clone(),
        market_event_source_identity_sha256: None,
    };
    let reference = |input| DurableSourceReferenceV1 {
        source: ImmutableSourceReferenceV1::Location(format!(
            "{}#input={input}",
            config.sim_log.display()
        )),
    };
    let port = MemoryNoeProjectionPortV3::new(
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
        market_event_source_identity_sha256: None,
    };
    let mut execution = PublicSimExecutionV1::create_new(
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
    let mut ledger = LedgerWriterV1::new(config.admission_policy.clone());
    execution.bind_ledger_recording_v2(&mut ledger, &config.ledger_root)?;
    let shared = SharedExecutionV1::from_opening(ledger, observation)?
        .expect("真实 create_session 已产生开户事实");
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
    Ok((
        ContinuousPublicConsumptionV1::new(execution, config.consumption_sequence_ref.clone()),
        shared,
    ))
}

async fn script_group(
    config: &PaperScriptConfigV1,
    step: &mut PaperScriptStepV1,
    driver: &mut ContinuousPublicConsumptionV1,
    shared: &mut SharedExecutionV1,
) -> Result<(), PaperError> {
    let Some(_before) = shared.action_before(driver)? else {
        return Ok(());
    };
    match step {
        PaperScriptStepV1::AwaitingNoTrade => {
            *step = PaperScriptStepV1::AwaitingTargetGroup;
        }
        PaperScriptStepV1::AwaitingTargetGroup => {
            let target = driver.frames().iter().any(|frame| {
                frame.observation.market.condition_id == config.condition_id
                    && frame
                        .observation
                        .assets
                        .iter()
                        .any(|asset| asset.asset.token_id == config.token_id)
            });
            if target {
                let intent = Arc::new(ModelIntentV1::SubmitSingleBuy {
                    condition_id: config.condition_id.clone(),
                    order: BuyLegIntentV1 {
                        token_id: config.token_id.clone(),
                        order: BuyOrderIntentV1::GtcLimit {
                            limit_price_pusd_per_share: config.limit_price_pusd_per_share,
                            size_shares: config.size_shares,
                            post_only: config.post_only,
                        },
                    },
                });
                driver.wait_for_recording_capacity()?;
                shared
                    .execute(
                        &mut driver.decision().expect("已应用组"),
                        config.submit_context.clone(),
                        Some(intent),
                    )
                    .await?;
                *step = PaperScriptStepV1::AwaitingCancellableOrder;
            }
        }
        PaperScriptStepV1::AwaitingCancellableOrder => {
            let id = shared.registry().orders().find_map(|order| {
                if order.origin.as_ref()?.intent.global_request_id()
                    != config.submit_context.global_request_id
                    || order.cancellable != ExecutionReadValueV1::Known(true)
                {
                    return None;
                }
                match &order.order_identity.key {
                    MemoryIdentityKeyV1::Entity(SourceNeutralEntityKeyV1::Order {
                        source_order_id,
                    }) => Some(source_order_id.clone()),
                    _ => None,
                }
            });
            if let Some(platform_order_id) = id {
                driver.wait_for_recording_capacity()?;
                shared
                    .execute(
                        &mut driver.decision().expect("已应用组"),
                        config.cancel_context.clone(),
                        Some(Arc::new(ModelIntentV1::CancelOrder { platform_order_id })),
                    )
                    .await?;
                *step = PaperScriptStepV1::AwaitingCancellationFeedback;
            }
        }
        PaperScriptStepV1::AwaitingCancellationFeedback => {
            for order in shared.registry().orders() {
                let Some(feedback) = order.latest_direct_feedback else {
                    continue;
                };
                let associated = matches!(&feedback.record().source_reference().record, SimV2SourceRecordReferenceV1::DirectResponse { association: SimV2DirectResponseAssociationV1::CancellationTarget { global_request_id, .. }, .. } if global_request_id == &config.cancel_context.global_request_id);
                if associated
                    && let NormalizationRecord::Evidence {
                        evidence:
                            SourceNeutralEvidenceKind::Control {
                                result: SourceNeutralControlResult::Cancellation { outcome, .. },
                            },
                        ..
                    } = feedback.record()
                {
                    *step = PaperScriptStepV1::CancellationFeedback(outcome.clone());
                    break;
                }
            }
        }
        PaperScriptStepV1::CancellationFeedback(_) => {}
    }
    Ok(())
}

/// binary 和受控 Unix 对端测试共用本入口。规则由原冷启动加载器在调用前封存。
/// 同一个 next_applied future 跨 timer 保留；完成段和计划停止排空段不再选择 timer。
pub async fn run_paper_v1(
    config: &PaperConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    stop: impl Future<Output = ()>,
) -> Result<PaperReportV1, PaperError> {
    let (mut driver, mut shared) = assemble(config, rules)?;
    let mut progress = PublicOnlineProgressV1::default();
    let mut step = PaperScriptStepV1::default();
    let mut completed = None;
    tokio::pin!(stop);
    let running = async {
        let stream = tokio::net::UnixStream::connect(&config.socket_path).await?;
        let mut session = PublicOnlineSessionV1::start(
            stream,
            &config.dpt_root,
            config.public_online.clone(),
            &mut progress,
        )
        .await?;
        let outcome = loop {
            let next = session.next_applied(&mut stop);
            tokio::pin!(next);
            let next = loop {
                let wait = async {
                    match driver.next_wake_after() {
                        Some(delay) => tokio::time::sleep(delay).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! { biased;
                    next = &mut next => break next?,
                    _ = wait => driver.advance_due()?,
                }
            };
            match next {
                PublicOnlineNextV1::Stopped(session) => {
                    break PublicOnlineOutcomeV1::Stopped(session.stop_and_drain().await?);
                }
                PublicOnlineNextV1::Applied(mut pending) => {
                    for group in PreparedPublicGroupV1::from_online(&mut pending) {
                        for context in &group.pm_contexts {
                            if !driver.has_enrolled_market(&context.market.condition_id) {
                                let assets = context
                                    .assets
                                    .iter()
                                    .map(|asset| asset.asset.clone())
                                    .collect::<Vec<_>>()
                                    .try_into()
                                    .map_err(|_| "原 PM 市场不是二元市场")?;
                                let market =
                                    PlatformMarketV1::try_new(context.market.clone(), assets)?;
                                let request = MarketEnrollmentRequestV1 {
                                    enrollment_request_id: context.market.condition_id.0.clone(),
                                    market,
                                };
                                loop {
                                    match driver.enroll_market(request.clone()) {
                                        Err(SimRunActorErrorV2::RecordingBackpressured) => {
                                            driver.wait_for_recording_capacity()?
                                        }
                                        result => {
                                            result?;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        driver.apply_group(group)?;
                        script_group(&config.script, &mut step, &mut driver, &mut shared).await?;
                    }
                    match pending.complete().await? {
                        PublicOnlineCompletedInputV1::Continue(next) => session = next,
                        PublicOnlineCompletedInputV1::End(end) => {
                            break PublicOnlineOutcomeV1::End(end);
                        }
                    }
                }
            }
        };
        completed = Some(outcome);
        driver.stop_and_drain()?;
        Ok::<_, PaperError>(())
    };
    if let Err(error) = running.await {
        let error = progress.failure(error);
        return Err(Box::new(PaperFailureV1 {
            reason: error.to_string(),
            dpt: completed,
            measurements: progress.measurements,
            last_received_transport_sequence: progress.last_received_transport_sequence,
            ledger_consumed: shared.ledger().projection_v2(),
            script: step,
        }));
    }
    Ok(PaperReportV1 {
        dpt: completed.expect("成功完成原 DPT 排空"),
        sim_noe_ledger_recording_drained: true,
        ledger_consumed: shared.ledger().projection_v2(),
        script: step,
        collector_exit: None,
    })
}
