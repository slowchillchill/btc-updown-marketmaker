//! PAPER 唯一装配和串行消费入口。仅等待 DPT 下一输入时服务原执行时钟。
use crate::{IntentReserveV1, ReservedIntentV1, SharedExecutionV2};
use ::execution_normalization::{
    MemoryIdentityKeyV1, NormalizationRecord, SourceNeutralEntityKeyV1, SourceNeutralEvidenceKind,
};
use contracts::{execution_normalization::*, *};
use data_pipeline_training::public_online::*;
use ledger::{
    AccountAdmissionPolicyV3, LedgerProjectionV2, LedgerRequestIdentityV1, LedgerWriterV1,
    ReserveIntentV1,
};
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
pub struct PaperRuntimeConfigV1 {
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
    pub admission_policy: AccountAdmissionPolicyV3,
    pub max_group_bytes: usize,
    pub finite_in_flight_groups: usize,
}
/// 旧有限脚本保留相同 JSON 字段；装配参数与脚本策略分开。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PaperConfigV1 {
    #[serde(flatten)]
    pub runtime: PaperRuntimeConfigV1,
    pub script: PaperScriptConfigV1,
}
impl std::ops::Deref for PaperConfigV1 {
    type Target = PaperRuntimeConfigV1;
    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}
impl std::ops::DerefMut for PaperConfigV1 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.runtime
    }
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
pub struct PaperGtcLegV1 {
    pub token_id: TokenId,
    pub limit_price_pusd_per_share: Decimal,
    pub size_shares: Decimal,
    pub post_only: bool,
}
impl PaperGtcLegV1 {
    fn intent(&self) -> BuyLegIntentV1 {
        BuyLegIntentV1 {
            token_id: self.token_id.clone(),
            order: BuyOrderIntentV1::GtcLimit {
                limit_price_pusd_per_share: self.limit_price_pusd_per_share,
                size_shares: self.size_shares,
                post_only: self.post_only,
            },
        }
    }
}
/// 有限接口验收脚本；精确业务值由配置给出，不选择或修改订单。
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum PaperScriptConfigV1 {
    SingleBuyThenCancel {
        condition_id: ConditionId,
        order: PaperGtcLegV1,
        submit_context: RequestContextV2,
        cancel_context: RequestContextV2,
    },
    PairedBuy {
        condition_id: ConditionId,
        orders: [PaperGtcLegV1; 2],
        submit_context: RequestContextV2,
    },
    CancelMarket {
        condition_id: ConditionId,
        cancel_context: RequestContextV2,
    },
}
impl PaperScriptConfigV1 {
    fn condition_id(&self) -> &ConditionId {
        match self {
            Self::SingleBuyThenCancel { condition_id, .. }
            | Self::PairedBuy { condition_id, .. }
            | Self::CancelMarket { condition_id, .. } => condition_id,
        }
    }
    fn cancel_context(&self) -> Option<&RequestContextV2> {
        match self {
            Self::SingleBuyThenCancel { cancel_context, .. }
            | Self::CancelMarket { cancel_context, .. } => Some(cancel_context),
            Self::PairedBuy { .. } => None,
        }
    }
}
impl PaperRuntimeConfigV1 {
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
    CancellationRequestCompleted(CancellationRequestCompletedV1),
    SubmissionSent,
    LocalFundsRejected {
        cash_available: Decimal,
    },
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

pub(super) fn assemble(
    config: &PaperRuntimeConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    resolution_policy: Option<GammaResolutionPolicyV1>,
) -> Result<(ContinuousPublicConsumptionV1, SharedExecutionV2), PaperError> {
    let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&std::fs::read(
        &config.finality_policy_path,
    )?)?);
    let artifact = SimulationScenarioArtifactV1 {
        schema_version: PLATFORM_SCENARIO_SCHEMA_V1.into(),
        markets: vec![],
    };
    let scenario = Arc::new(SealedPlatformScenarioV1::decode_and_seal(
        &serde_json::to_vec(&artifact)?,
    )?);
    let latency = load_embedded_proxy_latency_profile()?;
    let opened = RunOpenedV2 {
        projection_version: BusinessProjectionVersionV1::V4,
        latency_sampler_version: "xoshiro256plusplus_jump_multiply_high_empirical_rank_v2".into(),
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
    let mut ledger = LedgerWriterV1::new_with_admission_v3(config.admission_policy);
    execution.bind_ledger_recording_v2(&mut ledger, &config.ledger_root)?;
    let shared = SharedExecutionV2::from_opening(ledger, observation)?
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
    if let Some(policy) = resolution_policy {
        loop {
            match execution.configure_gamma_resolution_policy_v1(policy) {
                Err(SimRunActorErrorV2::RecordingBackpressured) => {
                    execution.wait_for_recording_capacity()?
                }
                result => {
                    result?;
                    break;
                }
            }
        }
    }
    Ok((
        ContinuousPublicConsumptionV1::new(execution, config.consumption_sequence_ref.clone()),
        shared,
    ))
}

/// 仅映射到成本所属模块的整数轴；不能表示的精确输入沿错误返回，不舍入业务值。
fn reserve_leg(
    rules: &SealedProcessRuleSnapshotV3,
    condition: &ConditionId,
    context: &RequestContextV2,
    ordinal: u16,
    leg: &PaperGtcLegV1,
    tick: &ExactDecimal,
) -> Result<IntentReserveV1, PaperError> {
    let tick_decimal: Decimal = tick.0.parse()?;
    let quantity = leg.size_shares.normalize();
    let quantity_axis: num_bigint::BigUint = quantity.mantissa().to_string().parse()?;
    let quantity_axis = quantity_axis
        * num_bigint::BigUint::from(10_u8).pow(
            2_u32
                .checked_sub(quantity.scale())
                .ok_or("份额不能精确表示为百分之一份")?,
        );
    let price_axis = (leg.limit_price_pusd_per_share / tick_decimal)
        .normalize()
        .to_string()
        .parse()?;
    let cost = rules.limit_buy_cost_v1(&quantity_axis, price_axis, tick, leg.post_only)?;
    let units = |amount: &num_bigint::BigUint, scale: usize| {
        let mut digits = format!("{:0>width$}", amount.to_string(), width = scale + 1);
        digits.insert(digits.len() - scale, '.');
        ExactDecimal(
            digits
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_owned(),
        )
    };
    Ok(IntentReserveV1 {
        batch_item_ordinal: ordinal,
        reserve: ReserveIntentV1 {
            request_identity: LedgerRequestIdentityV1(serde_json::to_string(&(
                &context.global_request_id,
                ordinal,
            ))?),
            condition_id: condition.clone(),
            requested_principal_pusd: units(&cost.principal_micro, 6),
            immediate_fee_reserve_pusd: units(&cost.fee_1e5_units, 5),
        },
    })
}

async fn script_group(
    config: &PaperScriptConfigV1,
    rules: &SealedProcessRuleSnapshotV3,
    step: &mut PaperScriptStepV1,
    driver: &mut ContinuousPublicConsumptionV1,
    shared: &mut SharedExecutionV2,
) -> Result<(), PaperError> {
    let Some(cutoff) = shared.prepare_action_group(driver)? else {
        return Ok(());
    };
    // 终态继续消费公共事实与经济反馈，不为已结束脚本逐组复制账户或登记。
    if matches!(
        step,
        PaperScriptStepV1::SubmissionSent
            | PaperScriptStepV1::LocalFundsRejected { .. }
            | PaperScriptStepV1::CancellationFeedback(_)
            | PaperScriptStepV1::CancellationRequestCompleted(_)
    ) {
        return Ok(());
    }
    let frames = driver.frames();
    let before = shared
        .borrow_action_before(cutoff, &frames)
        .expect("已准备真实开户与公共组");
    match step {
        PaperScriptStepV1::AwaitingNoTrade => {
            *step = PaperScriptStepV1::AwaitingTargetGroup;
        }
        PaperScriptStepV1::AwaitingTargetGroup => {
            let Some(frame) = frames
                .iter()
                .find(|f| &f.observation.market.condition_id == config.condition_id())
            else {
                return Ok(());
            };
            let (context, intent, legs): (_, _, Vec<&PaperGtcLegV1>) = match config {
                PaperScriptConfigV1::SingleBuyThenCancel {
                    condition_id,
                    order,
                    submit_context,
                    ..
                } => (
                    submit_context,
                    ModelIntentV1::SubmitSingleBuy {
                        condition_id: condition_id.clone(),
                        order: order.intent(),
                    },
                    vec![order],
                ),
                PaperScriptConfigV1::PairedBuy {
                    condition_id,
                    orders,
                    submit_context,
                } => (
                    submit_context,
                    ModelIntentV1::SubmitPairedBuy {
                        condition_id: condition_id.clone(),
                        orders: orders.each_ref().map(PaperGtcLegV1::intent),
                    },
                    orders.iter().collect(),
                ),
                PaperScriptConfigV1::CancelMarket {
                    condition_id,
                    cancel_context,
                } => (
                    cancel_context,
                    ModelIntentV1::CancelMarketOrders {
                        condition_id: condition_id.clone(),
                    },
                    vec![],
                ),
            };
            let mut reserves = Vec::with_capacity(legs.len());
            for (ordinal, leg) in legs.iter().enumerate() {
                let Some(tick) = frame
                    .observation
                    .assets
                    .iter()
                    .find(|asset| asset.asset.token_id == leg.token_id)
                    .and_then(|asset| asset.tick.value.as_ref())
                else {
                    return Ok(());
                };
                reserves.push(reserve_leg(
                    rules,
                    config.condition_id(),
                    context,
                    ordinal as u16,
                    leg,
                    tick,
                )?);
            }
            let cash_available = before.cash_available();
            let item = ReservedIntentV1 {
                context: context.clone(),
                intent: Arc::new(intent),
                reserves,
            };
            driver.wait_for_recording_capacity()?;
            match shared
                .execute_batch_reserved(&mut driver.decision().expect("已应用组"), &[item])
                .await
            {
                Ok(_) => {
                    *step = match config {
                        PaperScriptConfigV1::SingleBuyThenCancel { .. } => {
                            PaperScriptStepV1::AwaitingCancellableOrder
                        }
                        PaperScriptConfigV1::PairedBuy { .. } => PaperScriptStepV1::SubmissionSent,
                        PaperScriptConfigV1::CancelMarket { .. } => {
                            PaperScriptStepV1::AwaitingCancellationFeedback
                        }
                    }
                }
                Err(error) if error.is_local_funds_rejection() => {
                    *step = PaperScriptStepV1::LocalFundsRejected { cash_available }
                }
                Err(error) => return Err(format!("{error:?}").into()),
            }
        }
        PaperScriptStepV1::AwaitingCancellableOrder => {
            let PaperScriptConfigV1::SingleBuyThenCancel {
                submit_context,
                cancel_context,
                ..
            } = config
            else {
                unreachable!("单腿脚本阶段");
            };
            let id = before.orders().find_map(|order| {
                if order.origin.as_ref()?.intent.global_request_id()
                    != submit_context.global_request_id
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
                    .execute_batch_reserved(
                        &mut driver.decision().expect("已应用组"),
                        &[ReservedIntentV1 {
                            context: cancel_context.clone(),
                            intent: Arc::new(ModelIntentV1::CancelOrder { platform_order_id }),
                            reserves: vec![],
                        }],
                    )
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                *step = PaperScriptStepV1::AwaitingCancellationFeedback;
            }
        }
        PaperScriptStepV1::AwaitingCancellationFeedback => {
            let context = config.cancel_context().expect("取消脚本阶段");
            let requests = before.cancellation_requests(None);
            if let ExecutionReadValueV1::Known(Some(feedback)) = requests.latest_response
                && matches!(&feedback.record().source_reference().record,
                    SimV2SourceRecordReferenceV1::DirectResponse {
                        association: SimV2DirectResponseAssociationV1::Request { global_request_id }, ..
                    } if global_request_id == &context.global_request_id)
                && let NormalizationRecord::Evidence {
                    evidence:
                        SourceNeutralEvidenceKind::Control {
                            result:
                                SourceNeutralControlResult::CancellationRequestCompleted { result },
                        },
                    ..
                } = feedback.record()
            {
                if matches!(config, PaperScriptConfigV1::CancelMarket { .. }) {
                    *step = PaperScriptStepV1::CancellationRequestCompleted(result.clone());
                } else {
                    // 单目标结果仍来自该订单的正式目标反馈，请求完成不冒充目标成功。
                    for order in shared.registry().orders() {
                        let Some(feedback) = order.latest_direct_feedback else {
                            continue;
                        };
                        let associated = matches!(&feedback.record().source_reference().record,
                            SimV2SourceRecordReferenceV1::DirectResponse { association: SimV2DirectResponseAssociationV1::CancellationTarget { global_request_id, .. }, .. }
                            if global_request_id == &context.global_request_id);
                        if associated
                            && let NormalizationRecord::Evidence {
                                evidence:
                                    SourceNeutralEvidenceKind::Control {
                                        result:
                                            SourceNeutralControlResult::Cancellation { outcome, .. },
                                    },
                                ..
                            } = feedback.record()
                        {
                            *step = PaperScriptStepV1::CancellationFeedback(outcome.clone());
                            break;
                        }
                    }
                }
            }
        }
        PaperScriptStepV1::CancellationFeedback(_)
        | PaperScriptStepV1::CancellationRequestCompleted(_)
        | PaperScriptStepV1::SubmissionSent
        | PaperScriptStepV1::LocalFundsRejected { .. } => {}
    }
    Ok(())
}

/// 两个正式入口复用同一次准备材料登记市场；不重新解析或选择规则。
pub(super) fn enroll_markets(
    driver: &mut ContinuousPublicConsumptionV1,
    group: &PreparedPublicGroupV1,
) -> Result<(), PaperError> {
    for context in &group.pm_contexts {
        if !driver.has_enrolled_market(&context.market.condition_id) {
            let assets = context
                .assets
                .iter()
                .map(|asset| asset.asset.clone())
                .collect::<Vec<_>>()
                .try_into()
                .map_err(|_| "原 PM 市场不是二元市场")?;
            let market = PlatformMarketV1::try_new(context.market.clone(), assets)?;
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
    Ok(())
}

/// binary 和受控 Unix 对端测试共用本入口。规则由原冷启动加载器在调用前封存。
/// 同一个 next_applied future 跨 timer 保留；完成段和计划停止排空段不再选择 timer。
pub async fn run_paper_v1(
    config: &PaperConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    stop: impl Future<Output = ()>,
) -> Result<PaperReportV1, PaperError> {
    let (mut driver, mut shared) = assemble(config, rules.clone(), None)?;
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
                        enroll_markets(&mut driver, &group)?;
                        driver.apply_group(group)?;
                        script_group(&config.script, &rules, &mut step, &mut driver, &mut shared)
                            .await?;
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
