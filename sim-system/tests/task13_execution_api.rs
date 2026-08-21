use std::path::Path;
use std::sync::Arc;

use contracts::ExecutionMarketEventV3;
use sim_system::execution::{
    AcceptedClosedMarketBucketV2, BusinessProjectionPortV2, CancelAllOrdersRequestV2,
    CancelOrderRequestV2, CancelOrdersRequestV2, CreateSessionControlResultV2,
    MarketEnrollmentRequestV1, MutationRequestReceiptV2, OrderRequestV2, OrdersRequestV2,
    PlatformPrefixAdvanceV2, PostOrderRequestV2, PostOrdersRequestV2, RetentionAdvanceV2,
    RunOpenedV2, SessionInitializationControlResultV2, SessionSpecV3, SimExecution,
    SimRunActorErrorV2, TradesRequestV2, TrustedDecisionCutoffV2,
    UnpublishedRunArtifactCandidateV2,
};
use sim_system::finality::TradeFinalityPolicyV2;
use sim_system::latency_profile::EmpiricalDelayProfile;
use sim_system::platform_rules::SealedProcessRuleSnapshotV3;
use sim_system::scenario::SealedPlatformScenarioV1;

#[allow(clippy::too_many_arguments, dead_code)]
fn assert_public_create_and_recovery_signatures<F>(
    path: &Path,
    opened: RunOpenedV2,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    finality: Arc<TradeFinalityPolicyV2>,
    scenario: Arc<SealedPlatformScenarioV1>,
    latency: Arc<EmpiricalDelayProfile>,
    projection: Box<dyn BusinessProjectionPortV2>,
    read_bucket: F,
) where
    F: FnMut(
            &sim_system::execution::DurableMarketBucketReferenceV2,
        ) -> Result<Arc<[ExecutionMarketEventV3]>, SimRunActorErrorV2>
        + Clone,
{
    let _: Result<SimExecution, SimRunActorErrorV2> = SimExecution::create_new(
        path,
        opened,
        Arc::clone(&rules),
        Arc::clone(&finality),
        Arc::clone(&scenario),
        Arc::clone(&latency),
        projection,
        64 * 1024,
    );
    let _: Result<SimExecution, SimRunActorErrorV2> = SimExecution::recover_paper(
        path,
        Arc::clone(&rules),
        Arc::clone(&finality),
        Arc::clone(&scenario),
        Arc::clone(&latency),
        Box::new(RejectingProjectionPort),
        64 * 1024,
        read_bucket.clone(),
    );
    let _: Result<UnpublishedRunArtifactCandidateV2, SimRunActorErrorV2> =
        SimExecution::recover_sealed_clean_run(
            path,
            rules,
            finality,
            scenario,
            latency,
            64 * 1024,
            read_bucket,
        );
}

#[allow(clippy::too_many_arguments, dead_code)]
fn assert_public_operation_signatures(
    execution: &mut SimExecution,
    session: SessionSpecV3,
    enrollment: MarketEnrollmentRequestV1,
    cutoff: TrustedDecisionCutoffV2,
    post_order: PostOrderRequestV2,
    post_orders: PostOrdersRequestV2,
    cancel_order: CancelOrderRequestV2,
    cancel_orders: CancelOrdersRequestV2,
    cancel_all: CancelAllOrdersRequestV2,
    order: OrderRequestV2,
    orders: OrdersRequestV2,
    trades: TradesRequestV2,
    prefix: PlatformPrefixAdvanceV2,
    bucket: AcceptedClosedMarketBucketV2,
    retention: RetentionAdvanceV2,
) {
    let _: Result<CreateSessionControlResultV2, SimRunActorErrorV2> =
        execution.create_session(session.clone());
    let _: Result<CreateSessionControlResultV2, SimRunActorErrorV2> =
        execution.retry_create_session(session);
    let _: Result<SessionInitializationControlResultV2, SimRunActorErrorV2> =
        execution.activate_session("activate-1".to_owned());
    let _: Result<SessionInitializationControlResultV2, SimRunActorErrorV2> =
        execution.fail_session_initialization("failure-1".to_owned());
    let _ = execution.enroll_market(enrollment);
    let _: Result<MutationRequestReceiptV2, SimRunActorErrorV2> =
        execution.post_order(cutoff.clone(), post_order);
    let _ = execution.post_orders(cutoff.clone(), post_orders);
    let _ = execution.cancel_order(cutoff.clone(), cancel_order);
    let _ = execution.cancel_orders(cutoff.clone(), cancel_orders);
    let _ = execution.cancel_all_orders(cutoff.clone(), cancel_all);
    let _ = execution.order(cutoff.clone(), order);
    let _ = execution.orders(cutoff.clone(), orders);
    let _ = execution.trades(cutoff, trades);
    let _ = execution.drive_platform_prefix(prefix);
    let _ = execution.drive_closed_market_bucket(bucket);
    let _ = execution.advance_retention(retention);
}

struct RejectingProjectionPort;

impl BusinessProjectionPortV2 for RejectingProjectionPort {
    fn try_accept(
        &mut self,
        _batch: Arc<sim_system::execution::BusinessProjectionBatchV2>,
    ) -> bool {
        false
    }

    fn durable_through(&self, _run_id: &str) -> u64 {
        0
    }
}

#[test]
fn public_surface_type_checks_without_constructing_platform_state() {
    assert_eq!(8, 8);
}
