//! 复用正式既有小型夹具，检验新PAPER单项边界不能跨漏更早真实义务。
use super::*;
use crate::execution::{OrdersRequestV2, RequestContextV2};
use crate::public_consumption::RecordedPublicGroupSourceV1;
use std::time::Instant;
#[path = "../../../shared-run/tests/common/fixture_clock.rs"]
mod fixture_clock;
#[path = "../../../shared-run/tests/common/fixture_execution.rs"]
mod fixture_execution;
#[path = "../../../shared-run/tests/common/fixture_execution_v4.rs"]
mod fixture_execution_v4;
#[path = "../../../shared-run/tests/common/fixture_input.rs"]
mod fixture_input;
#[path = "../../../shared-run/tests/common/fixture_policy.rs"]
mod fixture_policy;
#[path = "../../../shared-run/tests/common/fixture_recording.rs"]
mod fixture_recording;
#[path = "../../../shared-run/tests/common/fixture_reference.rs"]
mod fixture_reference;
#[path = "../../../shared-run/tests/common/fixture_terminal.rs"]
mod fixture_terminal;
#[path = "../../../shared-run/tests/common/fixture_trading.rs"]
mod fixture_trading;

#[tokio::test(flavor = "current_thread")]
async fn actual_request_boundary_consumes_earlier_request_and_feedback_before_observing_now() {
    let temporary = tempfile::tempdir().unwrap();
    let root = std::env::var_os("PAPER_HOST_TEST_OUTPUT_ROOT")
        .map(|p| PathBuf::from(p).join("due-before-actual-boundary"))
        .unwrap_or_else(|| temporary.path().to_owned());
    std::fs::create_dir_all(&root).unwrap();
    let (core, times) = fixture_trading::recorded_trading(&root.join("market"), "2", false);
    let (execution, _observation, _) = fixture_execution_v4::execution_with_identity_v4(
        &root.join("sim.jsonl"),
        &core,
        fixture_policy::SUCCESS_POLICY,
        None,
        "28158a348a1d6c7c487c652331cc7457d85a359488a13aaf18e487127a33e45f".into(),
        "100",
        "due-boundary",
    );
    let source =
        RecordedPublicGroupSourceV1::open(&root.join("market"), 1, 1 << 20, times).unwrap();
    let mut finite = FinitePublicConsumptionV1::new(execution, source, "due-boundary".into());
    let (_, cutoff) = loop {
        let pair = finite.next_group().unwrap().unwrap();
        if pair.1.decision_time().logical_time_utc_ms() >= 22_000 {
            break pair;
        }
    };
    let request = BusinessRequestV2::Orders(OrdersRequestV2 {
        context: RequestContextV2 {
            global_request_id: "earlier-query".into(),
            caller_correlation_id: None,
        },
        filter: OrdersFilterV1::default(),
        cursor: None,
    });
    let receipt = finite
        .send_decision_at(cutoff.clone(), cutoff.decision_time().logical_time_utc_ms())
        .unwrap()
        .request(request)
        .unwrap();
    let record = &finite
        .state
        .execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap()
        .mutation_requests
        .requests_by_id["earlier-query"];
    let now = receipt.platform_arrived_at_utc_ms() + record.inbound_delay_ms as i64 + 1;
    assert!(record.completed_response.is_none());
    let controlled = Arc::new(std::sync::atomic::AtomicI64::new(now));
    let mut driver = ContinuousPublicConsumptionV1 {
        state: finite.state,
        clock: PaperExecutionClockV1 {
            utc_anchor_ms: 0,
            started: Instant::now(),
            controlled: Some(controlled),
        },
    };
    // 原队列可能需要移交；该等待必须发生在新实际时刻/资格之前。
    let mut decision = driver.send_decision(cutoff).unwrap();
    {
        let boundary = decision.prepare_request_boundary().unwrap();
        assert_eq!(boundary.sent_at_utc_ms(), now);
        assert_eq!(
            boundary
                .observation_driver()
                .observation_cutoff()
                .unwrap()
                .decision_time()
                .logical_time_utc_ms(),
            now
        );
        let session = boundary
            .driver
            .execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap();
        let record = &session.mutation_requests.requests_by_id["earlier-query"];
        assert_eq!(record.remaining_action_count, 0);
        assert!(
            record.completed_response.is_some(),
            "不能以一次Close(now-1)跨漏更早mutation/response"
        );
        assert!(
            session
                .public_visibility_frontier
                .closed_through_utc_ms
                .unwrap()
                >= now - 1
        );
    }
    drop(decision);
    driver.stop_and_drain().unwrap();
    let wire = std::fs::read_to_string(root.join("sim.jsonl")).unwrap();
    assert!(!wire.contains("RUN_SEAL"));
}
