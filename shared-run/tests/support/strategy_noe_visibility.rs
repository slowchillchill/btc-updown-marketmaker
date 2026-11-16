//! 单条已交付引用不能把同批尚未交付的反馈带入策略。
use super::*;
use ::execution_normalization::{
    MemoryEconomicFactReferenceV1, MemoryIdentityKeyV1, MemoryLogicalIdentityV1,
    NoeCheckpointBatchPoolV1, NoeConsumptionSourceV2, NormalizationBatchInput, NormalizationCore,
    NormalizationRecord, SourceNeutralEntityKeyV1, SourceNeutralEvidence,
    SourceNeutralEvidenceKind, SourceNeutralLogicalKeyV1,
};
use contracts::execution_normalization::*;

type Trade = SourceNeutralTradeLifecycleResultV2<MemoryLogicalIdentityV1>;
type Control = SourceNeutralControlResult<
    SourceNeutralSubmissionOutcomeV2<MemoryLogicalIdentityV1>,
    SourceNeutralOrderSnapshotV2<MemoryLogicalIdentityV1>,
    MemoryLogicalIdentityV1,
    CancellationRequestCompletedV1,
>;
type MemoryCore =
    NormalizationCore<Trade, Control, MemoryLogicalIdentityV1, MemoryEconomicFactReferenceV1>;

#[tokio::test(flavor = "current_thread")]
async fn strategy_single_record_view_cannot_expand_undelivered_batch_records() {
    let temp = tempfile::tempdir().unwrap();
    let (_, mut driver, observation) = setup_with_observation(temp.path());
    let context = Arc::new(observation.opening_record().unwrap().context().clone());
    let core = MemoryCore::new(context.as_ref().clone());
    let lifecycle = |sequence: u64, at: i64, name: &str| SourceNeutralEvidence {
        source_record: SimV2SourceRecordReferenceV1::OrderLifecycle {
            private_feedback_sequence: sequence,
            platform_event_at_utc_ms: at - 100,
            feedback_received_at_utc_ms: at,
        },
        source_ordinal: 0,
        logical_key: SourceNeutralLogicalKeyV1::OrderLifecycle {
            order_identity: name.into(),
            occurrence: sequence,
        },
        kind: SourceNeutralEvidenceKind::OrderLifecycle {
            result: SourceNeutralOrderLifecycleResultV1 {
                order_identity: MemoryLogicalIdentityV1 {
                    account: core.account_identity().clone(),
                    key: MemoryIdentityKeyV1::Entity(SourceNeutralEntityKeyV1::Order {
                        source_order_id: name.into(),
                    }),
                },
                condition_id: ConditionId(CONDITION_ID.into()),
                token_id: TokenId(UP_TOKEN_ID.into()),
                event: SourceNeutralOrderLifecycleEventV1::Accepted {
                    state: SourceNeutralOrderStateV1::Open,
                },
            },
        },
    };
    let prepared = core
        .prepare_batch(NormalizationBatchInput {
            source_watermark: SimV2SourceWatermarkV1 {
                run_id: context.source.account_instance_key.run_id.clone(),
                projection_batch_sequence: 12,
            },
            evidence: vec![
                lifecycle(1, 5000, "early-visible-order"),
                lifecycle(2, 6000, "late-hidden-order"),
            ],
        })
        .unwrap();
    let original_batch = Arc::new(prepared.batch().clone());
    assert_eq!(original_batch.records.len(), 2);
    let mut source = NoeConsumptionSourceV2::default();
    source.accept(context.clone(), original_batch.clone());
    let early = source.take_closed(5000, 5000);
    assert_eq!(early.len(), 1);
    assert_eq!(
        source.next_pending_feedback_received_at_utc_ms(),
        Some(6000)
    );

    // Prove this is the original regression: the host's early reference really
    // still owns the entire shared batch, including the later feedback.
    let host_json = serde_json::to_string(&early[0]).unwrap();
    assert!(host_json.contains("early-visible-order"));
    assert!(host_json.contains("late-hidden-order"));
    let view = StrategyNoeRecordReadV1::from(&early[0]);
    let visible_json = serde_json::to_string(&view).unwrap();
    let visible_debug = format!("{view:?}");
    for visible in [&visible_json, &visible_debug] {
        assert!(visible.contains("early-visible-order"));
        assert!(!visible.contains("late-hidden-order"));
        assert!(!visible.contains("source_watermark"));
        assert!(!visible.contains("source_reference"));
        assert!(!visible.contains("\"source_order\":"));
        assert!(!visible.contains("source_order: "));
        assert!(!visible.contains("context_reference"));
    }
    assert_eq!(view, StrategyNoeRecordReadV1::from(&early[0]));

    let changed_late_batch = core
        .prepare_batch(NormalizationBatchInput {
            source_watermark: original_batch.source_watermark.clone(),
            evidence: vec![
                lifecycle(1, 5000, "early-visible-order"),
                lifecycle(2, 6000, "different-hidden-order"),
            ],
        })
        .unwrap();
    let mut changed_source = NoeConsumptionSourceV2::default();
    changed_source.accept(
        context.clone(),
        Arc::new(changed_late_batch.batch().clone()),
    );
    let changed_early = changed_source.take_closed(5000, 5000);
    assert_eq!(changed_early.len(), 1);
    let changed_view = StrategyNoeRecordReadV1::from(&changed_early[0]);
    assert_ne!(host_json, serde_json::to_string(&changed_early[0]).unwrap());
    assert_eq!(
        view, changed_view,
        "unseen siblings cannot change strategy Eq"
    );
    assert_eq!(visible_json, serde_json::to_string(&changed_view).unwrap());
    assert_eq!(visible_debug, format!("{changed_view:?}"));

    let (
        NormalizationRecord::Evidence { evidence: raw, .. },
        StrategyNoeRecordV1::Evidence {
            evidence: visible, ..
        },
    ) = (early[0].record(), view.record())
    else {
        panic!("the formal normalization must produce a lifecycle evidence record");
    };
    assert!(
        std::ptr::eq(raw, visible),
        "the strategy projection borrows the single payload"
    );

    let mut registry = SharedExecutionRegistryV2::new(context);
    registry.observe(&early[0]);
    let early_order = StrategyExecutionOrderReadV1::from(registry.orders().next().unwrap());
    assert!(
        !serde_json::to_string(&early_order)
            .unwrap()
            .contains("late-hidden-order")
    );
    assert!(!format!("{early_order:?}").contains("late-hidden-order"));

    // Checkpoint capture retains the original shared container and its exact
    // per-record visibility. The safety boundary must not discard host facts.
    let mut pool = NoeCheckpointBatchPoolV1::default();
    let saved_early = pool.capture(&early[0]);
    let saved_pool = serde_json::to_value(&pool).unwrap();
    assert_eq!(saved_pool["batches"].as_array().unwrap().len(), 1);
    assert_eq!(
        saved_pool["batches"][0]["batch"]["records"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let restored_pool: NoeCheckpointBatchPoolV1<CancellationRequestCompletedV1> =
        serde_json::from_value(saved_pool).unwrap();
    let restored_early = restored_pool.restore(saved_early).unwrap();
    assert_eq!(view, StrategyNoeRecordReadV1::from(&restored_early));

    assert!(source.take_closed(5999, 5999).is_empty());
    let late = source.take_closed(6000, 6000);
    assert_eq!(late.len(), 1);
    assert!(
        serde_json::to_string(&StrategyNoeRecordReadV1::from(&late[0]))
            .unwrap()
            .contains("late-hidden-order")
    );
    assert_eq!(original_batch.records.len(), 2);
    assert_eq!(host_json, serde_json::to_string(&early[0]).unwrap());
    eprintln!(
        "strategy-noe-isolation batches=2 records_per_batch=2 early_delivered_per_batch=1 borrowed_payload=true checkpoint_batches=1"
    );
    driver.stop_and_drain().unwrap();
}
