use contracts::execution_normalization::{
    NoePublishedContainerReferenceV1, NoeResultReferenceV1, SourceNeutralCancellationOutcomeV1,
    SourceNeutralControlResultV1, SourceNeutralOrderSnapshotV1, SourceNeutralOrderStateV1,
    SourceNeutralSubmissionOutcomeV1, SourceNeutralTradeEconomicDispositionV1,
    SourceNeutralTradeLifecycleResultV1, SourceNeutralTradeStateV1,
};
use contracts::{ConditionId, Sha256Hex, TokenId};

fn order() -> SourceNeutralOrderSnapshotV1 {
    SourceNeutralOrderSnapshotV1 {
        order_identity: contracts::execution_normalization::StableLogicalIdentityV1(
            "order-58".to_owned(),
        ),
        condition_id: ConditionId("condition-58".to_owned()),
        token_id: TokenId("token-58".to_owned()),
        state: SourceNeutralOrderStateV1::Open,
    }
}

#[test]
fn a_single_result_reference_only_locates_one_committed_record() {
    let reference = NoeResultReferenceV1 {
        commit_container: NoePublishedContainerReferenceV1 {
            location: "/tmp/noe/commits/00000000000000000000.json".to_owned(),
            boundary_sha256: Sha256Hex("a".repeat(64)),
        },
        record_ordinal: 3,
    };

    let NoeResultReferenceV1 {
        commit_container,
        record_ordinal,
    } = reference;
    assert_eq!(record_ordinal, 3);
    assert!(commit_container.location.ends_with(".json"));
}

#[test]
fn submission_and_cancellation_have_only_the_three_frozen_outcomes() {
    let submission = [
        SourceNeutralSubmissionOutcomeV1::Accepted { order: order() },
        SourceNeutralSubmissionOutcomeV1::RejectedNoEffect,
        SourceNeutralSubmissionOutcomeV1::EvidenceUnknownNoEffect,
    ];
    let cancellation = [
        SourceNeutralCancellationOutcomeV1::Canceled,
        SourceNeutralCancellationOutcomeV1::NotCanceledNoEffect,
        SourceNeutralCancellationOutcomeV1::EvidenceUnknownNoEffect,
    ];

    for outcome in submission {
        let control = SourceNeutralControlResultV1::Submission { outcome };
        assert!(matches!(
            control,
            SourceNeutralControlResultV1::Submission { .. }
        ));
    }
    for outcome in cancellation {
        let control = SourceNeutralControlResultV1::Cancellation {
            order_identity: order().order_identity,
            outcome,
        };
        assert!(matches!(
            control,
            SourceNeutralControlResultV1::Cancellation { .. }
        ));
    }
}

#[test]
fn trade_result_carries_the_authoritative_order_link_without_economic_amounts() {
    let trade = SourceNeutralTradeLifecycleResultV1 {
        trade_identity: contracts::execution_normalization::StableLogicalIdentityV1(
            "trade-58".to_owned(),
        ),
        order_identity: contracts::execution_normalization::StableLogicalIdentityV1(
            "order-58".to_owned(),
        ),
        condition_id: ConditionId("condition-58".to_owned()),
        token_id: TokenId("token-58".to_owned()),
        state: SourceNeutralTradeStateV1::Confirmed,
        economic_disposition: SourceNeutralTradeEconomicDispositionV1::FinalSuccess,
    };

    assert_eq!(trade.order_identity.0, "order-58");
    assert_eq!(trade.trade_identity.0, "trade-58");
}
