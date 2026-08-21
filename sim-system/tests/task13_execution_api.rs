use contracts::{
    AssetIdentityV1, ClobMarketId, ConditionId, MarketIdentityV2, MarketWindowV2,
    OutcomeIdentityV1, TokenId,
};
use sim_system::execution::{MarketEnrollmentRequestV1, PublicVisibilityCursorV1};
use sim_system::scenario::PlatformMarketV1;

#[test]
fn public_execution_inputs_construct_and_round_trip_without_private_types() {
    let market = PlatformMarketV1::try_new(
        MarketIdentityV2 {
            condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
            clob_market_id: ClobMarketId("123".to_owned()),
            window: MarketWindowV2 {
                start_at_utc_ms: 1_000,
                end_at_utc_ms: 2_000,
            },
        },
        [
            AssetIdentityV1 {
                token_id: TokenId("1".to_owned()),
                outcome: OutcomeIdentityV1 {
                    index: 0,
                    label: "Up".to_owned(),
                },
            },
            AssetIdentityV1 {
                token_id: TokenId("2".to_owned()),
                outcome: OutcomeIdentityV1 {
                    index: 1,
                    label: "Down".to_owned(),
                },
            },
        ],
    )
    .expect("公开市场 DTO 必须可构造");
    let request = MarketEnrollmentRequestV1 {
        enrollment_request_id: "public-enrollment-1".to_owned(),
        market,
    };
    let encoded = serde_json::to_vec(&request).expect("公开登记请求必须可编码");
    let decoded: MarketEnrollmentRequestV1 =
        serde_json::from_slice(&encoded).expect("公开登记请求必须可解码");

    assert_eq!(decoded, request);
    assert_eq!(decoded.enrollment_request_id, "public-enrollment-1");
    assert_eq!(decoded.market.assets().len(), 2);

    let cursor = PublicVisibilityCursorV1::default();
    assert_eq!(cursor.closed_through_received_at_utc_ms(), None);
    assert_eq!(cursor.next_visibility_index_position(), 0);
}
