use data_collect::polymarket_market_metadata_layout::{
    ResolutionIntegrityErrorClass, ResolutionPollResult, TerminalPredicateFailureReason,
    parse_polymarket_gamma_closed_time_utc_ms,
};
use data_collect::sources::polymarket::{
    GammaTerminalAttemptContext, GammaTerminalFixedIdentity, GammaTerminalResponse,
    GammaTerminalWinner, evaluate_gamma_terminal_response,
};
use polymarket_client_sdk_v2::gamma::types::response::Market as SdkGammaMarket;
use serde_json::{Value, json};

fn gamma_terminal_market_json() -> Value {
    json!({
        "id": "market-1",
        "conditionId": "0x0000000000000000000000000000000000000000000000000000000000000001",
        "slug": "btc-updown-5m-1767225600",
        "outcomes": "[\"Up\",\"Down\"]",
        "outcomePrices": "[\"1\",\"0\"]",
        "active": true,
        "closed": true,
        "closedTime": "2026-01-01T00:00:30Z",
        "umaResolutionStatus": " resolved ",
        "clobTokenIds": "[\"101\",\"102\"]",
        "acceptingOrders": false,
        "automaticallyResolved": true
    })
}

fn decode_gamma_market(value: Value) -> SdkGammaMarket {
    serde_json::from_value(value).expect("official SDK Gamma Market fixture must decode")
}

fn gamma_terminal_identity() -> GammaTerminalFixedIdentity {
    GammaTerminalFixedIdentity {
        slug: "btc-updown-5m-1767225600".to_owned(),
        clob_market_id: "market-1".to_owned(),
        condition_id: "0x0000000000000000000000000000000000000000000000000000000000000001"
            .to_owned(),
        up_asset_id: "101".to_owned(),
        down_asset_id: "102".to_owned(),
        market_end_utc_ms: 1_767_225_600_000,
    }
}

fn gamma_terminal_attempt() -> GammaTerminalAttemptContext {
    GammaTerminalAttemptContext {
        scheduled_at_utc_ms: 1_767_225_780_000,
        fetch_start_utc_ms: 1_767_225_780_010,
        received_at_utc_ms: 1_767_225_780_020,
        response_bound_to_attempt: true,
    }
}

type GammaFailureMutation = fn(&mut Value, &mut GammaTerminalFixedIdentity);

#[test]
fn gamma_closed_time_accepts_utc_hour_offset_without_minutes() {
    assert_eq!(
        parse_polymarket_gamma_closed_time_utc_ms("2026-07-27 05:25:52+00"),
        Some(1_785_129_952_000)
    );
}

#[test]
fn gamma_terminal_predicate_contract() {
    let market = decode_gamma_market(gamma_terminal_market_json());
    let identity = gamma_terminal_identity();
    let evaluation = evaluate_gamma_terminal_response(
        &identity,
        gamma_terminal_attempt(),
        GammaTerminalResponse::Decoded(&market),
    );

    assert_eq!(evaluation.terminal_predicate_failure_reason, None);
    assert_eq!(
        evaluation.resolution_poll_result,
        ResolutionPollResult::Terminal
    );
    assert_eq!(evaluation.resolution_integrity_error_class, None);
    assert_eq!(evaluation.observed_active, Some(true));
    assert_eq!(
        evaluation.winner,
        Some(GammaTerminalWinner {
            winning_asset_id: "101".to_owned(),
            winning_outcome: "Up".to_owned(),
        })
    );
    let stored_payload = serde_json::to_string(&market).unwrap();
    let reconstructed_market: SdkGammaMarket = serde_json::from_str(&stored_payload).unwrap();
    assert_eq!(
        evaluate_gamma_terminal_response(
            &identity,
            gamma_terminal_attempt(),
            GammaTerminalResponse::Decoded(&reconstructed_market),
        ),
        evaluation,
        "the deterministic SDK payload plus fixed identity and attempt timing reconstructs the predicate"
    );

    let mut future_closed_time_json = gamma_terminal_market_json();
    future_closed_time_json["closedTime"] = json!("2026-01-02T00:00:00Z");
    let future_closed_time_market = decode_gamma_market(future_closed_time_json);
    let evaluation = evaluate_gamma_terminal_response(
        &identity,
        gamma_terminal_attempt(),
        GammaTerminalResponse::Decoded(&future_closed_time_market),
    );

    assert!(
        evaluation.closed_time_utc_ms.unwrap() > gamma_terminal_attempt().received_at_utc_ms,
        "closedTime has no future-skew gate against response receive time"
    );

    let mut down_winner_json = gamma_terminal_market_json();
    down_winner_json["outcomePrices"] = json!("[\"0\",\"1\"]");
    let down_winner_market = decode_gamma_market(down_winner_json);
    let evaluation = evaluate_gamma_terminal_response(
        &identity,
        gamma_terminal_attempt(),
        GammaTerminalResponse::Decoded(&down_winner_market),
    );
    assert_eq!(
        evaluation.winner,
        Some(GammaTerminalWinner {
            winning_asset_id: "102".to_owned(),
            winning_outcome: "Down".to_owned(),
        })
    );

    let mut active_null_json = gamma_terminal_market_json();
    active_null_json
        .as_object_mut()
        .expect("object")
        .remove("active");
    let active_null_market = decode_gamma_market(active_null_json);
    let evaluation = evaluate_gamma_terminal_response(
        &identity,
        gamma_terminal_attempt(),
        GammaTerminalResponse::Decoded(&active_null_market),
    );

    assert_eq!(evaluation.observed_active, None);
    assert_eq!(
        evaluation.resolution_poll_result,
        ResolutionPollResult::Terminal
    );

    let mut active_false_json = gamma_terminal_market_json();
    active_false_json["active"] = json!(false);
    let active_false_market = decode_gamma_market(active_false_json);
    let evaluation = evaluate_gamma_terminal_response(
        &identity,
        gamma_terminal_attempt(),
        GammaTerminalResponse::Decoded(&active_false_market),
    );

    assert_eq!(evaluation.observed_active, Some(false));
    assert_eq!(
        evaluation.resolution_poll_result,
        ResolutionPollResult::Terminal
    );

    let failure_cases: [(
        TerminalPredicateFailureReason,
        ResolutionPollResult,
        Option<ResolutionIntegrityErrorClass>,
        GammaFailureMutation,
    ); 19] = [
        (
            TerminalPredicateFailureReason::SlugMismatch,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["slug"] = json!("wrong-slug"),
        ),
        (
            TerminalPredicateFailureReason::MarketIdMismatch,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["id"] = json!("wrong-market"),
        ),
        (
            TerminalPredicateFailureReason::ConditionIdMismatch,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| {
                value["conditionId"] =
                    json!("0x0000000000000000000000000000000000000000000000000000000000000002");
            },
        ),
        (
            TerminalPredicateFailureReason::AssetCountNotTwo,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["clobTokenIds"] = json!("[\"101\"]"),
        ),
        (
            TerminalPredicateFailureReason::AssetDuplicate,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["clobTokenIds"] = json!("[\"101\",\"101\"]"),
        ),
        (
            TerminalPredicateFailureReason::AssetSetMismatch,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["clobTokenIds"] = json!("[\"101\",\"103\"]"),
        ),
        (
            TerminalPredicateFailureReason::OutcomesCountNotTwo,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["outcomes"] = json!("[\"Up\"]"),
        ),
        (
            TerminalPredicateFailureReason::OutcomeMappingInvalid,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["outcomes"] = json!("[\"Down\",\"Up\"]"),
        ),
        (
            TerminalPredicateFailureReason::OutcomePricesCountNotTwo,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["outcomePrices"] = json!("[\"1\"]"),
        ),
        (
            TerminalPredicateFailureReason::ClosedMissing,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| {
                value.as_object_mut().expect("object").remove("closed");
            },
        ),
        (
            TerminalPredicateFailureReason::ClosedFalse,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| value["closed"] = json!(false),
        ),
        (
            TerminalPredicateFailureReason::AcceptingOrdersMissing,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| {
                value
                    .as_object_mut()
                    .expect("object")
                    .remove("acceptingOrders");
            },
        ),
        (
            TerminalPredicateFailureReason::AcceptingOrdersTrue,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| value["acceptingOrders"] = json!(true),
        ),
        (
            TerminalPredicateFailureReason::ClosedTimeMissing,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| {
                value.as_object_mut().expect("object").remove("closedTime");
            },
        ),
        (
            TerminalPredicateFailureReason::ClosedTimeParseError,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::ClosedTimeInvalid),
            |value, _| value["closedTime"] = json!("not-a-timestamp"),
        ),
        (
            TerminalPredicateFailureReason::ClosedTimeBeforeMarketEnd,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::ClosedTimeInvalid),
            |value, _| value["closedTime"] = json!("2025-12-31T23:59:59Z"),
        ),
        (
            TerminalPredicateFailureReason::UmaResolutionStatusMissing,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| {
                value
                    .as_object_mut()
                    .expect("object")
                    .remove("umaResolutionStatus");
            },
        ),
        (
            TerminalPredicateFailureReason::UmaResolutionStatusNotResolved,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| value["umaResolutionStatus"] = json!("proposed"),
        ),
        (
            TerminalPredicateFailureReason::OutcomePricesNotOneHot,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| value["outcomePrices"] = json!("[\"0.5\",\"0.5\"]"),
        ),
    ];
    assert_eq!(
        failure_cases.map(|case| case.0),
        TerminalPredicateFailureReason::ALL
    );

    for (expected_reason, expected_result, expected_integrity, mutate) in failure_cases {
        let mut value = gamma_terminal_market_json();
        let mut identity = gamma_terminal_identity();
        mutate(&mut value, &mut identity);
        let market = decode_gamma_market(value);
        let evaluation = evaluate_gamma_terminal_response(
            &identity,
            gamma_terminal_attempt(),
            GammaTerminalResponse::Decoded(&market),
        );

        assert_eq!(
            evaluation.terminal_predicate_failure_reason,
            Some(expected_reason),
            "{} reason",
            expected_reason.as_str()
        );
        assert_eq!(
            evaluation.resolution_poll_result,
            expected_result,
            "{} result",
            expected_reason.as_str()
        );
        assert_eq!(
            evaluation.resolution_integrity_error_class,
            expected_integrity,
            "{} integrity class",
            expected_reason.as_str()
        );
        assert!(
            evaluation.winner.is_none(),
            "{} winner",
            expected_reason.as_str()
        );
    }

    let mut combined = gamma_terminal_market_json();
    combined["slug"] = json!("wrong-slug");
    combined["closed"] = json!(false);
    combined["outcomePrices"] = json!("[\"0.5\",\"0.5\"]");
    let combined = decode_gamma_market(combined);
    let evaluation = evaluate_gamma_terminal_response(
        &identity,
        gamma_terminal_attempt(),
        GammaTerminalResponse::Decoded(&combined),
    );
    assert_eq!(
        evaluation.terminal_predicate_failure_reason,
        Some(TerminalPredicateFailureReason::SlugMismatch)
    );
}

#[test]
fn gamma_terminal_predicate_not_evaluated_boundaries() {
    let market = decode_gamma_market(gamma_terminal_market_json());
    let identity = gamma_terminal_identity();
    let mut unbound_attempt = gamma_terminal_attempt();
    unbound_attempt.response_bound_to_attempt = false;
    let mut early_fetch_attempt = gamma_terminal_attempt();
    early_fetch_attempt.fetch_start_utc_ms = early_fetch_attempt.scheduled_at_utc_ms - 1;
    let mut receive_before_fetch_attempt = gamma_terminal_attempt();
    receive_before_fetch_attempt.received_at_utc_ms =
        receive_before_fetch_attempt.fetch_start_utc_ms - 1;
    for attempt in [
        unbound_attempt,
        early_fetch_attempt,
        receive_before_fetch_attempt,
    ] {
        let evaluation = evaluate_gamma_terminal_response(
            &identity,
            attempt,
            GammaTerminalResponse::Decoded(&market),
        );

        assert_eq!(evaluation.terminal_predicate_failure_reason, None);
        assert_eq!(evaluation.observed_active, Some(true));
        assert_eq!(
            evaluation.closed_time_raw.as_deref(),
            Some("2026-01-01T00:00:30Z")
        );
        assert_eq!(
            evaluation.resolution_integrity_error_class,
            Some(ResolutionIntegrityErrorClass::AttemptBindingInvariant)
        );
    }

    let evaluation = evaluate_gamma_terminal_response(
        &identity,
        gamma_terminal_attempt(),
        GammaTerminalResponse::SdkDecodeOrSchemaError,
    );

    assert_eq!(evaluation.terminal_predicate_failure_reason, None);
    assert_eq!(evaluation.observed_active, None);
    assert_eq!(evaluation.closed_time_raw, None);
    assert_eq!(
        evaluation.resolution_integrity_error_class,
        Some(ResolutionIntegrityErrorClass::SdkDecodeOrSchema)
    );
}
