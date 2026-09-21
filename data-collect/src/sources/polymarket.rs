use std::collections::BTreeSet;

use polymarket_client_sdk_v2::error::{Error as PolymarketSdkError, Status as PolymarketSdkStatus};
use polymarket_client_sdk_v2::gamma::types::response::Market as SdkGammaMarket;
use polymarket_client_sdk_v2::types::Decimal;

use crate::polymarket_market_metadata_layout::{
    ResolutionIntegrityErrorClass, ResolutionPollResult, ResolutionTransportErrorClass,
    TerminalPredicateFailureReason, parse_polymarket_gamma_closed_time_utc_ms,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GammaMarketBySlugTransportError {
    pub class: ResolutionTransportErrorClass,
    pub http_status: Option<u16>,
}

/// Classifies an SDK `market_by_slug` failure using only public typed error APIs.
///
/// `None` means the SDK failed while decoding a successful response and the caller
/// must route the attempt through the `sdk_decode_or_schema` integrity path.
pub fn classify_gamma_market_by_slug_transport_error(
    error: &PolymarketSdkError,
) -> Option<GammaMarketBySlugTransportError> {
    let reqwest_error = error.downcast_ref::<reqwest::Error>();
    let status = error
        .downcast_ref::<PolymarketSdkStatus>()
        .map(|status| status.status_code)
        .or_else(|| reqwest_error.and_then(reqwest::Error::status));

    if let Some(status) = status {
        return Some(GammaMarketBySlugTransportError {
            class: if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                ResolutionTransportErrorClass::RateLimited
            } else {
                ResolutionTransportErrorClass::HttpStatus
            },
            http_status: Some(status.as_u16()),
        });
    }

    if error.downcast_ref::<serde_json::Error>().is_some()
        || reqwest_error.is_some_and(reqwest::Error::is_decode)
    {
        return None;
    }
    if reqwest_error.is_some_and(reqwest::Error::is_timeout) {
        return Some(GammaMarketBySlugTransportError {
            class: ResolutionTransportErrorClass::Timeout,
            http_status: None,
        });
    }
    if reqwest_error.is_some_and(reqwest::Error::is_connect) {
        return Some(GammaMarketBySlugTransportError {
            class: ResolutionTransportErrorClass::Connect,
            http_status: None,
        });
    }
    Some(GammaMarketBySlugTransportError {
        class: ResolutionTransportErrorClass::SdkTransport,
        http_status: None,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GammaTerminalFixedIdentity {
    pub slug: String,
    pub clob_market_id: String,
    pub condition_id: String,
    pub up_asset_id: String,
    pub down_asset_id: String,
    pub market_end_utc_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GammaTerminalAttemptContext {
    pub scheduled_at_utc_ms: i64,
    pub fetch_start_utc_ms: i64,
    pub received_at_utc_ms: i64,
    pub response_bound_to_attempt: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum GammaTerminalResponse<'a> {
    Decoded(&'a SdkGammaMarket),
    SdkDecodeOrSchemaError,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GammaTerminalWinner {
    pub winning_asset_id: String,
    pub winning_outcome: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GammaTerminalEvaluation {
    pub terminal_predicate_passed: Option<bool>,
    pub terminal_predicate_failure_reason: Option<TerminalPredicateFailureReason>,
    pub resolution_poll_result: ResolutionPollResult,
    pub resolution_integrity_error_class: Option<ResolutionIntegrityErrorClass>,
    pub observed_active: Option<bool>,
    pub closed_time_raw: Option<String>,
    pub closed_time_utc_ms: Option<i64>,
    pub winner: Option<GammaTerminalWinner>,
}

pub fn evaluate_gamma_terminal_response(
    fixed_identity: &GammaTerminalFixedIdentity,
    attempt: GammaTerminalAttemptContext,
    response: GammaTerminalResponse<'_>,
) -> GammaTerminalEvaluation {
    let market = match response {
        GammaTerminalResponse::Decoded(market) => market,
        GammaTerminalResponse::SdkDecodeOrSchemaError => {
            return not_evaluated(ResolutionIntegrityErrorClass::SdkDecodeOrSchema, None, None);
        }
    };
    let observed_active = market.active;
    let closed_time_raw = market.closed_time.clone();

    if !attempt.response_bound_to_attempt
        || attempt.fetch_start_utc_ms < attempt.scheduled_at_utc_ms
        || attempt.received_at_utc_ms < attempt.fetch_start_utc_ms
    {
        return not_evaluated(
            ResolutionIntegrityErrorClass::AttemptBindingInvariant,
            observed_active,
            closed_time_raw,
        );
    }

    if market.slug.as_deref() != Some(fixed_identity.slug.as_str()) {
        return source_failure(
            TerminalPredicateFailureReason::SlugMismatch,
            observed_active,
            closed_time_raw,
            None,
        );
    }
    if market.id != fixed_identity.clob_market_id {
        return source_failure(
            TerminalPredicateFailureReason::MarketIdMismatch,
            observed_active,
            closed_time_raw,
            None,
        );
    }
    let condition_id = market
        .condition_id
        .as_ref()
        .map(|condition_id| format!("{condition_id:#x}"));
    if condition_id.as_deref() != Some(fixed_identity.condition_id.as_str()) {
        return source_failure(
            TerminalPredicateFailureReason::ConditionIdMismatch,
            observed_active,
            closed_time_raw,
            None,
        );
    }

    let asset_ids = market
        .clob_token_ids
        .as_ref()
        .map(|asset_ids| {
            asset_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if asset_ids.len() != 2 {
        return source_failure(
            TerminalPredicateFailureReason::AssetCountNotTwo,
            observed_active,
            closed_time_raw,
            None,
        );
    }
    if asset_ids[0] == asset_ids[1] {
        return source_failure(
            TerminalPredicateFailureReason::AssetDuplicate,
            observed_active,
            closed_time_raw,
            None,
        );
    }
    let actual_asset_set = asset_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let fixed_asset_set = [
        fixed_identity.up_asset_id.as_str(),
        fixed_identity.down_asset_id.as_str(),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if actual_asset_set != fixed_asset_set {
        return source_failure(
            TerminalPredicateFailureReason::AssetSetMismatch,
            observed_active,
            closed_time_raw,
            None,
        );
    }

    let outcomes = market.outcomes.as_deref().unwrap_or_default();
    if outcomes.len() != 2 {
        return source_failure(
            TerminalPredicateFailureReason::OutcomesCountNotTwo,
            observed_active,
            closed_time_raw,
            None,
        );
    }
    if !outcome_mapping_matches_fixed_identity(fixed_identity, &asset_ids, outcomes) {
        return source_failure(
            TerminalPredicateFailureReason::OutcomeMappingInvalid,
            observed_active,
            closed_time_raw,
            None,
        );
    }

    let outcome_prices = market.outcome_prices.as_deref().unwrap_or_default();
    if outcome_prices.len() != 2 {
        return source_failure(
            TerminalPredicateFailureReason::OutcomePricesCountNotTwo,
            observed_active,
            closed_time_raw,
            None,
        );
    }
    match market.closed {
        None => {
            return source_failure(
                TerminalPredicateFailureReason::ClosedMissing,
                observed_active,
                closed_time_raw,
                None,
            );
        }
        Some(false) => {
            return source_failure(
                TerminalPredicateFailureReason::ClosedFalse,
                observed_active,
                closed_time_raw,
                None,
            );
        }
        Some(true) => {}
    }
    match market.accepting_orders {
        None => {
            return source_failure(
                TerminalPredicateFailureReason::AcceptingOrdersMissing,
                observed_active,
                closed_time_raw,
                None,
            );
        }
        Some(true) => {
            return source_failure(
                TerminalPredicateFailureReason::AcceptingOrdersTrue,
                observed_active,
                closed_time_raw,
                None,
            );
        }
        Some(false) => {}
    }

    let Some(raw_closed_time) = closed_time_raw.as_deref() else {
        return source_failure(
            TerminalPredicateFailureReason::ClosedTimeMissing,
            observed_active,
            None,
            None,
        );
    };
    let closed_time_utc_ms = match parse_polymarket_gamma_closed_time_utc_ms(raw_closed_time) {
        Some(closed_time) => closed_time,
        None => {
            return source_failure(
                TerminalPredicateFailureReason::ClosedTimeParseError,
                observed_active,
                closed_time_raw,
                None,
            );
        }
    };
    if closed_time_utc_ms < fixed_identity.market_end_utc_ms {
        return source_failure(
            TerminalPredicateFailureReason::ClosedTimeBeforeMarketEnd,
            observed_active,
            closed_time_raw,
            Some(closed_time_utc_ms),
        );
    }

    match market.uma_resolution_status.as_deref() {
        None => {
            return source_failure(
                TerminalPredicateFailureReason::UmaResolutionStatusMissing,
                observed_active,
                closed_time_raw,
                Some(closed_time_utc_ms),
            );
        }
        Some(status) if !status.trim().eq_ignore_ascii_case("resolved") => {
            return source_failure(
                TerminalPredicateFailureReason::UmaResolutionStatusNotResolved,
                observed_active,
                closed_time_raw,
                Some(closed_time_utc_ms),
            );
        }
        Some(_) => {}
    }

    let winning_index = if outcome_prices[0] == Decimal::ONE && outcome_prices[1] == Decimal::ZERO {
        0
    } else if outcome_prices[0] == Decimal::ZERO && outcome_prices[1] == Decimal::ONE {
        1
    } else {
        return source_failure(
            TerminalPredicateFailureReason::OutcomePricesNotOneHot,
            observed_active,
            closed_time_raw,
            Some(closed_time_utc_ms),
        );
    };

    finalize_gamma_terminal(
        &asset_ids,
        outcomes,
        winning_index,
        observed_active,
        closed_time_raw,
        closed_time_utc_ms,
    )
}

fn outcome_mapping_matches_fixed_identity(
    fixed_identity: &GammaTerminalFixedIdentity,
    asset_ids: &[String],
    outcomes: &[String],
) -> bool {
    let mut saw_up = false;
    let mut saw_down = false;
    for (asset_id, outcome) in asset_ids.iter().zip(outcomes) {
        match outcome.trim().to_ascii_lowercase().as_str() {
            "up" if asset_id == &fixed_identity.up_asset_id && !saw_up => saw_up = true,
            "down" if asset_id == &fixed_identity.down_asset_id && !saw_down => saw_down = true,
            _ => return false,
        }
    }
    saw_up && saw_down
}

fn finalize_gamma_terminal(
    asset_ids: &[String],
    outcomes: &[String],
    winning_index: usize,
    observed_active: Option<bool>,
    closed_time_raw: Option<String>,
    closed_time_utc_ms: i64,
) -> GammaTerminalEvaluation {
    let winner = Some(GammaTerminalWinner {
        winning_asset_id: asset_ids[winning_index].clone(),
        winning_outcome: outcomes[winning_index].clone(),
    });

    GammaTerminalEvaluation {
        terminal_predicate_passed: Some(true),
        terminal_predicate_failure_reason: None,
        resolution_poll_result: ResolutionPollResult::Terminal,
        resolution_integrity_error_class: None,
        observed_active,
        closed_time_raw,
        closed_time_utc_ms: Some(closed_time_utc_ms),
        winner,
    }
}

fn source_failure(
    reason: TerminalPredicateFailureReason,
    observed_active: Option<bool>,
    closed_time_raw: Option<String>,
    closed_time_utc_ms: Option<i64>,
) -> GammaTerminalEvaluation {
    GammaTerminalEvaluation {
        terminal_predicate_passed: Some(false),
        terminal_predicate_failure_reason: Some(reason),
        resolution_poll_result: reason.poll_result(),
        resolution_integrity_error_class: reason.integrity_error_class(),
        observed_active,
        closed_time_raw,
        closed_time_utc_ms,
        winner: None,
    }
}

fn not_evaluated(
    class: ResolutionIntegrityErrorClass,
    observed_active: Option<bool>,
    closed_time_raw: Option<String>,
) -> GammaTerminalEvaluation {
    GammaTerminalEvaluation {
        terminal_predicate_passed: None,
        terminal_predicate_failure_reason: None,
        resolution_poll_result: ResolutionPollResult::IntegrityError,
        resolution_integrity_error_class: Some(class),
        observed_active,
        closed_time_raw,
        closed_time_utc_ms: None,
        winner: None,
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::time::Duration;

    use polymarket_client_sdk_v2::error::Kind as PolymarketSdkErrorKind;
    use reqwest::{Method, StatusCode};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn market_by_slug_transport_classifier_uses_typed_status_and_fallback() {
        assert_eq!(
            ResolutionTransportErrorClass::TYPED_CLASSIFICATION_PRECEDENCE,
            [
                ResolutionTransportErrorClass::RateLimited,
                ResolutionTransportErrorClass::HttpStatus,
                ResolutionTransportErrorClass::Timeout,
                ResolutionTransportErrorClass::Connect,
                ResolutionTransportErrorClass::SdkTransport,
            ]
        );
        for (status, expected_class) in [
            (
                StatusCode::TOO_MANY_REQUESTS,
                ResolutionTransportErrorClass::RateLimited,
            ),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                ResolutionTransportErrorClass::HttpStatus,
            ),
        ] {
            let error = PolymarketSdkError::status(
                status,
                Method::GET,
                "markets/slug/test".to_owned(),
                "test",
            );

            assert_eq!(
                classify_gamma_market_by_slug_transport_error(&error),
                Some(GammaMarketBySlugTransportError {
                    class: expected_class,
                    http_status: Some(status.as_u16()),
                })
            );
        }

        let error = PolymarketSdkError::with_source(
            PolymarketSdkErrorKind::Internal,
            io::Error::other("dns timeout connect 429"),
        );
        assert_eq!(
            classify_gamma_market_by_slug_transport_error(&error),
            Some(GammaMarketBySlugTransportError {
                class: ResolutionTransportErrorClass::SdkTransport,
                http_status: None,
            })
        );
    }

    #[tokio::test]
    async fn market_by_slug_transport_classifier_uses_reqwest_timeout_before_connect() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind timeout test listener");
        let address = listener.local_addr().expect("timeout listener address");
        let server = tokio::spawn(async move {
            let _accepted = listener
                .accept()
                .await
                .expect("accept timeout test request");
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let reqwest_error = reqwest::Client::builder()
            .timeout(Duration::from_millis(25))
            .build()
            .expect("build timeout test client")
            .get(format!("http://{address}/"))
            .send()
            .await
            .expect_err("request must time out");
        assert!(reqwest_error.is_timeout());
        let error: PolymarketSdkError = reqwest_error.into();

        assert_eq!(
            classify_gamma_market_by_slug_transport_error(&error),
            Some(GammaMarketBySlugTransportError {
                class: ResolutionTransportErrorClass::Timeout,
                http_status: None,
            })
        );
        server.abort();
    }

    #[tokio::test]
    async fn market_by_slug_transport_classifier_uses_reqwest_connect() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind connect test listener");
        let address = listener.local_addr().expect("connect listener address");
        drop(listener);
        let reqwest_error = reqwest::Client::new()
            .get(format!("http://{address}/"))
            .send()
            .await
            .expect_err("request must fail to connect");
        assert!(reqwest_error.is_connect());
        let error: PolymarketSdkError = reqwest_error.into();

        assert_eq!(
            classify_gamma_market_by_slug_transport_error(&error),
            Some(GammaMarketBySlugTransportError {
                class: ResolutionTransportErrorClass::Connect,
                http_status: None,
            })
        );
    }

    #[tokio::test]
    async fn market_by_slug_transport_classifier_excludes_decode_and_schema_errors() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind decode test listener");
        let address = listener.local_addr().expect("decode listener address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept decode test request");
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1\r\nConnection: close\r\n\r\n{",
                )
                .await
                .expect("write invalid JSON response");
        });
        let response = reqwest::Client::new()
            .get(format!("http://{address}/"))
            .send()
            .await
            .expect("receive decode test response");
        let reqwest_error = response
            .json::<serde_json::Value>()
            .await
            .expect_err("invalid JSON must fail decoding");
        assert!(reqwest_error.is_decode());
        let error: PolymarketSdkError = reqwest_error.into();
        assert_eq!(classify_gamma_market_by_slug_transport_error(&error), None);
        server.await.expect("decode test server");

        let schema_error = serde_json::from_str::<u64>(r#""not-a-number""#)
            .expect_err("schema mismatch must fail");
        let error: PolymarketSdkError = schema_error.into();
        assert_eq!(classify_gamma_market_by_slug_transport_error(&error), None);
    }
}
