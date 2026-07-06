use data_collect::market_discovery::{
    BtcUpdownMarketCandidate, CandidateRejectReason, DiscoverySelectionError,
    build_market_subscription_request_json, discovery_windows, select_btc_updown_pair,
};

const NOW_MS: i64 = 1_766_789_469_000;

#[test]
fn rejects_generic_btc_markets_instead_of_fallback() {
    let windows = discovery_windows(NOW_MS);
    let generic = BtcUpdownMarketCandidate {
        id: "generic-btc".to_owned(),
        slug: Some("bitcoin-price-today".to_owned()),
        question: Some("Will bitcoin go up today?".to_owned()),
        event_slug: None,
        event_title: None,
        series_slug: None,
        condition_id: Some(format!("0x{}", "1".repeat(64))),
        clob_token_ids: vec!["11".to_owned(), "12".to_owned()],
        outcomes: vec!["Up".to_owned(), "Down".to_owned()],
        start_ts_s: Some(windows.current.target_ts_s),
        end_ts_s: Some(windows.current.end_ts_s),
        active: Some(true),
        closed: Some(false),
        archived: Some(false),
        enable_order_book: Some(true),
        accepting_orders: Some(true),
        raw_json_sha256: Some("generic-digest".to_owned()),
    };

    let error = select_btc_updown_pair(NOW_MS, &[generic], &[]).expect_err("must fail closed");

    assert!(matches!(
        error,
        DiscoverySelectionError::InvalidCurrent {
            reason: CandidateRejectReason::SlugMismatch,
            ..
        }
    ));
}

#[test]
fn selects_exact_current_and_next_btc_updown_5m_pair() {
    let windows = discovery_windows(NOW_MS);
    let current = candidate_for_window(
        "current",
        &windows.current.slug,
        windows.current.target_ts_s,
        windows.current.end_ts_s,
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "101",
        "102",
    );
    let next = candidate_for_window(
        "next",
        &windows.next.slug,
        windows.next.target_ts_s,
        windows.next.end_ts_s,
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "201",
        "202",
    );

    let selection =
        select_btc_updown_pair(NOW_MS, &[current], &[next]).expect("exact pair should pass");

    assert_eq!(selection.current.slug, windows.current.slug);
    assert_eq!(selection.next.slug, windows.next.slug);
    assert_eq!(selection.current.up_asset_id, "101");
    assert_eq!(selection.current.down_asset_id, "102");
    assert_eq!(selection.next.up_asset_id, "201");
    assert_eq!(selection.next.down_asset_id, "202");
    assert!(selection.relation.same_series_prefix);
    assert!(selection.relation.windows_contiguous);
    assert!(selection.relation.condition_ids_distinct);
}

#[test]
fn preserves_platform_start_when_it_precedes_slug_window() {
    let windows = discovery_windows(NOW_MS);
    let current = candidate_for_window(
        "current",
        &windows.current.slug,
        windows.current.target_ts_s - 60,
        windows.current.end_ts_s,
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "101",
        "102",
    );
    let next = candidate_for_window(
        "next",
        &windows.next.slug,
        windows.next.target_ts_s,
        windows.next.end_ts_s,
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "201",
        "202",
    );

    let selection =
        select_btc_updown_pair(NOW_MS, &[current], &[next]).expect("platform start should pass");

    assert_eq!(selection.current.start_ts_s, windows.current.target_ts_s);
    assert_eq!(
        selection.current.candidate_start_ts_s,
        windows.current.target_ts_s - 60
    );
}

#[test]
fn subscription_evidence_requires_custom_features_and_real_asset_ids() {
    let json = build_market_subscription_request_json(&[
        "101".to_owned(),
        "102".to_owned(),
        "201".to_owned(),
        "202".to_owned(),
    ])
    .expect("subscription json");
    let value: serde_json::Value = serde_json::from_str(&json).expect("valid json");

    assert_eq!(value["type"], "market");
    assert_eq!(value["operation"], "subscribe");
    assert_eq!(value["custom_feature_enabled"], true);
    assert_eq!(value["assets_ids"][0], "101");
    assert_eq!(value["assets_ids"][1], "102");
    assert_eq!(value["assets_ids"][2], "201");
    assert_eq!(value["assets_ids"][3], "202");
}

fn candidate_for_window(
    id: &str,
    slug: &str,
    start_ts_s: i64,
    end_ts_s: i64,
    condition_id: &str,
    up_asset_id: &str,
    down_asset_id: &str,
) -> BtcUpdownMarketCandidate {
    BtcUpdownMarketCandidate {
        id: id.to_owned(),
        slug: Some(slug.to_owned()),
        question: Some("Bitcoin Up or Down - July 6, 5 min".to_owned()),
        event_slug: Some("btc-updown-5m".to_owned()),
        event_title: Some("Bitcoin Up or Down".to_owned()),
        series_slug: Some("btc-updown-5m".to_owned()),
        condition_id: Some(condition_id.to_owned()),
        clob_token_ids: vec![up_asset_id.to_owned(), down_asset_id.to_owned()],
        outcomes: vec!["Up".to_owned(), "Down".to_owned()],
        start_ts_s: Some(start_ts_s),
        end_ts_s: Some(end_ts_s),
        active: Some(true),
        closed: Some(false),
        archived: Some(false),
        enable_order_book: Some(true),
        accepting_orders: Some(true),
        raw_json_sha256: Some(format!("{id}-digest")),
    }
}
