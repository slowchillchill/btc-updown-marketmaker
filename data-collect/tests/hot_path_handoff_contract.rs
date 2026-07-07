use data_collect::handoff::{
    HandoffFrame, HandoffFrameType, HandoffRuntimeConfig, RuntimeReceivedFrame,
    run_single_frame_unix_handoff, run_single_frame_unix_server_handoff,
};
use data_collect::publish::PaperLiveEnvironment;
use tempfile::tempdir;

#[test]
fn paper_and_live_handoff_roots_are_distinct_but_schema_is_shared() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();

    let paper =
        HandoffRuntimeConfig::for_environment(&dataset_root, PaperLiveEnvironment::Paper).unwrap();
    let live =
        HandoffRuntimeConfig::for_environment(&dataset_root, PaperLiveEnvironment::Live).unwrap();

    assert!(
        paper
            .socket_path
            .ends_with("paper/data_collect_handoff/hot.sock")
    );
    assert!(
        live.socket_path
            .ends_with("live/data_collect_handoff/hot.sock")
    );
    assert_ne!(paper.socket_path, live.socket_path);
    assert_eq!(paper.frame_schema_version, live.frame_schema_version);
}

#[test]
fn external_market_event_frame_has_required_timestamp_namespace() {
    let frame = HandoffFrame::external_market_event(
        42,
        "binance_aggtrade",
        "aggTrade",
        "BTCUSDT",
        Some("1767225600000"),
        1_767_225_600_010,
        1_767_225_600_011,
        "collector-host",
        "eu-west-1",
        r#"{"e":"aggTrade"}"#,
    )
    .unwrap();

    assert_eq!(frame.frame_type, HandoffFrameType::ExternalMarketEvent);
    assert_eq!(frame.event_seq, 42);
    assert_eq!(frame.collector_recv_time_utc_ms, 1_767_225_600_010);
    assert_eq!(frame.handoff_send_time_utc_ms, 1_767_225_600_011);
    assert_eq!(frame.collector_region, "eu-west-1");
    assert!(frame.raw_json.as_deref().unwrap().contains("aggTrade"));
}

#[tokio::test]
async fn fake_consumer_records_runtime_receive_time_on_local_stream() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let config =
        HandoffRuntimeConfig::for_environment(&dataset_root, PaperLiveEnvironment::Paper).unwrap();
    let frame = HandoffFrame::status(
        1,
        "source_status",
        "polymarket_market",
        1_767_225_600_001,
        "collector-host",
        "eu-west-1",
    )
    .unwrap();

    let received = run_single_frame_unix_handoff(&config, frame).await.unwrap();

    assert_eq!(received.frame.frame_type, HandoffFrameType::SourceStatus);
    assert!(received.runtime_receive_time_utc_ms >= received.frame.handoff_send_time_utc_ms);
    assert_eq!(
        RuntimeReceivedFrame::runtime_timestamp_field_name(),
        "runtime_receive_time_utc_ms"
    );
}

#[tokio::test]
async fn data_collect_server_can_send_status_frame_to_fake_consumer() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let config =
        HandoffRuntimeConfig::for_environment(&dataset_root, PaperLiveEnvironment::Live).unwrap();
    let frame = HandoffFrame::status(
        7,
        "heartbeat",
        "data_collect_hot_handoff",
        1_767_225_600_001,
        "collector-host",
        "eu-west-1",
    )
    .unwrap();

    let received = run_single_frame_unix_server_handoff(&config, frame)
        .await
        .unwrap();

    assert_eq!(received.frame.frame_type, HandoffFrameType::SourceStatus);
    assert_eq!(received.frame.event_seq, 7);
    assert!(received.runtime_receive_time_utc_ms >= received.frame.handoff_send_time_utc_ms);
}
