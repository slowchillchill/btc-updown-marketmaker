use std::time::Duration;

use data_collect::handoff::{
    HANDOFF_FRAME_SCHEMA_VERSION, HandoffFrame, HandoffFrameType, HandoffRuntimeConfig,
    run_heartbeat_handoff_server,
};
use data_collect::publish::PaperLiveEnvironment;
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::net::UnixStream;
use tokio::time::{sleep, timeout};

#[test]
fn paper_and_live_handoff_roots_and_sockets_are_distinct() {
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

    assert!(
        HandoffRuntimeConfig::for_environment(
            std::path::Path::new("relative-dataset-root"),
            PaperLiveEnvironment::Paper,
        )
        .is_err()
    );
    assert!(
        HandoffRuntimeConfig::for_environment(
            &dataset_root.join("..").join("escape"),
            PaperLiveEnvironment::Live,
        )
        .is_err()
    );
}

#[tokio::test]
async fn heartbeat_server_preserves_the_v1_source_status_line() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let config =
        HandoffRuntimeConfig::for_environment(&dataset_root, PaperLiveEnvironment::Paper).unwrap();
    let socket_path = config.socket_path.clone();
    let server = tokio::spawn(async move {
        run_heartbeat_handoff_server(&config, "collector-host", "eu-west-1", Some(1)).await
    });

    timeout(Duration::from_secs(1), async {
        while !socket_path.exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let stream = UnixStream::connect(&socket_path).await.unwrap();
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).await.unwrap();
    let frame: HandoffFrame = serde_json::from_str(&line).unwrap();

    assert_eq!(frame.frame_schema_version, HANDOFF_FRAME_SCHEMA_VERSION);
    assert_eq!(frame.frame_type, HandoffFrameType::SourceStatus);
    assert_eq!(frame.event_seq, 1);
    assert_eq!(frame.source_kind, "data_collect_hot_handoff");
    assert_eq!(frame.source_event_type, "source_status");
    assert_eq!(frame.source_identifier, "source_status");
    assert_eq!(frame.status.as_deref(), Some("heartbeat"));
    assert_eq!(frame.collector_host_id, "collector-host");
    assert_eq!(frame.collector_region, "eu-west-1");
    assert!(frame.source_timestamp.is_none());
    assert!(frame.raw_json.is_none());
    frame.validate().unwrap();

    server.await.unwrap().unwrap();
    assert!(!socket_path.exists());
}
