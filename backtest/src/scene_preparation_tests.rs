use super::*;
fn event(at_ms: i64, sequence: u64, token: &str) -> Event {
    Event {
        at_ms,
        source_sequence: sequence,
        level: Level {
            condition: token.into(),
            buy_token: token.into(),
            price_micro: 500_000,
        },
        kind: EventKind::Cancel {
            targets: vec![Slice {
                segment: sequence,
                start: 0,
                end: u128::MAX,
            }],
            recovery: false,
        },
    }
}
fn material() -> OfflineSceneMaterialV1 {
    OfflineSceneMaterialV1 {
        scene: PreparedScene {
            model_version: MODEL_VERSION.into(),
            parameters: Parameters {
                association_ms: 250,
                allocation: Allocation::Nearest,
                cancellation: Cancellation::SegmentProportionalV2,
                internal_time: InternalTime::SourceWhenPresent,
                seed: 7,
                association_trade_shift_ms: 0,
            },
            report: PreparationReport::default(),
            events: Vec::new(),
        },
        markets: BTreeMap::new(),
        market_assets: BTreeMap::new(),
        source_manifests: Vec::new(),
    }
}
fn output(path: PathBuf, events: &[(usize, Event)]) -> ConditionOutput {
    let mut encoder =
        zstd::stream::write::Encoder::new(std::fs::File::create(&path).unwrap(), 3).unwrap();
    encoder.include_checksum(true).unwrap();
    let mut bytes = Vec::new();
    let mut max_event_bytes = 0;
    for (rank, event) in events {
        max_event_bytes =
            max_event_bytes.max(write_ranked(&mut encoder, *rank, event, &mut bytes).unwrap());
    }
    encoder.finish().unwrap().flush().unwrap();
    ConditionOutput {
        path,
        count: events.len() as u64,
        report: PreparationReport::default(),
        max_event_bytes,
    }
}
#[test]
fn rank_transfer_preserves_multiple_events_per_rank_and_same_ms_order() {
    let root = tempfile::tempdir().unwrap();
    let a = vec![
        (0, event(10, 7, "a")),
        (0, event(10, 6, "a")),
        (2, event(12, 9, "a")),
        (2, event(12, 8, "a")),
    ];
    let b = vec![(1, event(10, 4, "b")), (3, event(13, 3, "b"))];
    let expected = vec![
        a[0].1.clone(),
        a[1].1.clone(),
        b[0].1.clone(),
        a[2].1.clone(),
        a[3].1.clone(),
        b[1].1.clone(),
    ];
    let outputs = [
        output(root.path().join("a.zst"), &a),
        output(root.path().join("b.zst"), &b),
    ];
    for workers in [1, 2, 4, 6] {
        let path = root.path().join(format!("{workers}.scene"));
        merge(
            &outputs,
            &path,
            workers,
            expected.len() as u64,
            0,
            material(),
        )
        .unwrap();
        let (header, events, _) = open_scene_material(&path).unwrap();
        assert_eq!(events.collect::<Result<Vec<_>, _>>().unwrap(), expected);
        assert_eq!(header.scene.parameters.seed, 7);
        let manifest: StreamManifestV2 =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let payload = std::fs::read(path.with_extension("events.zst")).unwrap();
        assert_eq!(
            manifest.event_sha256,
            format!("{:x}", Sha256::digest(&payload)),
            "摘要包含zstd收尾校验字节"
        );
    }
}
#[test]
fn malformed_ranked_stream_does_not_publish_manifest() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("bad.zst");
    let mut bytes = Vec::new();
    write_ranked(&mut bytes, 2, &event(10, 1, "a"), &mut Vec::new()).unwrap();
    bytes.truncate(bytes.len() - 1);
    std::fs::write(
        &source,
        zstd::stream::encode_all(bytes.as_slice(), 3).unwrap(),
    )
    .unwrap();
    let path = root.path().join("failed.scene");
    let outputs = [ConditionOutput {
        path: source,
        count: 1,
        max_event_bytes: 1 << 20,
        report: PreparationReport::default(),
    }];
    assert!(merge(&outputs, &path, 4, 1, 0, material()).is_err());
    assert!(!path.exists());
}
#[test]
fn same_length_temporary_frame_bit_corruption_does_not_publish_manifest() {
    let root = tempfile::tempdir().unwrap();
    let source = output(
        root.path().join("original.zst"),
        &[(0, event(10, 7, "condition-a"))],
    );
    let mut compressed = std::fs::read(&source.path).unwrap();
    let original_len = compressed.len();
    // zstd帧的末四字节是checksum；修改它之前的压缩载荷，不改变任何文件长度。
    let position = compressed.len() - 5;
    compressed[position] ^= 1;
    std::fs::write(&source.path, &compressed).unwrap();
    assert_eq!(
        std::fs::metadata(&source.path).unwrap().len(),
        original_len as u64
    );
    let path = root.path().join("failed-bitflip.scene");
    let error = merge(&[source], &path, 4, 1, 0, material()).unwrap_err();
    assert!(!path.exists(), "同长度临时流损坏不得发布成品清单: {error}");
}
#[test]
fn compressed_tail_failure_does_not_publish_manifest_for_parallel_workers() {
    #[cfg(unix)]
    {
        let root = tempfile::tempdir().unwrap();
        for workers in [1, 2, 4, 6] {
            let path = root.path().join(format!("{workers}.scene"));
            std::os::unix::fs::symlink("/dev/full", path.with_extension("events.zst")).unwrap();
            let mut writer =
                SceneStreamWriterV2::create_with_merge_workers(&path, workers).unwrap();
            writer.push(event(10, 1, "a")).unwrap();
            assert!(
                writer.finish(material()).is_err(),
                "缓冲小事件到收尾才暴露磁盘写入失败"
            );
            assert!(!path.exists());
            assert!(!path.with_extension("scene-manifest.pending").exists());
        }
    }
}
#[test]
fn hashing_writer_tracks_only_successful_short_writes() {
    struct Short(Vec<u8>);
    impl Write for Short {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let n = bytes.len().min(3);
            self.0.extend_from_slice(&bytes[..n]);
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = HashingWriter::new(Short(Vec::new()));
    writer
        .write_all(b"actual compressed bytes and tail")
        .unwrap();
    writer.flush().unwrap();
    assert_eq!(
        writer.finish(),
        format!("{:x}", Sha256::digest(b"actual compressed bytes and tail"))
    );
}
