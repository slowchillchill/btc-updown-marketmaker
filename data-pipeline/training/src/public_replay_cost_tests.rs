//! 原构造夹具与显式ignored真实材料入口；只在测试构建可见。
use super::*;
use crate::public_replay::cost;
use serde_json::{Value, json};
use std::{io::Write, time::Instant};
#[derive(Serialize, Deserialize)]
struct DiagnosticConfig {
    public_root: PathBuf,
    read_page_rows: usize,
    max_read_batch_bytes: usize,
    expected_normal_prefix: CommittedIngressPrefixV1,
    initialization_input_positions: Vec<u64>,
    purpose: String,
}
#[derive(Default, Serialize)]
struct Summary {
    inputs: u64,
    groups: u64,
    observations: u64,
    input_kinds: BTreeMap<&'static str, u64>,
    widths: BTreeMap<usize, u64>,
    first_input: Option<u64>,
    last_input: Option<u64>,
    last_group: Option<u64>,
    input_non_increasing: u64,
    group_non_increasing: u64,
    initialization_inputs_seen: Vec<u64>,
    first_three_inputs: Vec<Value>,
    nonempty_dependencies: u64,
    dependency_refs: u64,
    dependency_examples: Vec<Value>,
    dependency_examples_omitted: u64,
    historical_repair_inputs: u64,
    historical_repair_members: u64,
    end_inputs: u64,
    eof: bool,
}
#[derive(Default)]
struct HashWriter(Sha256);
impl Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Summary {
    fn observe(
        &mut self,
        c: &DiagnosticConfig,
        input: &RecordedIngressV1,
        result: &PublicAppliedIngressV1<'_>,
        hash: &mut HashWriter,
    ) {
        self.inputs += 1;
        *self.input_kinds.entry(cost::kind(input)).or_default() += 1;
        if self.last_input.is_some_and(|p| input.input_position <= p) {
            self.input_non_increasing += 1;
        }
        self.first_input.get_or_insert(input.input_position);
        self.last_input = Some(input.input_position);
        if c.initialization_input_positions
            .contains(&input.input_position)
        {
            self.initialization_inputs_seen.push(input.input_position);
        }
        if self.first_three_inputs.len() < 3 {
            self.first_three_inputs
                .push(json!({"position":input.input_position,"kind":cost::kind(input)}));
        }
        self.end_inputs += u64::from(matches!(input.ingress, DptIngressV1::EndOfInput));
        if !input.dependency_record_refs.is_empty() {
            self.nonempty_dependencies += 1;
            self.dependency_refs += input.dependency_record_refs.len() as u64;
            if self.dependency_examples.len() < 16 {
                self.dependency_examples.push(json!({"input_position":input.input_position,"kind":cost::kind(input),"total_refs":input.dependency_record_refs.len(),"first_eight_refs":input.dependency_record_refs.iter().take(8).collect::<Vec<_>>(),"omitted_refs":input.dependency_record_refs.len().saturating_sub(8)}));
            } else {
                self.dependency_examples_omitted += 1;
            }
        }
        serde_json::to_writer(
            &mut *hash,
            &(
                &input.input_stream_id,
                input.input_position,
                &input.dependency_record_refs,
            ),
        )
        .unwrap();
        if let DptIngressV1::Message {
            row,
            source_evidence,
        } = &input.ingress
        {
            serde_json::to_writer(
                &mut *hash,
                &(
                    &source_evidence.source_message_id,
                    source_evidence.source_event_at_utc_ms,
                    source_evidence.received_at_utc_ms,
                ),
            )
            .unwrap();
            if let TypedRawRowV2::HistoricalPriceAnchorRepairBatch(batch) = row {
                self.historical_repair_inputs += 1;
                self.historical_repair_members += batch.rows.len() as u64;
            }
        }
        for g in &result.applied.closed_groups {
            self.groups += 1;
            self.observations += g.observations.len() as u64;
            *self.widths.entry(g.observations.len()).or_default() += 1;
            if self.last_group.is_some_and(|p| g.group_position <= p) {
                self.group_non_increasing += 1;
            }
            self.last_group = Some(g.group_position);
            serde_json::to_writer(
                &mut *hash,
                &(
                    &g.group_id,
                    g.group_position,
                    &g.member_input_positions,
                    g.closed_at_input_position,
                ),
            )
            .unwrap();
            for f in g.records.iter() {
                serde_json::to_writer(
                    &mut *hash,
                    &(
                        &f.position,
                        &f.source_refs,
                        &f.direct_time,
                        &f.dependency_time,
                        &f.quality,
                    ),
                )
                .unwrap();
            }
        }
    }
}
#[derive(Default, Serialize)]
struct DriverTimes {
    open_ns: u128,
    summary_ns: u128,
    returned_objects_drop_ns: u128,
    into_core_and_reader_release_ns: u128,
    core_drop_ns: u128,
}
fn diagnose(c: &DiagnosticConfig) -> (Value, RecordingResult<()>) {
    let before = fs::read_to_string("/proc/self/io").ok();
    let started = Instant::now();
    let session = cost::Session::enable();
    let mut summary = Summary::default();
    let mut hash = HashWriter::default();
    let mut times = DriverTimes::default();
    let mut prefix = None;
    let mut applied_prefix = None;
    let mut canonical_next = None;
    let open_started = Instant::now();
    let opened = PublicReplayV1::open(&c.public_root, c.read_page_rows, c.max_read_batch_bytes);
    times.open_ns = open_started.elapsed().as_nanos();
    let result = match opened {
        Err(error) => Err(error),
        Ok(mut replay) => {
            prefix = cost::reader_prefix(&replay);
            let result = loop {
                match replay.next_prepared_applied() {
                    Err(error) => break Err(error),
                    Ok(None) => {
                        summary.eof = true;
                        break Ok(());
                    }
                    Ok(Some((input, applied, pm))) => {
                        let at = Instant::now();
                        summary.observe(c, &input, &applied, &mut hash);
                        times.summary_ns += at.elapsed().as_nanos();
                        let at = Instant::now();
                        drop((input, applied, pm));
                        times.returned_objects_drop_ns += at.elapsed().as_nanos();
                    }
                }
            };
            applied_prefix = Some(replay.core.applied_prefix());
            canonical_next = Some(replay.core.next_canonical_position());
            let at = Instant::now();
            let core = replay.into_core();
            times.into_core_and_reader_release_ns = at.elapsed().as_nanos();
            let at = Instant::now();
            drop(core);
            times.core_drop_ns = at.elapsed().as_nanos();
            result
        }
    };
    let wall = started.elapsed().as_secs_f64();
    let after = fs::read_to_string("/proc/self/io").ok();
    let original_error = result.as_ref().err().map(ToString::to_string);
    let normal_end_observed = result.is_ok()
        && summary.eof
        && summary.end_inputs == 1
        && prefix.as_ref().is_some_and(|p| p.normal_end);
    let internal = cost::report_value(session.finish());
    let report = json!({"schema":"public_replay_internal_cost_v1","effective_config":c,"status":if result.is_ok(){"success"}else{"error"},"original_error":original_error,"normal_end_observed":normal_end_observed,"actual_reader_prefix":prefix,"applied_prefix":applied_prefix,"next_canonical_position":canonical_next,"summary":summary,"fixed_identity_time_quality_sha256":format!("{:x}",hash.0.finalize()),"digest_coverage":"input stream/position/dependency refs and source id/source/received times; group id/position/member/close coordinates; fact position/source refs/direct and dependency time/quality; excludes full payload, source bodies and observation business fields","internal":internal,"driver":{"times":times,"wall_seconds":wall,"proc_io_before":before,"proc_io_after":after,"boundary":"returned-object explicit drop is separate from summary; replay.into_core includes reader release and move; final core drop separate; report serialization/write and diagnostic-state finalization excluded from driver wall, included in outer GNU process","last_successful_returned_input":summary.last_input}});
    (report, result)
}
fn persist(report: &Value, result: RecordingResult<()>, path: &Path) -> RecordingResult<()> {
    let written = (|| -> RecordingResult<()> {
        fs::write(path, serde_json::to_vec_pretty(report)?)?;
        Ok(())
    })();
    match (result, written) {
        (Err(original), Err(writing)) => {
            eprintln!("诊断报告写入另有错误：{writing}");
            Err(original)
        }
        (Err(original), Ok(())) => Err(original),
        (Ok(()), Err(writing)) => Err(writing),
        (Ok(()), Ok(())) => Ok(()),
    }
}
#[test]
#[ignore = "需显式提供已批准十二分钟材料/报告配置；普通回归不执行真实材料"]
fn existing_twelve_minute_replay_internal_cost() -> RecordingResult<()> {
    let config = std::env::var("DPT_REPLAY_COST_CONFIG")?;
    let output = std::env::var("DPT_REPLAY_COST_REPORT")?;
    let c: DiagnosticConfig = serde_json::from_slice(&fs::read(config)?)?;
    let (report, result) = diagnose(&c);
    persist(&report, result, Path::new(&output))
}
fn configured(root: &Path) -> DiagnosticConfig {
    DiagnosticConfig {
        public_root: root.to_path_buf(),
        read_page_rows: 1,
        max_read_batch_bytes: 1 << 20,
        expected_normal_prefix: PublicRecordingReaderV1::recover(root, 1, 1 << 20)
            .unwrap()
            .prefix
            .unwrap(),
        initialization_input_positions: vec![],
        purpose: "有效小夹具计量一致及原错验证".into(),
    }
}
fn reference_fixture(root: &Path, twap_window: u64) {
    let mut c = core();
    let mut writer = PublicRecordingWriterV1::create(root, c.header(), config()).unwrap();
    for i in [
        twap_record(0, twap_window, 21_002, "65000000000000000000000"),
        rule_record(1, 60, None),
    ] {
        let out = apply(&mut c, &i);
        writer.record(i, out).unwrap();
    }
    writer.stabilize().unwrap();
    let mut input = meta(2);
    let ids = BTreeMap::new();
    let mut page = ReferenceCandidateReaderV1::open(root, 1, 1 << 20, 2).unwrap();
    let prepared =
        prepare_public_ingress_with_reference_v1(&c, &mut input, &ids, &[], &market(), |cursor| {
            Ok(page.page(cursor).unwrap())
        })
        .unwrap();
    let out = c.push(&prepared).unwrap().into_owned();
    drop(prepared);
    writer.record(input, out).unwrap();
    writer.stabilize().unwrap();
    let input = end(3);
    let out = apply(&mut c, &input);
    writer.record(input, out).unwrap();
    writer.stabilize().unwrap();
}
fn complete_returns(root: &Path, enabled: bool) -> (Vec<Value>, Option<Value>) {
    let session = enabled.then(cost::Session::enable);
    let mut replay = PublicReplayV1::open(root, 1, 1 << 20).unwrap();
    let mut rows = vec![];
    while let Some((input, result, pm)) = replay.next_prepared_applied().unwrap() {
        rows.push(json!({"input":input,"complete_applied":fingerprint(&result),"prepared_pm_message":pm.as_ref().map(|p|p.message())}));
    }
    (rows, session.map(|s| cost::report_value(s.finish())))
}
#[test]
fn enabled_and_disabled_complete_results_match_and_empty_refs_still_scan_candidates() {
    let temp = tempfile::tempdir().unwrap();
    for window in [30, 60] {
        let root = temp.path().join(window.to_string());
        reference_fixture(&root, window);
        let (plain, none) = complete_returns(&root, false);
        assert!(none.is_none());
        let (measured, some) = complete_returns(&root, true);
        assert_eq!(plain, measured);
        let d = some.unwrap();
        assert_eq!(d["returned_inputs"], 4);
        assert_eq!(d["eof_returns"], 1);
        assert_eq!(d["negative_residual_entries"], 0);
        assert_eq!(d["primary"]["identity"]["calls"], 4);
        assert!(d["scan_totals"]["identity_scan"].is_null());
        assert!(
            d["scan_totals"]["candidate_open"]["open_successes"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(
            d["scan_by_input"]["2"]["kinds"]["candidate_page"]["twap_encountered"]
                .as_u64()
                .unwrap()
                > 0
        );
        let refs = plain[2]["input"]["dependency_record_refs"]
            .as_array()
            .unwrap()
            .len();
        if window == 30 {
            assert_eq!(refs, 0);
            assert!(d["scan_totals"]["dependency_scan"].is_null());
        } else {
            assert_eq!(refs, 1);
            assert_eq!(d["scan_totals"]["dependency_scan"]["open_successes"], 1);
            assert_eq!(d["scan_totals"]["dependency_scan"]["matched_records"], 1);
        }
    }
}
#[test]
fn original_reader_error_keeps_partial_spans_and_report_error_cannot_replace_it() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("bad");
    reference_fixture(&root, 60);
    let c = configured(&root);
    let segment = &c.expected_normal_prefix.stable_storage_ref;
    fs::write(root.join(segment).join("captured.parquet"), b"broken").unwrap();
    let mut plain = PublicReplayV1::open(&root, 1, 1 << 20).unwrap();
    for _ in 0..2 {
        assert!(plain.next_prepared_applied().unwrap().is_some());
    }
    let error = plain.next_prepared_applied().err().unwrap().to_string();
    let (report, result) = diagnose(&c);
    assert_eq!(result.as_ref().err().unwrap().to_string(), error);
    assert_eq!(report["status"], "error");
    assert_eq!(report["normal_end_observed"], false);
    assert_eq!(report["summary"]["eof"], false);
    assert_eq!(report["internal"]["failed_entries"], 1);
    assert_eq!(report["summary"]["inputs"], 2);
    assert_eq!(
        report["internal"]["last_entry"]["context"]["input_position"],
        2
    );
    assert_eq!(
        report["internal"]["primary"]["dependencies"]["completed"],
        2
    );
    assert_eq!(
        report["internal"]["scan_totals"]["dependency_scan"]["completed"],
        0
    );
    assert!(
        report["internal"]["scan_totals"]["dependency_scan"]["rows"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        report["internal"]["last_entry"]["failed_spans"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "dependencies")
    );
    let path = temp.path().join("partial.json");
    assert_eq!(
        persist(&report, result, &path).unwrap_err().to_string(),
        error
    );
    let partial: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(partial["original_error"], error);
    let (_, again) = diagnose(&c);
    assert_eq!(
        persist(&report, again, &temp.path().join("missing/partial.json"))
            .unwrap_err()
            .to_string(),
        error
    );
}
