//! #377 固定65调用的有界材料准备；不装载模型、不重算标签、不改原S/E/H。
use crate::{ConsumerRunConfig, FrozenRuleBindingV1};
use backtest::{ContinuousPublicExportProgressV1, MaterialError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, value::RawValue};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

const CALLS: usize = 65;
const RECORDS: usize = 131;
const RECORD_BYTES: u64 = 1 << 20;
const TRAJECTORY_BYTES: usize = 16 << 20;
const LABEL_BYTES: usize = 1 << 20;
const SINGLE_LABEL_BYTES: usize = 64 << 10;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrefixSource {
    pub consumer_config: PathBuf,
    pub trajectory: PathBuf,
    pub labels: Option<PathBuf>,
    pub account: String,
    pub group: String,
    pub trajectory_version: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureSampleConfig {
    pub rules: FrozenRuleBindingV1,
    pub finality_policy_path: PathBuf,
    pub implementation_identity: sim_system::platform_rules::ImplementationIdentityV1,
    pub encoding: crate::encoding::EncodingConfig,
    pub sampling_seed: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrefixSampleConfig {
    pub consumer: PrefixSource,
    pub learner: PrefixSource,
    pub output_directory: PathBuf,
    pub stop_file: PathBuf,
    pub max_wall_seconds: u64,
    #[serde(default)]
    pub f4: Option<FixtureSampleConfig>,
}
struct Budget<'a> {
    config: &'a PrefixSampleConfig,
    started: Instant,
}
impl Budget<'_> {
    fn stopped(&self) -> bool {
        self.config.stop_file.exists()
            || self.started.elapsed() >= Duration::from_secs(self.config.max_wall_seconds)
    }
    fn check(&self) -> Result<(), MaterialError> {
        if self.stopped() {
            Err("#377有限材料准备到限，保留原始结果，不重试".into())
        } else {
            Ok(())
        }
    }
}
struct LimitedRead<R> {
    inner: R,
    maximum: usize,
    bytes: usize,
    sha: Sha256,
}
impl<R: Read> Read for LimitedRead<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let available = buffer.len().min(self.maximum.saturating_sub(self.bytes));
        if available == 0 {
            return Ok(0);
        }
        let count = self.inner.read(&mut buffer[..available])?;
        self.bytes += count;
        self.sha.update(&buffer[..count]);
        Ok(count)
    }
}
impl<R> LimitedRead<R> {
    fn new(inner: R, maximum: usize) -> Self {
        Self {
            inner,
            maximum,
            bytes: 0,
            sha: Sha256::new(),
        }
    }
    fn report(&self) -> Value {
        json!({"bytes_read_from_file_including_buffer_read_ahead":self.bytes,"read_prefix_sha256":format!("{:x}",self.sha.clone().finalize()),"full_file_verified":false,"ordinary_os_physical_read_ahead":"Unknown"})
    }
}
struct PrefixRows {
    binding: FrozenRuleBindingV1,
    rows: Vec<Value>,
    report: Value,
}
fn trajectory_prefix(
    source: &PrefixSource,
    count: usize,
    mut check: impl FnMut() -> Result<(), MaterialError>,
) -> Result<PrefixRows, MaterialError> {
    let mut reader = BufReader::with_capacity(
        1,
        LimitedRead::new(File::open(&source.trajectory)?, TRAJECTORY_BYTES),
    );
    let mut rows = Vec::new();
    let mut binding = None;
    let mut records = 0usize;
    let mut parsed_bytes = 0usize;
    let mut update = false;
    let outcome = (|| {
        while rows.len() < count {
            check()?;
            if records == RECORDS {
                return Err("65调用前缀超出131生产记录，不继续扫描".into());
            }
            let mut line = Vec::new();
            let read = Read::by_ref(&mut reader)
                .take(RECORD_BYTES)
                .read_until(b'\n', &mut line)?;
            if read == 0 || line.last() != Some(&b'\n') {
                return Err("65调用前缀缺失/截断或单记录超过1MiB".into());
            }
            records += 1;
            parsed_bytes += read;
            let row: Value = serde_json::from_slice(&line)?;
            match row.get("record_type") {
                Some(Value::String(kind)) if kind == "rules_binding" && records == 1 => {
                    binding = Some(serde_json::from_value::<FrozenRuleBindingV1>(
                        row["binding"].clone(),
                    )?);
                }
                Some(Value::String(kind))
                    if kind == "confirmed_economic_update" && binding.is_some() && !update =>
                {
                    update = true;
                }
                None if binding.is_some() => {
                    let frozen = binding.as_ref().unwrap();
                    if row["account"] != source.account
                        || row["group"] != source.group
                        || row["version"] != source.trajectory_version
                        || row["call"].as_u64() != Some(rows.len() as u64 + 1)
                        || row["rules_identity"]
                            != serde_json::to_value(&frozen.expected_file_sha256)?
                    {
                        return Err("有限原轨迹的账户/组/版本/call/规则身份不符".into());
                    }
                    if row["processing_start_utc_ms"].as_i64().is_none() {
                        return Err("有限轨迹缺实际处理时间".into());
                    }
                    rows.push(row);
                    update = false;
                }
                _ => return Err("有限原轨迹存在未知类型或重复/错序生产记录".into()),
            }
        }
        let mut report = reader.get_ref().report();
        report["records_parsed"] = records.into();
        report["parsed_prefix_bytes"] = parsed_bytes.into();
        report["calls"] = count.into();
        Ok(PrefixRows {
            binding: binding.ok_or("有限原轨迹缺规则绑定首行")?,
            rows,
            report,
        })
    })();
    outcome.map_err(|error: MaterialError| {
        format!("{error}; 实际轨迹前缀读取={}", reader.get_ref().report()).into()
    })
}
fn byte(reader: &mut impl BufRead) -> Result<u8, MaterialError> {
    let value = *reader
        .fill_buf()?
        .first()
        .ok_or("有限标签在完整对象边界前耗尽")?;
    reader.consume(1);
    Ok(value)
}
fn non_space(reader: &mut impl BufRead) -> Result<u8, MaterialError> {
    loop {
        let value = byte(reader)?;
        if !value.is_ascii_whitespace() {
            return Ok(value);
        }
    }
}
fn label_object(reader: &mut impl BufRead, first: u8) -> Result<Box<RawValue>, MaterialError> {
    if first != b'{' {
        return Err("冻结label必须是原对象".into());
    }
    let mut raw = vec![first];
    let mut nesting = vec![b'{'];
    let mut quoted = false;
    let mut escaped = false;
    while !nesting.is_empty() {
        if raw.len() == SINGLE_LABEL_BYTES {
            return Err("单个冻结label超过64KiB，不继续读取该对象".into());
        }
        let value = byte(reader)?;
        raw.push(value);
        if quoted {
            if escaped {
                escaped = false;
            } else if value == b'\\' {
                escaped = true;
            } else if value == b'"' {
                quoted = false;
            }
        } else {
            match value {
                b'"' => quoted = true,
                b'{' | b'[' => nesting.push(value),
                b'}' | b']' => {
                    let opening = nesting.pop().ok_or("冻结label括号越界")?;
                    if (opening == b'{') != (value == b'}') {
                        return Err("冻结label括号不匹配".into());
                    }
                }
                _ => {}
            }
        }
    }
    Ok(serde_json::from_slice(&raw)?)
}
fn label_prefix(
    source: &PrefixSource,
    rows: &[Value],
) -> Result<(Vec<Box<RawValue>>, Value), MaterialError> {
    let path = source.labels.as_ref().ok_or("L65缺原冻结标签路径")?;
    let mut reader = BufReader::with_capacity(1, LimitedRead::new(File::open(path)?, LABEL_BYTES));
    let outcome = (|| {
        if non_space(&mut reader)? != b'{' || non_space(&mut reader)? != b'"' {
            return Err("冻结标签不是原账户对象".into());
        }
        let mut account = vec![b'"'];
        let mut escaped = false;
        loop {
            if account.len() == SINGLE_LABEL_BYTES {
                return Err("冻结标签账户键超过有界接口".into());
            }
            let value = byte(&mut reader)?;
            account.push(value);
            if escaped {
                escaped = false;
            } else if value == b'\\' {
                escaped = true;
            } else if value == b'"' {
                break;
            }
        }
        if serde_json::from_slice::<String>(&account)? != source.account {
            return Err("有限标签首账户与冻结绑定不同，不扫描其他账户".into());
        }
        if non_space(&mut reader)? != b':' || non_space(&mut reader)? != b'[' {
            return Err("冻结标签账户缺原数组".into());
        }
        let mut labels = Vec::new();
        for index in 0..rows.len() {
            if index > 0 && non_space(&mut reader)? != b',' {
                return Err("冻结标签不足65个或数组损坏".into());
            }
            let first = non_space(&mut reader)?;
            labels.push(label_object(&mut reader, first)?);
        }
        for (row, raw) in rows.iter().zip(&labels) {
            let label: Value = serde_json::from_str(raw.get())?;
            for key in ["processing_start_utc_ms", "k_exact"] {
                if row.get(key).is_none() || label.get(key).is_none() || row[key] != label[key] {
                    return Err(format!("冻结标签与原frame的{key}不同").into());
                }
            }
            for key in ["old_value", "old_joint_logp"] {
                if row[key].as_f64().map(f64::to_bits) != label[key].as_f64().map(f64::to_bits)
                    || row[key].as_f64().is_none()
                {
                    return Err(format!("冻结标签与原frame的{key}位值不同").into());
                }
            }
        }
        let mut report = reader.get_ref().report();
        report["labels"] = labels.len().into();
        Ok((labels, report))
    })();
    outcome.map_err(|error: MaterialError| {
        format!("{error}; 实际标签前缀读取={}", reader.get_ref().report()).into()
    })
}
#[derive(Serialize)]
struct SampleRecord<'a> {
    ordinal: usize,
    expected_call: usize,
    row: &'a Value,
    actual_public_group: Value,
    frame: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<&'a RawValue>,
}
/// RawValue keeps original number/string lexemes. Remove only insignificant JSON
/// whitespace outside strings so a complete record always has one physical line.
fn compact_label(raw: &RawValue) -> Result<Box<RawValue>, MaterialError> {
    let mut bytes = Vec::with_capacity(raw.get().len());
    let mut quoted = false;
    let mut escaped = false;
    for byte in raw.get().bytes() {
        if quoted {
            bytes.push(byte);
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else if byte == b'"' {
            quoted = true;
            bytes.push(byte);
        } else if !matches!(byte, b' ' | b'\t' | b'\n' | b'\r') {
            bytes.push(byte);
        }
    }
    // Safe validation, without parsing numbers into Value/f64 or unchecked raw JSON.
    Ok(RawValue::from_string(String::from_utf8(bytes)?)?)
}
fn write_sample_record(
    writer: &mut impl Write,
    record: SampleRecord<'_>,
) -> Result<(), MaterialError> {
    let label = record.label.map(compact_label).transpose()?;
    let compact = SampleRecord {
        label: label.as_deref(),
        ..record
    };
    serde_json::to_writer(&mut *writer, &compact)?;
    writer.write_all(b"\n")?;
    Ok(())
}
fn source_samples(
    source: &PrefixSource,
    name: &str,
    output: &Path,
    budget: &Budget<'_>,
) -> Result<Value, MaterialError> {
    budget.check()?;
    let bytes = fs::read(&source.consumer_config)?;
    let raw: Value = serde_json::from_slice(&bytes)?;
    let mut consumer: ConsumerRunConfig =
        serde_json::from_value(raw.get("consumer").unwrap_or(&raw).clone())?;
    let PrefixRows {
        binding,
        rows,
        report: trajectory_read,
    } = trajectory_prefix(source, CALLS, || budget.check())?;
    let (labels, labels_read) = if source.labels.is_some() {
        let (values, report) = label_prefix(source, &rows)?;
        (Some(values), report)
    } else {
        (None, Value::Null)
    };
    consumer.rules = crate::ConsumerRulesV1::explicit(crate::RuleOpenV1::Recovery {
        binding: binding.clone(),
    });
    crate::worker::configure_runtime(&consumer.worker)?;
    let opened = crate::open_consumer_rules(&consumer)?;
    let mut day = consumer.days.first().ok_or("固定样本缺原日配置")?.clone();
    let candidate = day
        .candidates
        .iter()
        .find(|candidate| {
            candidate.strategy.effective_parameters["model_consumer"]["account_key"]
                == source.account
        })
        .ok_or("固定样本缺原账户候选")?
        .clone();
    let account: crate::ConsumerConfig =
        serde_json::from_value(candidate.strategy.effective_parameters["model_consumer"].clone())?;
    day.candidates = vec![candidate];
    let times = rows
        .iter()
        .map(|row| {
            row["market_frame"]["market_cutoff"]["decision_time"]["logical_time_utc_ms"]
                .as_i64()
                .ok_or("原frame缺市场冻结截止")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let queries = [(source.account.clone(), times.iter().copied().collect())]
        .into_iter()
        .collect();
    let done = AtomicBool::new(false);
    let progress = Mutex::new(ContinuousPublicExportProgressV1::default());
    let mut frames = (0..rows.len()).map(|_| None).collect::<Vec<_>>();
    crate::learning::visit_day_frames_controlled(
        &binding.public_rules(),
        Some(opened.rules),
        &day,
        &queries,
        |_, at, group, public| {
            budget.check()?;
            for (index, row) in rows
                .iter()
                .enumerate()
                .filter(|(index, _)| times[*index] == at)
            {
                if row["market_frame"]["market_cutoff"]["receipt"]["group"]
                    != serde_json::to_value(group)?
                {
                    return Err("有限公共重建与原frame可信group不同".into());
                }
                let (mut frame, _) = crate::learning::compose_trajectory_frame(
                    row,
                    public,
                    &consumer.worker.encoding,
                    false,
                )?;
                let mut domains = Vec::new();
                for id in row["market_ids"].as_array().ok_or("原frame缺市场顺序")? {
                    let market = public
                        .iter()
                        .find(|market| {
                            serde_json::to_value(&market.observation.market.condition_id)
                                .ok()
                                .as_ref()
                                == Some(id)
                        })
                        .ok_or("有限公共重建缺原市场")?;
                    let (ticks, max_prices) = crate::domain::market_limit_bounds(market)?;
                    domains.push(json!({"ticks":ticks,"max_prices":max_prices}));
                }
                frame["seed"] = account.sampling_seed.into();
                frame["phase"] = "frame".into();
                frame["domain_context"] =
                    json!({"cash":row["global_private"]["cash_available"],"markets":domains});
                frame["native_rules"] = json!({"snapshot_path":binding.snapshot_path,"expected_file_sha256":binding.expected_file_sha256,"implementation_identity":binding.implementation_identity});
                frames[index] = Some((frame, serde_json::to_value(group)?));
            }
            if frames.iter().all(Option::is_some) {
                done.store(true, Ordering::Relaxed);
            }
            Ok(())
        },
        || budget.stopped() || done.load(Ordering::Relaxed),
        |state| {
            *progress.lock().unwrap() = state.clone();
        },
    )?;
    budget.check()?;
    if !done.load(Ordering::Relaxed) {
        return Err("有限公共前缀未完成，不生成可用样本".into());
    }
    let path = output.join(format!("{name}.jsonl"));
    let mut file = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?,
    );
    for (index, (row, frame)) in rows.iter().zip(frames).enumerate() {
        budget.check()?;
        let (frame, actual_public_group) = frame.ok_or("有限公共前缀缺frame")?;
        let record = SampleRecord {
            ordinal: index,
            expected_call: index + 1,
            row,
            actual_public_group,
            frame,
            label: labels.as_ref().map(|labels| labels[index].as_ref()),
        };
        write_sample_record(&mut file, record)?;
    }
    file.flush()?;
    Ok(
        json!({"sample":name,"source":source,"config_sha256":format!("{:x}",Sha256::digest(&bytes)),"trajectory_prefix":trajectory_read,"labels_prefix":labels_read,"first_call":rows.first().unwrap()["call"],"last_call":rows.last().unwrap()["call"],"declared_scope":day.preparation.scope,"metadata_initialization":day.preparation.metadata_initialization,"public_prefix":progress.into_inner().unwrap(),"output_file":path,"records":rows.len(),"elapsed_seconds":budget.started.elapsed().as_secs_f64()}),
    )
}
/// started由CLI在读取任务配置前建立；外层监督仍承担原硬截止。
pub fn prepare_prefix_samples(
    config: &PrefixSampleConfig,
    started: Instant,
) -> Result<Value, MaterialError> {
    if config.max_wall_seconds == 0 || config.max_wall_seconds > 360 {
        return Err("#377材料准备只能采用已冻结的360秒内额度".into());
    }
    if config.learner.labels.is_none() {
        return Err("L65必须绑定原完整窗冻结标签".into());
    }
    let budget = Budget { config, started };
    budget.check()?;
    fs::create_dir(&config.output_directory)?;
    let mut report = json!({"version":"b-performance-prefix-v1","consumer":null,"learner":null,"elapsed_seconds":0.0});
    let outcome = (|| {
        report["consumer"] =
            source_samples(&config.consumer, "C65", &config.output_directory, &budget)?;
        report["learner"] =
            source_samples(&config.learner, "L65", &config.output_directory, &budget)?;
        if let Some(fixture) = &config.f4 {
            budget.check()?;
            let rules = sim_system::platform_rules::recover_process_snapshot_v3(
                sim_system::platform_rules::RecoveryProcessSnapshotRequestV1 {
                    snapshot_path: fixture.rules.snapshot_path.clone(),
                    expected_file_sha256: fixture.rules.expected_file_sha256.clone(),
                    expected_implementation_identity: fixture.rules.implementation_identity.clone(),
                },
            )?;
            let policy = std::sync::Arc::new(
                sim_system::finality::TradeFinalityPolicyV2::decode_and_seal(&fs::read(
                    &fixture.finality_policy_path,
                )?)?,
            );
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let values = runtime.block_on(crate::performance_fixture::run_f4_fixture(
                &config.output_directory.join("F4-construction"),
                rules,
                policy,
                fixture.implementation_identity.clone(),
                &fixture.encoding,
                fixture.sampling_seed,
            ))?;
            let mut file = BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(config.output_directory.join("F4.jsonl"))?,
            );
            for value in &values {
                serde_json::to_writer(&mut file, value)?;
                file.write_all(b"\n")?;
            }
            file.flush()?;
            report["f4"] = json!({"records":values.len(),"constants":crate::performance_fixture::fixture_constants(),"model_forwards":0});
            budget.check()?;
        }
        Ok::<(), MaterialError>(())
    })();
    report["elapsed_seconds"] = started.elapsed().as_secs_f64().into();
    report["complete"] = outcome.is_ok().into();
    if let Err(error) = &outcome {
        report["error"] = error.to_string().into();
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(config.output_directory.join("prefix-report.json"))?;
    serde_json::to_writer_pretty(&mut file, &report)?;
    file.flush()?;
    outcome?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn source(root: &Path) -> PrefixSource {
        PrefixSource {
            consumer_config: root.join("unused"),
            trajectory: root.join("trajectory.jsonl"),
            labels: Some(root.join("labels.json")),
            account: "a".into(),
            group: "g".into(),
            trajectory_version: "fixed-test".into(),
        }
    }
    fn binding() -> FrozenRuleBindingV1 {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
        let bytes = fs::read(&path).unwrap();
        let rules = sim_system::platform_rules::load_historical_process_snapshot_v3(
            sim_system::platform_rules::HistoricalProcessSnapshotRequestV1 {
                snapshot_path: path,
                expected_file_sha256: contracts::Sha256Hex(format!("{:x}", Sha256::digest(bytes))),
                replay_causal_start_utc_ms: i64::MAX,
            },
        )
        .unwrap();
        FrozenRuleBindingV1::from_sealed(&rules, rules.access_mode())
    }
    #[test]
    fn finite_rows_count_frames_instead_of_physical_lines_and_stop_before_tail() {
        let root = tempfile::tempdir().unwrap();
        let source = source(root.path());
        let rules = binding();
        let mut output = File::create(&source.trajectory).unwrap();
        writeln!(
            output,
            "{}",
            json!({"record_type":"rules_binding","binding":rules})
        )
        .unwrap();
        for call in 1..=2 {
            writeln!(
                output,
                "{}",
                json!({"record_type":"confirmed_economic_update","revision":call})
            )
            .unwrap();
            writeln!(output,"{}",json!({"account":"a","group":"g","version":"fixed-test","call":call,"rules_identity":rules.expected_file_sha256,"processing_start_utc_ms":call,"old_value":0.5,"old_joint_logp":-0.25,"k_exact":{"numerator":"0","denominator":"1"}})).unwrap();
        }
        let frame_prefix_bytes = output.metadata().unwrap().len();
        output
            .write_all(b"unparsed tail is not claimed valid\n")
            .unwrap();
        drop(output);
        let prefix = trajectory_prefix(&source, 2, || Ok(())).unwrap();
        assert_eq!(prefix.rows.len(), 2);
        assert_eq!(prefix.rows[1]["call"], 2);
        assert_eq!(prefix.report["records_parsed"], 5);
        assert_eq!(
            prefix.report["bytes_read_from_file_including_buffer_read_ahead"].as_u64(),
            Some(frame_prefix_bytes),
            "不能预读第65帧后的轨迹尾部"
        );
        assert_eq!(prefix.report["full_file_verified"], false);
        fs::write(source.labels.as_ref().unwrap(),br#"{"a":[{"processing_start_utc_ms":1,"old_value":0.5,"old_joint_logp":-0.25,"k_exact":{"numerator":"0","denominator":"1"},"advantage":1.2300e-7,"special":{"keep":"raw"}},{"processing_start_utc_ms":2,"old_value":0.5,"old_joint_logp":-0.25,"k_exact":{"numerator":"0","denominator":"1"},"advantage":-1},unparsed tail"#).unwrap();
        let (labels, report) = label_prefix(&source, &prefix.rows).unwrap();
        assert_eq!(labels.len(), 2);
        assert!(labels[0].get().contains("1.2300e-7"));
        assert!(labels[0].get().contains("\"keep\":\"raw\""));
        assert_eq!(report["full_file_verified"], false);
        let raw = fs::read(source.labels.as_ref().unwrap()).unwrap();
        let tail = b",unparsed tail";
        let prefix_bytes = raw
            .windows(tail.len())
            .position(|part| part == tail)
            .unwrap();
        assert_eq!(
            report["bytes_read_from_file_including_buffer_read_ahead"].as_u64(),
            Some(prefix_bytes as u64),
            "不能预读第65个label后的余数组"
        );
    }
    #[test]
    fn malformed_label_or_oversized_object_is_not_silently_projected() {
        let mut wrong = io::Cursor::new(br#""x":[}"#.to_vec());
        assert!(label_object(&mut wrong, b'{').is_err());
        let bytes = [vec![b' '; SINGLE_LABEL_BYTES], vec![b'}']].concat();
        assert!(label_object(&mut io::Cursor::new(bytes), b'{').is_err());
    }
    #[test]
    #[ignore = "requires the explicitly bound external short-config builder; no material preparation"]
    fn raw_labels_use_one_line_through_real_writer_builder_and_slot_reader() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("synthetic-label-records.jsonl");
        let original = RawValue::from_string(
            r#"{
            "advantage": -0.0000,
            "old_joint_logp": 1.2300e-7,
            "long": 1.234567890123456789012345678901,
            "text": " spaces \t \n \"quote\" \\ 终",
            "special": { "j_end_exact": "123456789012345678901" }
        }"#
            .into(),
        )
        .unwrap();
        let expected = r#"{"advantage":-0.0000,"old_joint_logp":1.2300e-7,"long":1.234567890123456789012345678901,"text":" spaces \t \n \"quote\" \\ 终","special":{"j_end_exact":"123456789012345678901"}}"#;
        let row = json!({"account":"synthetic-label-only"});
        let mut file = File::create(&path).unwrap();
        for ordinal in 0..2 {
            write_sample_record(
                &mut file,
                SampleRecord {
                    ordinal,
                    expected_call: ordinal + 1,
                    row: &row,
                    actual_public_group: json!({}),
                    frame: json!({"call":ordinal+1}),
                    label: Some(&original),
                },
            )
            .unwrap();
        }
        drop(file);
        let bytes = fs::read(&path).unwrap();
        assert_eq!(bytes.iter().filter(|b| **b == b'\n').count(), 2);
        let binding = crate::performance_slot::BoundFile {
            path: path.clone(),
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            max_bytes: bytes.len(),
        };
        let builder = std::env::var("B_PERFORMANCE_BUILDER_SOURCE")
            .expect("explicit bound external builder source required");
        let builder_sha = std::env::var("B_PERFORMANCE_BUILDER_SHA256")
            .expect("explicit bound external builder SHA required");
        let output = std::process::Command::new("python3").args([
            "-c",
            r#"import hashlib, importlib.util, json, struct, sys
from pathlib import Path
p = Path(sys.argv[1])
assert hashlib.sha256(p.read_bytes()).hexdigest() == sys.argv[2]
spec = importlib.util.spec_from_file_location('bound_short_config_builder', p)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
f = Path(sys.argv[3])
r = m.read_first_sample({'path': str(f), 'max_bytes': f.stat().st_size})
assert r['ordinal'] == 0 and r['expected_call'] == 1
print(json.dumps({k: struct.pack('>d', r['label'][k]).hex() for k in ('advantage','old_joint_logp','long')}))"#,
            &builder, &builder_sha, path.to_str().unwrap(),
        ]).output().unwrap();
        assert!(
            output.status.success(),
            "builder helper: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let python_bits: Value = serde_json::from_slice(&output.stdout).unwrap();
        let parsed_original: Value = serde_json::from_str(original.get()).unwrap();
        let records = binding.lines().unwrap();
        assert_eq!(records.len(), 2);
        #[derive(Deserialize)]
        struct RawLabelRecord {
            label: Box<RawValue>,
        }
        for (index, line) in bytes
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .enumerate()
        {
            let raw: RawLabelRecord = serde_json::from_slice(line).unwrap();
            assert_eq!(
                raw.label.get(),
                expected,
                "all label lexemes and alpha1 fields retained"
            );
            assert_eq!(records[index]["ordinal"], index);
            for key in ["advantage", "old_joint_logp", "long"] {
                let bits = records[index]["label"][key].as_f64().unwrap().to_bits();
                assert_eq!(bits, parsed_original[key].as_f64().unwrap().to_bits());
                assert_eq!(python_bits[key], format!("{bits:016x}"));
            }
        }
    }
}
