//! 从已有公共录制顺序消费完整原生输出，供有限真实连续验证和投影成本测量。
//! 模拟账户无交易；停止点是显式测量范围，不是生产丢帧或采样策略。
use contracts::{execution_normalization::*, *};
use data_pipeline_core::public_input::*;
use data_pipeline_training::{canonical_run::CanonicalRunConfig, public_recording::*};
use ledger::{AccountAdmissionPolicyV2, LedgerWriterV1};
use model_native_inputs::{HiddenOp, InputRole, NativeInputRoot};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shared_run::SharedExecutionV2;
use sim_system::{
    execution::*, finality::TradeFinalityPolicyV2,
    latency_profile::load_embedded_proxy_latency_profile, platform_rules::*, public_consumption::*,
    scenario::*,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

// 仅显式诊断启用：从同批精确观测重建生产查键序列，不持有或影响生产缓存。
// 诊断开销在 consume 计时之外；它仍会影响机器状态，不能作为正式性能轮。
#[derive(Default, Serialize)]
struct ProjectionInputs {
    requests: u64,
    previous_frame_hits: u64,
    peak_keys: usize,
    peak_key_value_payload_bytes: usize,
    #[serde(skip)]
    previous: BTreeMap<(String, usize), BTreeSet<contracts::economics::EconomicRational>>,
}
impl ProjectionInputs {
    fn observe(&mut self, observation: &MarketBusinessObservationV1) {
        use contracts::economics::EconomicRational as R;
        let decimal = |v: &ExactDecimal| {
            R::from_decimal(rust_decimal::Decimal::from_str_exact(&v.0).unwrap())
        };
        let visible =
            |quality| matches!(quality, MarketDataQuality::Fresh | MarketDataQuality::Stale);
        let mut sides: [Vec<(R, R)>; 6] = std::array::from_fn(|_| Vec::new());
        for asset in &observation.assets {
            let index = asset.asset.outcome.index as usize;
            if index < 2 {
                for (side, fact) in [&asset.bids, &asset.asks].into_iter().enumerate() {
                    if visible(fact.quality)
                        && let Some(levels) = &fact.value
                    {
                        sides[index * 2 + side] = levels
                            .iter()
                            .map(|v| {
                                (
                                    decimal(&v.price_pusd_per_share),
                                    decimal(&v.quantity_shares),
                                )
                            })
                            .collect();
                    }
                }
            }
        }
        if visible(observation.binance_depth.quality)
            && let Some(depth) = &observation.binance_depth.value
        {
            for (side, levels) in [&depth.bids, &depth.asks].into_iter().enumerate() {
                sides[4 + side] = levels
                    .iter()
                    .map(|v| (decimal(&v.price_usdt_per_btc), decimal(&v.quantity_btc)))
                    .collect();
            }
        }
        for (side, levels) in sides.into_iter().enumerate() {
            let levels: Vec<_> = levels.into_iter().filter(|(_, q)| q > &R::zero()).collect();
            let key = (observation.market.condition_id.0.clone(), side);
            let old = self.previous.entry(key).or_default();
            let mut next = BTreeSet::new();
            for (p, q) in &levels {
                let distance = if side % 2 == 0 {
                    levels[0].0.sub(p)
                } else {
                    p.sub(&levels[0].0)
                };
                let inputs = if side < 4 {
                    vec![q.clone(), distance]
                } else {
                    vec![p.clone(), q.clone(), distance]
                };
                for input in inputs {
                    self.requests += 1;
                    self.previous_frame_hits += u64::from(old.contains(&input));
                    next.insert(input);
                }
            }
            *old = next;
        }
        let keys = self.previous.values().map(BTreeSet::len).sum();
        let bytes = self
            .previous
            .values()
            .flatten()
            .map(|v| {
                // 不包含 BTree 节点、分配器容量或临时双表；不是堆峰值。
                std::mem::size_of::<R>()
                    + std::mem::size_of::<f32>()
                    + v.numerator().magnitude().to_u64_digits().len() * 8
                    + v.denominator().magnitude().to_u64_digits().len() * 8
            })
            .sum();
        self.peak_keys = self.peak_keys.max(keys);
        self.peak_key_value_payload_bytes = self.peak_key_value_payload_bytes.max(bytes);
    }
}
#[derive(Deserialize)]
struct Config {
    received_start: i64,
    condition: String,
    root_started_at: i64,
    canonical: CanonicalRunConfig,
    header: PublicInputStreamHeaderV1,
    rule_snapshot: PathBuf,
    rule_snapshot_sha256: Sha256Hex,
    finality_path: PathBuf,
    implementation_identity: ImplementationIdentityV1,
    initial_cash: String,
    max_group_bytes: usize,
    finite_in_flight_groups: usize,
}
fn json(path: impl AsRef<Path>, value: &impl Serialize) -> RecordingResult<()> {
    serde_json::to_writer_pretty(File::create(path)?, value)?;
    Ok(())
}
fn line(w: &mut impl Write, value: &impl Serialize) -> RecordingResult<()> {
    serde_json::to_writer(&mut *w, value)?;
    w.write_all(b"\n")?;
    Ok(())
}
fn sha(path: &Path) -> RecordingResult<Sha256Hex> {
    let mut f = File::open(path)?;
    let mut h = Sha256::new();
    std::io::copy(&mut f, &mut h)?;
    Ok(Sha256Hex(format!("{:x}", h.finalize())))
}
// 本次离线宿主单调交付：max(前组交付时刻,关闭输入原received)。原source时间/次序完全保留。
struct ReceivedTimes {
    reader: PublicRecordingReaderV1,
    current: Option<(u64, i64)>,
    last: i64,
    audit: std::rc::Rc<std::cell::RefCell<BufWriter<File>>>,
}
impl PublicConsumptionTimeSourceV1 for ReceivedTimes {
    fn execution_time_for_group(
        &mut self,
        group: &PublicGroupReferenceV1,
    ) -> Result<i64, SimRunActorErrorV2> {
        loop {
            if let Some((position, at)) = self.current
                && position == group.closed_at_input_position
            {
                // 离线测量宿主的显式交付规则；不冒称恢复了原生产策略时钟。
                let delivered = self.last.max(at);
                line(&mut *self.audit.borrow_mut(), &serde_json::json!({"group_position":group.group_position,"closed_at_input_position":position,"source_closing_received_at_utc_ms":at,"assigned_delivery_at_utc_ms":delivered,"delay_ms":delivered-at}))
                        .map_err(|e| SimRunActorErrorV2::PublicConsumptionSource(e.to_string()))?;
                self.last = delivered;
                return Ok(delivered);
            }
            let row = self
                .reader
                .next_row()
                .map_err(|e| SimRunActorErrorV2::PublicConsumptionSource(e.to_string()))?
                .ok_or_else(|| {
                    SimRunActorErrorV2::PublicConsumptionSource("组关闭输入未找到".into())
                })?;
            if let PublicStoredRowV1::Input(input) = row {
                match input.ingress {
                    DptIngressV1::Message {
                        source_evidence, ..
                    } => {
                        self.current =
                            Some((input.input_position, source_evidence.received_at_utc_ms))
                    }
                    DptIngressV1::EndOfInput => {
                        if let Some((_, at)) = self.current {
                            self.current = Some((input.input_position, at));
                        }
                    }
                    DptIngressV1::Aborted { .. } => {
                        return Err(SimRunActorErrorV2::PublicConsumptionSource(
                            "输入已中止".into(),
                        ));
                    }
                }
            }
        }
    }
}
fn retry<T, R>(
    state: &mut T,
    mut op: impl FnMut(&mut T) -> Result<R, SimRunActorErrorV2>,
    mut wait: impl FnMut(&mut T) -> Result<(), SimRunActorErrorV2>,
) -> Result<R, SimRunActorErrorV2> {
    loop {
        match op(state) {
            Err(SimRunActorErrorV2::RecordingBackpressured) => wait(state)?,
            result => return result,
        }
    }
}
fn trace(
    cfg: &Config,
    root: &Path,
    out: &Path,
    stop_role: &str,
    limit: u64,
) -> RecordingResult<()> {
    fs::create_dir(out)?;
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: cfg.rule_snapshot.clone(),
        expected_file_sha256: cfg.rule_snapshot_sha256.clone(),
        replay_causal_start_utc_ms: cfg.received_start,
    })?;
    let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&fs::read(
        &cfg.finality_path,
    )?)?);
    let scenario = Arc::new(SealedPlatformScenarioV1::decode_and_seal(&fs::read(
        root.join("scenario.json"),
    )?)?);
    let latency = load_embedded_proxy_latency_profile()?;
    let run_id = cfg.header.input_stream_id.clone();
    let opened = RunOpenedV2 {
        projection_version: BusinessProjectionVersionV1::V4,
        run_id: run_id.clone(),
        durability_role: RunDurabilityRoleV2::CleanRun,
        platform_rule_snapshot_file_sha256: rules.file_sha256().clone(),
        trade_finality_policy_file_sha256: finality.policy_id().clone(),
        platform_scenario_file_sha256: scenario.file_sha256().clone(),
        proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
        implementation_identity: cfg.implementation_identity.clone(),
        market_event_source_identity_sha256: Some(sha(&root.join("record-completion.json"))?),
    };
    let log = out.join("sim.jsonl");
    let reference = |position| DurableSourceReferenceV1 {
        source: ImmutableSourceReferenceV1::Location(format!("{}#input={position}", log.display())),
    };
    let port = MemoryNoeProjectionPortV4::new(
        out.join("noe"),
        opened.clone(),
        reference(0),
        reference(1),
        "create".into(),
        "realmarket-state-benchmark-no-trading".into(),
    );
    let observation = port.observation();
    let mut execution = PublicSimExecutionV1::create_new(
        &log,
        PublicRunOpenedV1 {
            projection_version: opened.projection_version,
            run_id: opened.run_id,
            platform_rule_snapshot_file_sha256: opened.platform_rule_snapshot_file_sha256,
            trade_finality_policy_file_sha256: opened.trade_finality_policy_file_sha256,
            platform_scenario_file_sha256: opened.platform_scenario_file_sha256,
            proxy_latency_profile_file_sha256: opened.proxy_latency_profile_file_sha256,
            implementation_identity: opened.implementation_identity,
            market_event_source_identity_sha256: opened.market_event_source_identity_sha256,
        },
        rules.clone(),
        finality,
        scenario,
        latency,
        Box::new(port),
        cfg.max_group_bytes,
        cfg.finite_in_flight_groups,
    )?;
    let session = SessionSpecV3 {
        run_id: run_id.clone(),
        session_id: "benchmark-session".into(),
        account_id: "benchmark-sim-account".into(),
        caller_role: SimCallerRoleV1::Paper,
        caller_instance_id: "benchmark-caller".into(),
        creation_request_id: "create".into(),
        response_bridge_binding_id: "benchmark-bridge".into(),
        initial_account_state: InitialAccountStateV2 {
            available_pusd: ExactDecimal(cfg.initial_cash.clone()),
        },
        session_seed: Sha256Hex(format!("{:x}", Sha256::digest(run_id.as_bytes()))),
    };
    retry(
        &mut execution,
        |e| e.create_session(session.clone()),
        PublicSimExecutionV1::wait_for_recording_capacity,
    )?;
    let mut ledger =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    execution.bind_ledger_recording_v2(&mut ledger, &out.join("ledger"))?;
    let mut shared = SharedExecutionV2::from_opening(ledger, observation)?.ok_or("缺少开户事实")?;
    retry(
        &mut execution,
        |e| e.activate_session("activate".into()),
        PublicSimExecutionV1::wait_for_recording_capacity,
    )?;
    let account = shared
        .ledger()
        .projection_v2()
        .ok_or("无账户投影")?
        .account_instance_key;
    let mut native = NativeInputRoot::new(run_id, account, cfg.root_started_at, rules);
    let rec = root.join("recording");
    let time_audit = std::rc::Rc::new(std::cell::RefCell::new(BufWriter::new(File::create(
        out.join("host-time-assignment.jsonl"),
    )?)));
    let times = ReceivedTimes {
        reader: PublicRecordingReaderV1::recover(
            &rec,
            cfg.canonical.decode_batch_rows,
            cfg.canonical.max_decode_batch_bytes,
        )?,
        current: None,
        last: i64::MIN,
        audit: time_audit.clone(),
    };
    let source = RecordedPublicGroupSourceV1::open(
        &rec,
        cfg.canonical.decode_batch_rows,
        cfg.canonical.max_decode_batch_bytes,
        times,
    )?;
    let mut driver = FinitePublicConsumptionV1::new(
        execution,
        source,
        "offline-host-monotone-closing-received-high-watermark-v1".into(),
    );
    let mut writer = BufWriter::new(File::create(out.join("groups.jsonl"))?);
    let mut meta = BufWriter::new(File::create(out.join("rows.jsonl"))?);
    let (mut groups, mut context, mut decisions, mut all_context, mut all_decisions) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut selected_consume_ns = Vec::new();
    let mut projection_inputs =
        std::env::var_os("PROJECTION_CACHE_DIAGNOSTICS").map(|_| ProjectionInputs::default());
    let start = Instant::now();
    loop {
        let next = retry(
            &mut driver,
            FinitePublicConsumptionV1::next_group,
            FinitePublicConsumptionV1::wait_for_recording_capacity,
        )?;
        let Some((_, cutoff)) = next else {
            break;
        };
        if cutoff.decision_time().logical_time_utc_ms() < cfg.root_started_at {
            // 先恢复公开依赖与共享读取；原生根在真实声明时刻开始，不提前参与模型资格。
            shared.action_before(&mut driver)?;
            continue;
        }
        let consume_start = Instant::now();
        let group = native
            .consume(&mut shared, &mut driver)
            .map_err(|e| format!("NativeInputRoot::consume: {e:?}"))?;
        let native_consume_ns = consume_start.elapsed().as_nanos();
        if let Some(group) = group {
            if let Some(diagnostic) = &mut projection_inputs {
                for operation in &group.pre_frame_hidden_ops {
                    if matches!(operation, HiddenOp::ResetRootHidden { .. }) {
                        diagnostic.previous.clear();
                    }
                }
                let decision = driver.decision().ok_or("缺少诊断公共帧")?;
                for row in &group.rows {
                    let frames = decision.frames();
                    let frame = frames
                        .iter()
                        .find(|frame| {
                            frame.observation.market.condition_id
                                == row.hidden_state_key.condition_id
                        })
                        .ok_or("缺少诊断行对应观测")?;
                    diagnostic.observe(frame.observation);
                }
                for operation in &group.post_frame_hidden_ops {
                    if let HiddenOp::Retire { hidden_state_key } = operation {
                        diagnostic.previous.retain(|(condition, _), _| {
                            condition != &hidden_state_key.condition_id.0
                        });
                    }
                }
            }
            if group
                .rows
                .iter()
                .any(|r| r.hidden_state_key.condition_id.0 == cfg.condition)
            {
                selected_consume_ns.push(native_consume_ns);
            }
            for (row_index, row) in group.rows.iter().enumerate() {
                match row.role {
                    InputRole::ContextOnly => all_context += 1,
                    InputRole::DecisionRequired => all_decisions += 1,
                }
                if row.hidden_state_key.condition_id.0 == cfg.condition {
                    match row.role {
                        InputRole::ContextOnly => context += 1,
                        InputRole::DecisionRequired => decisions += 1,
                    }
                }
                let depths: Vec<_> = row.book_levels.iter().map(|side| side.len()).collect();
                line(
                    &mut meta,
                    &serde_json::json!({"native_group":groups,"row_index":row_index,"condition_id":row.hidden_state_key.condition_id,"role":row.role,"cutoff":group.cutoff,"depths":depths,"fast_len":row.fast.len(),"slow_len":row.slow.len()}),
                )?;
            }
            if !group.rows.is_empty()
                || !group.pre_frame_hidden_ops.is_empty()
                || !group.post_frame_hidden_ops.is_empty()
            {
                line(
                    &mut writer,
                    &serde_json::json!({"decision_time_utc_ms":group.cutoff.decision_time().logical_time_utc_ms(),"native_consume_ns":native_consume_ns,"group":group}),
                )?;
            }
            groups += 1;

            if (if stop_role == "contexts" {
                context
            } else {
                decisions
            }) >= limit
            {
                break;
            }
        }
    }
    writer.flush()?;
    meta.flush()?;
    time_audit.borrow_mut().flush()?;
    driver.stop_and_drain()?;
    json(
        out.join("completion.json"),
        &serde_json::json!({
            "complete": true, "bounded_only": true, "stop_role": stop_role, "limit": limit,
            "native_groups": groups, "target_context_rows": context, "target_decision_rows": decisions,
            "all_context_rows": all_context, "all_decision_rows": all_decisions,
            "target_consume_samples_ns": selected_consume_ns, "elapsed_seconds": start.elapsed().as_secs_f64(),
            "groups_sha256": sha(&out.join("groups.jsonl"))?, "groups_bytes": fs::metadata(out.join("groups.jsonl"))?.len(),
            "rayon_threads": rayon::current_num_threads(), "input_contract_version": "native_inputs_v7", "account_economics_version": shared_run::ACCOUNT_ECONOMICS_VERSION,
            "projection_cache_diagnostic": projection_inputs,
            "process_peak_rss": fs::read_to_string("/proc/self/status")?.lines().find(|line| line.starts_with("VmHWM:")).map(str::to_owned),
            "scope": "从录制起点完整恢复公共事实、按原根开始时间消费历史及全部原生输出；显式有界、无交易账户，不是五分钟完整回放或全交易链计时"
        }),
    )?;
    Ok(())
}
fn main() -> RecordingResult<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 6 {
        return Err("参数：配置JSON 公共录制根 输出目录 contexts|decisions 行数".into());
    }
    let cfg = serde_json::from_reader(File::open(&args[1])?)?;
    trace(
        &cfg,
        Path::new(&args[2]),
        Path::new(&args[3]),
        &args[4],
        args[5].parse()?,
    )
}
