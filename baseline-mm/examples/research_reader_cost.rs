//! 既有公共材料的三层入口成本测量；不准备材料、不复制归约或业务选择。
use contracts::*;
use data_pipeline_core::{public_input::*, typed_raw::TypedRawRow};
use data_pipeline_training::{public_recording::*, public_replay::PublicReplayV1};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sim_system::public_consumption::{PublicConsumptionTimeSourceV1, RecordedPublicGroupSourceV1};
use std::{collections::BTreeMap, error::Error, fs, io::Write, path::PathBuf, time::Instant};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const RETAIN: usize = 16;
#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Record,
    Replay,
    Group,
}
#[derive(Deserialize, Serialize)]
struct Config {
    mode: Mode,
    public_root: PathBuf,
    read_page_rows: usize,
    max_read_batch_bytes: usize,
    expected_normal_prefix: CommittedIngressPrefixV1,
    purpose: String,
}
#[derive(Default)]
struct OriginalVisibility(Option<i64>);
impl PublicConsumptionTimeSourceV1 for OriginalVisibility {
    fn execution_time_for_group(
        &mut self,
        _: &PublicGroupReferenceV1,
    ) -> std::result::Result<i64, sim_system::execution::SimRunActorErrorV2> {
        Err(
            sim_system::execution::SimRunActorErrorV2::PublicConsumptionSource(
                "需要原来源可见时刻".into(),
            ),
        )
    }
    fn execution_time_for_visible_group(
        &mut self,
        _: &PublicGroupReferenceV1,
        visible: i64,
    ) -> std::result::Result<i64, sim_system::execution::SimRunActorErrorV2> {
        let at = self.0.map_or(visible, |last| last.max(visible));
        self.0 = Some(at);
        Ok(at)
    }
}
fn kind(i: &RecordedIngressV1) -> &'static str {
    match &i.ingress {
        DptIngressV1::EndOfInput => "end_of_input",
        DptIngressV1::Aborted { .. } => "aborted",
        DptIngressV1::Message { row, .. } => match row {
            TypedRawRowV2::Existing(row) => match row {
                TypedRawRow::PolymarketOrderbook(_) => "polymarket_orderbook",
                TypedRawRow::PolymarketMarketMetadata(_) => "metadata_existing",
                TypedRawRow::PolymarketRtdsChainlinkBtcUsd(_) => "chainlink_spot",
                TypedRawRow::BinanceDepthSnapshot(_) => "binance_depth_snapshot",
                TypedRawRow::BinanceDepthUpdate(_) => "binance_depth_update",
                TypedRawRow::BinanceAggTrade(_) => "binance_agg_trade",
            },
            TypedRawRowV2::PolymarketMarketMetadataV6(m) => match m.as_ref() {
                PolymarketMarketMetadataV6Input::Existing(_) => "metadata_v6_existing",
                PolymarketMarketMetadataV6Input::AnchorRule(_) => "anchor_rule",
            },
            TypedRawRowV2::PolymarketRtdsChainlinkTwap(_) => "chainlink_twap",
            TypedRawRowV2::HistoricalPriceAnchorRepairBatch(_) => "historical_anchor_repair",
        },
    }
}
#[derive(Default, Serialize)]
struct Positions {
    count: u64,
    first: Option<u64>,
    last: Option<u64>,
    min: Option<u64>,
    max: Option<u64>,
    non_increasing: u64,
}
impl Positions {
    fn add(&mut self, p: u64) {
        self.count += 1;
        self.first.get_or_insert(p);
        if self.last.is_some_and(|last| p <= last) {
            self.non_increasing += 1;
        }
        self.last = Some(p);
        self.min = Some(self.min.map_or(p, |v| v.min(p)));
        self.max = Some(self.max.map_or(p, |v| v.max(p)));
    }
}
#[derive(Default, Serialize)]
struct Timing {
    calls: u64,
    sum_ns: u128,
    max_ns: u128,
    duration_log2_ns: BTreeMap<u32, u64>,
}
impl Timing {
    fn add(&mut self, ns: u128) {
        self.calls += 1;
        self.sum_ns += ns;
        self.max_ns = self.max_ns.max(ns);
        *self
            .duration_log2_ns
            .entry(if ns == 0 { 0 } else { 127 - ns.leading_zeros() })
            .or_default() += 1;
    }
}
#[derive(Serialize)]
struct Slow {
    call_ordinal: u64,
    kind: String,
    input_or_group_position: Option<u64>,
    elapsed_ns: u128,
    dependency_count: usize,
    dependency_positions_first_eight: Vec<u64>,
}
struct HashWriter(Sha256);
impl Default for HashWriter {
    fn default() -> Self {
        Self(Sha256::new())
    }
}
impl Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
#[derive(Default)]
struct Summary {
    record_kinds: BTreeMap<&'static str, u64>,
    input_kinds: BTreeMap<&'static str, u64>,
    input_positions: Positions,
    canonical_positions: Positions,
    group_positions: Positions,
    observation_positions: Positions,
    dependencies_nonempty: u64,
    dependency_refs: u64,
    dependency_relations: BTreeMap<&'static str, u64>,
    dependency_positions: Positions,
    dependency_examples: Vec<Value>,
    repair_inputs: u64,
    repair_members: u64,
    observations: u64,
    group_widths: BTreeMap<usize, u64>,
    groups_per_return: BTreeMap<usize, u64>,
    end_inputs: u64,
    timings: BTreeMap<&'static str, Timing>,
    dependency_return_timings: BTreeMap<&'static str, Timing>,
    slow: Vec<Slow>,
    call_ordinal: u64,
    consume_summary_ns: u128,
    input_digest: HashWriter,
    group_digest: HashWriter,
    delivery_digest: HashWriter,
}
impl Summary {
    fn call(
        &mut self,
        k: &'static str,
        p: Option<u64>,
        ns: u128,
        deps: &[CanonicalInputRecordRefV1],
    ) {
        self.call_ordinal += 1;
        self.timings.entry(k).or_default().add(ns);
        if !matches!(k, "group" | "stored_non_input" | "eof") {
            self.dependency_return_timings
                .entry(if deps.is_empty() { "empty" } else { "nonempty" })
                .or_default()
                .add(ns);
        }
        if self.slow.len() < RETAIN || ns > self.slow.last().unwrap().elapsed_ns {
            self.slow.push(Slow {
                call_ordinal: self.call_ordinal,
                kind: k.into(),
                input_or_group_position: p,
                elapsed_ns: ns,
                dependency_count: deps.len(),
                dependency_positions_first_eight: deps
                    .iter()
                    .take(8)
                    .map(|d| d.input_position)
                    .collect(),
            });
            self.slow.sort_by_key(|v| std::cmp::Reverse(v.elapsed_ns));
            self.slow.truncate(RETAIN);
        }
    }
    fn input(&mut self, i: &RecordedIngressV1) -> Result<()> {
        let k = kind(i);
        *self.input_kinds.entry(k).or_default() += 1;
        self.input_positions.add(i.input_position);
        if matches!(i.ingress, DptIngressV1::EndOfInput) {
            self.end_inputs += 1;
        }
        serde_json::to_writer(
            &mut self.input_digest,
            &(
                &i.input_stream_id,
                i.input_position,
                k,
                &i.dependency_record_refs,
            ),
        )?;
        if let DptIngressV1::Message {
            source_evidence, ..
        } = &i.ingress
        {
            serde_json::to_writer(
                &mut self.input_digest,
                &(
                    &source_evidence.source_message_id,
                    source_evidence.source_event_at_utc_ms,
                    source_evidence.received_at_utc_ms,
                ),
            )?;
        }
        if !i.dependency_record_refs.is_empty() {
            self.dependencies_nonempty += 1;
            if self.dependency_examples.len() < RETAIN {
                self.dependency_examples.push(json!({"input_position":i.input_position,"kind":k,"total_refs":i.dependency_record_refs.len(),"refs_first_eight":i.dependency_record_refs.iter().take(8).collect::<Vec<_>>(),"omitted_refs":i.dependency_record_refs.len().saturating_sub(8)}));
            }
        }
        for dep in &i.dependency_record_refs {
            self.dependency_refs += 1;
            self.dependency_positions.add(dep.input_position);
            let relation = if dep.input_stream_id != i.input_stream_id {
                "other_stream"
            } else if dep.input_position < i.input_position {
                "earlier_same_stream"
            } else if dep.input_position == i.input_position {
                "same_input"
            } else {
                "later_same_stream"
            };
            *self.dependency_relations.entry(relation).or_default() += 1;
        }
        if let DptIngressV1::Message {
            row: TypedRawRowV2::HistoricalPriceAnchorRepairBatch(batch),
            ..
        } = &i.ingress
        {
            self.repair_inputs += 1;
            self.repair_members += batch.rows.len() as u64;
        }
        Ok(())
    }
    fn group(&mut self, g: &ClosedBusinessGroupV1) -> Result<()> {
        self.group_positions.add(g.group_position);
        self.observations += g.observations.len() as u64;
        *self.group_widths.entry(g.observations.len()).or_default() += 1;
        serde_json::to_writer(
            &mut self.group_digest,
            &(
                &g.group_id,
                g.group_position,
                &g.member_input_positions,
                g.closed_at_input_position,
            ),
        )?;
        for f in g.records.iter() {
            // 只编码坐标、来源身份、时钟与原质量，不编码payload或来源正文。
            serde_json::to_writer(
                &mut self.group_digest,
                &(
                    &f.position,
                    &f.source_refs,
                    &f.direct_time,
                    &f.dependency_time,
                    &f.quality,
                ),
            )?;
            for source in &f.direct_sources {
                serde_json::to_writer(&mut self.group_digest, &source.source_message_id)?;
            }
        }
        for o in &g.observations {
            serde_json::to_writer(
                &mut self.group_digest,
                &(
                    &o.market.condition_id,
                    &o.group_id,
                    o.group_position,
                    &o.visible_prefix,
                ),
            )?;
            let q = [
                o.lifecycle.quality,
                o.active.quality,
                o.closed.quality,
                o.accepting_orders.quality,
                o.terminal.quality,
                o.official_anchor.quality,
                o.settlement_reference.quality,
                o.chainlink_spot.quality,
                o.chainlink_twap_thirty.quality,
                o.chainlink_twap_sixty.quality,
                o.binance_depth.quality,
                o.binance_trade.quality,
            ];
            serde_json::to_writer(&mut self.group_digest, &q)?;
            for a in &o.assets {
                serde_json::to_writer(
                    &mut self.group_digest,
                    &(
                        &a.asset.token_id,
                        [
                            a.bids.quality,
                            a.asks.quality,
                            a.best_bid_ask.quality,
                            a.valuation_best_bid.quality,
                            a.tick.quality,
                            a.public_trade.quality,
                        ],
                    ),
                )?;
            }
        }
        Ok(())
    }
    fn record(&mut self, row: &PublicStoredRowV1) -> Result<()> {
        let k = match row {
            PublicStoredRowV1::Input(i) => {
                self.input(i)?;
                "input"
            }
            PublicStoredRowV1::Canonical {
                canonical_position, ..
            } => {
                self.canonical_positions.add(*canonical_position);
                "canonical"
            }
            PublicStoredRowV1::BinanceDepthReference {
                canonical_position, ..
            } => {
                self.canonical_positions.add(*canonical_position);
                "binance_depth_reference"
            }
            PublicStoredRowV1::GroupObservationRange(_) => "group_observation_range",
            PublicStoredRowV1::Report { .. } => "report",
            PublicStoredRowV1::PmReport { .. } => "pm_report",
            PublicStoredRowV1::PmStepBinding(_) => "pm_step_binding",
            PublicStoredRowV1::Group(g) => {
                self.group_positions.add(g.group_position);
                *self
                    .group_widths
                    .entry(g.observation_positions.len())
                    .or_default() += 1;
                "group"
            }
            PublicStoredRowV1::Observation(o) => {
                self.observation_positions.add(o.observation_position);
                self.observations += 1;
                "observation"
            }
            PublicStoredRowV1::SourceBinding(_) => "source_binding",
        };
        *self.record_kinds.entry(k).or_default() += 1;
        Ok(())
    }
    fn finish(self, mode: Mode) -> Value {
        let mut value = json!({"stored_record_kinds":self.record_kinds,"input_kinds":self.input_kinds,"input_positions":self.input_positions,"canonical_positions":self.canonical_positions,"group_positions":self.group_positions,"observation_positions":self.observation_positions,
        "dependencies":{"nonempty_inputs":self.dependencies_nonempty,"refs":self.dependency_refs,"position_occurrences":self.dependency_positions,"relations":self.dependency_relations,"first_nonempty_inputs":self.dependency_examples,"omitted_nonempty_inputs":self.dependencies_nonempty.saturating_sub(RETAIN as u64),"retention":"最早16个非空依赖Input，每项至多前8引用；总数/省略数分开，非完整触发集合。"},
        "historical_anchor_repair_inputs":self.repair_inputs,"historical_anchor_repair_members":self.repair_members,"end_of_input_count":self.end_inputs,"observations":self.observations,"observations_per_group":self.group_widths,"groups_per_replay_return":self.groups_per_return,
        "entry_call_timings_by_return_kind":self.timings,"entry_call_timings_by_returned_input_dependency_presence":self.dependency_return_timings,"entry_calls_including_eof":self.call_ordinal,"slowest_calls":self.slow,"slow_calls_omitted":self.call_ordinal.saturating_sub(RETAIN as u64),"consume_summary_ns":self.consume_summary_ns,
        "input_identity_order_sha256":format!("{:x}",self.input_digest.0.finalize()),"group_fact_identity_time_quality_order_sha256":format!("{:x}",self.group_digest.0.finalize()),"group_delivery_time_order_sha256":format!("{:x}",self.delivery_digest.0.finalize())});
        // group未暴露Input依赖；非Input/EOF也没有对应的输入引用计数。
        // 统一计时调用传入的空数组不能在最终输出中冒充实际零依赖。
        for call in value["slowest_calls"].as_array_mut().unwrap() {
            if matches!(
                call["kind"].as_str(),
                Some("group" | "stored_non_input" | "eof")
            ) {
                call["dependency_count"] = Value::Null;
                call["dependency_positions_first_eight"] = Value::Null;
            }
        }
        value["slow_call_dependency_fields_note"] = json!(
            "依赖字段只对record/replay返回Input计量；group未暴露、非Input/EOF不适用，均为null，不表示零依赖。"
        );
        // 未由此公开入口暴露的计数不能用零冒充实际观察。
        let unmeasured = match mode {
            Mode::Record => vec![
                "groups_per_replay_return",
                "group_fact_identity_time_quality_order_sha256",
                "group_delivery_time_order_sha256",
            ],
            Mode::Replay => vec![
                "stored_record_kinds",
                "canonical_positions",
                "observation_positions",
                "group_delivery_time_order_sha256",
            ],
            Mode::Group => vec![
                "stored_record_kinds",
                "input_kinds",
                "input_positions",
                "canonical_positions",
                "observation_positions",
                "dependencies",
                "historical_anchor_repair_inputs",
                "historical_anchor_repair_members",
                "end_of_input_count",
                "groups_per_replay_return",
                "entry_call_timings_by_returned_input_dependency_presence",
                "input_identity_order_sha256",
            ],
        };
        for field in &unmeasured {
            value[*field] = Value::Null;
        }
        value["not_measured_by_this_entry"] = json!(unmeasured);
        value
    }
}
fn measure(c: &Config) -> Result<Value> {
    let began = Instant::now();
    let mut s = Summary::default();
    let opened = Instant::now();
    let (open_ns, actual_prefix, applied_prefix, canonical_next) = match c.mode {
        Mode::Record => {
            let mut reader = PublicRecordingReaderV1::recover(
                &c.public_root,
                c.read_page_rows,
                c.max_read_batch_bytes,
            )?;
            let ns = opened.elapsed().as_nanos();
            let prefix = reader.prefix.clone();
            loop {
                let call = Instant::now();
                let row = reader.next_row()?;
                let elapsed = call.elapsed().as_nanos();
                let consume = Instant::now();
                match row {
                    Some(row) => {
                        let (k, p, deps) = if let PublicStoredRowV1::Input(i) = &row {
                            (
                                kind(i),
                                Some(i.input_position),
                                i.dependency_record_refs.as_slice(),
                            )
                        } else {
                            ("stored_non_input", None, &[][..])
                        };
                        s.call(k, p, elapsed, deps);
                        s.record(&row)?;
                        drop(row);
                    }
                    None => {
                        s.call("eof", None, elapsed, &[]);
                        s.consume_summary_ns += consume.elapsed().as_nanos();
                        break;
                    }
                }
                s.consume_summary_ns += consume.elapsed().as_nanos();
            }
            (ns, json!(prefix), Value::Null, Value::Null)
        }
        Mode::Replay => {
            let mut replay =
                PublicReplayV1::open(&c.public_root, c.read_page_rows, c.max_read_batch_bytes)?;
            let ns = opened.elapsed().as_nanos();
            loop {
                let call = Instant::now();
                let next = replay.next_prepared_applied()?;
                let elapsed = call.elapsed().as_nanos();
                let consume = Instant::now();
                match next {
                    Some((i, applied, pm)) => {
                        s.call(
                            kind(&i),
                            Some(i.input_position),
                            elapsed,
                            &i.dependency_record_refs,
                        );
                        s.input(&i)?;
                        *s.groups_per_return
                            .entry(applied.applied.closed_groups.len())
                            .or_default() += 1;
                        for g in &applied.applied.closed_groups {
                            s.group(g)?;
                        }
                        drop((i, applied, pm));
                    }
                    None => {
                        s.call("eof", None, elapsed, &[]);
                        s.consume_summary_ns += consume.elapsed().as_nanos();
                        break;
                    }
                }
                s.consume_summary_ns += consume.elapsed().as_nanos();
            }
            (
                ns,
                Value::Null,
                json!(replay.core.applied_prefix()),
                json!(replay.core.next_canonical_position()),
            )
        }
        Mode::Group => {
            let mut source = RecordedPublicGroupSourceV1::open(
                &c.public_root,
                c.read_page_rows,
                c.max_read_batch_bytes,
                OriginalVisibility::default(),
            )?;
            let ns = opened.elapsed().as_nanos();
            loop {
                let call = Instant::now();
                let next = source.next();
                let elapsed = call.elapsed().as_nanos();
                let consume = Instant::now();
                match next {
                    Some(result) => {
                        let delivery = result.map_err(|e| format!("{e:?}"))?;
                        let g = &delivery.prepared.group;
                        s.call("group", Some(g.group_position), elapsed, &[]);
                        s.group(g)?;
                        serde_json::to_writer(
                            &mut s.delivery_digest,
                            &(g.group_position, delivery.delivered_at_utc_ms),
                        )?;
                        drop(delivery);
                    }
                    None => {
                        s.call("eof", None, elapsed, &[]);
                        s.consume_summary_ns += consume.elapsed().as_nanos();
                        break;
                    }
                }
                s.consume_summary_ns += consume.elapsed().as_nanos();
            }
            (ns, Value::Null, Value::Null, Value::Null)
        }
    };
    Ok(
        json!({"open_entry_ns":open_ns,"measure_wall_seconds":began.elapsed().as_secs_f64(),"actual_record_reader_prefix":actual_prefix,"replay_applied_prefix":applied_prefix,"replay_next_canonical_position":canonical_next,"summary":s.finish(c.mode),"measurement_note":"入口调用时间包括内部跳过的稳定记录/依赖候选读取/归约等可达路径；返回Input分类只表相关性，不是Input独占成本。group可含多Input推进或pending出队。consume_summary_ns包括小字段摘要、分类、慢项保留和释放；Instant及外层循环开销未单独隔离。未计量各内部步骤次数/占比。"}),
    )
}
fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err("用法：research_reader_cost <CONFIG_JSON> <REPORT_JSON>".into());
    }
    let c: Config = serde_json::from_slice(&fs::read(&args[0])?)?;
    let io_before = fs::read_to_string("/proc/self/io")?;
    let started = Instant::now();
    let result = measure(&c);
    let io_after = fs::read_to_string("/proc/self/io")?;
    let status = fs::read_to_string("/proc/self/status")?;
    let report = match &result {
        Ok(v) => json!({"status":"success","result":v}),
        Err(e) => json!({"status":"error","original_error":e.to_string()}),
    };
    let out = json!({"schema":"complementary_pair_reader_cost_v1","effective_config":c,"measurement":{"wall_seconds":started.elapsed().as_secs_f64(),"proc_io_before":io_before,"proc_io_after":io_after,"proc_status_after":status},"outcome":report});
    fs::write(&args[1], serde_json::to_vec_pretty(&out)?)?;
    result.map(|_| ())
}
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../shared-run/tests/common/mod.rs"]
mod common;
#[cfg(test)]
mod tests {
    use super::*;
    fn config(root: PathBuf, mode: Mode) -> Config {
        let reader = PublicRecordingReaderV1::recover(&root, 2, 1 << 20).unwrap();
        Config {
            mode,
            public_root: root,
            read_page_rows: 2,
            max_read_batch_bytes: 1 << 20,
            expected_normal_prefix: reader.prefix.unwrap(),
            purpose: "有效原写入夹具的三层计量".into(),
        }
    }
    #[test]
    fn original_written_fixture_agrees_across_layers_without_copying_payloads() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("public");
        common::recorded_memory_complete(&root);
        let a = measure(&config(root.clone(), Mode::Record)).unwrap();
        let b = measure(&config(root.clone(), Mode::Replay)).unwrap();
        let g = measure(&config(root, Mode::Group)).unwrap();
        assert_eq!(
            a["summary"]["input_positions"],
            b["summary"]["input_positions"]
        );
        assert_eq!(
            a["summary"]["input_identity_order_sha256"],
            b["summary"]["input_identity_order_sha256"]
        );
        for field in ["group_positions", "observations", "observations_per_group"] {
            assert_eq!(a["summary"][field], b["summary"][field]);
            assert_eq!(b["summary"][field], g["summary"][field]);
        }
        assert_eq!(
            b["summary"]["group_fact_identity_time_quality_order_sha256"],
            g["summary"]["group_fact_identity_time_quality_order_sha256"]
        );
        assert_eq!(
            a["actual_record_reader_prefix"]["next_input_position"],
            b["replay_applied_prefix"]["next_input_position"]
        );
        assert_eq!(a["summary"]["end_of_input_count"], 1);
        for call in g["summary"]["slowest_calls"].as_array().unwrap() {
            assert!(call["dependency_count"].is_null());
            assert!(call["dependency_positions_first_eight"].is_null());
        }
    }
    #[test]
    fn slow_input_dependency_counts_remain_known_zero_and_one() {
        // 只验证研究汇总的编码，不伪造归约输入或业务状态。
        let dependency = CanonicalInputRecordRefV1 {
            input_stream_id: "summary-reference".into(),
            input_position: 0,
        };
        for mode in [Mode::Record, Mode::Replay] {
            let mut summary = Summary::default();
            summary.call("chainlink_spot", Some(1), 1, &[]);
            summary.call("anchor_rule", Some(2), 2, std::slice::from_ref(&dependency));
            summary.call("eof", None, 3, &[]);
            let value = summary.finish(mode);
            let calls = value["slowest_calls"].as_array().unwrap();
            let by_kind = |kind| calls.iter().find(|v| v["kind"] == kind).unwrap();
            assert_eq!(by_kind("chainlink_spot")["dependency_count"], 0);
            assert_eq!(by_kind("anchor_rule")["dependency_count"], 1);
            assert_eq!(
                by_kind("anchor_rule")["dependency_positions_first_eight"],
                json!([0])
            );
            assert!(by_kind("eof")["dependency_count"].is_null());
        }
    }
    #[test]
    fn finite_prefix_without_end_keeps_each_original_entry_eof_contract() {
        let temp = tempfile::tempdir().unwrap();
        let complete = temp.path().join("complete");
        common::recorded_memory_complete(&complete);
        let mut original = PublicRecordingReaderV1::recover(&complete, 2, 1 << 20).unwrap();
        let input = match original.next_row().unwrap().unwrap() {
            PublicStoredRowV1::Input(i) => i,
            _ => panic!("原夹具首行应是Input"),
        };
        let root = temp.path().join("prefix");
        let mut core = data_pipeline_core::public_observation_core::PublicObservationCoreV1::new(
            original.header.clone(),
        )
        .unwrap();
        let mut writer =
            PublicRecordingWriterV1::create(&root, core.header(), common::config()).unwrap();
        let prepared = prepare_public_ingress_v1(&core, &input, &BTreeMap::new(), &[]).unwrap();
        let applied = core.push(&prepared).unwrap().into_owned();
        drop(prepared);
        writer.record(input, applied).unwrap();
        writer.stabilize().unwrap();
        let mut c = config(root.clone(), Mode::Record);
        assert!(!c.expected_normal_prefix.normal_end);
        let record = measure(&c).unwrap();
        assert_eq!(record["summary"]["end_of_input_count"], 0);
        c.mode = Mode::Replay;
        assert_eq!(measure(&c).unwrap()["summary"]["end_of_input_count"], 0);
        let original =
            RecordedPublicGroupSourceV1::open(&root, 2, 1 << 20, OriginalVisibility::default())
                .unwrap()
                .find_map(|v| v.err())
                .unwrap();
        c.mode = Mode::Group;
        let error = measure(&c).unwrap_err().to_string();
        assert_eq!(error, format!("{original:?}"));
        assert!(error.contains("稳定前缀尚无正常 EndOfInput"));
    }
    #[test]
    fn original_stable_prefix_mismatch_is_propagated_instead_of_eof() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("public");
        common::recorded_memory_complete(&root);
        let mut c = config(root.clone(), Mode::Record);
        let path = root
            .join(&c.expected_normal_prefix.stable_storage_ref)
            .join("prefix.json");
        let mut prefix = c.expected_normal_prefix.clone();
        prefix.input_stream_id = "wrong-stream".into();
        fs::write(path, serde_json::to_vec(&prefix).unwrap()).unwrap();
        let original = PublicRecordingReaderV1::recover(&root, 2, 1 << 20)
            .err()
            .unwrap()
            .to_string();
        assert_eq!(original, "稳定分片不连续或流身份不符");
        for mode in [Mode::Record, Mode::Replay, Mode::Group] {
            c.mode = mode;
            assert_eq!(measure(&c).unwrap_err().to_string(), original);
        }
    }
}
