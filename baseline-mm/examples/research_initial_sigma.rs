//! #185 所选真实 RTDS 文件的原有限录制与来源统计；不创建市场或账户。
use baseline_mm::*;
use contracts::*;
use data_pipeline_training::{
    finite_recording::*, historical_preparation_progress::*, public_replay::*,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
#[derive(Serialize, Deserialize)]
struct Config {
    recording: FinitePublicRecordingConfigV1,
    selected_source_identity_sha256: String,
    selection_evidence: Value,
    read_page_rows: usize,
    max_read_batch_bytes: usize,
    progress_interval_seconds: u64,
    purpose: String,
}
#[derive(Default, Serialize)]
struct Bucket {
    closed_groups: u64,
    reference_facts: u64,
    anomalies: BTreeMap<String, u64>,
    quality: BTreeMap<String, u64>,
    admission: BTreeMap<String, u64>,
    accepted_points: u64,
    first_received_utc_ms: Option<i64>,
    last_received_utc_ms: Option<i64>,
    maximum_positive_gap_seconds: f64,
    cross_midnight_pairs: u64,
    variance: SpotVarianceStatisticsV1,
}
fn day(t: i64) -> i64 {
    t.div_euclid(86_400_000)
}
fn accept(bucket: &mut Bucket, t: i64) {
    bucket.accepted_points += 1;
    bucket.first_received_utc_ms.get_or_insert(t);
    bucket.last_received_utc_ms = Some(t);
}
fn summarize(b: &Bucket) -> Value {
    let mut v = serde_json::to_value(b).expect("固定标量统计可序列化");
    v["sigma_usd_per_btc_per_sqrt_second"] = json!(b.variance.sigma_usd_per_sqrt_second());
    v["accepted_span_seconds"] = json!(
        b.first_received_utc_ms
            .zip(b.last_received_utc_ms)
            .map(|(a, z)| (z - a) as f64 / 1000.0)
    );
    v
}
fn prepare(c: &Config) -> Result<Value> {
    let began = Instant::now();
    let mut last = Instant::now();
    let mut output = true;
    let mut raw_rows = [0u64; 3];
    let mut batches = [0u64; 3];
    let mut selected = [0u64; 3];
    let mut files = [0u64; 3];
    let mut sort = BTreeMap::new();
    let mut stage = HistoricalPreparationStageV1::EpochPass;
    let mut notify = |n| {
        use HistoricalPreparationNoticeV1::*;
        let changed = matches!(n, Stage(_));
        match n {
            Stage(s) => stage = s,
            RawBatch { pass, rows, .. } => {
                let i = pass as usize;
                raw_rows[i] += rows;
                batches[i] += 1;
            }
            RawFileComplete { pass, .. } => files[pass as usize] += 1,
            SelectedRow { pass, .. } => selected[pass as usize] += 1,
            SourceSortFinished { index, metrics } => {
                sort.insert(index, metrics);
            }
            _ => {}
        }
        if output
            && c.progress_interval_seconds > 0
            && (changed || last.elapsed() >= Duration::from_secs(c.progress_interval_seconds))
        {
            if writeln!(std::io::stderr(),"{}",json!({"stage":stage,"physical_rows":raw_rows,"selected_rows":selected,"elapsed_seconds":began.elapsed().as_secs_f64(),"total":null,"eta_seconds":null})).is_err(){output=false;}
            last = Instant::now();
        }
    };
    let notifier = HistoricalPreparationNotifierV1::new(&mut notify);
    let (mut inputs, mut groups, mut observations) = (0u64, 0u64, 0u64);
    let prefix =
        prepare_finite_public_recording_v1(&c.recording, Some(&notifier), |_, applied, _| {
            if let Some(a) = applied {
                inputs += 1;
                groups += a.applied.closed_groups.len() as u64;
                observations += a
                    .applied
                    .closed_groups
                    .iter()
                    .map(|g| g.observations.len() as u64)
                    .sum::<u64>();
            }
        })?;
    Ok(
        json!({"prefix":prefix,"inputs":inputs,"closed_groups":groups,"observations":observations,"physical_rows_by_pass":raw_rows,"batches_by_pass":batches,"selected_rows_by_pass":selected,"completed_files_by_pass":files,"sort_metrics":sort}),
    )
}
fn replay(c: &Config, prepared: &Value) -> Result<Value> {
    let prefix: CommittedIngressPrefixV1 =
        serde_json::from_value(prepared["result"]["prefix"].clone())?;
    let mut replay = PublicReplayV1::open_committed(
        &c.recording.work_root.join("public"),
        prefix.clone(),
        c.read_page_rows,
        c.max_read_batch_bytes,
    )?;
    replay.set_progress(
        Duration::from_secs(c.progress_interval_seconds),
        std::io::stderr(),
    );
    let (mut inputs, mut observations, mut now) = (0u64, 0u64, i64::MIN);
    let mut days: BTreeMap<i64, Bucket> = (day(c.recording.received_start_utc_ms)
        ..day(c.recording.received_end_utc_ms))
        .map(|d| (d, Bucket::default()))
        .collect();
    let mut overall = Bucket::default();
    let mut previous: Option<(i64, f64)> = None;
    let mut cross_midnight = Vec::new();
    let (mut first_state, mut last_state) = (None, None);
    while let Some((input, applied, _)) = replay.next_prepared_applied()? {
        inputs += 1;
        for group in &applied.applied.closed_groups {
            observations += group.observations.len() as u64;
            let visible = sim_system::public_consumption::original_public_group_visible_at_utc_ms(
                &input, group,
            );
            now = now.max(visible);
            let bucket = days.entry(day(visible)).or_default();
            bucket.closed_groups += 1;
            overall.closed_groups += 1;
            for fact in group.records.iter() {
                if let PublicBusinessPayloadV1::ReferenceFact(reference) = &fact.payload {
                    match reference {
                        CanonicalReferenceEventPayloadV2::ChainlinkBtcUsd(_) => {
                            bucket.reference_facts += 1;
                            overall.reference_facts += 1;
                        }
                        CanonicalReferenceEventPayloadV2::RtdsObservedAnomaly(a) => {
                            let key = format!("{:?}", a.classification);
                            *bucket.anomalies.entry(key.clone()).or_default() += 1;
                            *overall.anomalies.entry(key).or_default() += 1;
                        }
                        _ => {}
                    }
                }
            }
            // 本轮RTDS V4每输入至多闭合一组；立即读取当前完整质量/来源，不绑定后续输入。
            let fact = replay.core.current_chainlink_spot();
            let quality = format!("{:?}", fact.quality);
            *bucket.quality.entry(quality.clone()).or_default() += 1;
            *overall.quality.entry(quality).or_default() += 1;
            let Some(value) = eligible_chainlink_spot_v1(fact) else {
                *bucket
                    .admission
                    .entry("no_eligible_value".into())
                    .or_default() += 1;
                *overall
                    .admission
                    .entry("no_eligible_value".into())
                    .or_default() += 1;
                continue;
            };
            let time = direct_received_time_v1(fact, now);
            let price = spot_price_number_v1(&value.price_usd_per_btc)?;
            let decision = spot_sample_acceptance_v1(time, now, previous.map(|p| p.0));
            let key = serde_json::to_value(decision)?.as_str().unwrap().to_owned();
            *bucket.admission.entry(key.clone()).or_default() += 1;
            *overall.admission.entry(key).or_default() += 1;
            if decision != SpotSampleAcceptanceV1::Accepted {
                continue;
            }
            let state = json!({"group_position":group.group_position,"input_position":input.input_position,"visible_at_utc_ms":visible,"monotonic_now_utc_ms":now,"received_at_utc_ms":time,"spot":fact});
            first_state.get_or_insert_with(|| state.clone());
            last_state = Some(state);
            let right_bucket = days.entry(day(time)).or_default();
            accept(right_bucket, time);
            accept(&mut overall, time);
            if let Some(left) = previous
                && let Some(pair) =
                    spot_variance_pair_v1(left, (time, price), c.recording.received_start_utc_ms)
            {
                right_bucket.variance.include(pair);
                overall.variance.include(pair);
                right_bucket.maximum_positive_gap_seconds = right_bucket
                    .maximum_positive_gap_seconds
                    .max(pair.elapsed_seconds);
                overall.maximum_positive_gap_seconds = overall
                    .maximum_positive_gap_seconds
                    .max(pair.elapsed_seconds);
                if day(left.0) != day(time) {
                    right_bucket.cross_midnight_pairs += 1;
                    overall.cross_midnight_pairs += 1;
                    cross_midnight.push(json!({"left_received_utc_ms":left.0,"right_received_utc_ms":time,"left_price_usd_per_btc":left.1,"right_price_usd_per_btc":price,"right_group_position":group.group_position,"pair":pair}));
                }
            }
            previous = Some((time, price));
        }
    }
    Ok(
        json!({"expected_prefix":prefix,"actual_prefix":replay.core.applied_prefix(),"inputs":inputs,"observations":observations,"daily":days.into_iter().map(|(d,b)|(d.to_string(),summarize(&b))).collect::<BTreeMap<_,_>>(),"overall":summarize(&overall),"first_accepted_state":first_state,"last_accepted_state":last_state,"cross_midnight_pairs":cross_midnight,"last_monotonic_now_utc_ms":now,"semantics":"来源候选；不是每condition的SignalMemory sigma或六源可见前沿。日事实/质量/接纳按组可见UTC日；接纳点与完整价对按右端接收UTC日。全程f64独立原序相加；没有生产fallback。覆盖为实际接纳端点及最大间隔，不宣称连续有效价或交易许可。"}),
    )
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        return Err("用法 prepare CONFIG OUTPUT 或 replay CONFIG PREPARED OUTPUT".into());
    }
    let c: Config = serde_json::from_slice(&fs::read(&args[1])?)?;
    let before = fs::read_to_string("/proc/self/io").ok();
    let began = Instant::now();
    let result = if args[0] == "prepare" {
        prepare(&c)
    } else if args[0] == "replay" {
        replay(&c, &serde_json::from_slice(&fs::read(&args[2])?)?)
    } else {
        return Err("模式必须为prepare或replay".into());
    };
    let out = if args[0] == "prepare" {
        &args[2]
    } else {
        args.get(3).ok_or("缺少输出路径")?
    };
    let report = json!({"status":if result.is_ok(){"success"}else{"failed"},"effective_config":c,"result":result.as_ref().ok(),"error":result.as_ref().err().map(|e|e.to_string()),"measurement":{"wall_seconds":began.elapsed().as_secs_f64(),"proc_io_before":before,"proc_io_after":fs::read_to_string("/proc/self/io").ok()}});
    let written = fs::write(PathBuf::from(out), serde_json::to_vec_pretty(&report)?);
    match result {
        Err(error) => {
            let _ = written;
            Err(error)
        }
        Ok(_) => {
            written?;
            Ok(())
        }
    }
}

#[cfg(test)]
#[path = "../../shared-run/tests/common/fixture_input.rs"]
mod fixture_input;
#[cfg(test)]
#[path = "../../shared-run/tests/common/fixture_recording.rs"]
mod fixture_recording;
#[cfg(test)]
mod common {
    pub use super::fixture_input::*;
    pub use super::fixture_recording::*;
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_pipeline_core::{public_input::*, typed_raw::*};
    fn apply(
        core: &mut data_pipeline_core::public_observation_core::PublicObservationCoreV1,
        input: &RecordedIngressV1,
    ) -> AppliedIngressV1 {
        core.push(&prepare_public_ingress_v1(core, input, &BTreeMap::new(), &[]).unwrap())
            .unwrap()
            .into_owned()
            .applied
    }
    #[test]
    fn multi_source_and_selected_spot_share_complete_business_state_but_not_visibility_frontier() {
        let mut selected = common::core();
        let mut multi = common::core();
        let mut metadata = common::meta(0);
        let DptIngressV1::Message {
            row: TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(m)),
            ..
        } = &mut metadata.ingress
        else {
            panic!()
        };
        m.collector_admission_time_utc_ms = 20000;
        m.row_evidence_validation_completed_at_utc = Some(20000);
        // 经过原身份构造重新记录，更晚准入使多源now推进，但不是RTDS自身可见时间。
        let DptIngressV1::Message { row, .. } = metadata.ingress else {
            panic!()
        };
        let metadata = common::record(0, row);
        let meta_result = apply(&mut multi, &metadata);
        let _meta_visible = sim_system::public_consumption::original_public_group_visible_at_utc_ms(
            &metadata,
            &meta_result.closed_groups[0],
        );
        let row = TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(
            PolymarketRtdsChainlinkBtcUsdRow {
                timestamp_received: 11000,
                message_timestamp: 10999,
                payload_timestamp: 10998,
                age_ms: Some(2),
                symbol: "btc/usd".into(),
                value: "65000".into(),
                topic: "crypto_prices_chainlink".into(),
                message_type: "update".into(),
                freshness_status: RtdsFreshnessStatus::Stale,
                subscription_filter: "{\"symbol\":\"btc/usd\"}".into(),
                source_native_payload_json: "{}".into(),
                collector_host_id: "host".into(),
                collector_region: "hk".into(),
                source_endpoint: "wss://ws-live-data.polymarket.com".into(),
                schema_version: POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into(),
            },
        ));
        let selected_input = common::record(0, row.clone());
        let TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(mut twap_common)) =
            row.clone()
        else {
            panic!()
        };
        twap_common.timestamp_received = 10500;
        twap_common.message_timestamp = 10499;
        twap_common.payload_timestamp = 10498;
        twap_common.schema_version = POLYMARKET_RTDS_CHAINLINK_TWAP_SCHEMA_V1.into();
        twap_common.topic = "crypto_prices_twap_sixty".into();
        let twap = common::record(
            1,
            TypedRawRowV2::PolymarketRtdsChainlinkTwap(PolymarketRtdsChainlinkTwapInput {
                common: twap_common,
                full_accuracy_value: "65000000000000000000000".into(),
                window_s: 60,
                source_sequence_epoch_id: "twap-epoch".into(),
                collector_admission_sequence: 1,
                collector_admission_time_utc_ms: 20000,
            }),
        );
        let twap_applied = apply(&mut multi, &twap);
        let prior_visible = sim_system::public_consumption::original_public_group_visible_at_utc_ms(
            &twap,
            &twap_applied.closed_groups[0],
        );
        let multi_input = common::record(2, row);
        let a = apply(&mut selected, &selected_input);
        let b = apply(&mut multi, &multi_input);
        assert_eq!(a.closed_groups.len(), 1);
        assert_eq!(b.closed_groups.len(), 1);
        let a_fact = &a.closed_groups[0].records[0];
        let b_fact = &b.closed_groups[0].records[0];
        assert_eq!(a_fact.payload, b_fact.payload);
        assert_eq!(a_fact.lineage, b_fact.lineage);
        assert_eq!(a_fact.direct_time, b_fact.direct_time);
        assert_eq!(a_fact.dependency_time, b_fact.dependency_time);
        assert_eq!(a_fact.quality, b_fact.quality);
        assert_eq!(a_fact.direct_sources, b_fact.direct_sources);
        let mut b_spot = multi.current_chainlink_spot().clone();
        let a_spot = selected.current_chainlink_spot();
        // 仅流内物理位置/组身份/依赖前沿不同；完整来源身份、质量、价格与真实时间保持。
        b_spot.direct_anchors[0].position = a_spot.direct_anchors[0].position.clone();
        b_spot.direct_anchors[0].group_id = a_spot.direct_anchors[0].group_id.clone();
        b_spot.dependency_frontier = a_spot.dependency_frontier.clone();
        assert_eq!(&b_spot, a_spot);
        let selected_visible =
            sim_system::public_consumption::original_public_group_visible_at_utc_ms(
                &selected_input,
                &a.closed_groups[0],
            );
        let multi_spot_visible =
            sim_system::public_consumption::original_public_group_visible_at_utc_ms(
                &multi_input,
                &b.closed_groups[0],
            );
        assert_eq!(selected_visible, 11000);
        assert_eq!(multi_spot_visible, 11000);
        assert_eq!(prior_visible.max(multi_spot_visible), 20000);
    }
    #[test]
    fn cross_midnight_pairs_belong_to_right_day_and_overall_keeps_original_sum_order() {
        let points = [
            (86_399_000, 100.0),
            (86_400_000, 104.0),
            (86_401_000, 103.0),
        ];
        let mut daily = BTreeMap::<i64, SpotVarianceStatisticsV1>::new();
        let mut total = SpotVarianceStatisticsV1::default();
        for w in points.windows(2) {
            let p = spot_variance_pair_v1(w[0], w[1], 0).unwrap();
            daily.entry(day(w[1].0)).or_default().include(p);
            total.include(p);
        }
        assert_eq!(daily.len(), 1);
        assert_eq!(daily[&1].positive_pair_count, 2);
        assert_eq!(total.squared_change_sum_usd_squared, 17.0);
        assert_eq!(total.elapsed_seconds, 2.0);
    }
}
