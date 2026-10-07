//! 当前概率研究复用公共类型化元数据/终局归约。
//! 复用公共精确数值规范化、临时参考规则及深度数量状态。
//! 常驻前台边界进程不生成观察、录制、SIM 或账本。
use contracts::*;
use data_pipeline_core::{
    pm_reduction::PreparedPmEvidenceV1, public_input::*,
    public_observation_core::PublicObservationCoreV1, public_reference::*,
    reduction::BinanceDepthReducer, typed_raw::*,
};
use data_pipeline_training::{
    historical_initialization::{
        MetadataInitializationConfigV1, MetadataInitializationSelectionV1,
        metadata_admission_order_v1,
    },
    public_input::{PublicFiniteFileV1, open_finite_public_input_v1},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Write},
};

type Error = Box<dyn std::error::Error + Send + Sync>;
#[derive(Deserialize)]
struct MarketFile {
    file: PublicFiniteFileV1,
    clocks: Vec<i64>,
}
#[derive(Deserialize)]
struct Markets {
    files: Vec<MarketFile>,
    initialization: MetadataInitializationConfigV1,
    #[serde(default)]
    initialization_clocks: Vec<i64>,
}
#[derive(Deserialize)]
struct DepthLine {
    at: i64,
    #[serde(default)]
    sample: bool,
    #[serde(default)]
    row: Option<Value>,
    #[serde(default)]
    kind: Option<String>,
}
fn output(value: Value) -> Result<(), Error> {
    let stdout = io::stdout();
    let mut out = stdout.lock();
    writeln!(out, "{}", value)?;
    out.flush()?;
    Ok(())
}
fn apply_market(
    core: &mut PublicObservationCoreV1,
    out: &mut impl Write,
    position: &mut u64,
    clock: i64,
    row: TypedRawRowV2,
) -> Result<(), Error> {
    let source = source_evidence_v3_from_typed_raw_v2(&row)?;
    let at = clock.max(source.received_at_utc_ms).max(
        source
            .collector_admission
            .as_ref()
            .map_or(i64::MIN, |c| c.admitted_at_utc_ms),
    );
    let record = RecordedIngressV1 {
        input_stream_id: "direct-probability-boundary-v1".into(),
        input_position: *position,
        ingress: DptIngressV1::Message {
            row,
            source_evidence: source,
        },
        dependency_record_refs: vec![],
    };
    *position += 1;
    let prepared = prepare_public_ingress_v1(core, &record, &BTreeMap::new(), &[])?;
    for batch in core.push_reference_facts(&prepared)? {
        for fact in batch.facts.iter() {
            match fact.payload {
                PublicBusinessPayloadV1::MarketFact {
                    payload:
                        CanonicalMarketFactPayloadV1::FixedIdentity { .. }
                        | CanonicalMarketFactPayloadV1::Terminal(_)
                        | CanonicalMarketFactPayloadV1::QualityBoundary(_),
                    ..
                }
                | PublicBusinessPayloadV1::ReferenceFact(_) => {
                    writeln!(
                        out,
                        "{}",
                        json!({"at":at,"payload":fact.payload,"quality":fact.quality,
                    "dependency_received":fact.dependency_time.received_at_utc_ms})
                    )?;
                }
                _ => {}
            }
        }
    }
    Ok(())
}
type MarketQueued = (i64, (i64, u64, String), TypedRawRowV2);
fn native_order(row: &TypedRawRowV2) -> Result<(i64, u64, String), Error> {
    let source = source_evidence_v3_from_typed_raw_v2(row)?;
    let metadata = match row {
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(r)) => Some((
            r.collector_admission_time_utc_ms,
            r.collector_admission_sequence,
        )),
        TypedRawRowV2::PolymarketMarketMetadataV6(r) => Some(match r.as_ref() {
            PolymarketMarketMetadataV6Input::Existing(r) => (
                r.collector_admission_time_utc_ms,
                r.collector_admission_sequence,
            ),
            PolymarketMarketMetadataV6Input::AnchorRule(r) => (
                r.collector_admission_time_utc_ms,
                r.collector_admission_sequence,
            ),
        }),
        _ => None,
    };
    if let Some((admitted, sequence)) = metadata {
        Ok(metadata_admission_order_v1(
            admitted,
            sequence,
            &source.source_message_id,
        ))
    } else {
        let admission = source
            .collector_admission
            .as_ref()
            .ok_or("terminal admission")?;
        Ok((
            0,
            admission.collector_admission_sequence,
            source.source_message_id.0,
        ))
    }
}

fn market_file(file: MarketFile) -> Result<VecDeque<MarketQueued>, Error> {
    let mut result = Vec::new();
    let mut clocks = file.clocks.into_iter();
    for batch in open_finite_public_input_v1(&file.file, 1024, 64 * 1024 * 1024)? {
        for row in batch? {
            result.push((
                clocks.next().ok_or("missing legal clock")?,
                native_order(&row)?,
                row,
            ));
        }
    }
    if clocks.next().is_some() {
        return Err("extra legal clock".into());
    }
    result.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    Ok(result.into())
}
fn write_metadata_clock(
    out: &mut impl Write,
    file_id: i64,
    row_id: usize,
    row: TypedRawRowV2,
) -> Result<(), Error> {
    let source = source_evidence_v3_from_typed_raw_v2(&row)?;
    let (admitted, sequence, source_message_id) = native_order(&row)?;
    writeln!(
        out,
        "{}",
        json!({"file_id":file_id,"row_id":row_id,"received":source.received_at_utc_ms,
        "admitted":admitted,"sequence":sequence,"source_message_id":source_message_id})
    )?;
    Ok(())
}
fn next_market_queue(active: &BTreeMap<usize, VecDeque<MarketQueued>>) -> Option<(usize, i64)> {
    active
        .iter()
        .filter_map(|(id, rows)| rows.front().map(|r| (*id, r.0, &r.1)))
        .min_by_key(|(id, at, key)| (*at, *key, *id))
        .map(|(id, at, _)| (id, at))
}
fn main() -> Result<(), Error> {
    let mut depth: BinanceDepthReducer<PreparedPmEvidenceV1<SourceEvidenceRefV3>> =
        BinanceDepthReducer::new("direct-probability-v1")?;
    for line in io::stdin().lock().lines() {
        let cmd: Value = serde_json::from_str(&line?)?;
        match cmd["op"].as_str() {
            Some("normalize") => {
                let mut out =
                    BufWriter::new(File::create(cmd["output"].as_str().ok_or("output")?)?);
                for line in
                    BufReader::new(File::open(cmd["input"].as_str().ok_or("input")?)?).lines()
                {
                    let row: Value = serde_json::from_str(&line?)?;
                    let raw = row["raw"].as_str().ok_or("raw decimal")?;
                    let value = if row["e18"].as_bool() == Some(true) {
                        normalize_e18_price_decimal_v1(raw, "full_accuracy_value")?
                    } else {
                        normalize_price_decimal_lexical_v1(raw, "price")?
                    };
                    writeln!(out, "{}", json!({"raw":raw,"value":value.0}))?;
                }
                out.flush()?;
                output(json!({"done":true}))?;
            }
            Some("depth") => {
                let mut out =
                    BufWriter::new(File::create(cmd["output"].as_str().ok_or("output")?)?);
                for line in
                    BufReader::new(File::open(cmd["input"].as_str().ok_or("input")?)?).lines()
                {
                    let input: DepthLine = serde_json::from_str(&line?)?;
                    if input.sample {
                        let totals = depth.current_quantity_totals()?;
                        writeln!(
                            out,
                            "{}",
                            json!({"at":input.at,"bid":totals.as_ref().map(|v|&v.0),"ask":totals.as_ref().map(|v|&v.1)})
                        )?;
                    } else {
                        let mut row = input.row.ok_or("depth row")?;
                        // 原 Parquet 保存 JSON 价量对；沿公共类型边界解析。
                        for side in ["bids", "asks"] {
                            let levels = parse_depth_levels_json(
                                row[side].as_str().ok_or("depth levels")?,
                                side,
                                true,
                            )?;
                            row[side] = serde_json::to_value(levels)?;
                        }
                        let typed = match input.kind.as_deref() {
                            Some("binance_depth_snapshot") => {
                                TypedRawRow::BinanceDepthSnapshot(serde_json::from_value::<
                                    BinanceDepthSnapshotRow,
                                >(
                                    row
                                )?)
                            }
                            Some("binance_depth_update") => {
                                row["stitch_verdict"] =
                                    serde_json::to_value(BinanceDepthStitchVerdict::try_from(
                                        row["stitch_verdict"].as_str().ok_or("verdict")?,
                                    )?)?;
                                TypedRawRow::BinanceDepthUpdate(serde_json::from_value::<
                                    BinanceDepthUpdateRow,
                                >(
                                    row
                                )?)
                            }
                            _ => return Err("depth kind".into()),
                        };
                        let evidence = source_evidence_v3_from_typed_raw_v2(
                            &TypedRawRowV2::Existing(typed.clone()),
                        )?;
                        let evidence = PreparedPmEvidenceV1::prepare(evidence, None)?;
                        match typed {
                            TypedRawRow::BinanceDepthSnapshot(row) => {
                                depth.apply_quantity_snapshot(&row, evidence)?
                            }
                            TypedRawRow::BinanceDepthUpdate(row) => {
                                depth.apply_quantity_update(&row, evidence)?
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                out.flush()?;
                output(json!({"done":true}))?;
            }
            Some("metadata_clocks") => {
                let config: Markets =
                    serde_json::from_reader(File::open(cmd["input"].as_str().ok_or("input")?)?)?;
                let mut out =
                    BufWriter::new(File::create(cmd["output"].as_str().ok_or("output")?)?);
                let init = MetadataInitializationSelectionV1::open(
                    &config.initialization,
                    1024,
                    64 * 1024 * 1024,
                )?;
                for (row_id, row) in init.read(1024, 64 * 1024 * 1024).enumerate() {
                    write_metadata_clock(&mut out, -1, row_id, row?.0)?;
                }
                for (file_id, file) in config.files.into_iter().enumerate() {
                    let mut row_id = 0;
                    for batch in open_finite_public_input_v1(&file.file, 1024, 64 * 1024 * 1024)? {
                        for row in batch? {
                            write_metadata_clock(&mut out, file_id as i64, row_id, row)?;
                            row_id += 1;
                        }
                    }
                }
                out.flush()?;
                output(json!({"done":true}))?;
            }
            Some("markets") => {
                let config: Markets =
                    serde_json::from_reader(File::open(cmd["input"].as_str().ok_or("input")?)?)?;
                let mut core = PublicObservationCoreV1::new(PublicInputStreamHeaderV1 {
                    input_stream_id: "direct-probability-boundary-v1".into(),
                    canonical_rule_version: "direct-probability-v1".into(),
                    observation_version: "public-observation-v1".into(),
                    ordering_version: "causal-source-projection-v1".into(),
                    input_scope: "metadata-and-terminal-only".into(),
                    max_pending_ws_terminals: cmd["max_pending_ws_terminals"]
                        .as_u64()
                        .ok_or("pending limit")?
                        as usize,
                    initial_state: PublicInitialStateV1::Empty,
                })?;
                let mut out =
                    BufWriter::new(File::create(cmd["output"].as_str().ok_or("output")?)?);
                let mut position = 0;
                let init = MetadataInitializationSelectionV1::open(
                    &config.initialization,
                    1024,
                    64 * 1024 * 1024,
                )?;
                let mut seeds = Vec::new();
                let mut seed_clocks = config.initialization_clocks.into_iter();
                for row in init.read(1024, 64 * 1024 * 1024) {
                    let (row, _) = row?;
                    seeds.push((
                        seed_clocks.next().ok_or("seed clock")?,
                        native_order(&row)?,
                        row,
                    ));
                }
                if seed_clocks.next().is_some() {
                    return Err("extra seed clock".into());
                }
                seeds.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
                // 仅同时打开物理重叠的必要语义子集；种子与普通行共用已计算的元数据前沿。
                let mut files = config.files;
                files.sort_by_key(|f| f.clocks.iter().min().copied().unwrap_or(i64::MAX));
                let mut files = files.into_iter().peekable();
                let mut active: BTreeMap<usize, VecDeque<MarketQueued>> = BTreeMap::new();
                if !seeds.is_empty() {
                    active.insert(usize::MAX, seeds.into());
                }
                let mut next_id = 0;
                loop {
                    let next = next_market_queue(&active);
                    if files.peek().is_some_and(|f| {
                        next.is_none_or(|(_, at)| {
                            f.clocks.iter().min().is_some_and(|first| *first <= at)
                        })
                    }) {
                        let rows = market_file(files.next().unwrap())?;
                        if !rows.is_empty() {
                            active.insert(next_id, rows);
                        }
                        next_id += 1;
                        continue;
                    }
                    let Some((id, _)) = next else { break };
                    let rows = active.get_mut(&id).unwrap();
                    let (clock, _, row) = rows.pop_front().unwrap();
                    if rows.is_empty() {
                        active.remove(&id);
                    }
                    apply_market(&mut core, &mut out, &mut position, clock, row)?;
                }
                out.flush()?;
                output(json!({"done":true}))?;
            }
            Some("temporary") => {
                let reference: BtcSettlementReferenceV1 =
                    serde_json::from_value(cmd["reference"].clone())?;
                let eligible = temporary_twap_candidate_matches_v1(
                    reference,
                    cmd["start"].as_i64().ok_or("start")?,
                    cmd["end"].as_i64().ok_or("end")?,
                    cmd["window_s"].as_u64().ok_or("window")?,
                    cmd["event"].as_i64().ok_or("event")?,
                    cmd["received"].as_i64().ok_or("received")?,
                    serde_json::from_value(cmd["quality"].clone())?,
                );
                output(json!({"eligible":eligible}))?;
            }
            _ => return Err("normalize | depth | markets | temporary".into()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    fn metadata(received: i64, admitted: i64, sequence: u64) -> TypedRawRowV2 {
        let payload = r#"{"orderPriceMinTickSize":"0.01"}"#;
        let row: PolymarketMarketMetadataRow=serde_json::from_value(json!({
            "metadata_row_kind":"MetadataSnapshot","timestamp_received":received,
            "collector_admission_sequence":sequence,"collector_admission_time_utc_ms":admitted,
            "metadata_origin":"GammaMarketsDiscovery","collector_host_id":"fixture-collector",
            "collector_region":"test","schema_version":"polymarket_market_metadata_v5","slug":"btc-updown-5m-fixture",
            "condition_id":format!("0x{}","11".repeat(32)),"clob_market_id":"123","asset_ids":"[\"1\",\"2\"]",
            "market_start_utc":3000,"market_end_utc":303000,"fixed_identity_verified_at_utc":received-2,
            "market_lifecycle_state":"Prefetched","market_runtime_role":"Next",
            "source_endpoint":"https://gamma-api.polymarket.com/markets","source_evidence_origin":"GammaMarketsDiscovery",
            "source_evidence_event_type":"GammaMarketsDiscoveryResponse","source_evidence_received_at_utc":received,
            "row_evidence_validation_completed_at_utc":admitted,"source_evidence_payload_json":payload,
            "source_evidence_raw_sha256":format!("{:x}",Sha256::digest(payload.as_bytes())),"metadata_fetch_start_utc":received-10,
            "closed":false,"outcomes_raw":"[\"UP\",\"DOWN\"]","outcome_prices_raw":"[\"0.5\",\"0.5\"]","active":true,"accepting_orders":true
        })).unwrap();
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(row)))
    }
    #[test]
    fn late_admitted_seed_follows_earlier_normal_metadata_in_actual_core() {
        let seed = metadata(2000, 4000, 1);
        let normal = metadata(3100, 3200, 2);
        let mut active = BTreeMap::from([
            (
                usize::MAX,
                VecDeque::from([(4000, native_order(&seed).unwrap(), seed)]),
            ),
            (
                0,
                VecDeque::from([(3200, native_order(&normal).unwrap(), normal)]),
            ),
        ]);
        let mut core = PublicObservationCoreV1::new(PublicInputStreamHeaderV1 {
            input_stream_id: "direct-probability-boundary-v1".into(),
            canonical_rule_version: "direct-probability-v1".into(),
            observation_version: "public-observation-v1".into(),
            ordering_version: "causal-source-projection-v1".into(),
            input_scope: "metadata-and-terminal-only".into(),
            max_pending_ws_terminals: 65536,
            initial_state: PublicInitialStateV1::Empty,
        })
        .unwrap();
        let mut output = Vec::new();
        let mut position = 0;
        let (id, at) = next_market_queue(&active).unwrap();
        assert_eq!((id, at), (0, 3200));
        let (_, _, row) = active.remove(&id).unwrap().pop_front().unwrap();
        apply_market(&mut core, &mut output, &mut position, at, row).unwrap();
        let facts: Vec<Value> = String::from_utf8(output.clone())
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert!(facts.iter().any(|v| {
            v["at"] == 3200
                && v["payload"]["MarketFact"]["payload"]
                    .get("FixedIdentity")
                    .is_some()
        }));
        let (id, at) = next_market_queue(&active).unwrap();
        assert_eq!((id, at), (usize::MAX, 4000));
        let (_, _, row) = active.remove(&id).unwrap().pop_front().unwrap();
        apply_market(&mut core, &mut output, &mut position, at, row).unwrap();
    }
}
