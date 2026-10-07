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
use serde::{Deserialize, Serialize};
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
    raw_path: String,
    row_ids: Vec<usize>,
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
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ResearchOrigin {
    File {
        path: String,
        row: usize,
    },
    Initialization {
        selected_ordinal: usize,
        binding: Option<SourceArtifactBindingV1>,
    },
}
#[derive(Clone, Debug, Serialize)]
struct ResearchCoordinate {
    visible_at: i64,
    received_at: i64,
    source_message_id: String,
    origin: ResearchOrigin,
}
#[derive(Serialize)]
struct RawMarketVariant {
    market: MarketIdentityV2,
    asset_ids: Vec<String>,
    first: ResearchCoordinate,
    last: ResearchCoordinate,
}
#[derive(Serialize)]
struct MissingIdentity {
    reason: &'static str,
    rows: usize,
    row_kinds: BTreeMap<String, usize>,
    first: ResearchCoordinate,
    last: ResearchCoordinate,
}
#[derive(Serialize)]
struct RawMarketCensus {
    condition_id: String,
    raw_first_seen_at: i64,
    raw_rows: usize,
    variants: Vec<RawMarketVariant>,
    missing_identity_snapshot: Option<MissingIdentity>,
}
#[derive(Default)]
struct ResearchCensus(BTreeMap<String, RawMarketCensus>);
impl ResearchCensus {
    fn observe(
        &mut self,
        condition: &str,
        fixed: Option<(MarketIdentityV2, Vec<String>)>,
        coordinate: &ResearchCoordinate,
    ) {
        let census = self
            .0
            .entry(condition.to_owned())
            .or_insert_with(|| RawMarketCensus {
                condition_id: condition.to_owned(),
                raw_first_seen_at: coordinate.visible_at,
                raw_rows: 0,
                variants: Vec::new(),
                missing_identity_snapshot: None,
            });
        census.raw_rows += 1;
        if let Some((market, mut asset_ids)) = fixed {
            asset_ids.sort();
            if let Some(v) = census
                .variants
                .iter_mut()
                .find(|v| v.market == market && v.asset_ids == asset_ids)
            {
                v.last = coordinate.clone();
            } else {
                census.variants.push(RawMarketVariant {
                    market,
                    asset_ids,
                    first: coordinate.clone(),
                    last: coordinate.clone(),
                });
            }
        }
    }
    fn missing(
        &mut self,
        condition: &str,
        kind: PolymarketMetadataRowKind,
        coordinate: &ResearchCoordinate,
    ) {
        let census = self.0.get_mut(condition).expect("原始目录已记录当前行");
        let gap = census
            .missing_identity_snapshot
            .get_or_insert_with(|| MissingIdentity {
                reason: "missing_identity_snapshot",
                rows: 0,
                row_kinds: BTreeMap::new(),
                first: coordinate.clone(),
                last: coordinate.clone(),
            });
        gap.rows += 1;
        *gap.row_kinds.entry(format!("{kind:?}")).or_default() += 1;
        gap.last = coordinate.clone();
    }
    fn write(&self, out: &mut impl Write) -> Result<(), Error> {
        for census in self.0.values() {
            writeln!(out, "{}", json!({"research_census":census}))?;
        }
        Ok(())
    }
}
fn metadata_fixed(
    condition: &str,
    clob: &str,
    start: i64,
    end: i64,
    assets: &str,
) -> Result<(MarketIdentityV2, Vec<String>), Error> {
    Ok((
        MarketIdentityV2 {
            condition_id: ConditionId(condition.to_owned()),
            clob_market_id: ClobMarketId(clob.to_owned()),
            window: MarketWindowV2 {
                start_at_utc_ms: start,
                end_at_utc_ms: end,
            },
        },
        serde_json::from_str(assets)?,
    ))
}
fn apply_market(
    core: &mut PublicObservationCoreV1,
    out: &mut impl Write,
    position: &mut u64,
    clock: i64,
    row: TypedRawRowV2,
    research: &mut ResearchCensus,
    origin: ResearchOrigin,
) -> Result<(), Error> {
    let source = source_evidence_v3_from_typed_raw_v2(&row)?;
    let at = clock.max(source.received_at_utc_ms).max(
        source
            .collector_admission
            .as_ref()
            .map_or(i64::MIN, |c| c.admitted_at_utc_ms),
    );
    let coordinate = ResearchCoordinate {
        visible_at: at,
        received_at: source.received_at_utc_ms,
        source_message_id: source.source_message_id.0.clone(),
        origin,
    };
    let record = RecordedIngressV1 {
        input_stream_id: "direct-probability-boundary-v1".into(),
        input_position: *position,
        ingress: DptIngressV1::Message {
            row,
            source_evidence: source,
        },
        dependency_record_refs: vec![],
    };
    let prepared = prepare_public_ingress_v1(core, &record, &BTreeMap::new(), &[])?;
    // 原始目录只是事后覆盖证据；合法身份仍仅来自成功公共FixedIdentity。
    match &prepared.current {
        PreparedCanonicalInputV1::Metadata(metadata) => {
            let row = metadata.row;
            research.observe(
                &row.condition_id,
                Some(metadata_fixed(
                    &row.condition_id,
                    &row.clob_market_id,
                    row.market_start_utc,
                    row.market_end_utc,
                    &row.asset_ids,
                )?),
                &coordinate,
            );
            if !matches!(
                row.metadata_row_kind,
                PolymarketMetadataRowKind::MetadataSnapshot
                    | PolymarketMetadataRowKind::ResolutionTerminalWs
            ) && core.prepared_pm_context(&row.condition_id).is_none()
            {
                research.missing(&row.condition_id, row.metadata_row_kind, &coordinate);
                return Ok(());
            }
        }
        PreparedCanonicalInputV1::AnchorRule { input, .. } => {
            research.observe(
                &input.condition_id,
                Some(metadata_fixed(
                    &input.condition_id,
                    &input.clob_market_id,
                    input.market_start_utc,
                    input.market_end_utc,
                    &input.asset_ids,
                )?),
                &coordinate,
            );
        }
        PreparedCanonicalInputV1::PolymarketWsTerminal { row, .. } => {
            research.observe(&row.market, None, &coordinate)
        }
        _ => {}
    }
    // 跳过的普通元数据不进入公共应用序列，也不保存等待未来回放。
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
                    let mut line = json!({"at":at,"payload":fact.payload,"quality":fact.quality,
                        "dependency_received":fact.dependency_time.received_at_utc_ms});
                    if matches!(
                        fact.payload,
                        PublicBusinessPayloadV1::MarketFact {
                            payload: CanonicalMarketFactPayloadV1::FixedIdentity { .. },
                            ..
                        }
                    ) {
                        line["identity_source_ids"] = json!(
                            fact.lineage
                                .source_evidence
                                .iter()
                                .map(|e| &e.source_message_id)
                                .collect::<Vec<_>>()
                        );
                    }
                    writeln!(out, "{}", line)?;
                }
                _ => {}
            }
        }
    }
    *position += 1;
    Ok(())
}
type MarketQueued = (i64, (i64, u64, String), TypedRawRowV2, ResearchOrigin);
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
    let mut coordinates = file.row_ids.into_iter();
    for batch in open_finite_public_input_v1(&file.file, 1024, 64 * 1024 * 1024)? {
        for row in batch? {
            result.push((
                clocks.next().ok_or("missing legal clock")?,
                native_order(&row)?,
                row,
                ResearchOrigin::File {
                    path: file.raw_path.clone(),
                    row: coordinates.next().ok_or("missing raw coordinate")?,
                },
            ));
        }
    }
    if clocks.next().is_some() || coordinates.next().is_some() {
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
                let mut research = ResearchCensus::default();
                let init = MetadataInitializationSelectionV1::open(
                    &config.initialization,
                    1024,
                    64 * 1024 * 1024,
                )?;
                let mut seeds = Vec::new();
                let mut seed_clocks = config.initialization_clocks.into_iter();
                for (seed_id, row) in init.read(1024, 64 * 1024 * 1024).enumerate() {
                    let (row, binding) = row?;
                    seeds.push((
                        seed_clocks.next().ok_or("seed clock")?,
                        native_order(&row)?,
                        row,
                        ResearchOrigin::Initialization {
                            selected_ordinal: seed_id,
                            binding,
                        },
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
                    let (clock, _, row, origin) = rows.pop_front().unwrap();
                    if rows.is_empty() {
                        active.remove(&id);
                    }
                    apply_market(
                        &mut core,
                        &mut out,
                        &mut position,
                        clock,
                        row,
                        &mut research,
                        origin,
                    )?;
                }
                research.write(&mut out)?;
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
                VecDeque::from([(
                    4000,
                    native_order(&seed).unwrap(),
                    seed,
                    ResearchOrigin::File {
                        path: "seed".into(),
                        row: 0,
                    },
                )]),
            ),
            (
                0,
                VecDeque::from([(
                    3200,
                    native_order(&normal).unwrap(),
                    normal,
                    ResearchOrigin::File {
                        path: "normal".into(),
                        row: 0,
                    },
                )]),
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
        let mut research = ResearchCensus::default();
        let (id, at) = next_market_queue(&active).unwrap();
        assert_eq!((id, at), (0, 3200));
        let (_, _, row, origin) = active.remove(&id).unwrap().pop_front().unwrap();
        apply_market(
            &mut core,
            &mut output,
            &mut position,
            at,
            row,
            &mut research,
            origin,
        )
        .unwrap();
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
        let (_, _, row, origin) = active.remove(&id).unwrap().pop_front().unwrap();
        apply_market(
            &mut core,
            &mut output,
            &mut position,
            at,
            row,
            &mut research,
            origin,
        )
        .unwrap();
    }
    #[test]
    fn missing_identity_census_keeps_legal_pending_and_actual_public_serde() {
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
        let mut research = ResearchCensus::default();
        let condition = format!("0x{}", "11".repeat(32));
        let TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(mut transition)) =
            metadata(3100, 3101, 1)
        else {
            unreachable!()
        };
        transition.metadata_row_kind = PolymarketMetadataRowKind::MarketStateTransition;
        transition.clob_market_id = "999".into();
        transition.metadata_origin = PolymarketMetadataOrigin::LocalMarketStateTimer;
        transition.market_lifecycle_state = MetadataMarketLifecycleState::Active;
        transition.market_runtime_role = MetadataMarketRuntimeRole::Current;
        transition.market_lifecycle_transition =
            Some(MetadataMarketLifecycleTransition::PrefetchedToActive);
        transition.market_runtime_role_transition =
            Some(MetadataMarketRuntimeRoleTransition::NextToCurrent);
        transition.lifecycle_applied_at_utc = Some(3100);
        transition.runtime_role_applied_at_utc = Some(3100);
        transition.source_endpoint = None;
        transition.source_endpoint_unavailable_reason =
            Some(MetadataSourceEndpointUnavailableReason::NotApplicableNoSourceEvidence);
        transition.source_evidence_origin = None;
        transition.source_evidence_event_type = None;
        transition.source_evidence_received_at_utc = None;
        transition.row_evidence_validation_completed_at_utc = None;
        transition.source_evidence_payload_json = None;
        transition.source_evidence_raw_sha256 = None;
        transition.metadata_fetch_start_utc = None;
        transition.closed = None;
        transition.active = None;
        transition.active_unavailable_reason =
            Some(MetadataActiveUnavailableReason::NotApplicableForRowKind);
        transition.accepting_orders = None;
        transition.outcomes_raw = None;
        transition.outcome_prices_raw = None;
        let mut original_core = PublicObservationCoreV1::new(core.header().clone()).unwrap();
        let original_row =
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(transition.clone()));
        let original_source = source_evidence_v3_from_typed_raw_v2(&original_row).unwrap();
        let original_record = RecordedIngressV1 {
            input_stream_id: "direct-probability-boundary-v1".into(),
            input_position: 0,
            ingress: DptIngressV1::Message {
                row: original_row,
                source_evidence: original_source,
            },
            dependency_record_refs: vec![],
        };
        let original_prepared =
            prepare_public_ingress_v1(&original_core, &original_record, &BTreeMap::new(), &[])
                .unwrap();
        assert!(matches!(original_core.push_reference_facts(&original_prepared),
            Err(data_pipeline_core::public_observation_core::DptCoreErrorV1::MetadataBeforeIdentitySnapshot)));
        apply_market(
            &mut core,
            &mut output,
            &mut position,
            3101,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(transition)),
            &mut research,
            ResearchOrigin::File {
                path: "fixture/transition".into(),
                row: 0,
            },
        )
        .unwrap();
        assert_eq!(position, 0);
        assert!(core.prepared_pm_context(&condition).is_none());
        let TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(mut known)) =
            metadata(3200, 3201, 2)
        else {
            unreachable!()
        };
        known.condition_id = format!("0x{}", "22".repeat(32));
        known.asset_ids = "[\"3\",\"4\"]".into();
        known.market_start_utc = 603000;
        known.market_end_utc = 903000;
        apply_market(
            &mut core,
            &mut output,
            &mut position,
            3201,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(known)),
            &mut research,
            ResearchOrigin::File {
                path: "fixture/known".into(),
                row: 0,
            },
        )
        .unwrap();
        assert_eq!(position, 1);
        let anchor:MarketAnchorRuleUpdateInput=serde_json::from_value(json!({
            "schema_version":"polymarket_market_metadata_v6","timestamp_received":3300,"collector_admission_sequence":3,
            "collector_admission_time_utc_ms":3301,"metadata_origin":"gamma_event_by_slug","collector_host_id":"fixture-collector","collector_region":"test",
            "slug":"btc-updown-5m-fixture","condition_id":condition,"clob_market_id":"123","asset_ids":"[\"1\",\"2\"]",
            "market_start_utc":3000,"market_end_utc":303000,"fixed_identity_verified_at_utc":1000,"market_lifecycle_state":"active","market_runtime_role":"current",
            "metadata_fetch_start_utc":3290,"anchor_query_source_epoch_id":"fixture-query","anchor_query_ordinal":1,"anchor_query_result":"observed",
            "price_to_beat_decimal":"100","price_to_beat_source_field":"eventMetadata.priceToBeat","twap_enabled":true,"twap_lookback_seconds":60,
            "http_received_at_utc":3300,"http_request_url":"https://gamma-api.polymarket.com/events/slug/fixture","http_body_sha256":"123"
        })).unwrap();
        apply_market(
            &mut core,
            &mut output,
            &mut position,
            3301,
            TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
                PolymarketMarketMetadataV6Input::AnchorRule(anchor),
            )),
            &mut research,
            ResearchOrigin::File {
                path: "fixture/anchor".into(),
                row: 0,
            },
        )
        .unwrap();
        assert_eq!(position, 2);
        assert!(core.prepared_pm_context(&condition).is_none());
        let TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(mut terminal)) =
            metadata(303010, 303012, 4)
        else {
            unreachable!()
        };
        terminal.metadata_row_kind = PolymarketMetadataRowKind::ResolutionTerminalWs;
        terminal.metadata_origin = PolymarketMetadataOrigin::MarketResolvedWs;
        terminal.market_lifecycle_state = MetadataMarketLifecycleState::Retired;
        terminal.market_runtime_role = MetadataMarketRuntimeRole::None;
        terminal.market_lifecycle_transition =
            Some(MetadataMarketLifecycleTransition::ActiveToRetired);
        terminal.market_runtime_role_transition =
            Some(MetadataMarketRuntimeRoleTransition::ResolutionPendingToNone);
        terminal.lifecycle_applied_at_utc = Some(303013);
        terminal.runtime_role_applied_at_utc = Some(303013);
        terminal.source_endpoint =
            Some("wss://ws-subscriptions-clob.polymarket.com/ws/market".into());
        terminal.source_evidence_origin = Some(MetadataSourceEvidenceOrigin::MarketResolvedWs);
        terminal.source_evidence_event_type = Some(MetadataSourceEvidenceEventType::MarketResolved);
        terminal.source_evidence_channel = Some(MetadataSourceEvidenceChannel::Market);
        terminal.source_evidence_received_at_utc = Some(303010);
        terminal.row_evidence_validation_completed_at_utc = Some(303011);
        terminal.source_evidence_payload_json = None;
        terminal.source_evidence_raw_sha256 = Some("a".repeat(64));
        terminal.metadata_fetch_start_utc = None;
        terminal.closed = None;
        terminal.active = None;
        terminal.active_unavailable_reason =
            Some(MetadataActiveUnavailableReason::NotApplicableForRowKind);
        terminal.accepting_orders = None;
        terminal.outcomes_raw = None;
        terminal.outcome_prices_raw = None;
        terminal.ws_platform_timestamp_raw = Some("303005".into());
        terminal.ws_platform_timestamp_utc = Some(303005);
        terminal.winning_asset_id = Some("1".into());
        terminal.winning_outcome = Some("UP".into());
        terminal.resolution_evidence_source =
            Some(MetadataResolutionEvidenceSource::MarketResolvedWs);
        terminal.terminal_claim_owner = Some(MetadataTerminalClaimOwner::MarketResolvedWs);
        terminal.resolution_budget_horizon_utc = Some(783000);
        terminal.resolution_followup_status = Some(MetadataResolutionFollowupStatus::TerminalWs);
        terminal.ws_claimant_source_sequence_epoch_id = Some("fixture-ws".into());
        terminal.ws_claimant_collector_admission_sequence = Some(1);
        apply_market(
            &mut core,
            &mut output,
            &mut position,
            303012,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(terminal)),
            &mut research,
            ResearchOrigin::File {
                path: "fixture/terminal-metadata".into(),
                row: 0,
            },
        )
        .unwrap();
        let ws = PolymarketOrderbookRow {
            timestamp_received: 303010,
            timestamp: Some(303005),
            market: condition.clone(),
            slug: Some("btc-updown-5m-fixture".into()),
            collector_host_id: "fixture-collector".into(),
            collector_region: "test".into(),
            source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
            schema_version: POLYMARKET_ORDERBOOK_SCHEMA.into(),
            source_sequence_epoch_id: "fixture-ws".into(),
            collector_admission_sequence: 1,
            collector_admission_time_utc_ms: 303012,
            event: PolymarketOrderbookEvent::MarketResolved {
                clob_market_id: "123".into(),
                question: None,
                description: None,
                asset_ids: vec!["1".into(), "2".into()],
                outcomes: vec![],
                event_message: None,
                winning_asset_id: "1".into(),
                winning_outcome: "UP".into(),
            },
        };
        apply_market(
            &mut core,
            &mut output,
            &mut position,
            303012,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(ws)),
            &mut research,
            ResearchOrigin::File {
                path: "fixture/ws".into(),
                row: 0,
            },
        )
        .unwrap();
        assert_eq!(core.pending_ws_terminal_count(), 1);
        assert!(core.prepared_pm_context(&condition).is_none());
        let before = String::from_utf8(output.clone()).unwrap();
        assert!(!before.contains("\"Terminal\""));
        let mut before_projection = output.clone();
        research.write(&mut before_projection).unwrap();
        let before_projection = String::from_utf8(before_projection).unwrap();
        apply_market(
            &mut core,
            &mut output,
            &mut position,
            304001,
            metadata(304000, 304001, 5),
            &mut research,
            ResearchOrigin::File {
                path: "fixture/late-snapshot".into(),
                row: 0,
            },
        )
        .unwrap();
        assert_eq!(position, 5);
        assert_eq!(core.pending_ws_terminal_count(), 0);
        assert!(core.prepared_pm_context(&condition).is_some());
        research.write(&mut output).unwrap();
        let actual = String::from_utf8(output).unwrap();
        let facts: Vec<Value> = actual
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let terminal = facts
            .iter()
            .find(|f| f["payload"]["MarketFact"]["payload"]["Terminal"].is_object())
            .unwrap();
        assert_eq!(terminal["at"], 304001);
        assert_eq!(
            terminal["payload"]["MarketFact"]["payload"]["Terminal"]["status"],
            "RESOLVED"
        );
        assert_eq!(
            research.0[&condition]
                .missing_identity_snapshot
                .as_ref()
                .unwrap()
                .rows,
            1
        );
        if let Ok(path) = std::env::var("PROBABILITY_SCENARIO_OUTPUT") {
            std::fs::write(&path, &actual).unwrap();
            std::fs::write(
                std::path::Path::new(&path).with_extension("before-identity.jsonl"),
                &before_projection,
            )
            .unwrap();
        } else {
            assert_eq!(
                actual,
                include_str!("../../../tests/probability/fixtures/public-market-scenario.jsonl")
            );
            assert_eq!(
                before_projection,
                include_str!(
                    "../../../tests/probability/fixtures/public-market-scenario.before-identity.jsonl"
                )
            );
        }
    }
}
