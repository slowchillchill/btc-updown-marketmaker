//! 当前概率研究复用公共类型化元数据/终局归约。
//! 复用公共精确数值规范化、临时参考规则及深度数量状态。
//! 常驻前台边界进程不生成观察、录制、SIM 或账本。
use contracts::*;
use data_pipeline_core::{
    ContractError,
    pm_reduction::PmEvidenceV1,
    pm_reduction::PreparedPmEvidenceV1,
    public_input::*,
    public_reference::*,
    reduction::{
        BinanceDepthReducer, PendingBusinessIdentityV1, PolymarketOrderbookInputV1,
        PolymarketReductionBatch, PolymarketTerminalContextV1, PolymarketTerminalMerger,
        PolymarketTerminalMetadataInputV1,
    },
    typed_raw::*,
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
    #[serde(default)]
    semantic_projection: Option<String>,
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
    #[serde(default)]
    catalog: Option<String>,
    #[serde(default)]
    clock_projection: Option<String>,
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
struct RawMarketCensus {
    condition_id: String,
    raw_first_seen_at: i64,
    raw_rows: usize,
    variants: Vec<RawMarketVariant>,
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
            });
        census.raw_rows += 1;
        census.raw_first_seen_at = census.raw_first_seen_at.min(coordinate.visible_at);
        if let Some((market, mut asset_ids)) = fixed {
            asset_ids.sort();
            if let Some(v) = census
                .variants
                .iter_mut()
                .find(|v| v.market == market && v.asset_ids == asset_ids)
            {
                if coordinate.visible_at < v.first.visible_at {
                    v.first = coordinate.clone();
                }
                if coordinate.visible_at >= v.last.visible_at {
                    v.last = coordinate.clone();
                }
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
// SQL只发送研究实际字段；完整生产行只用于既有初始化和WS终局读取。
enum ResearchPayload {
    Metadata(Box<PolymarketTerminalMetadataFieldsV1>),
    Anchor {
        market: MarketIdentityV2,
        assets: Vec<String>,
        fields: MarketReferenceResponseFieldsV1,
    },
    Ws(Box<PolymarketOrderbookRow>),
}
struct ResearchRow {
    payload: ResearchPayload,
    source: SourceEvidenceRefV3,
    metadata_order: Option<(i64, u64)>,
}
impl ResearchRow {
    fn from_typed(row: TypedRawRowV2) -> Result<Self, Error> {
        let source = source_evidence_v3_from_typed_raw_v2(&row)?;
        let metadata_order = match &row {
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
        let payload = match row {
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(r)) => {
                ResearchPayload::Metadata(Box::new((&*r).into()))
            }
            TypedRawRowV2::PolymarketMarketMetadataV6(r) => match *r {
                PolymarketMarketMetadataV6Input::Existing(r) => {
                    ResearchPayload::Metadata(Box::new((&r).into()))
                }
                PolymarketMarketMetadataV6Input::AnchorRule(r) => {
                    let (market, assets) = metadata_fixed(
                        &r.condition_id,
                        &r.clob_market_id,
                        r.market_start_utc,
                        r.market_end_utc,
                        &r.asset_ids,
                    )?;
                    ResearchPayload::Anchor {
                        market,
                        assets,
                        fields: (&r).into(),
                    }
                }
            },
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r))
                if matches!(r.event, PolymarketOrderbookEvent::MarketResolved { .. }) =>
            {
                ResearchPayload::Ws(Box::new(r))
            }
            _ => return Err("unexpected research semantic row".into()),
        };
        Ok(Self {
            payload,
            source,
            metadata_order,
        })
    }
    fn from_projection(mut row: Value) -> Result<Self, Error> {
        let version = row["schema_version"].as_str().ok_or("semantic schema")?;
        if !matches!(
            version,
            "polymarket_market_metadata_v5" | "polymarket_market_metadata_v6"
        ) {
            return Err("unsupported semantic schema".into());
        }
        let admitted = row["collector_admission_time_utc_ms"]
            .as_i64()
            .ok_or("semantic admission")?;
        let sequence = row["collector_admission_sequence"]
            .as_u64()
            .ok_or("semantic sequence")?;
        if admitted <= 0 || sequence == 0 {
            return Err("invalid semantic admission".into());
        }
        let metadata_order = Some((admitted, sequence));
        if row["metadata_row_kind"] == "market_anchor_rule_update" {
            let source = market_anchor_source_evidence_v3_from_fields(&serde_json::from_value::<
                MarketAnchorSourceIdentityInputV1,
            >(row.clone())?)?;
            let (market, assets) = metadata_fixed(
                row["condition_id"].as_str().ok_or("condition")?,
                row["clob_market_id"].as_str().ok_or("clob")?,
                row["market_start_utc"].as_i64().ok_or("start")?,
                row["market_end_utc"].as_i64().ok_or("end")?,
                row["asset_ids"].as_str().ok_or("assets")?,
            )?;
            return Ok(Self {
                payload: ResearchPayload::Anchor {
                    market,
                    assets,
                    fields: serde_json::from_value(row)?,
                },
                source,
                metadata_order,
            });
        }
        // 与原V6 Existing读取完全相同的来源ID规范化，不伪造交易字段。
        row["schema_version"] = json!(POLYMARKET_MARKET_METADATA_SCHEMA);
        macro_rules! enum_field {
            ($name:literal,$kind:ty) => {
                if let Some(raw) = row[$name].as_str() {
                    row[$name] = serde_json::to_value(<$kind>::try_from(raw)?)?;
                }
            };
        }
        enum_field!("metadata_row_kind", PolymarketMetadataRowKind);
        enum_field!("metadata_origin", PolymarketMetadataOrigin);
        enum_field!("market_lifecycle_state", MetadataMarketLifecycleState);
        enum_field!("market_runtime_role", MetadataMarketRuntimeRole);
        enum_field!("source_evidence_origin", MetadataSourceEvidenceOrigin);
        enum_field!(
            "source_evidence_event_type",
            MetadataSourceEvidenceEventType
        );
        enum_field!("source_evidence_channel", MetadataSourceEvidenceChannel);
        enum_field!(
            "market_lifecycle_transition",
            MetadataMarketLifecycleTransition
        );
        enum_field!(
            "market_runtime_role_transition",
            MetadataMarketRuntimeRoleTransition
        );
        enum_field!(
            "closed_time_check_failure_reason",
            MetadataClosedTimeCheckFailureReason
        );
        enum_field!(
            "resolution_evidence_source",
            MetadataResolutionEvidenceSource
        );
        enum_field!("terminal_claim_owner", MetadataTerminalClaimOwner);
        enum_field!("resolution_poll_result", MetadataResolutionPollResult);
        enum_field!(
            "resolution_integrity_error_class",
            MetadataResolutionIntegrityErrorClass
        );
        enum_field!(
            "resolution_followup_status",
            MetadataResolutionFollowupStatus
        );
        let source = metadata_source_evidence_v3_from_fields(&serde_json::from_value::<
            data_pipeline_core::MetadataSourceIdentityInputV1,
        >(row.clone())?)?;
        if let (Some(payload), Some(digest)) = (
            row["source_evidence_payload_json"].as_str(),
            row["source_evidence_raw_sha256"].as_str(),
        ) {
            use sha2::{Digest, Sha256};
            if format!("{:x}", Sha256::digest(payload.as_bytes())) != digest {
                return Err("metadata payload digest mismatch".into());
            }
        }
        Ok(Self {
            payload: ResearchPayload::Metadata(Box::new(serde_json::from_value(row)?)),
            source,
            metadata_order,
        })
    }
}
// 研究只依赖真实映射、公共参考和公共终局合并，不启动生产观察Core。
#[derive(Default)]
struct ResearchMarkets {
    terminals:
        BTreeMap<String, PolymarketTerminalMerger<PreparedPmEvidenceV1<SourceEvidenceRefV3>>>,
    ws_metadata:
        BTreeMap<(String, String, u64), (PolymarketTerminalMetadataFieldsV1, SourceEvidenceRefV3)>,
    ws_rows: BTreeMap<(String, String, u64), (PolymarketOrderbookRow, SourceEvidenceRefV3)>,
    snapshot_known: BTreeMap<String, i64>,
    catalog_loaded: bool,
}
impl ResearchMarkets {
    fn write_terminal(
        out: &mut impl Write,
        at: i64,
        condition: &str,
        batch: PolymarketReductionBatch<
            PreparedPmEvidenceV1<SourceEvidenceRefV3>,
            PendingBusinessIdentityV1,
        >,
    ) -> Result<(), Error> {
        for event in batch.events {
            match event.payload {
                ExecutionMarketEventPayloadV3::MarketResolutionTerminal(terminal) => {
                    writeln!(
                        out,
                        "{}",
                        json!({"research_label":{"condition_id":condition,"at":at.max(event.time.received_at_utc_ms),"terminal":terminal,"quality":event.quality.availability}})
                    )?;
                }
                ExecutionMarketEventPayloadV3::QualityBoundary(q)
                    if q.current_quality == MarketDataQuality::Invalid =>
                {
                    writeln!(
                        out,
                        "{}",
                        json!({"research_label":{"condition_id":condition,"at":at,"terminal":null,"quality":"invalid","reason":q.reason_code}})
                    )?;
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn apply(
        &mut self,
        out: &mut impl Write,
        clock: i64,
        row: ResearchRow,
        census: &mut ResearchCensus,
        origin: ResearchOrigin,
        pending_limit: usize,
    ) -> Result<(), Error> {
        let source = row.source;
        let at = clock.max(source.received_at_utc_ms).max(
            source
                .collector_admission
                .as_ref()
                .map_or(i64::MIN, |a| a.admitted_at_utc_ms),
        );
        let coordinate = ResearchCoordinate {
            visible_at: at,
            received_at: source.received_at_utc_ms,
            source_message_id: source.source_message_id.0.clone(),
            origin,
        };
        let metadata = match &row.payload {
            ResearchPayload::Metadata(r) => Some(r.as_ref()),
            _ => None,
        };
        if let Some(r) = metadata {
            let (market, assets) = metadata_fixed(
                &r.condition_id,
                &r.clob_market_id,
                r.market_start_utc,
                r.market_end_utc,
                &r.asset_ids,
            )?;
            if !self.catalog_loaded
                || matches!(coordinate.origin, ResearchOrigin::Initialization { .. })
            {
                census.observe(&r.condition_id, Some((market.clone(), assets)), &coordinate);
            }
            if r.metadata_row_kind == PolymarketMetadataRowKind::MetadataSnapshot {
                self.snapshot_known
                    .entry(r.condition_id.clone())
                    .or_insert(at);
                writeln!(
                    out,
                    "{}",
                    json!({"research_snapshot_observed":{"condition_id":r.condition_id,"at":at}})
                )?;
            }
            let outcomes = r.outcome_evidence()?;
            if source.is_metadata_identity()
                && outcomes.len() == 2
                && outcomes.iter().any(|o| {
                    o.outcome
                        .as_ref()
                        .is_some_and(|s| s.trim().eq_ignore_ascii_case("up"))
                })
                && outcomes.iter().any(|o| {
                    o.outcome
                        .as_ref()
                        .is_some_and(|s| s.trim().eq_ignore_ascii_case("down"))
                })
            {
                let mapping: Vec<_> = outcomes
                    .iter()
                    .enumerate()
                    .map(|(i, o)| AssetIdentityV1 {
                        token_id: TokenId(o.asset_id.clone()),
                        outcome: OutcomeIdentityV1 {
                            index: i as u32,
                            label: o.outcome.clone().unwrap(),
                        },
                    })
                    .collect();
                if !self.terminals.contains_key(&r.condition_id) {
                    self.terminals.insert(
                        r.condition_id.clone(),
                        PolymarketTerminalMerger::from_terminal_context(
                            PolymarketTerminalContextV1 {
                                market: market.clone(),
                                assets: mapping.clone(),
                                identity_source_evidence: vec![PreparedPmEvidenceV1::prepare(
                                    source.clone(),
                                    None,
                                )?],
                            },
                            "direct-probability-v1",
                        )?,
                    );
                    writeln!(
                        out,
                        "{}",
                        json!({"research_market":{"identity":market,"mapping_known_at":at,"production_identity_not_recomputed":true,"asset_ids":mapping.iter().map(|a|&a.token_id.0).collect::<Vec<_>>(),"asset_outcomes":mapping,"source_ids":[source.source_message_id],"snapshot_row_at":self.snapshot_known.get(&r.condition_id)}})
                    )?;
                }
            }
            if self.terminals.contains_key(&r.condition_id)
                && r.resolution_followup_status
                    == Some(MetadataResolutionFollowupStatus::TerminalGammaFallback)
            {
                let batch = self
                    .terminals
                    .get_mut(&r.condition_id)
                    .unwrap()
                    .compute_gamma_terminal_from_fields(
                        PolymarketTerminalMetadataInputV1 {
                            row: r.clone(),
                            evidence: PreparedPmEvidenceV1::prepare(source.clone(), None)?,
                        },
                        &outcomes,
                    )?;
                Self::write_terminal(out, at, &r.condition_id, batch)?;
            }
            if r.metadata_row_kind == PolymarketMetadataRowKind::ResolutionTerminalWs {
                let key = (
                    r.condition_id.clone(),
                    r.ws_claimant_source_sequence_epoch_id
                        .clone()
                        .ok_or("WS metadata epoch")?,
                    r.ws_claimant_collector_admission_sequence
                        .ok_or("WS metadata sequence")?,
                );
                self.ws_metadata.insert(key, (r.clone(), source));
            }
        } else if let ResearchPayload::Anchor {
            market,
            assets,
            fields,
        } = &row.payload
        {
            if !self.catalog_loaded
                || matches!(coordinate.origin, ResearchOrigin::Initialization { .. })
            {
                census.observe(
                    &market.condition_id.0,
                    Some((market.clone(), assets.clone())),
                    &coordinate,
                );
            }
            for event in canonicalize_market_reference_response_from_fields_v1(
                fields,
                market,
                &source,
                "direct-probability-v1",
            )? {
                writeln!(
                    out,
                    "{}",
                    json!({"research_reference":{"at":at.max(event.time.received_at_utc_ms),"payload":event.payload}})
                )?;
            }
        } else if let ResearchPayload::Ws(r) = row.payload {
            census.observe(&r.market, None, &coordinate);
            self.ws_rows.insert(
                (
                    r.market.clone(),
                    r.source_sequence_epoch_id.clone(),
                    r.collector_admission_sequence,
                ),
                (*r, source),
            );
        }
        let ready: Vec<_> = self
            .ws_metadata
            .keys()
            .filter(|k| self.ws_rows.contains_key(*k) && self.terminals.contains_key(&k.0))
            .cloned()
            .collect();
        for key in ready {
            let (r, e) = self.ws_metadata.remove(&key).unwrap();
            let (ws, wse) = self.ws_rows.remove(&key).unwrap();
            let outcomes = r.outcome_evidence()?;
            let batch = self
                .terminals
                .get_mut(&key.0)
                .unwrap()
                .compute_ws_terminal_from_fields(
                    PolymarketTerminalMetadataInputV1 {
                        row: r,
                        evidence: PreparedPmEvidenceV1::prepare(e, None)?,
                    },
                    &outcomes,
                    PolymarketOrderbookInputV1 {
                        row: ws,
                        evidence: PreparedPmEvidenceV1::prepare(wse, None)?,
                    },
                )?;
            Self::write_terminal(out, at, &key.0, batch)?;
        }
        if self.ws_metadata.len() + self.ws_rows.len() > pending_limit {
            return Err("research terminal pending limit exceeded".into());
        }
        Ok(())
    }
}
type MarketQueued = (i64, (i64, u64, String), ResearchRow, ResearchOrigin);
fn native_order(row: &TypedRawRowV2) -> Result<(i64, u64, String), Error> {
    research_order(&ResearchRow::from_typed(row.clone())?)
}
fn research_order(row: &ResearchRow) -> Result<(i64, u64, String), Error> {
    if let Some((admitted, sequence)) = row.metadata_order {
        Ok(metadata_admission_order_v1(
            admitted,
            sequence,
            &row.source.source_message_id,
        ))
    } else {
        let admission = row
            .source
            .collector_admission
            .as_ref()
            .ok_or("terminal admission")?;
        Ok((
            0,
            admission.collector_admission_sequence,
            row.source.source_message_id.0.clone(),
        ))
    }
}

fn market_file(file: MarketFile) -> Result<VecDeque<MarketQueued>, Error> {
    let mut result = Vec::new();
    let mut clocks = file.clocks.into_iter();
    let mut coordinates = file.row_ids.into_iter();
    let rows: Vec<ResearchRow> = if let Some(path) = file.semantic_projection {
        use sha2::{Digest, Sha256};
        let content = std::fs::read(path)?;
        if content.len() as u64 != file.file.bytes
            || format!("{:x}", Sha256::digest(&content)) != file.file.sha256
        {
            return Err("semantic projection identity mismatch".into());
        }
        let rows = std::str::from_utf8(&content)?
            .lines()
            .map(|line| ResearchRow::from_projection(serde_json::from_str(line)?))
            .collect::<Result<Vec<_>, _>>()?;
        if rows.len() as u64 != file.file.rows {
            return Err("semantic projection rows mismatch".into());
        }
        rows
    } else {
        let mut rows = Vec::new();
        for batch in open_finite_public_input_v1(&file.file, 1024, 64 * 1024 * 1024)? {
            for row in batch? {
                rows.push(ResearchRow::from_typed(row)?);
            }
        }
        rows
    };
    for row in rows {
        result.push((
            clocks.next().ok_or("missing legal clock")?,
            research_order(&row)?,
            row,
            ResearchOrigin::File {
                path: file.raw_path.clone(),
                row: coordinates.next().ok_or("missing raw coordinate")?,
            },
        ));
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
    let mut depth_missing: Option<Value> = None;
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
                            json!({"at":input.at,"bid":totals.as_ref().map(|v|&v.0),"ask":totals.as_ref().map(|v|&v.1),"missing":depth_missing.as_ref().cloned().or_else(||totals.is_none().then(||json!({"start":null,"end":null,"reason":"no_quantity_snapshot_observed","coordinates":null})))})
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
                                depth.apply_quantity_snapshot(&row, evidence)?;
                                if let Some(mut gap) = depth_missing.take() {
                                    gap["end"] = json!(input.at);
                                    writeln!(out, "{}", json!({"depth_missing_interval":gap}))?;
                                }
                            }
                            TypedRawRow::BinanceDepthUpdate(row) => {
                                let declared_missing = row.stitch_verdict
                                    == BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable;
                                if declared_missing || depth_missing.is_some() {
                                    depth.invalidate_quantity_state(&row)?;
                                    if depth_missing.is_none() {
                                        depth_missing = Some(
                                            json!({"start":input.at,"reason":"source_snapshot_unavailable","end":null,"coordinates":{"epoch":row.source_sequence_epoch_id,"generation":row.connection_generation,"admission":row.collector_admission_sequence,"ingress":row.depth_ingress_sequence,"received":row.timestamp_received,"snapshot_last_update_id":row.snapshot_last_update_id,"local_u_before":row.local_u_before,"first_update_id":row.first_update_id,"final_update_id":row.final_update_id,"stitch_verdict":row.stitch_verdict}}),
                                        );
                                    }
                                } else if let Err(error) =
                                    depth.apply_quantity_update(&row, evidence)
                                {
                                    match &error {
                                        ContractError::InvalidField { field, .. }
                                            if field
                                                == "binance_depth_update.stitch_coordinates"
                                                || field
                                                    == "binance_depth_update.stitch_verdict" =>
                                        {
                                            depth.invalidate_quantity_state(&row)?;
                                            depth_missing = Some(
                                                json!({"start":input.at,"reason":error.to_string(),"end":null,"coordinates":{"epoch":row.source_sequence_epoch_id,"generation":row.connection_generation,"admission":row.collector_admission_sequence,"ingress":row.depth_ingress_sequence,"received":row.timestamp_received,"snapshot_last_update_id":row.snapshot_last_update_id,"local_u_before":row.local_u_before,"first_update_id":row.first_update_id,"final_update_id":row.final_update_id,"stitch_verdict":row.stitch_verdict}}),
                                            );
                                        }
                                        _ => return Err(error.into()),
                                    }
                                } else if row.stitch_verdict
                                    == BinanceDepthStitchVerdict::NotAppliedConfirmedHole
                                {
                                    depth_missing = Some(
                                        json!({"start":input.at,"reason":"confirmed_depth_hole","end":null,"coordinates":{"epoch":row.source_sequence_epoch_id,"generation":row.connection_generation,"admission":row.collector_admission_sequence,"ingress":row.depth_ingress_sequence,"received":row.timestamp_received,"snapshot_last_update_id":row.snapshot_last_update_id,"local_u_before":row.local_u_before,"first_update_id":row.first_update_id,"final_update_id":row.final_update_id,"stitch_verdict":row.stitch_verdict}}),
                                    );
                                }
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
                for line in BufReader::new(File::open(
                    config.clock_projection.ok_or("metadata clock projection")?,
                )?)
                .lines()
                {
                    let mut row: Value = serde_json::from_str(&line?)?;
                    let file_id = row["file_id"].as_i64().ok_or("clock file")?;
                    let row_id = row["row_id"].as_u64().ok_or("clock row")?;
                    let received = row["timestamp_received"].as_i64().ok_or("clock receive")?;
                    let admitted = row["collector_admission_time_utc_ms"]
                        .as_i64()
                        .ok_or("clock admission")?;
                    let sequence = row["collector_admission_sequence"]
                        .as_u64()
                        .ok_or("clock sequence")?;
                    let source = if row["metadata_row_kind"] == "market_anchor_rule_update" {
                        market_anchor_source_evidence_v3_from_fields(&serde_json::from_value::<
                            MarketAnchorSourceIdentityInputV1,
                        >(
                            row
                        )?)?
                    } else {
                        // 完全复用原V6 Existing物理reader的V5来源身份规范化。
                        row["schema_version"] = json!(POLYMARKET_MARKET_METADATA_SCHEMA);
                        row["metadata_row_kind"] =
                            serde_json::to_value(PolymarketMetadataRowKind::try_from(
                                row["metadata_row_kind"].as_str().ok_or("clock kind")?,
                            )?)?;
                        if let Some(raw) = row["source_evidence_origin"].as_str() {
                            row["source_evidence_origin"] =
                                serde_json::to_value(MetadataSourceEvidenceOrigin::try_from(raw)?)?;
                        }
                        metadata_source_evidence_v3_from_fields(&serde_json::from_value::<
                            data_pipeline_core::MetadataSourceIdentityInputV1,
                        >(row)?)?
                    };
                    writeln!(
                        out,
                        "{}",
                        json!({"file_id":file_id,"row_id":row_id,"received":received,"admitted":admitted,"sequence":sequence,"source_message_id":source.source_message_id})
                    )?;
                }
                out.flush()?;
                output(json!({"done":true}))?;
            }
            Some("markets") => {
                let config: Markets =
                    serde_json::from_reader(File::open(cmd["input"].as_str().ok_or("input")?)?)?;
                let mut research_markets = ResearchMarkets::default();
                let pending_limit = cmd["max_pending_ws_terminals"]
                    .as_u64()
                    .ok_or("pending limit")? as usize;
                let mut out =
                    BufWriter::new(File::create(cmd["output"].as_str().ok_or("output")?)?);
                let mut research = ResearchCensus::default();
                if let Some(path) = &config.catalog {
                    for line in BufReader::new(File::open(path)?).lines() {
                        let row: Value = serde_json::from_str(&line?)?;
                        let condition = row["condition_id"].as_str().ok_or("catalog condition")?;
                        let coordinate = ResearchCoordinate {
                            visible_at: row["visible_at"].as_i64().ok_or("catalog clock")?,
                            received_at: row["received_at"].as_i64().ok_or("catalog receive")?,
                            source_message_id: row["source_message_id"]
                                .as_str()
                                .ok_or("catalog source")?
                                .into(),
                            origin: ResearchOrigin::File {
                                path: row["path"].as_str().ok_or("catalog path")?.into(),
                                row: row["row_id"].as_u64().ok_or("catalog row")? as usize,
                            },
                        };
                        research.observe(
                            condition,
                            Some(metadata_fixed(
                                condition,
                                row["clob_market_id"].as_str().ok_or("catalog clob")?,
                                row["start"].as_i64().ok_or("catalog start")?,
                                row["end_time"].as_i64().ok_or("catalog end")?,
                                row["asset_ids"].as_str().ok_or("catalog assets")?,
                            )?),
                            &coordinate,
                        );
                        if row["metadata_row_kind"] == "metadata_snapshot" {
                            research_markets
                                .snapshot_known
                                .entry(condition.into())
                                .or_insert(coordinate.visible_at);
                            writeln!(
                                out,
                                "{}",
                                json!({"research_snapshot_observed":{"condition_id":condition,"at":coordinate.visible_at}})
                            )?;
                        }
                    }
                    research_markets.catalog_loaded = true;
                }

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
                        ResearchRow::from_typed(row)?,
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
                    research_markets.apply(
                        &mut out,
                        clock,
                        row,
                        &mut research,
                        origin,
                        pending_limit,
                    )?;
                }
                for raw in research.0.values() {
                    let mut body = serde_json::to_value(raw)?;
                    body["production_identity_not_recomputed"] = json!(true);
                    body["missing_identity_snapshot"] = Value::Null;
                    writeln!(out, "{}", json!({"research_census":body}))?;
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
    fn research_mapping_without_production_snapshot_or_tick() {
        let TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(mut row)) =
            metadata(3100, 3101, 1)
        else {
            unreachable!()
        };
        row.metadata_row_kind = PolymarketMetadataRowKind::ResolutionPollAttempt;
        row.metadata_origin = PolymarketMetadataOrigin::GammaBySlug;
        row.source_evidence_origin = Some(MetadataSourceEvidenceOrigin::GammaBySlug);
        row.source_evidence_event_type =
            Some(MetadataSourceEvidenceEventType::GammaMarketBySlugResponse);
        let payload = r#"{"orderPriceMinTickSize":"invalid-not-used"}"#;
        row.source_evidence_payload_json = Some(payload.into());
        row.source_evidence_raw_sha256 = Some(format!("{:x}", Sha256::digest(payload.as_bytes())));
        row.fixed_identity_verified_at_utc = 0;
        assert!(
            row.validate().is_err(),
            "production validator still rejects this transaction-ineligible row"
        );
        let condition = row.condition_id.clone();
        let mut research = ResearchMarkets::default();
        let mut census = ResearchCensus::default();
        let mut out = Vec::new();
        research
            .apply(
                &mut out,
                3101,
                ResearchRow::from_typed(TypedRawRowV2::Existing(
                    TypedRawRow::PolymarketMarketMetadata(row),
                ))
                .unwrap(),
                &mut census,
                ResearchOrigin::File {
                    path: "synthetic-mapping".into(),
                    row: 0,
                },
                10,
            )
            .unwrap();
        assert!(research.terminals.contains_key(&condition));
        assert!(!research.snapshot_known.contains_key(&condition));
        let facts: Vec<Value> = String::from_utf8(out)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(
            facts[0]["research_market"]["asset_outcomes"][0]["outcome"]["label"],
            "UP"
        );
        assert!(facts.iter().all(|v| v.get("research_label").is_none()));
    }
}
