//! R3原材料的有限配对顺序反例；明确重编接纳位置，不声称历史实际顺序。
use super::*;
use data_pipeline_core::public_reduction::materialize_public_reduction_v1;
use std::path::Path;

const CONDITION: &str = "0xfdf68d373e45f59d9bb1ebc776b3a45eaaaf5f6845b52d9f51dae9bb90394169";

pub fn run(config: &Config, out: &Path, recording: &Path) -> RecordingResult<()> {
    let mut reader = PublicRecordingReaderV1::recover(
        recording,
        config.canonical.decode_batch_rows,
        config.canonical.max_decode_batch_bytes,
    )?;
    let (mut identity, mut metadata, mut ws) = (None, None, None);
    while let Some(row) = reader.next_row()? {
        let PublicStoredRowV1::Input(input) = row else {
            continue;
        };
        let DptIngressV1::Message {
            row: TypedRawRowV2::Existing(row),
            ..
        } = &input.ingress
        else {
            continue;
        };
        match row {
            TypedRawRow::PolymarketMarketMetadata(m) if m.condition_id == CONDITION => {
                match m.metadata_row_kind {
                    PolymarketMetadataRowKind::MetadataSnapshot if identity.is_none() => {
                        identity = Some(input)
                    }
                    PolymarketMetadataRowKind::ResolutionTerminalWs => metadata = Some(input),
                    _ => {}
                }
            }
            TypedRawRow::PolymarketOrderbook(m)
                if m.market == CONDITION
                    && matches!(m.event, PolymarketOrderbookEvent::MarketResolved { .. }) =>
            {
                ws = Some(input)
            }
            _ => {}
        }
    }
    let identity = identity.ok_or("R3固定身份材料缺失")?;
    let metadata = metadata.ok_or("R3终态元数据材料缺失")?;
    let ws = ws.ok_or("R3终态WS材料缺失")?;
    let mut output = BufWriter::new(File::create(out.join("terminal-orders.jsonl"))?);
    for (name, originals) in [
        ("identity-metadata-ws", [&identity, &metadata, &ws]),
        ("identity-ws-metadata", [&identity, &ws, &metadata]),
    ] {
        let mut core = PublicObservationCoreV1::new(config.header.clone())?;
        let mut terminal_records = Vec::new();
        let mut groups_per_input = Vec::new();
        let mut constructed = Vec::new();
        for (position, original) in originals.into_iter().enumerate() {
            let mut input = original.clone();
            input.input_stream_id = config.header.input_stream_id.clone();
            input.input_position = position as u64;
            let prepared = prepare_public_ingress_v1(&core, &input, &BTreeMap::new(), &[])?;
            let result = core.push(&prepared)?;
            groups_per_input.push(result.applied.closed_groups.len());
            // 三个实际输入及同次应用结果是此构造反例的有限证据，不另建记录流。
            constructed.push(
                serde_json::json!({"original_input_position": original.input_position,
                "constructed_input_position": position, "original_ingress": original.ingress,
                "applied": result.applied}),
            );
            for reduction in result.reductions {
                for record in materialize_public_reduction_v1(reduction)?.records {
                    if matches!(&record, CanonicalEventRecordV2::MarketFact(fact) if matches!(&fact.payload, CanonicalMarketFactPayloadV1::Terminal(_)))
                    {
                        terminal_records.push(record);
                    }
                }
            }
        }
        let end = RecordedIngressV1 {
            input_stream_id: config.header.input_stream_id.clone(),
            input_position: 3,
            ingress: DptIngressV1::EndOfInput,
            dependency_record_refs: vec![],
        };
        let prepared = prepare_public_ingress_v1(&core, &end, &BTreeMap::new(), &[])?;
        let ended = core.push(&prepared)?;
        write_line(
            &mut output,
            &serde_json::json!({"constructed_order": name,
            "groups_per_input": groups_per_input, "inputs": constructed,
            "terminal_records": terminal_records, "end": ended.applied}),
        )?;
    }
    output.flush()?;
    Ok(())
}
