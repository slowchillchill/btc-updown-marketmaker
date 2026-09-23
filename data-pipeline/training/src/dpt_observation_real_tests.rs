//! 可显式执行的本地真实输入发布与缺失组件读取场景；不再要求证书或进程环境。
use crate::canonical_run::CanonicalRunScope;
use crate::gate_a::{
    CanonicalEventStoreCursor, EventTimeIndexCursor, MarketObservationCursor,
    ReductionReportCursor, VisibilityIndexCursor,
};
use crate::{
    PublishedGateAFinal, build_gate_a_publication, freeze_direct_source_input_set,
    gate_a_formal_canonical_config, gate_a_formal_output_config, open_published_gate_a_final,
    publish_gate_a_candidate,
};
use contracts::{CanonicalEventRecordV1, MarketDataQuality, ObservationComponentId};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
type AnyResult<T> = Result<T, Box<dyn std::error::Error>>;
const MANIFEST: &str =
    "/home/chillchill/new-datasets/2026-08-08/typed_raw/utc_day=2026-08-08/manifest.json";
const CONDITION: &str = "0xfdf68d373e45f59d9bb1ebc776b3a45eaaaf5f6845b52d9f51dae9bb90394169";
const START: i64 = 1_786_162_857_624;
#[test]
#[ignore = "需要本地 MS-REAL-001 原始输入；普通回归不运行外部数据场景"]
fn ms_real_001_publication_reads_missing_components() -> AnyResult<()> {
    let root = tempfile::TempDir::new()?;
    let input = freeze_direct_source_input_set(&[PathBuf::from(MANIFEST)])?;
    let scope = CanonicalRunScope::new(CONDITION, START, START + 1)?;
    let stage = root.path().join("stage");
    build_gate_a_publication(
        &input,
        &scope,
        &stage,
        &gate_a_formal_canonical_config(),
        &gate_a_formal_output_config(),
    )?;
    let published = publish_gate_a_candidate(&stage, &root.path().join("published"))?;
    let opened = open_published_gate_a_final(&published)?;
    read_all_published_facts(&opened, root.path())
}
fn read_all_published_facts(opened: &PublishedGateAFinal, run_root: &Path) -> AnyResult<()> {
    let candidate = opened.artifacts();
    let mut events = CanonicalEventStoreCursor::open(
        candidate.canonical_event_store.manifest_path(),
        1,
        16 * 1024 * 1024,
        256,
    )?;
    let mut event_index = EventTimeIndexCursor::open(
        candidate.event_time_index.manifest_path(),
        None,
        1,
        16 * 1024 * 1024,
        256,
    )?;
    let mut visibility = VisibilityIndexCursor::open(
        candidate.visibility_index.manifest_path(),
        None,
        1,
        16 * 1024 * 1024,
        256,
    )?;
    let mut observations = MarketObservationCursor::open(
        candidate.market_observations.manifest_path(),
        None,
        None,
        1,
        16 * 1024 * 1024,
        256,
    )?;
    let mut reductions = ReductionReportCursor::open(
        candidate.reduction_reports.manifest_path(),
        1,
        16 * 1024 * 1024,
        256,
    )?;
    let mut facts = BTreeMap::new();
    let mut records = Vec::new();
    loop {
        let page = events.next_page(1)?;
        if page.is_empty() {
            break;
        }
        for record in page {
            let time = match &record {
                CanonicalEventRecordV1::ExecutionMarketEvent(event) => &event.time,
                CanonicalEventRecordV1::ReferenceEvent(event) => &event.time,
            };
            assert_eq!(time.received_at_utc_ms, START);
            assert_eq!(time.source_event_at_utc_ms, None);
            records.push(serde_json::to_value(record)?);
        }
    }
    assert_eq!(
        records.len() as u64,
        candidate.canonical_event_store.manifest().file.rows
    );
    facts.insert("canonical_records", records);
    assert!(event_index.next_page(1)?.is_empty());
    facts.insert("event_time_index", Vec::new());
    let mut rows = Vec::new();
    loop {
        let page = visibility.next_page(1)?;
        if page.is_empty() {
            break;
        }
        rows.extend(
            page.into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    assert_eq!(
        rows.len() as u64,
        candidate.visibility_index.manifest().file.rows
    );
    facts.insert("visibility_index", rows);
    let mut rows = Vec::new();
    loop {
        let page = observations.next_page(1)?;
        if page.is_empty() {
            break;
        }
        for row in page {
            assert_eq!(row.visibility_cutoff.received_at_utc_ms, START);
            for id in [
                ObservationComponentId::PolymarketL2,
                ObservationComponentId::BestBidAsk,
                ObservationComponentId::ChainlinkBtcUsd,
                ObservationComponentId::BinanceBtcUsdtDepth,
                ObservationComponentId::BinanceBtcUsdtTrade,
            ] {
                let state = row
                    .component_states
                    .iter()
                    .find(|state| state.component == id)
                    .unwrap();
                assert_eq!(state.quality, MarketDataQuality::UnknownNotObserved);
                assert_eq!(state.last_visible_event_id, None);
                assert_eq!(state.received_at_utc_ms, None);
                assert_eq!(state.source_event_at_utc_ms, None);
                assert!(row.missing_mask.contains(&id));
            }
            rows.push(serde_json::to_value(row)?);
        }
    }
    assert_eq!(
        rows.len() as u64,
        candidate.market_observations.manifest().file.rows
    );
    facts.insert("observations", rows);
    let mut rows = Vec::new();
    loop {
        let page = reductions.next_page(1)?;
        if page.is_empty() {
            break;
        }
        rows.extend(
            page.into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    assert_eq!(
        rows.len() as u64,
        candidate.reduction_reports.manifest().file.rows
    );
    facts.insert("reduction_reports", rows);
    std::fs::write(
        run_root.join("published-readback-facts.json"),
        serde_json::to_vec(&facts)?,
    )?;
    Ok(())
}
