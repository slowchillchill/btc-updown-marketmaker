//! 持续经营的公共报价变化引用；没有 BLB 请求消费者，不计算/保存 BLB 档位。
use super::streaming_research::{line, pm};
use backtest::{HistoricalPublicEvidenceV1, HistoricalPublicObserverV1, MaterialError};
use contracts::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
#[derive(Debug)]
struct State {
    writer: BufWriter<std::fs::File>,
    markets: BTreeMap<String, (Value, u64)>,
}
#[derive(Debug)]
pub(super) struct InventoryResearchV1 {
    root: PathBuf,
    state: Mutex<Option<State>>,
    next: AtomicUsize,
}
impl InventoryResearchV1 {
    pub fn open(root: &Path) -> Result<Self, MaterialError> {
        std::fs::create_dir(root)?;
        Ok(Self {
            root: root.into(),
            state: Mutex::new(None),
            next: AtomicUsize::new(0),
        })
    }
    fn observe_inner(
        &self,
        reference: &PublicGroupReferenceV1,
        group: &ClosedBusinessGroupV1,
        at: Option<i64>,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        let mut guard = self.state.lock().unwrap();
        let state = guard.as_mut().ok_or("持续经营观察器尚未绑定输入流")?;
        let mut links = serde_json::Map::new();
        let mut visit = |market: &MarketIdentityV2,
                         assets: &[AssetBusinessObservationV1]|
         -> Result<(), MaterialError> {
            let id = &market.condition_id.0;
            let mut value = pm(assets);
            for (projected, asset) in value
                .as_array_mut()
                .expect("两腿标量")
                .iter_mut()
                .zip(assets)
            {
                projected["bids_materialized"] = json!(asset.bids.value.is_some());
                projected["asks_materialized"] = json!(asset.asks.value.is_some());
            }
            let old = state.markets.get(id);
            let changed = old.is_none_or(|(old, _)| old != &value);
            let version = if changed {
                reference.group_position
            } else {
                old.unwrap().1
            };
            if changed {
                line(
                    &mut state.writer,
                    &json!({"record_type":"pm_state","group":reference,"visible_at_utc_ms":at,
                    "market":market,"version":version,"state":value}),
                )?;
                state.markets.insert(id.clone(), (value, version));
            }
            links.insert(
                id.clone(),
                json!({"input_stream_id":reference.input_stream_id,"pm_group":version}),
            );
            Ok(())
        };
        if let Some((markets, _)) = group.observations.shared_parts() {
            for m in markets.values() {
                visit(&m.market, &m.assets)?;
            }
        } else {
            for m in group.observations.iter() {
                visit(&m.market, &m.assets)?;
            }
        }
        Ok(Arc::new(HistoricalPublicEvidenceV1 {
            scalars: json!(links),
            request_evidence: BTreeMap::new(),
        }))
    }
}
impl HistoricalPublicObserverV1 for InventoryResearchV1 {
    fn for_stream(
        &self,
        stream: &str,
    ) -> Result<Option<Arc<dyn HistoricalPublicObserverV1>>, MaterialError> {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.root.join(format!("public-{n}.jsonl")))?;
        let mut writer = BufWriter::with_capacity(1024 * 1024, file);
        line(
            &mut writer,
            &json!({"record_type":"format","format":"continuous-inventory-public-v1","input_stream_id":stream,
            "blb":"not_requested","interval_coverage":"unknown_no_coverage_evidence"}),
        )?;
        Ok(Some(Arc::new(Self {
            root: self.root.clone(),
            state: Mutex::new(Some(State {
                writer,
                markets: BTreeMap::new(),
            })),
            next: AtomicUsize::new(0),
        })))
    }
    fn requirements(&self) -> PublicObservationRequirementsV1 {
        PublicObservationRequirementsV1 {
            binance_depth_quantities: false,
        }
    }
    fn observe(
        &self,
        r: &PublicGroupReferenceV1,
        g: &ClosedBusinessGroupV1,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        self.observe_inner(r, g, None)
    }
    fn observe_at(
        &self,
        r: &PublicGroupReferenceV1,
        g: &ClosedBusinessGroupV1,
        at: i64,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        self.observe_inner(r, g, Some(at))
    }
    fn production_finished(&self) -> Result<(), MaterialError> {
        if let Some(state) = self.state.lock().unwrap().as_mut() {
            line(
                &mut state.writer,
                &json!({"record_type":"public_input_end","meaning":"public producer natural EOF; account economic finality independent"}),
            )?;
            state.writer.flush()?;
        }
        Ok(())
    }
    fn finish(&self) -> Result<(), MaterialError> {
        if let Some(state) = self.state.lock().unwrap().as_mut() {
            state.writer.flush()?;
        }
        Ok(())
    }
}
