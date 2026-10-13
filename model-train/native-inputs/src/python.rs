//! 同进程边界只传特征、支持和索引；精确状态始终由 NativeActor 持有。
use crate::{NativeActor, NativeInputGroup};
use pyo3::{
    prelude::*,
    types::{PyDict, PyList},
};
use std::sync::Arc;

#[pyclass(name = "NativeGroup", frozen)]
pub struct PythonGroup {
    pub group: Arc<NativeInputGroup>,
}
#[pyclass(name = "NativeActor")]
pub struct PythonActor {
    pub actor: NativeActor,
}
fn error(e: impl std::fmt::Debug) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(format!("{e:?}"))
}
#[cfg(test)]
pub(crate) fn value(py: Python<'_>, input: serde_json::Value) -> PyResult<Py<PyAny>> {
    Ok(match input {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(v) => v.into_pyobject(py)?.to_owned().into_any().unbind(),
        serde_json::Value::String(v) => v.into_pyobject(py)?.into_any().unbind(),
        serde_json::Value::Number(v) => {
            if let Some(n) = v.as_i64() {
                n.into_pyobject(py)?.into_any().unbind()
            } else if let Some(n) = v.as_u64() {
                n.into_pyobject(py)?.into_any().unbind()
            } else {
                v.as_f64().unwrap().into_pyobject(py)?.into_any().unbind()
            }
        }
        serde_json::Value::Array(v) => PyList::new(
            py,
            v.into_iter()
                .map(|v| value(py, v))
                .collect::<PyResult<Vec<_>>>()?,
        )?
        .into_any()
        .unbind(),
        serde_json::Value::Object(v) => {
            let d = PyDict::new(py);
            for (k, v) in v {
                d.set_item(k, value(py, v)?)?;
            }
            d.into_any().unbind()
        }
    })
}
fn time<'py>(
    py: Python<'py>,
    t: &sim_system::public_consumption::TrustedActorExecutionTimeV1,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("run_id", t.run_id())?;
    out.set_item("drive_position", t.drive_position())?;
    out.set_item("logical_time_utc_ms", t.logical_time_utc_ms())?;
    Ok(out)
}
fn account<'py>(
    py: Python<'py>,
    a: &contracts::AccountInstanceKeyV1,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("run_id", &a.run_id)?;
    out.set_item(
        "caller_role",
        match a.caller_role {
            contracts::SimCallerRoleV1::ModelTrain => "MODEL_TRAIN",
            contracts::SimCallerRoleV1::OfflineEvaluation => "OFFLINE_EVALUATION",
            contracts::SimCallerRoleV1::HistoricalBacktest => "HISTORICAL_BACKTEST",
            contracts::SimCallerRoleV1::Paper => "PAPER",
        },
    )?;
    out.set_item("caller_instance_id", &a.caller_instance_id)?;
    out.set_item("account_id", &a.account_id)?;
    out.set_item(
        "platform_source",
        match a.platform_source {
            contracts::PlatformSourceV1::Simulated => "SIMULATED",
        },
    )?;
    Ok(out)
}
fn hidden_key<'py>(py: Python<'py>, key: &crate::HiddenKey) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("trajectory_id", &key.trajectory_id)?;
    out.set_item("account_id", account(py, &key.account_id)?)?;
    out.set_item("condition_id", &key.condition_id.0)?;
    Ok(out)
}
fn hidden_ops<'py>(py: Python<'py>, ops: &[crate::HiddenOp]) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for op in ops {
        let tagged = PyDict::new(py);
        let fields = PyDict::new(py);
        let tag = match op {
            crate::HiddenOp::ResetRootHidden {
                trajectory_id,
                account_id,
            } => {
                fields.set_item("trajectory_id", trajectory_id)?;
                fields.set_item("account_id", account(py, account_id)?)?;
                "ResetRootHidden"
            }
            crate::HiddenOp::InitializeZero { hidden_state_key } => {
                fields.set_item("hidden_state_key", hidden_key(py, hidden_state_key)?)?;
                "InitializeZero"
            }
            crate::HiddenOp::Retire { hidden_state_key } => {
                fields.set_item("hidden_state_key", hidden_key(py, hidden_state_key)?)?;
                "Retire"
            }
        };
        tagged.set_item(tag, fields)?;
        out.append(tagged)?;
    }
    Ok(out)
}
#[pymethods]
impl PythonGroup {
    #[pyo3(signature = (*, shared_books=false))]
    fn features(&self, py: Python<'_>, shared_books: bool) -> PyResult<Py<PyAny>> {
        let group = self.group.as_ref();
        let out = PyDict::new(py);
        let cutoff = PyDict::new(py);
        let receipt = PyDict::new(py);
        let source = group.cutoff.receipt();
        receipt.set_item("session_id", source.session_id())?;
        let reference = PyDict::new(py);
        let r = source.group();
        reference.set_item("input_stream_id", &r.input_stream_id)?;
        let prefix = PyDict::new(py);
        prefix.set_item("input_stream_id", &r.applied_prefix.input_stream_id)?;
        prefix.set_item("next_input_position", r.applied_prefix.next_input_position)?;
        reference.set_item("applied_prefix", prefix)?;
        reference.set_item("group_position", r.group_position)?;
        reference.set_item("group_id", &r.group_id.0)?;
        reference.set_item("closed_at_input_position", r.closed_at_input_position)?;
        receipt.set_item("group", reference)?;
        receipt.set_item("actor_input_sequence", source.actor_input_sequence())?;
        receipt.set_item("execution_time", time(py, source.execution_time())?)?;
        cutoff.set_item("receipt", receipt)?;
        cutoff.set_item("decision_time", time(py, group.cutoff.decision_time())?)?;
        cutoff.set_item(
            "private_feedback_sequence",
            group.cutoff.private_feedback_sequence(),
        )?;
        out.set_item("cutoff", cutoff)?;
        out.set_item(
            "pre_frame_hidden_ops",
            hidden_ops(py, &group.pre_frame_hidden_ops)?,
        )?;
        out.set_item(
            "post_frame_hidden_ops",
            hidden_ops(py, &group.post_frame_hidden_ops)?,
        )?;
        let rows = PyList::empty(py);
        for row in &group.rows {
            let item = PyDict::new(py);
            item.set_item("hidden_state_key", hidden_key(py, &row.hidden_state_key)?)?;
            item.set_item(
                "role",
                match row.role {
                    crate::InputRole::ContextOnly => "ContextOnly",
                    crate::InputRole::DecisionRequired => "DecisionRequired",
                },
            )?;
            item.set_item("slow", &row.slow)?;
            item.set_item("fast", &row.fast)?;
            if shared_books {
                let sides = PyList::empty(py);
                for rows in &row.book_levels {
                    sides.append(Py::new(py, SharedBookSide { rows: rows.clone() })?)?;
                }
                item.set_item("book_levels", sides)?;
            } else {
                // 默认序列化形状不变；实际消费者可保留不可变原生侧以避免重复装箱。
                item.set_item(
                    "book_levels",
                    row.book_levels
                        .iter()
                        .map(|s| s.as_slice())
                        .collect::<Vec<_>>(),
                )?;
            }
            item.set_item("rule", row.rule.as_slice())?;
            rows.append(item)?;
        }
        out.set_item("rows", rows)?;
        out.set_item("current", group.current.as_ref().map(|c| c.0.as_str()))?;
        out.set_item(
            "old",
            group.old.iter().map(|c| c.0.as_str()).collect::<Vec<_>>(),
        )?;
        out.set_item("cancel", &group.cancel)?;
        Ok(out.into_any().unbind())
    }
    #[cfg(test)]
    fn validation_cancel_objects(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let intents = &self.group.cancel_intents;
        value(
            py,
            serde_json::json!(
                intents
                    .iter()
                    .map(|intent| format!("{intent:?}"))
                    .collect::<Vec<_>>()
            ),
        )
    }
    fn actor(&self, row: usize) -> PyResult<PythonActor> {
        Ok(PythonActor {
            actor: NativeActor::new(self.group.clone(), row).map_err(error)?,
        })
    }
}
#[pyclass(frozen)]
struct SharedBookSide {
    rows: Arc<Vec<[f32; 9]>>,
}
#[pymethods]
impl SharedBookSide {
    fn __len__(&self) -> usize {
        self.rows.len()
    }
    fn same_storage(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.rows, &other.rows)
    }
    fn values(&self) -> Vec<[f32; 9]> {
        self.rows.as_ref().clone()
    }
}
#[pymethods]
impl PythonActor {
    fn complete(&self) -> bool {
        self.actor.is_complete()
    }
    fn support(&self) -> PyResult<Vec<bool>> {
        self.actor.support().map_err(error)
    }
    fn prefix_features(&self) -> Vec<f32> {
        self.actor.prefix_features().to_vec()
    }
    fn price_indices(&self) -> PyResult<Vec<i64>> {
        self.actor.price_indices().map_err(error)
    }
    fn action_contract_version(&self) -> &'static str {
        "native_actions_v2"
    }
    fn price_fee_features(&self) -> PyResult<Vec<f32>> {
        self.actor.price_fee_features().map_err(error)
    }
    fn head_index(&self) -> usize {
        self.actor.head_index()
    }
    fn choose(&mut self, index: usize) -> PyResult<()> {
        self.actor.choose(index).map_err(error)
    }
    #[cfg(test)]
    fn validation_state(&self) -> String {
        format!("{:?}", self.actor.state)
    }
    #[cfg(test)]
    fn validation_intent(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        value(
            py,
            serde_json::json!(format!("{:?}", self.actor.intent().map_err(error)?)),
        )
    }
    #[cfg(test)]
    fn validation_selection(&self, py: Python<'_>, request: &str) -> PyResult<Py<PyAny>> {
        use crate::prefix::Family;
        let selected=match self.actor.state.family {
            Some(Family::NoTrade)=>crate::SelectedAction::NoTrade,
            Some(Family::SubmitSingleBuy | Family::SubmitPairedBuy)=>crate::SelectedAction::Buy(&self.actor.state),
            _=>crate::SelectedAction::CancelIndex(self.actor.cancel_index.ok_or_else(||error("未完成撤单选择"))?),
        };
        let context=sim_system::execution::RequestContextV2 {global_request_id:request.into(),caller_correlation_id:None};
        let (intent,reserves)=self.actor.group.select_action::<()>(self.actor.row,selected,&context,|_,_,_|unreachable!())
            .map_err(error)?;
        value(py,serde_json::json!({"intent":intent.as_ref().map(|i|crate::support::gpu_intent_reference(i)),"reserves":reserves}))
    }
    fn path(&self) -> Vec<usize> {
        self.actor.path.clone()
    }
    fn restart(&self) -> PyResult<Self> {
        Ok(Self {
            actor: NativeActor::new(self.actor.group.clone(), self.actor.row).map_err(error)?,
        })
    }
}
#[pyfunction]
fn post_terms_batch_v2(input: &str) -> PyResult<String> {
    sim_system::order_encoding::post_terms_batch_v2(input).map_err(error)
}

#[pyfunction]
fn cancel_targets_batch_v2(input: &str) -> PyResult<String> {
    sim_system::cancellation::cancel_targets_batch_v2(input).map_err(error)
}

#[pyfunction]
fn model_intent_requests_v2(input: &str) -> PyResult<String> {
    let items: Vec<(sim_system::execution::RequestContextV2, contracts::ModelIntentV1)> =
        serde_json::from_str(input).map_err(error)?;
    let requests = items.into_iter().map(|(context,intent)|
        intent_bridge::request_from_intent_v2(context,&intent)).collect::<Result<Vec<_>,_>>().map_err(error)?;
    serde_json::to_string(&requests).map_err(error)
}

#[pymodule]
fn model_native_inputs(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(normalization_identities_v1, module)?)?;
    module.add_function(wrap_pyfunction!(cpu_noe_posterior_v1, module)?)?;
    module.add_function(wrap_pyfunction!(public_rule_context_v1, module)?)?;
    module.add_function(wrap_pyfunction!(open_actor_account_v2, module)?)?;
    module.add_function(wrap_pyfunction!(sim_order_ids_v1, module)?)?;
    module.add_function(wrap_pyfunction!(post_terms_batch_v2, module)?)?;
    module.add_function(wrap_pyfunction!(cancel_targets_batch_v2, module)?)?;
    module.add_function(wrap_pyfunction!(model_intent_requests_v2, module)?)?;
    module.add_class::<PythonGroup>()?;
    module.add_class::<PythonActor>()?;
    Ok(())
}

/// 冷端使用原身份编码器；调用方只传真实来源键，不复制字符串协议。
#[pyfunction]
fn normalization_identities_v1(input: &str) -> PyResult<String> {
    use execution_normalization::{NormalizationIdentity, SourceNeutralEntityKeyV1,
        SourceNeutralLogicalKeyV1};
    use contracts::execution_normalization::{SourceNeutralAccountIdentityV1,
        StableLogicalIdentityV1, NoeEconomicFactReferenceV1};
    #[derive(serde::Deserialize)]
    struct Request {
        account: SourceNeutralAccountIdentityV1,
        entities: Vec<SourceNeutralEntityKeyV1>,
        logical: Vec<SourceNeutralLogicalKeyV1>,
    }
    let request: Request = serde_json::from_str(input).map_err(error)?;
    let account = Arc::new(request.account);
    type Identity = StableLogicalIdentityV1;
    let entities = request.entities.iter().map(|key|
        <Identity as NormalizationIdentity<NoeEconomicFactReferenceV1>>::entity(&account, key))
        .collect::<Result<Vec<_>, _>>().map_err(error)?;
    let logical = request.logical.iter().map(|key|
        <Identity as NormalizationIdentity<NoeEconomicFactReferenceV1>>::logical(&account, key))
        .collect::<Result<Vec<_>, _>>().map_err(error)?;
    serde_json::to_string(&(entities, logical)).map_err(error)
}

/// 有限后验只接实际记录的表示交接；不产生撮合、反馈或资金期望。
#[pyfunction]
fn cpu_noe_posterior_v1(input: &str) -> PyResult<String> {
    use contracts::execution_normalization::ExecutionNormalizationContextV1;
    use execution_normalization::{MemoryLogicalIdentityV1, MemoryEconomicFactReferenceV1,
        NormalizationBatch, NormalizationBatchInput, NormalizationCore, NoeConsumptionSourceV2,
        NoeRecordPositionV1};
    use sim_system::execution::{MemoryNoeRecord, MeasuredCancellationEvidence};
    type Trade = contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2<MemoryLogicalIdentityV1>;
    type Control = contracts::execution_normalization::SourceNeutralControlResult<
        contracts::execution_normalization::SourceNeutralSubmissionOutcomeV2<MemoryLogicalIdentityV1>,
        contracts::execution_normalization::SourceNeutralOrderSnapshotV2<MemoryLogicalIdentityV1>,
        MemoryLogicalIdentityV1, contracts::execution_normalization::CancellationRequestCompletedV1>;
    type Input = NormalizationBatchInput<Trade, Control, MemoryLogicalIdentityV1>;
    #[derive(serde::Deserialize)]
    struct Request {
        context: ExecutionNormalizationContextV1,
        opening_position: NoeRecordPositionV1,
        opening_record: MemoryNoeRecord<MeasuredCancellationEvidence>,
        batches: Vec<Input>,
        deliveries: Vec<[i64; 2]>,
        // 有限低层代表保留实际发布/领取交错；不是新的生产可见性门。
        #[serde(default)]
        accepted_through: Option<Vec<contracts::execution_normalization::SimV2SourceWatermarkV1>>,
        #[serde(default)]
        initial_noe_container: Option<std::path::PathBuf>,
        // 仅选择报告读面；不截断原source领取或Ledger消费。
        #[serde(default)]
        report_consumed: Option<Vec<Option<usize>>>,
    }
    let request: Request = serde_json::from_str(input).map_err(error)?;
    let context = Arc::new(request.context);
    let mut source = NoeConsumptionSourceV2::default();
    // 可选冷容器仅来自本次实际唯一开户批。正式耐久读口完成身份映射，
    // 不打开可写store，也不把耐久视图直接当消费能力。
    let initial = if let Some(path) = request.initial_noe_container {
        use contracts::execution_normalization::{NoePublishedContainerReferenceV1,
            NoeResultReferenceV1};
        use execution_normalization::{read_durable_noe_record_v2,
            read_durable_noe_record_position, NormalizationRecord, SourceNeutralEvidenceKind};
        let (_, rows) = execution_normalization::record_file::read(&path,
            "noe_arrow_parquet_v1").map_err(error)?;
        let mut records = Vec::new();
        for (ordinal, _, _) in rows {
            let reference = NoeResultReferenceV1 {
                commit_container: NoePublishedContainerReferenceV1 {
                    location: path.to_string_lossy().into_owned(),
                }, record_ordinal: ordinal,
            };
            let view = read_durable_noe_record_v2(&reference).map_err(error)?;
            let position = read_durable_noe_record_position(&reference).map_err(error)?;
            if view.context() != context.as_ref() || view.position() != position ||
                position.source_watermark != request.opening_position.source_watermark ||
                position.record_ordinal != records.len() as u64 {
                return Err(error("后验冷批与实际开户来源/ordinal不一致"));
            }
            if ordinal == request.opening_position.record_ordinal {
                if position != request.opening_position || view.record() != &request.opening_record {
                    return Err(error("后验冷批与实际开户记录不一致"));
                }
            } else if !matches!(view.record(), NormalizationRecord::Evidence {
                evidence: SourceNeutralEvidenceKind::InitialMarket { .. }, .. }) {
                return Err(error("有限后验冷批只支持本次开户及InitialMarket"));
            }
            records.push(view.record().clone());
        }
        if records.get(request.opening_position.record_ordinal as usize) != Some(&request.opening_record) {
            return Err(error("后验冷批缺实际开户记录"));
        }
        NormalizationBatch {source_watermark: request.opening_position.source_watermark, records}
    } else {
        NormalizationBatch {source_watermark: request.opening_position.source_watermark,
            records: vec![request.opening_record]}
    };
    let initial_records = initial.records.len();
    let initial_market_records = initial.records.iter().filter(|record| matches!(record,
        execution_normalization::NormalizationRecord::Evidence {
            evidence: execution_normalization::SourceNeutralEvidenceKind::InitialMarket { .. }, .. }
    )).count();
    source.accept(context.clone(), Arc::new(initial));
    let mut ledger = ledger::LedgerWriterV1::new_with_admission_v2(
        ledger::AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    ledger.apply_noe_memory_v2(&source.opening_record().ok_or_else(||error("后验缺实际开户记录"))?)
        .map_err(error)?;
    let opening_projection = ledger.economic_read().ok_or_else(||error("后验缺原开户读面"))?
        .base().clone();
    let mut core = NormalizationCore::<Trade, Control, MemoryLogicalIdentityV1,
        MemoryEconomicFactReferenceV1>::new((*context).clone());
    let mut normalized = Vec::new();
    for input in request.batches {
        let batch = Arc::new(core.normalize_and_accept(input).map_err(error)?);
        normalized.push(batch);
    }
    if let Some(bounds) = &request.accepted_through {
        if bounds.len() != request.deliveries.len() {
            return Err(error("有限后验发布位置与实际领取人口不一致"));
        }
    } else {
        for batch in &normalized { source.accept(context.clone(), batch.clone()); }
    }
    // 本有限材料的admission均为实际开户InitialMarket；先经所属消费口
    // 交给Ledger，不为未来MarketEnrollment建立提前可见协议。
    for record in source.take_admitted_records() {
        ledger.apply_noe_memory_v2(&record).map_err(error)?;
    }
    let read = ledger.economic_read().ok_or_else(||error("后验缺原冷经济读面"))?;
    let initial_projection = serde_json::json!({"projection":read.base(),"cash":read.cash(),
        "markets":read.market_totals().collect::<Vec<_>>()});
    let mut pending = Vec::new();
    let mut checkpoints = Vec::new();
    let mut accepted = 0;
    let mut received_population = 0;
    let mut last_position = None;
    let mut consumed_population = 0;
    fn snapshot(ledger: &ledger::LedgerWriterV1,
        position: &Option<NoeRecordPositionV1>, consumed_population: usize, compact: bool) -> PyResult<serde_json::Value> {
        let read = ledger.economic_read().ok_or_else(||error("后验缺原经济读面"))?;
        let base = read.base();
        let projection = if compact { serde_json::json!({
            "platform_source_prefix":base.platform_source_prefix,
            "platform_confirmed_projection":base.platform_confirmed_projection
        }) } else { serde_json::json!(base) };
        let mut value = serde_json::json!({"entry":ledger.applied_entry_sequence(),
            "last_position":position,"consumed_population":consumed_population,"projection":projection,"cash":read.cash()});
        if !compact { value["markets"] = serde_json::json!(read.market_totals().collect::<Vec<_>>()); }
        Ok(value)
    }
    if let Some(counts) = &request.report_consumed {
        if counts.len() != request.deliveries.len() {
            return Err(error("后验报告人口须对应原领取顺序"));
        }
    }
    let targets = request.report_consumed.as_ref().map(|counts|
        counts.iter().flatten().copied().collect::<std::collections::BTreeSet<_>>());
    let mut prefixes = Vec::new();
    if targets.as_ref().is_some_and(|values|values.contains(&0)) {
        prefixes.push(snapshot(&ledger, &last_position, 0, true)?);
    }
    for (delivery, [closed, visible]) in request.deliveries.into_iter().enumerate() {
        let mut committed = Vec::new();
        if let Some(bounds) = &request.accepted_through {
            let bound = &bounds[delivery];
            while let Some(batch) = normalized.get(accepted) {
                if batch.source_watermark.run_id != bound.run_id {
                    return Err(error("有限后验发布位置与原运行不一致"));
                }
                if batch.source_watermark.projection_batch_sequence > bound.projection_batch_sequence { break; }
                source.accept(context.clone(), batch.clone());
                accepted += 1;
            }
            for record in source.take_admitted_records() {
                ledger.apply_noe_memory_v2(&record).map_err(error)?;
            }
        }
        let received = source.take_closed(closed, visible);
        received_population += received.len();
        pending.extend(received);
        // 有限后验只调原Ledger消费口；合法前驱未齐保同一原引用。
        loop {
            let mut waiting = Vec::new();
            let mut progressed = false;
            for record in std::mem::take(&mut pending) {
                let before = ledger.applied_entry_sequence();
                let position = record.position();
                let outcome = ledger.apply_noe_memory_v2(&record).map_err(error)?;
                let consumed = match outcome {
                    ledger::LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation |
                    ledger::LedgerMemoryApplyOutcomeV1::Applied(
                        ledger::LedgerApplyOutcomeV1::MissingPlatformPredecessor { .. }) => { waiting.push(record); false },
                    _ => { progressed = true; consumed_population += 1; true },
                };
                if ledger.applied_entry_sequence() != before { last_position = Some(position); }
                if consumed && targets.as_ref().is_some_and(|values|values.contains(&consumed_population)) {
                    prefixes.push(snapshot(&ledger, &last_position, consumed_population, true)?);
                } else if targets.is_none() && ledger.applied_entry_sequence() != before {
                    committed.push(snapshot(&ledger, &last_position, consumed_population, false)?);
                }
            }
            pending = waiting;
            if pending.is_empty() || !progressed { break; }
        }
        let mut point = if targets.is_some() { serde_json::json!({
            "entry":ledger.applied_entry_sequence(),"last_position":last_position,
            "consumed_population":consumed_population
        }) } else { snapshot(&ledger, &last_position, consumed_population, false)? };
        point["closed"] = serde_json::json!(closed);
        point["visible"] = serde_json::json!(visible);
        point["received_population"] = serde_json::json!(received_population);
        point["waiting"] = serde_json::json!(pending.len());
        point["committed"] = serde_json::json!(committed);
        checkpoints.push(point);
    }
    serde_json::to_string(&serde_json::json!({"normalized":normalized,
        "initial_records":initial_records,"initial_market_records":initial_market_records,
        "opening_projection":opening_projection,"initial":initial_projection,
        "checkpoints":checkpoints,"prefixes":prefixes,
        "final":snapshot(&ledger, &last_position, consumed_population, true)?,
        "waiting":pending.len()})).map_err(error)
}

/// 冷装配只读同一封存快照；规则与市场身份仍由原公共构造器解释。
#[pyfunction]
fn public_rule_context_v1(input: &str) -> PyResult<String> {
    use sim_system::platform_rules::{HistoricalProcessSnapshotRequestV1,
        load_historical_process_snapshot_v3};
    #[derive(serde::Deserialize)]
    struct Request {
        rule_path: std::path::PathBuf,
        rule_sha256: contracts::Sha256Hex,
        rules_as_of_utc_ms: i64,
        market: contracts::MarketIdentityV2,
    }
    let request: Request = serde_json::from_str(input).map_err(error)?;
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: request.rule_path, expected_file_sha256: request.rule_sha256,
        replay_causal_start_utc_ms: request.rules_as_of_utc_ms,
    }).map_err(error)?;
    serde_json::to_string(&rules.public_rule_context(request.market)).map_err(error)
}

/// 冷端只调用公共开户链；返回原事实和前沿后释放CPU活动对象。
#[pyfunction]
fn open_actor_account_v2(input: &str) -> PyResult<String> {
    use sim_system::execution::*;
    use sim_system::platform_rules::{HistoricalProcessSnapshotRequestV1, ImplementationIdentityV1,
        load_historical_process_snapshot_v3, derive_model_train_maker_snapshot_v3};
    #[derive(serde::Deserialize)]
    struct Request {
        log_path: std::path::PathBuf,
        noe_root: std::path::PathBuf,
        rule_path: std::path::PathBuf,
        rule_sha256: contracts::Sha256Hex,
        rules_as_of_utc_ms: i64,
        implementation_identity: ImplementationIdentityV1,
        source_sha256: contracts::Sha256Hex,
        scenario: sim_system::scenario::SimulationScenarioArtifactV1,
        finality_policy: serde_json::Value,
        session: SessionSpecV3,
        activation_id: String,
        #[serde(default)]
        model_train_maker_snapshot: Option<std::path::PathBuf>,
    }
    let request: Request = serde_json::from_str(input).map_err(error)?;
    let source = HistoricalProcessSnapshotRequestV1 {
        snapshot_path: request.rule_path, expected_file_sha256: request.rule_sha256,
        replay_causal_start_utc_ms: request.rules_as_of_utc_ms,
    };
    let rules = if let Some(target) = request.model_train_maker_snapshot {
        derive_model_train_maker_snapshot_v3(source, target, request.implementation_identity.clone())
    } else { load_historical_process_snapshot_v3(source) }.map_err(error)?;
    let maker_model = rules.maker_model_version_for(request.session.caller_role).to_owned();
    let rule_path = rules.path().to_owned();
    let rule_sha256 = rules.file_sha256().clone();
    let finality = Arc::new(sim_system::finality::TradeFinalityPolicyV2::decode_and_seal(
        &serde_json::to_vec(&request.finality_policy).map_err(error)?).map_err(error)?);
    // 原构造器形成正式资产次序；GPU物理槽次序不决定场景身份。
    let scenario_artifact = sim_system::scenario::SimulationScenarioArtifactV1 {
        schema_version: request.scenario.schema_version,
        markets: request.scenario.markets.into_iter().map(|market|
            sim_system::scenario::PlatformMarketV1::try_new(market.market().clone(), market.assets().clone()))
            .collect::<Result<Vec<_>, _>>().map_err(error)?,
    };
    let rule_context = rules.public_rule_context(scenario_artifact.markets[0].market().clone());
    let scenario = Arc::new(sim_system::scenario::SealedPlatformScenarioV1::decode_and_seal(
        &serde_json::to_vec(&scenario_artifact).map_err(error)?).map_err(error)?);
    let latency = sim_system::latency_profile::load_embedded_proxy_latency_profile().map_err(error)?;
    let opened = RunOpenedV2 {
        projection_version: BusinessProjectionVersionV1::V4,
        run_id: request.session.run_id.clone(), durability_role: RunDurabilityRoleV2::FiniteHistorical,
        platform_rule_snapshot_file_sha256: rules.file_sha256().clone(),
        trade_finality_policy_file_sha256: finality.policy_id().clone(),
        platform_scenario_file_sha256: scenario.file_sha256().clone(),
        proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
        implementation_identity: request.implementation_identity,
        market_event_source_identity_sha256: Some(request.source_sha256),
    };
    let reference = |position| contracts::execution_normalization::DurableSourceReferenceV1 {
        source: contracts::execution_normalization::ImmutableSourceReferenceV1::Location(
            format!("{}#input={position}", request.log_path.display())),
    };
    let port = MemoryNoeProjectionPortV4::new(&request.noe_root, opened.clone(), reference(0),
        reference(1), request.session.creation_request_id.clone(), "native-gpu-opening-v2".into());
    let observation = port.observation();
    let public = sim_system::public_consumption::PublicRunOpenedV1 {
        projection_version: opened.projection_version, run_id: opened.run_id,
        platform_rule_snapshot_file_sha256: opened.platform_rule_snapshot_file_sha256,
        trade_finality_policy_file_sha256: opened.trade_finality_policy_file_sha256,
        platform_scenario_file_sha256: opened.platform_scenario_file_sha256,
        proxy_latency_profile_file_sha256: opened.proxy_latency_profile_file_sha256,
        implementation_identity: opened.implementation_identity,
        market_event_source_identity_sha256: opened.market_event_source_identity_sha256,
    };
    let mut execution = PublicSimExecutionV1::create_new_historical(&request.log_path, public, rules, finality,
        scenario, latency, Box::new(port), 1 << 20, 8).map_err(error)?;
    let (shared, activation) = shared_run::open_session_v2(&mut execution, observation.clone(),
        request.session, request.activation_id,
        ledger::AccountAdmissionPolicyV2::DualCashNoMarketLimit).map_err(error)?;
    let opening = observation.opening_record().ok_or_else(||error("原SIM缺开户记录"))?;
    let read = shared.ledger().economic_read().ok_or_else(|| error("开户账本缺少经济读面"))?;
    let result = serde_json::to_string(&serde_json::json!({
        "rule_context": rule_context, "maker_model": maker_model, "rule_path": rule_path, "rule_sha256": rule_sha256,
        "opening_record": opening.record(), "opening_position": opening.position(),
        "opening_context": opening.context(), "ledger_projection": read.base(),
        "session_sequences": execution.session_sequences().map_err(error)?,
        "activation": format!("{activation:?}")
    })).map_err(error)?;
    execution.stop_and_drain().map_err(error)?;
    Ok(result)
}

#[pyfunction]
fn sim_order_ids_v1(sequences: Vec<u64>) -> Vec<String> {
    sequences.into_iter().map(sim_system::platform_order_id_v1).collect()
}
