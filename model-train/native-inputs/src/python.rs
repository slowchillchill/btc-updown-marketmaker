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

#[pymodule]
fn model_native_inputs(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(post_terms_batch_v2, module)?)?;
    module.add_function(wrap_pyfunction!(cancel_targets_batch_v2, module)?)?;
    module.add_class::<PythonGroup>()?;
    module.add_class::<PythonActor>()?;
    Ok(())
}
