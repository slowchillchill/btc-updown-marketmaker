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
#[pymethods]
impl PythonGroup {
    fn features(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // serde 数据模型只投影已有公开特征，未编码为 JSON 字符串或文件。
        value(
            py,
            serde_json::to_value(self.group.as_ref()).map_err(error)?,
        )
    }
    fn actor(&self, row: usize) -> PyResult<PythonActor> {
        Ok(PythonActor {
            actor: NativeActor::new(self.group.clone(), row).map_err(error)?,
        })
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
    fn path(&self) -> Vec<usize> {
        self.actor.path.clone()
    }
    fn restart(&self) -> PyResult<Self> {
        Ok(Self {
            actor: NativeActor::new(self.actor.group.clone(), self.actor.row).map_err(error)?,
        })
    }
}
#[pymodule]
fn model_native_inputs(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PythonGroup>()?;
    module.add_class::<PythonActor>()?;
    Ok(())
}
