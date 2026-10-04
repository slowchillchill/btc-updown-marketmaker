//! 已交接公开研究 JSON 的单实例纯读。按原 JSON Pointer 下钻，不建研究归约或领域索引。
use crate::dto::exact_json;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{path::Path, time::Instant};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ResearchQuery {
    pub path: String,
    pub start: Option<usize>,
    pub count: Option<usize>,
}
#[derive(Default)]
pub struct ResearchDocument {
    document: Option<Value>,
    status: Value,
    latest: Option<(ResearchQuery, Bytes)>,
    encoding: Value,
}
// 单个对象只返回非数组字段；原数组以精确路径/长度显式交接，由下一请求选择区段。
// omitted_arrays 是 DTO，不写回原产物，也不以空数组/零代替尚未请求的内容。
pub(crate) fn fragment(v: &Value, path: &str, arrays: &mut Vec<Value>) -> Value {
    match v {
        Value::Array(v) if v.iter().all(|item| !item.is_object() && !item.is_array()) => {
            Value::Array(v.clone())
        }
        Value::Array(v) => {
            arrays.push(json!({"path":path,"length":v.len().to_string()}));
            Value::Null
        }
        Value::Object(v) => Value::Object(
            v.iter()
                .filter_map(|(k, v)| {
                    let p = format!("{path}/{}", k.replace('~', "~0").replace('/', "~1"));
                    if let Value::Array(a) = v
                        && a.iter().any(|item| item.is_object() || item.is_array())
                    {
                        arrays.push(json!({"path":p,"length":a.len().to_string()}));
                        None
                    } else {
                        Some((k.clone(), fragment(v, &p, arrays)))
                    }
                })
                .collect(),
        ),
        v => v.clone(),
    }
}
impl ResearchDocument {
    pub fn open(path: Option<&Path>) -> Self {
        let started = Instant::now();
        let mut reader = Self::default();
        let Some(path) = path else {
            reader.status = json!({"status":"not_configured","detail":"未交接显式描述研究 JSON"});
            return reader;
        };
        let source = path.file_name().map(|v| v.to_string_lossy().into_owned());
        let result = std::fs::read(path);
        let bytes_read = result.as_ref().map_or(0, Vec::len);
        let decode_attempted = result.is_ok();
        let parsed = result
            .map_err(|e| e.to_string())
            .and_then(|b| serde_json::from_slice::<Value>(&b).map_err(|e| e.to_string()));
        let mut status = json!({"source":source,"read_attempts":"1","decode_attempts":if decode_attempted{"1"}else{"0"},"bytes_read":bytes_read.to_string()});
        match parsed {
            Err(e) => {
                status["status"] = json!("read_error");
                status["detail"] = json!(e);
            }
            Ok(v)
                if v["schema"] != "complementary_pair_first_fit_hour_budget_trial_v1"
                    || v["research"]["schema"] != "complementary_pair_first_fit_hour_v1" =>
            {
                status["status"] = json!("unsupported_version");
                status["detail"] = json!("未知研究合同，不作语义解释");
                status["schema"] = v["schema"].clone();
                status["research_schema"] = v["research"]["schema"].clone();
            }
            Ok(v) => {
                status["status"] = json!("ready");
                let mut fields = Vec::new();
                for path in [
                    "/schema",
                    "/status",
                    "/execution",
                    "/build_identity/source_commit",
                    "/source_binding",
                    "/effective_preparation/header",
                    "/effective_preparation/scope",
                    "/effective_preparation/metadata_initialization",
                    "/research/schema",
                    "/research/scope",
                    "/research/counts",
                    "/research/input_set_identity_sha256",
                    "/research/source_identity_sha256",
                    "/research/normal_prefix",
                    "/research/method_note",
                    "/projection",
                    "/result_summary",
                    "/limits",
                ] {
                    if let Some(value) = v.pointer(path) {
                        let mut arrays = Vec::new();
                        fields.push(json!({"path":path,"exact":exact_json(fragment(value,path,&mut arrays)),"omitted_arrays":arrays}));
                    }
                }
                status["summary"] = json!(fields);
                status["catalog"] = exact_json(v["research"]["actual_market_catalog"].clone());
                status["targets"] = exact_json(json!(v["research"]["markets"].as_array().map(|markets|markets.iter().enumerate().map(|(i,m)|json!({"index":i.to_string(),"identity":m["identity"],"freeze_count":m["frozen_sampling"].as_array().map(Vec::len)})).collect::<Vec<_>>())));
                status["global_update_count"] = json!(
                    v["research"]["global_spot"]["updates_original_visible_order"]
                        .as_array()
                        .map(Vec::len)
                        .map(|n| n.to_string())
                );
                reader.document = Some(v);
            }
        }
        status["load_elapsed_us"] = json!(started.elapsed().as_micros().to_string());
        reader.status = status;
        reader
    }
    pub fn status(&self) -> Value {
        self.status.clone()
    }
    pub fn last_encoding_cost(&self) -> &Value {
        &self.encoding
    }
    pub fn select(&mut self, query: ResearchQuery) -> Result<Bytes, String> {
        if let Some((q, body)) = &self.latest
            && *q == query
        {
            return Ok(body.clone());
        }
        let v = self.document.as_ref().ok_or("研究来源不可读取")?;
        // 浏览器只访问这个显式文档。首屏不允许请求整个根；允许的分支均来自公开产物。
        if ![
            "/research/",
            "/projection",
            "/measurement",
            "/storage_snapshot",
            "/first_two_market_finite_comparison",
            "/build_identity",
            "/source_binding",
            "/result_summary",
            "/effective_preparation",
            "/limits",
            "/execution",
        ]
        .iter()
        .any(|p| {
            query.path == *p
                || query.path.starts_with(&format!("{p}/"))
                || (*p == "/research/" && query.path.starts_with(p))
        }) {
            return Err("请选择明确原字段路径，不读取整个文档".into());
        }
        let selected = v
            .pointer(&query.path)
            .ok_or("原字段不存在；不能补算或邻近替换")?;
        let started = Instant::now();
        let mut arrays = Vec::new();
        let body = if let Some(rows) = selected.as_array() {
            let start = query.start.ok_or("数组请求须指定原起点")?;
            let count = query.count.ok_or("数组请求须指定区段长度")?;
            if start > rows.len() {
                return Err("原数组起点越界".into());
            }
            let end = start.saturating_add(count).min(rows.len());
            let rows = rows[start..end].iter().enumerate().map(|(i,r)| {
                // 投影矩阵的一行是原列数组，必须保留列值；其他对象中的数组按需下钻。
                let exact = if r.is_array(){r.clone()}else{fragment(r,&format!("{}/{index}",query.path,index=start+i),&mut arrays)};
                json!({"path":format!("{}/{index}",query.path,index=start+i),"exact":exact_json(exact)})
            }).collect::<Vec<_>>();
            json!({"path":query.path,"start":start.to_string(),"end":end.to_string(),"total":selected.as_array().unwrap().len().to_string(),"rows":rows,"omitted_arrays":arrays})
        } else {
            json!({"path":query.path,"exact":exact_json(fragment(selected,&query.path,&mut arrays)),"omitted_arrays":arrays})
        };
        let selected_us = started.elapsed().as_micros();
        let encode = Instant::now();
        let bytes: Bytes = serde_json::to_vec(&body).map_err(|e| e.to_string())?.into();
        self.encoding = json!({"selection_and_dto_us":selected_us.to_string(),"complete_encoding_us":encode.elapsed().as_micros().to_string(),"response_bytes":bytes.len().to_string()});
        self.latest = Some((query, bytes.clone()));
        Ok(bytes)
    }
}
