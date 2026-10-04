//! 已交接工程/历史摘要。一个显式实例保留原文档，原字段下钻；不归约业务或扫描目录。
use crate::{dto::exact_json, sources::research::fragment};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf, time::Instant};
#[derive(Debug, Deserialize)]
pub struct ExistingSourceConfig {
    /// completion / continuous / collection_manifest；未知类别只使此来源失败。
    pub kind: String,
    /// 明确文件角色。manifest 是 day_dir，所属原 loader 固定读取 manifest.json。
    pub paths: BTreeMap<String, PathBuf>,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ExistingQuery {
    pub document: String,
    pub path: String,
    pub start: Option<usize>,
    pub count: Option<usize>,
}
#[derive(Default)]
pub struct ExistingDocument {
    documents: BTreeMap<String, Value>,
    status: Value,
    latest: Option<(ExistingQuery, Bytes)>,
    encoding: Value,
}
impl ExistingDocument {
    pub fn open(config: Option<&ExistingSourceConfig>) -> Self {
        let mut result = Self::default();
        let Some(config) = config else {
            result.status = json!({"status":"not_configured"});
            return result;
        };
        let roles: &[&str] = match config.kind.as_str() {
            "completion" => &["completion"],
            "continuous" => &[
                "summary",
                "pooled",
                "calls",
                "provenance",
                "run_order",
                "report",
            ],
            "collection_manifest" => &["manifest"],
            _ => {
                result.status = json!({"status":"unsupported_version","kind":config.kind,"detail":"未知显式来源类别"});
                return result;
            }
        };
        let mut statuses = Vec::new();
        for role in roles {
            let started = Instant::now();
            let Some(path) = config.paths.get(*role) else {
                statuses.push(json!({"document":role,"status":"not_configured"}));
                continue;
            };
            let file = if *role == "manifest" {
                path.join("manifest.json")
            } else {
                path.clone()
            };
            let mut meta = json!({"document":role,"source":file.file_name().map(|v|v.to_string_lossy()),"read_attempts":"1","decode_attempts":"0","bytes_read":"0"});
            let loaded: Result<Value, String> = if *role == "manifest" {
                // 唯一所属模块纯读入口；不触及清单声明的任何 Parquet 文件。
                let loaded = data_collect::daily_manifest::load_day_manifest(path);
                // 原 loader 不返回实际 payload 长度，文件长度与 read 返回字节分开。
                meta["file_bytes"] =
                    json!(std::fs::metadata(&file).ok().map(|m| m.len().to_string()));
                meta["bytes_read"] = Value::Null;
                meta["decode_attempts"] = json!(if matches!(&loaded, Ok(Some(_)))
                    || matches!(&loaded,Err(e) if e.kind()==std::io::ErrorKind::InvalidData)
                {
                    "1"
                } else {
                    "0"
                });
                loaded
                    .map_err(|e| e.to_string())
                    .and_then(|v| v.ok_or_else(|| "显式日期清单不存在".into()))
            } else {
                std::fs::read(&file)
                    .map_err(|e| e.to_string())
                    .and_then(|b| {
                        meta["bytes_read"] = json!(b.len().to_string());
                        meta["decode_attempts"] = json!("1");
                        match *role {
                            "calls" => {
                                let mut csv = csv::Reader::from_reader(
                                    flate2::read::GzDecoder::new(b.as_slice()),
                                );
                                let headers = csv.headers().map_err(|e| e.to_string())?.clone();
                                if !["run", "now", "position", "service_ns", "model", "version"]
                                    .iter()
                                    .all(|k| headers.iter().any(|h| h == *k))
                                {
                                    return Err("未知 CSV 字段版本".into());
                                }
                                let mut rows = Vec::new();
                                for row in csv.records() {
                                    let row = row.map_err(|e| e.to_string())?;
                                    // 所有 CSV cell 保留原字符串，Debug/JSON 文本不升级为业务事件。
                                    rows.push(Value::Object(
                                        headers
                                            .iter()
                                            .zip(row.iter())
                                            .map(|(h, v)| (h.into(), json!(v)))
                                            .collect(),
                                    ));
                                }
                                Ok(json!(rows))
                            }
                            "run_order" => std::str::from_utf8(&b)
                                .map_err(|e| e.to_string())
                                .and_then(|s| {
                                    s.lines()
                                        .map(|l| serde_json::from_str(l).map_err(|e| e.to_string()))
                                        .collect::<Result<Vec<Value>, _>>()
                                })
                                .map(Value::Array),
                            "report" => String::from_utf8(b)
                                .map(Value::String)
                                .map_err(|e| e.to_string()),
                            _ => serde_json::from_slice(&b).map_err(|e| e.to_string()),
                        }
                    })
            };
            match loaded {
                Err(e) => {
                    meta["status"] = json!("read_error");
                    meta["detail"] = json!(e);
                }
                Ok(v) => {
                    let checked = match *role {
                        "manifest"
                            if v["schema_version"]
                                != data_collect::daily_manifest::DAILY_MANIFEST_SCHEMA_VERSION =>
                        {
                            Err("未知采集清单版本".to_string())
                        }
                        "manifest" => {
                            data_collect::daily_manifest::manifest_collection_day_coverage(&v)
                                .map(|_| ())
                                .map_err(|e| e.to_string())
                        }
                        "completion" => v
                            .get("final_ledger")
                            .ok_or_else(|| "缺 final_ledger，不解释为容量完成记录".to_string())
                            .and_then(|p| {
                                serde_json::from_value::<ledger::LedgerProjectionV2>(p.clone())
                                    .map(|_| ())
                                    .map_err(|e| e.to_string())
                            }),
                        "summary" | "pooled" if !v.is_array() => Err("原摘要不是数组".into()),
                        _ => Ok(()),
                    };
                    match checked {
                        Err(e) => {
                            meta["status"] = json!("unsupported_version");
                            meta["detail"] = json!(e);
                        }
                        Ok(()) => {
                            meta["status"] = json!("ready");
                            let mut arrays = Vec::new();
                            // completion 仅 7.8k，直接复用原最终投影；其他首屏不传完整数组。
                            let summary = if *role == "completion" {
                                v.clone()
                            } else {
                                fragment(&v, "", &mut arrays)
                            };
                            meta["summary"] = exact_json(summary);
                            meta["arrays"] = json!(arrays);
                            result.documents.insert((*role).into(), v);
                        }
                    }
                }
            }
            meta["load_elapsed_us"] = json!(started.elapsed().as_micros().to_string());
            statuses.push(meta);
        }
        result.status = json!({"status":if result.documents.is_empty(){"read_error"}else{"ready"},"kind":config.kind,"documents":statuses});
        result
    }
    pub fn status(&self) -> Value {
        self.status.clone()
    }
    pub fn last_encoding_cost(&self) -> &Value {
        &self.encoding
    }
    pub fn select(&mut self, query: ExistingQuery) -> Result<Bytes, String> {
        if let Some((previous, body)) = &self.latest
            && previous == &query
        {
            return Ok(body.clone());
        }
        let started = Instant::now();
        let v = self
            .documents
            .get(&query.document)
            .ok_or("此原文档未成功加载")?;
        let selected = v.pointer(&query.path).ok_or("原字段不存在；不补默认值")?;
        if query.path.is_empty() && !v.is_array() && !v.is_string() {
            return Err("请选择明确字段或原数组区段，不读取整个文档".into());
        }
        let mut arrays = Vec::new();
        let mut body = if let Some(rows) = selected.as_array() {
            let start = query.start.ok_or("数组请求须指定原起点")?;
            let count = query.count.ok_or("数组请求须指定区段长度")?;
            if start > rows.len() {
                return Err("原数组起点越界".into());
            }
            let end = start.saturating_add(count).min(rows.len());
            let selected=rows[start..end].iter().enumerate().map(|(i,row)| {
                let path=format!("{}/{}",query.path,start+i);
                json!({"path":path,"csv_record_index":if query.document=="calls"{Some((start+i).to_string())}else{None},"csv_line":if query.document=="calls"{Some((start+i+2).to_string())}else{None},"exact":exact_json(fragment(row,&path,&mut arrays))})
            }).collect::<Vec<_>>();
            json!({"path":query.path,"start":start.to_string(),"end":end.to_string(),"total":rows.len().to_string(),"rows":selected})
        } else {
            json!({"path":query.path,"exact":exact_json(fragment(selected,&query.path,&mut arrays))})
        };
        body["document"] = json!(query.document);
        body["kind"] = self.status["kind"].clone();
        body["omitted_arrays"] = json!(arrays);
        body["source"] = self.status["documents"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["document"] == query.document)
            .unwrap()["source"]
            .clone();
        let selected_us = started.elapsed().as_micros();
        let encode = Instant::now();
        let bytes: Bytes = serde_json::to_vec(&body).map_err(|e| e.to_string())?.into();
        self.encoding = json!({"selection_and_dto_us":selected_us.to_string(),"complete_encoding_us":encode.elapsed().as_micros().to_string(),"response_bytes":bytes.len().to_string()});
        self.latest = Some((query, bytes.clone()));
        Ok(bytes)
    }
}
