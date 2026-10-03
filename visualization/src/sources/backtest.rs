//! 原报告只有 Serialize；忠实读取 JSON，不复制领域类型或调用业务归约。
use crate::dto::{Document, exact_json};
use std::{path::Path, time::Instant};

pub fn load_document(path: Option<&Path>, report: bool) -> Document {
    let started = Instant::now();
    let mut doc = Document {
        source: path.map(|p| p.display().to_string()),
        status: "not_configured",
        detail: None,
        exact: None,
        raw_json: None,
        bytes_read: "0".into(),
        read_attempts: "0".into(),
        decode_attempts: "0".into(),
        load_elapsed_us: "0".into(),
    };
    let Some(path) = path else { return doc };
    doc.read_attempts = "1".into();
    match std::fs::read(path) {
        Err(e) => {
            doc.status = "read_error";
            doc.detail = Some(e.to_string());
        }
        Ok(bytes) => {
            doc.bytes_read = bytes.len().to_string();
            doc.decode_attempts = "1".into();
            match serde_json::from_slice::<serde_json::Value>(&bytes) {
                Err(e) => {
                    doc.status = "invalid_json";
                    doc.detail = Some(e.to_string());
                    doc.raw_json = String::from_utf8(bytes).ok();
                }
                Ok(value) => {
                    doc.raw_json = String::from_utf8(bytes).ok();
                    doc.status = "ready";
                    // economics_version 是报告/启动元数据目前明确发布的版本标识。
                    if value.get("economics_version").and_then(|v| v.as_str())
                        != Some("account_economics_v2")
                    {
                        doc.status = "unknown_version";
                        doc.detail =
                            Some("经济版本缺失或不支持；仅保留原 JSON，不作语义解释".into());
                    } else if report
                        && !matches!(
                            value.get("end_reason").and_then(|v| v.as_str()),
                            Some("RangeEnd" | "StrategyExit" | "Failure")
                        )
                    {
                        doc.status = "unsupported_report";
                        doc.detail =
                            Some("结束原因缺失或不支持；仅保留原 JSON，不作语义解释".into());
                    }
                    doc.exact = Some(exact_json(value));
                }
            }
        }
    }
    doc.load_elapsed_us = started.elapsed().as_micros().to_string();
    doc
}
