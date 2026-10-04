//! 浏览器边界：所有 JSON 数字转换为精确十进制文本；原 JSON 另行保留。
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct SourcesConfig {
    pub runs: Vec<RunSourceConfig>,
}
#[derive(Debug, Deserialize)]
pub struct RunSourceConfig {
    pub label: String,
    pub report: Option<PathBuf>,
    pub metadata: Option<PathBuf>,
    pub public_recording: Option<PublicSourceConfig>,
    #[serde(default)]
    pub noe_results: Vec<NoeSourceConfig>,
}
#[derive(Debug, Deserialize, Serialize)]
pub struct NoeSourceConfig {
    pub label: String,
    /// 引用没有载荷版本；调用者明确指定公开 reader，不依次尝试。
    pub version: String,
    pub reference_file: PathBuf,
}
#[derive(Debug, Deserialize)]
pub struct PublicSourceConfig {
    pub root: PathBuf,
    /// 必须由写入方交接，不通过目录扫描或报告中的路径推导。
    pub committed_prefix: PathBuf,
    pub page_rows: usize,
    pub max_batch_bytes: usize,
}
#[derive(Debug, Serialize)]
pub struct Document {
    pub source: Option<String>,
    pub status: &'static str,
    pub detail: Option<String>,
    pub exact: Option<Value>,
    pub raw_json: Option<String>,
    pub bytes_read: String,
    pub read_attempts: String,
    pub decode_attempts: String,
    pub load_elapsed_us: String,
}
#[derive(Debug, Serialize)]
pub struct Snapshot {
    pub label: String,
    pub report: Document,
    pub metadata: Document,
    pub public_recording: Value,
    pub noe_results: Vec<Value>,
    pub gaps: Vec<&'static str>,
}
/// 数字字符串是必要的传输表示，不改变金额、身份、时间或枚举值。
pub fn exact_json(value: Value) -> Value {
    match value {
        Value::Number(n) => Value::String(n.to_string()),
        Value::Array(values) => Value::Array(values.into_iter().map(exact_json).collect()),
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(k, v)| (k, exact_json(v)))
                .collect(),
        ),
        v => v,
    }
}
