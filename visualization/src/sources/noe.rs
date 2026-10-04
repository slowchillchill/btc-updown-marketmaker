//! 仅对显式交接的原引用使用版本化纯读入口；不恢复、枚举或猜引用。
use crate::dto::{NoeSourceConfig, exact_json};
use bytes::Bytes;
use contracts::execution_normalization::NoeResultReferenceV1;
use execution_normalization::{
    ResolvedNoeResult, read_noe_result_v1, read_noe_result_v2, read_noe_result_v3,
    read_noe_result_v4,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::time::Instant;

#[derive(Debug, Clone, Copy, Serialize, Default)]
pub struct NoeEncodingCost {
    pub response_encoding_us: u128,
    pub response_bytes: usize,
}
pub struct NoeResults {
    configured: Vec<NoeSourceConfig>,
    // 最近单引用响应用于重试；不建立逐条/全历史缓存或自动批读。
    last: Option<(usize, Bytes)>,
    native_calls: u64,
    encoding_cost: NoeEncodingCost,
}
impl NoeResults {
    pub fn new(configured: Vec<NoeSourceConfig>) -> Self {
        Self {
            configured,
            last: None,
            native_calls: 0,
            encoding_cost: NoeEncodingCost::default(),
        }
    }
    /// 最近完整响应的一次编码成本；重试沿用旧值，不代表新增编码。
    pub fn last_encoding_cost(&self) -> NoeEncodingCost {
        self.encoding_cost
    }
    pub fn index(&self) -> Vec<Value> {
        self.configured.iter().enumerate().map(|(index,c)|json!({"index":index.to_string(),"label":c.label,"version":c.version,"reference_file":c.reference_file,
            "status":if supported(&c.version){"configured"}else{"unknown_version"},"detail":"显式原引用；按需读取，未枚举历史"})).collect()
    }
    pub fn load(&mut self, index: usize) -> Result<Bytes, String> {
        if let Some((previous, body)) = &self.last
            && *previous == index
        {
            return Ok(body.clone());
        }
        let config = self
            .configured
            .get(index)
            .ok_or("NOE 引用不在显式配置索引中")?;
        let started = Instant::now();
        let mut reference_bytes = 0_usize;
        let mut native_attempts = 0_u64;
        let mut native_elapsed = 0_u128;
        let mut reference = None;
        let mut container_file_bytes = None;
        let result = (|| -> Result<Value, String> {
            if !supported(&config.version) {
                return Err(format!(
                    "未知 NOE 载荷版本 {}；未尝试其他版本",
                    config.version
                ));
            }
            let bytes = std::fs::read(&config.reference_file).map_err(|e| e.to_string())?;
            reference_bytes = bytes.len();
            let original: NoeResultReferenceV1 =
                serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
            reference = Some(exact_json(
                serde_json::to_value(&original).map_err(|e| e.to_string())?,
            ));
            // 文件长度仅为编码文件规模；不是实际 syscall/物理 I/O 字节。
            container_file_bytes = std::fs::metadata(&original.commit_container.location)
                .ok()
                .map(|m| m.len().to_string());
            let read_started = Instant::now();
            native_attempts = 1;
            self.native_calls += 1;
            let decoded = match config.version.as_str() {
                "v1" => read_noe_result_v1(&original)
                    .map_err(|e| e.to_string())
                    .and_then(resolved),
                "v2" => read_noe_result_v2(&original)
                    .map_err(|e| e.to_string())
                    .and_then(resolved),
                "v3" => read_noe_result_v3(&original)
                    .map_err(|e| e.to_string())
                    .and_then(resolved),
                "v4" => read_noe_result_v4(&original)
                    .map_err(|e| e.to_string())
                    .and_then(resolved),
                _ => unreachable!("显式版本分派"),
            };
            native_elapsed = read_started.elapsed().as_micros();
            decoded
        })();
        let (status, detail, exact) = match result {
            Ok(value) => ("ready", None, Some(exact_json(value))),
            Err(error) => (
                if supported(&config.version) {
                    "read_error"
                } else {
                    "unknown_version"
                },
                Some(error),
                None,
            ),
        };
        let body = json!({"index":index.to_string(),"label":config.label,"version":config.version,"reference_file":config.reference_file,
            "status":status,"detail":detail,"reference":reference,"exact":exact,
            "representation":"版本化读取器解码后的精确记录；不是容器原字节",
            "association":"没有该条 NOE 被此报告 Ledger 消费的原关联证据；账户/配置/相邻时间不证明消费",
            "cost":{"reference_json_bytes_read":reference_bytes.to_string(),"native_read_attempts":native_attempts.to_string(),"native_read_calls_total":self.native_calls.to_string(),
                "container_file_bytes":container_file_bytes,"native_read_and_dto_us":native_elapsed.to_string(),"load_elapsed_us":started.elapsed().as_micros().to_string(),
                "allocation_bytes":null,"underlying_io_bytes":null,"note":"单引用原接口解码整个提交容器和上下文，再选 ordinal；未测底层实际 I/O/分配；不自动逐条批读"}});
        let encoding_started = Instant::now();
        let body: Bytes = serde_json::to_vec(&body).map_err(|e| e.to_string())?.into();
        self.encoding_cost = NoeEncodingCost {
            response_encoding_us: encoding_started.elapsed().as_micros(),
            response_bytes: body.len(),
        };
        self.last = Some((index, body.clone()));
        Ok(body)
    }
}
fn supported(version: &str) -> bool {
    matches!(version, "v1" | "v2" | "v3" | "v4")
}
fn resolved<T: Serialize, C: Serialize>(value: ResolvedNoeResult<T, C>) -> Result<Value, String> {
    // 外层没有 Serialize，薄 DTO 直接复用三个原合同对象。
    Ok(
        json!({"context":serde_json::to_value(value.context).map_err(|e|e.to_string())?,
        "boundary":serde_json::to_value(value.boundary).map_err(|e|e.to_string())?,
        "record":serde_json::to_value(value.record).map_err(|e|e.to_string())?}),
    )
}
