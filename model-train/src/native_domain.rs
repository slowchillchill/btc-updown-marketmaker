//! #377：同一Rust精确域的薄C ABI，仅持有已封存不可变规则，不持有账户状态。
use crate::domain::{self, LimitLeg};
use contracts::{ExactDecimal, Sha256Hex};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Value, json};
use sim_system::platform_rules::{
    ImplementationIdentityV1, RecoveryProcessSnapshotRequestV1, SealedProcessRuleSnapshotV3,
    recover_process_snapshot_v3,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

static RULES: OnceLock<Mutex<BTreeMap<u64, Arc<SealedProcessRuleSnapshotV3>>>> = OnceLock::new();
static NEXT: AtomicU64 = AtomicU64::new(1);

#[derive(Deserialize)]
struct RuleInput {
    snapshot_path: PathBuf,
    expected_file_sha256: Sha256Hex,
    implementation_identity: ImplementationIdentityV1,
}
#[derive(Deserialize)]
struct DomainInput {
    handle: u64,
    cash: String,
    ticks: [ExactDecimal; 2],
    max_prices: [u16; 2],
    sides: Vec<usize>,
    selected: Vec<LimitLeg>,
    #[serde(default)]
    ordinal: usize,
    #[serde(default)]
    post_only: bool,
}
fn dispatch(request: Value) -> Result<Value, String> {
    let op = request["op"].as_str().ok_or("原生域缺op")?.to_owned();
    let table = RULES.get_or_init(Default::default);
    if op == "open" {
        let input: RuleInput =
            serde_json::from_value(request["rules"].clone()).map_err(|e| e.to_string())?;
        let rules = recover_process_snapshot_v3(RecoveryProcessSnapshotRequestV1 {
            snapshot_path: input.snapshot_path,
            expected_file_sha256: input.expected_file_sha256,
            expected_implementation_identity: input.implementation_identity,
        })
        .map_err(|e| e.to_string())?;
        let handle = NEXT.fetch_add(1, Ordering::Relaxed);
        table
            .lock()
            .map_err(|e| e.to_string())?
            .insert(handle, rules);
        return Ok(json!({"handle":handle,"version":"rust-exact-domain-v1"}));
    }
    if op == "close" {
        let handle = request["handle"].as_u64().ok_or("原生域缺handle")?;
        table.lock().map_err(|e| e.to_string())?.remove(&handle);
        return Ok(json!({"closed":true}));
    }
    let input: DomainInput = serde_json::from_value(request).map_err(|e| e.to_string())?;
    let rules = table
        .lock()
        .map_err(|e| e.to_string())?
        .get(&input.handle)
        .cloned()
        .ok_or("原生域规则句柄不存在")?;
    let cash = Decimal::from_str_exact(&input.cash).map_err(|e| e.to_string())?;
    match op.as_str() {
        "post" => Ok(
            json!({"allowed":domain::supported_posts(&rules, cash, &input.ticks, &input.sides, &input.selected, input.ordinal)?}),
        ),
        "price" => Ok(
            json!({"maximum":domain::maximum_affordable_price(&rules, cash, &input.ticks, &input.max_prices, &input.sides, &input.selected, input.ordinal, input.post_only)?}),
        ),
        "quantity" => Ok(
            json!({"count":domain::affordable_count(&rules, cash, &input.selected)?.to_string()}),
        ),
        _ => Err("未知原生精确域操作".into()),
    }
}

/// 输入和返回值都是UTF-8 JSON小标量，长度由调用方显式传递；token不通过此接口。
///
/// # Safety
/// input必须指向length个可读字节，output_length必须可写；返回值须以相同长度交free。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn model_train_domain_v1(
    input: *const u8,
    length: usize,
    output_length: *mut usize,
) -> *mut u8 {
    if input.is_null() || output_length.is_null() {
        return std::ptr::null_mut();
    }
    let result = std::panic::catch_unwind(|| {
        let bytes = unsafe { std::slice::from_raw_parts(input, length) };
        let value: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        dispatch(value)
    })
    .unwrap_or_else(|_| Err("原生精确域执行panic".into()));
    let value = result.unwrap_or_else(|error| json!({"error":error}));
    let mut bytes = serde_json::to_vec(&value)
        .expect("JSON域响应可序列化")
        .into_boxed_slice();
    unsafe {
        *output_length = bytes.len();
    }
    let pointer = bytes.as_mut_ptr();
    std::mem::forget(bytes);
    pointer
}

/// # Safety
/// pointer和length必须是本库返回且尚未释放的一对值。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn model_train_domain_free_v1(pointer: *mut u8, length: usize) {
    if !pointer.is_null() {
        unsafe {
            drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                pointer, length,
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    fn abi(raw: &[u8]) -> Value {
        let mut length = 0;
        let pointer = unsafe { model_train_domain_v1(raw.as_ptr(), raw.len(), &mut length) };
        assert!(!pointer.is_null());
        let value =
            serde_json::from_slice(unsafe { std::slice::from_raw_parts(pointer, length) }).unwrap();
        unsafe {
            model_train_domain_free_v1(pointer, length);
        }
        value
    }
    #[test]
    fn native_boundary_keeps_shared_exact_cost_and_reports_malformed_input() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
        let bytes = std::fs::read(&path).unwrap();
        let snapshot: Value = serde_json::from_slice(&bytes).unwrap();
        let open = json!({"op":"open","rules":{"snapshot_path":path,"expected_file_sha256":format!("{:x}",Sha256::digest(&bytes)),"implementation_identity":snapshot["provenance"]["implementation_identity"]}});
        let response = abi(&serde_json::to_vec(&open).unwrap());
        let handle = response["handle"].as_u64().unwrap();
        let input = json!({"op":"quantity","handle":handle,"cash":"2.63125","ticks":["0.01","0.01"],"max_prices":[99,99],"sides":[0,1],"selected":[{"price_index":25,"tick":"0.01","post_only":false},{"price_index":25,"tick":"0.01","post_only":false}]});
        assert_eq!(
            abi(&serde_json::to_vec(&input).unwrap()),
            json!({"count":"499"})
        );
        assert!(abi(b"{")["error"].as_str().unwrap().contains("EOF"));
        assert_eq!(
            abi(&serde_json::to_vec(&json!({"op":"close","handle":handle})).unwrap()),
            json!({"closed":true})
        );
    }
}
