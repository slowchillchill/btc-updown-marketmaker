//! #185 已指定18天的只读资格审计；不归约行情、不计算策略或运行账户。
use contracts::Sha256Hex;
use data_pipeline_training::validate_public_source_daily_manifest_admission_v1;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sim_system::platform_rules::{
    HistoricalProcessSnapshotRequestV1, load_historical_process_snapshot_v3,
};
use std::{error::Error, path::Path, time::Instant};
type AuditError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Copy, Serialize)]
struct ProcessIo {
    rchar: u64,
    read_bytes: u64,
    syscr: u64,
}
impl ProcessIo {
    fn read() -> Result<Self, AuditError> {
        let text = std::fs::read_to_string("/proc/self/io")?;
        let field = |name: &str| -> Result<u64, AuditError> {
            Ok(text
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .ok_or("缺少实际进程I/O计数")?
                .trim()
                .parse()?)
        };
        Ok(Self {
            rchar: field("rchar:")?,
            read_bytes: field("read_bytes:")?,
            syscr: field("syscr:")?,
        })
    }
    fn since(self, before: Self) -> Self {
        Self {
            rchar: self.rchar - before.rchar,
            read_bytes: self.read_bytes - before.read_bytes,
            syscr: self.syscr - before.syscr,
        }
    }
}
fn peak_rss_kib() -> Result<u64, AuditError> {
    Ok(std::fs::read_to_string("/proc/self/status")?
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .ok_or("缺少实际进程RSS计数")?
        .split_whitespace()
        .next()
        .ok_or("RSS计数为空")?
        .parse()?)
}
fn public_error(error: &dyn std::fmt::Display, input_root: &Path, rules: &Path) -> String {
    error
        .to_string()
        .replace(&input_root.to_string_lossy().to_string(), "<INPUT_ROOT>")
        .replace(&rules.to_string_lossy().to_string(), "<RULES_FILE>")
}
fn main() -> Result<(), AuditError> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 5 {
        return Err(
            "用法：research_input_audit <INPUT_ROOT> <RULES_FILE> <RULES_SHA256> <FINALITY_FILE> <REPORT_FILE>"
                .into(),
        );
    }
    let input_root = Path::new(&args[0]);
    let rules_path = Path::new(&args[1]);
    let report_path = Path::new(&args[4]);
    let started = Instant::now();
    let io_start = ProcessIo::read()?;
    let mut days = Vec::new();
    for number in 9..=26 {
        let day = format!("2026-09-{number:02}");
        let logical = format!("{day}/typed_raw/utc_day={day}/manifest.json");
        let manifest = input_root.join(&logical);
        let before = ProcessIo::read()?;
        let day_started = Instant::now();
        // 清单声明仅解释资源和gaps；资格只能由原DPT准入决定。
        let declaration = std::fs::read(&manifest).and_then(|bytes| {
            let hash = format!("{:x}", Sha256::digest(&bytes));
            serde_json::from_slice::<Value>(&bytes)
                .map(|v| (v, hash))
                .map_err(std::io::Error::other)
        });
        let declared = match declaration {
            Ok((v, hash)) => {
                let empty = Vec::new();
                let files = v["files"].as_array().unwrap_or(&empty);
                json!({"manifest_sha256": hash, "files": files.len(),
                    "bytes": files.iter().filter_map(|f| f["bytes"].as_u64()).sum::<u64>(),
                    "rows": files.iter().filter_map(|f| f["rows"].as_u64()).sum::<u64>(),
                    "gaps_count": v["gaps"].as_array().map(Vec::len),
                    "closure_reason": v["closure_reason"], "coverage": v["collection_day_coverage"],
                    "source_counters": v["source_counters"],
                    "schemas": files.iter().filter_map(|f| Some((f["source_kind"].as_str()?.to_owned(), f["schema_version"].as_str()?.to_owned()))).collect::<std::collections::BTreeSet<_>>()})
            }
            Err(error) => {
                json!({"declaration_error": public_error(&error, input_root, rules_path)})
            }
        };
        let admission = match validate_public_source_daily_manifest_admission_v1(&manifest) {
            Ok(input) => json!({"status":"admitted", "utc_day":input.utc_day(),
                "manifest_sha256":input.manifest_sha256(), "verified_files":input.files().len(),
                "verified_bytes":input.files().iter().map(|f| f.bytes()).sum::<u64>(),
                "verified_rows":input.files().iter().map(|f| f.rows()).sum::<u64>()}),
            Err(error) => {
                json!({"status":"rejected", "original_error":public_error(&error, input_root, rules_path),
                "original_error_debug": format!("{error:?}").replace(&input_root.to_string_lossy().to_string(), "<INPUT_ROOT>")})
            }
        };
        let io = ProcessIo::read()?.since(before);
        println!("{day}: {}", admission["status"]);
        days.push(json!({"utc_day":day,"purpose":if number < 24 {"research"} else {"development"},
            "manifest_logical_path":logical,"declared":declared,"admission":admission,
            "wall_seconds":day_started.elapsed().as_secs_f64(),"io":io,"peak_rss_kib":peak_rss_kib()?}));
    }
    let mut rules_results = Vec::new();
    for (purpose, start) in [("research", 1788912000000), ("development", 1790208000000)] {
        let result = match load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
            snapshot_path: rules_path.to_path_buf(),
            expected_file_sha256: Sha256Hex(args[2].clone()),
            replay_causal_start_utc_ms: start,
        }) {
            Ok(sealed) => json!({"status":"loaded", "file_sha256":sealed.file_sha256()}),
            Err(error) => {
                json!({"status":"rejected", "original_error":public_error(&error, input_root, rules_path)})
            }
        };
        rules_results.push(json!({"purpose":purpose,"causal_start_utc_ms":start,"result":result}));
    }
    let finality_bytes = std::fs::read(&args[3])?;
    let finality =
        match sim_system::finality::TradeFinalityPolicyV2::decode_and_seal(&finality_bytes) {
            Ok(policy) => json!({"status":"sealed", "policy_id":policy.policy_id(),
            "source_bound_material":serde_json::from_slice::<Value>(&finality_bytes)?}),
            Err(error) => json!({"status":"rejected", "original_error":error.to_string()}),
        };
    let report = json!({"schema":"baseline_research_input_audit_v1", "audit_source_sha256":format!("{:x}",Sha256::digest(include_bytes!("research_input_audit.rs"))),
        "admission_entry":"validate_public_source_daily_manifest_admission_v1", "days":days,"rules":rules_results,"finality":finality,
        "input_set_identity_sha256":null,"input_set_note":"逐日复用原准入的清单摘要；完整集合身份由后续公共freeze入口生成，本审计不哈希失败子集冒充完整输入。",
        "wall_seconds":started.elapsed().as_secs_f64(),"peak_rss_kib":peak_rss_kib()?,"io":ProcessIo::read()?.since(io_start)});
    serde_json::to_writer_pretty(std::fs::File::create(report_path)?, &report)?;
    Ok(())
}
