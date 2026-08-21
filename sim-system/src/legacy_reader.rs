use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;
use serde_json::value::RawValue;

#[derive(Debug)]
pub(crate) enum LegacyReadErrorV1 {
    Read(std::io::Error),
    Decode(serde_json::Error),
}

impl From<std::io::Error> for LegacyReadErrorV1 {
    fn from(error: std::io::Error) -> Self {
        Self::Read(error)
    }
}

impl From<serde_json::Error> for LegacyReadErrorV1 {
    fn from(error: serde_json::Error) -> Self {
        Self::Decode(error)
    }
}

/// `5676e37` 生成的组件黄金测量报告顶层线格式。
///
/// 历史正文保持为原始 JSON；本模块只读文件并反序列化，不构造、验证或续写报告。
#[derive(Debug, Deserialize)]
pub(crate) struct LegacyComponentGoldenMeasurementReportV1 {
    pub(crate) schema_version: String,
    pub(crate) workload_contract: Box<RawValue>,
    pub(crate) workload_contract_sha256: String,
    pub(crate) environment: Box<RawValue>,
    pub(crate) runtime_hard_guards: Box<RawValue>,
    pub(crate) trials: BTreeMap<String, Box<RawValue>>,
    pub(crate) budget_observations: BTreeMap<String, Box<RawValue>>,
    pub(crate) process_resident_peak_bytes: u64,
    pub(crate) process_swap_peak_bytes: u64,
    pub(crate) process_memory_peak_bytes: u64,
    pub(crate) frozen_budgets: Box<RawValue>,
    pub(crate) frozen_budget_verification_trials: BTreeMap<String, Box<RawValue>>,
}

pub(crate) fn read_component_golden_measurement_report_v1(
    path: &Path,
) -> Result<LegacyComponentGoldenMeasurementReportV1, LegacyReadErrorV1> {
    let bytes = std::fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "需要通过 SIM_SYSTEM_LEGACY_COMPONENT_GOLDEN_REPORT 显式提供真实历史报告"]
    fn reads_real_5676e37_component_golden_report() {
        let path = std::env::var_os("SIM_SYSTEM_LEGACY_COMPONENT_GOLDEN_REPORT")
            .map(std::path::PathBuf::from)
            .expect("显式提供真实历史报告路径");
        let report = read_component_golden_measurement_report_v1(&path)
            .expect("纯只读解码真实 5676e37 组件黄金测量报告");
        assert_eq!(
            report.schema_version,
            "component_golden_measurement_report_v1"
        );
        assert_eq!(report.trials.len(), 6);
        assert_eq!(report.frozen_budget_verification_trials.len(), 2);
    }
}
