//! 当前B公共需求：FULL数量，未选用完整来源进展；F9仍由同前缀事实和统计提供。
use contracts::PublicObservationRequirementsV1;
use serde_json::{Value, json};
pub const TRAJECTORY_VERSION: &str = "learning-trajectory-v2";
pub const VERSION: &str = "b-public-input-full-no-progress-v1";
pub const OBSERVATION: PublicObservationRequirementsV1 = PublicObservationRequirementsV1::FULL;
pub const SOURCE_PROGRESS: bool = false;
pub fn contract() -> Value {
    json!({"version":VERSION,"observation":OBSERVATION,"source_progress":SOURCE_PROGRESS})
}
/// 沿既有effective_parameters/group身份记录当前语义；不迁移原完成记录或消费事务。
pub fn bind(config: &crate::ConsumerRunConfig) -> crate::ConsumerRunConfig {
    let mut config = config.clone();
    for day in &mut config.days {
        for c in &mut day.candidates {
            c.strategy.effective_parameters["model_public_input"] = contract();
        }
    }
    config
}
