//! 单次SIM开户与公共Ledger初始化；GPU冷交接仍使用这一原公共链。
use sim_system::execution::{MeasuredCancellationEvidence, MemoryNoeObservation, SessionSpecV3,
    SessionInitializationControlResultV2, PublicSimExecutionV1};
use ledger::{AccountAdmissionPolicyV2, LedgerWriterV1};
use crate::SharedExecutionV2;

pub fn open_session_v2(
    execution: &mut PublicSimExecutionV1,
    observation: MemoryNoeObservation<MeasuredCancellationEvidence>,
    spec: SessionSpecV3,
    activation_id: String,
    admission: AccountAdmissionPolicyV2,
) -> Result<(SharedExecutionV2, SessionInitializationControlResultV2), Box<dyn std::error::Error>> {
    execution.create_session(spec)?;
    let shared = SharedExecutionV2::from_opening(
        LedgerWriterV1::new_with_admission_v2(admission), observation)?
        .ok_or("原SIM开户未产生NOE记录")?;
    let activated = execution.activate_session(activation_id)?;
    Ok((shared, activated))
}
