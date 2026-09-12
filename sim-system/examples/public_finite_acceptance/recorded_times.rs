//! 固定有限案例的公开可见时序：只引用关闭输入保存的原接收时间。
use contracts::PublicGroupReferenceV1;
use data_pipeline_core::public_input::DptIngressV1;
use data_pipeline_training::public_recording::{
    PublicRecordingReaderV1, PublicStoredRowV1, RecordingResult,
};
use sim_system::{
    execution::SimRunActorErrorV2, public_consumption::PublicConsumptionTimeSourceV1,
};
use std::path::Path;

pub struct RecordedReceivedTimes {
    reader: PublicRecordingReaderV1,
    current: Option<(u64, i64)>,
}
impl RecordedReceivedTimes {
    pub fn open(root: &Path, rows: usize, bytes: usize) -> RecordingResult<Self> {
        Ok(Self {
            reader: PublicRecordingReaderV1::recover(root, rows, bytes)?,
            current: None,
        })
    }
}
impl PublicConsumptionTimeSourceV1 for RecordedReceivedTimes {
    fn execution_time_for_group(
        &mut self,
        group: &PublicGroupReferenceV1,
    ) -> Result<i64, SimRunActorErrorV2> {
        loop {
            if let Some((position, received)) = self.current {
                if position == group.closed_at_input_position {
                    return Ok(received);
                }
            }
            let row = self
                .reader
                .next_row()
                .map_err(|e| SimRunActorErrorV2::PublicConsumptionSource(e.to_string()))?;
            let Some(row) = row else {
                return Err(SimRunActorErrorV2::PublicConsumptionSource(
                    "原记录未包含该组关闭输入的接收时间".into(),
                ));
            };
            if let PublicStoredRowV1::Input(input) = row {
                if let DptIngressV1::Message {
                    source_evidence, ..
                } = input.ingress
                {
                    self.current = Some((input.input_position, source_evidence.received_at_utc_ms));
                }
            }
        }
    }
}
