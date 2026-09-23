use std::sync::Mutex;

use serde::Serialize;

use crate::resource::ResourceQuantityV1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MeasurementContext {
    PublicRequest,
    CommitGroup,
    CleanSeal,
    Recovery,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MeasurementPhase {
    PublicEndToEnd,
    Reducer,
    ProjectionFinalize,
    ProjectionDelivery,
    LogEncode,
    LogWrite,
    LogSync,
    RecoveryReplay,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MeasurementObservation {
    pub context: MeasurementContext,
    pub phase: MeasurementPhase,
    pub elapsed_ns: u64,
}

#[derive(Debug, Default)]
pub(crate) struct MeasurementProbe {
    observations: Mutex<Vec<MeasurementObservation>>,
}

impl MeasurementProbe {
    pub(crate) fn record(
        &self,
        context: MeasurementContext,
        phase: MeasurementPhase,
        elapsed_ns: u64,
    ) {
        self.observations
            .lock()
            .expect("measurement probe mutex poisoned")
            .push(MeasurementObservation {
                context,
                phase,
                elapsed_ns,
            });
    }

    pub(crate) fn drain(&self) -> Vec<MeasurementObservation> {
        std::mem::take(
            &mut *self
                .observations
                .lock()
                .expect("measurement probe mutex poisoned"),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
/// 调用时扫描现有业务对象和待投影批次，不在业务变更时维护计量副本。
/// records 表示所列逻辑对象数量，bytes 表示这些对象的自有文本长度；
/// 不包含容器容量、固定宽度字段、分配器开销，也不是 RSS 或逐事件峰值。
pub struct MeasurementResourceSnapshot {
    pub business: ResourceQuantityV1,
    pub pending_projection: ResourceQuantityV1,
    pub log_byte_count: u64,
    pub next_input_sequence: u64,
}

impl MeasurementResourceSnapshot {
    pub(crate) const fn new(
        business: ResourceQuantityV1,
        pending_projection: ResourceQuantityV1,
        log_byte_count: u64,
        next_input_sequence: u64,
    ) -> Self {
        Self {
            business,
            pending_projection,
            log_byte_count,
            next_input_sequence,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_preserves_phase_context_order_and_drains_once() {
        let probe = MeasurementProbe::default();
        probe.record(
            MeasurementContext::CommitGroup,
            MeasurementPhase::Reducer,
            11,
        );
        probe.record(
            MeasurementContext::CommitGroup,
            MeasurementPhase::ProjectionFinalize,
            7,
        );

        assert_eq!(
            probe.drain(),
            vec![
                MeasurementObservation {
                    context: MeasurementContext::CommitGroup,
                    phase: MeasurementPhase::Reducer,
                    elapsed_ns: 11,
                },
                MeasurementObservation {
                    context: MeasurementContext::CommitGroup,
                    phase: MeasurementPhase::ProjectionFinalize,
                    elapsed_ns: 7,
                },
            ]
        );
        assert!(probe.drain().is_empty());
    }
}
