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

    #[test]
    fn resource_snapshot_copies_only_current_counters() {
        let snapshot = MeasurementResourceSnapshot::new(
            ResourceQuantityV1::new(3, 30),
            ResourceQuantityV1::new(2, 20),
            50,
            9,
        );

        assert_eq!(snapshot.business.records, 3);
        assert_eq!(snapshot.pending_projection.bytes, 20);
        assert_eq!(snapshot.log_byte_count, 50);
        assert_eq!(snapshot.next_input_sequence, 9);
    }
}
