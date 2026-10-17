use super::fixture_clock::TradingTimes;
use super::fixture_input::core;
use super::fixture_recording::{apply, config};
use super::fixture_reference_inputs::reference_then_market_inputs;
use data_pipeline_core::public_observation_core::*;
use data_pipeline_training::public_recording::*;
use std::path::Path;

pub fn recorded_reference_then_market(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let inputs = reference_then_market_inputs();
    for input in inputs {
        let applied = apply(&mut core, &input);
        writer.record(input, applied).unwrap();
    }
    writer.stabilize().unwrap();
    (core, TradingTimes(vec![9_001, 10_001, 22_000]))
}
