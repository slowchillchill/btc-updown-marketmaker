//! 有限目标的只读公共观察；不创建策略、执行会话或账户。
use crate::{HistoricalPreparationConfigV1, MaterialError};
use contracts::*;
use data_pipeline_training::{
    day_producer::DayProducerV1, day_stream::DayIngressV1, historical_initialization::*,
};
use serde::Serialize;
use std::io::BufRead;

/// 完整保留研究清单正文；只解释正式定位字段。
#[derive(Clone, Debug)]
pub struct HistoricalObservationTargetV1 {
    pub original: serde_json::Value,
    pub group: Option<PublicGroupReferenceV1>,
    pub condition_id: String,
}
pub fn read_observation_targets_v1(
    reader: impl BufRead,
) -> Result<Vec<HistoricalObservationTargetV1>, MaterialError> {
    reader
        .lines()
        .enumerate()
        .map(|(i, line)| {
            let original: serde_json::Value =
                serde_json::from_str(&line?).map_err(|e| format!("目标第{}行: {e}", i + 1))?;
            let condition_id = original
                .get("condition_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("目标第{}行缺少condition_id", i + 1))?
                .to_owned();
            let group = original
                .pointer("/cutoff/receipt/group")
                .filter(|v| !v.is_null())
                .map(|v| serde_json::from_value(v.clone()))
                .transpose()
                .map_err(|e| format!("目标第{}行group无法解析: {e}", i + 1))?;
            Ok(HistoricalObservationTargetV1 {
                original,
                group,
                condition_id,
            })
        })
        .collect()
}
#[derive(Clone, Debug, Serialize)]
pub struct HistoricalObservationExportRowV1 {
    pub schema_version: &'static str,
    pub target: serde_json::Value,
    pub matched_group: Option<PublicGroupReferenceV1>,
    pub observation: Option<MarketBusinessObservationV1>,
    pub unknown_reason: Option<String>,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct HistoricalObservationExportProgressV1 {
    pub processed_inputs: u64,
    pub completed_targets: usize,
    pub total_targets: usize,
    pub stopped: bool,
}
/// sink成功才计入完成数。调用方应在返回成功前完成行写入及所需落盘。
pub fn export_historical_observations_v1(
    config: &HistoricalPreparationConfigV1,
    targets: &[HistoricalObservationTargetV1],
    mut sink: impl FnMut(&HistoricalObservationExportRowV1) -> Result<(), MaterialError>,
    mut progress: impl FnMut(&HistoricalObservationExportProgressV1),
    mut should_stop: impl FnMut() -> bool,
) -> Result<HistoricalObservationExportProgressV1, MaterialError> {
    let mut state = HistoricalObservationExportProgressV1 {
        total_targets: targets.len(),
        ..Default::default()
    };
    progress(&state);
    if targets.is_empty() {
        return Ok(state);
    }
    if should_stop() {
        state.stopped = true;
        progress(&state);
        return Ok(state);
    }
    let inputs =
        data_pipeline_training::freeze_public_source_input_set_v1(&config.scope.manifest_paths)?;
    let admitted = inputs.admitted_daily_inputs();
    let files = admitted.iter().flat_map(|d| d.files()).collect::<Vec<_>>();
    let selection = config
        .metadata_initialization
        .as_ref()
        .map(|initial| {
            if initial.received_start_utc_ms != config.scope.received_start_utc_ms
                || initial.received_end_utc_ms != config.scope.received_end_utc_ms
            {
                return Err::<_, MaterialError>("初始化用途的研究范围与日范围不一致".into());
            }
            MetadataInitializationSelectionV1::open(
                initial,
                config.read_page_rows,
                config.max_read_batch_bytes,
            )
        })
        .transpose()?;
    let initial = selection
        .as_ref()
        .map(|s| {
            s.read(config.read_page_rows, config.max_read_batch_bytes)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    let ingress = DayIngressV1::open(
        &files,
        config.header.input_stream_id.clone(),
        config.read_page_rows,
        config.max_read_batch_bytes,
        config.scope.received_start_utc_ms..config.scope.received_end_utc_ms,
        initial,
        config.canonical.max_epoch_count_per_source,
    )?;
    let mut producer = DayProducerV1::new(
        ingress,
        config.effective_header(),
        Sha256Hex(inputs.input_set_identity_sha256().into()),
        config.hidden_stream_id.clone(),
    )?;
    producer
        .core
        .set_observation_requirements(PublicObservationRequirementsV1::FULL);
    // 复用现有接纳与同步证据投影，保留已选深度的来源坐标。
    producer.core.enable_source_progress();
    let mut by_prefix = std::collections::BTreeMap::<u64, Vec<usize>>::new();
    for (index, target) in targets.iter().enumerate() {
        if let Some(g) = &target.group {
            by_prefix
                .entry(g.applied_prefix.next_input_position)
                .or_default()
                .push(index);
        }
    }
    let mut pending = (0..targets.len()).collect::<std::collections::BTreeSet<_>>();
    while !pending.is_empty() {
        if should_stop() {
            state.stopped = true;
            break;
        }
        let Some(applied) = producer.next_applied()? else {
            break;
        };
        state.processed_inputs += 1;
        let candidates = by_prefix
            .get(&applied.applied.applied_prefix.next_input_position)
            .into_iter()
            .flatten()
            .filter(|i| pending.contains(i))
            .copied()
            .collect();
        for row in rows_at_applied(&applied.applied, targets, &candidates) {
            sink(&row.1)?;
            pending.remove(&row.0);
            state.completed_targets += 1;
        }
        progress(&state);
    }
    if !state.stopped {
        for index in pending {
            if should_stop() {
                state.stopped = true;
                break;
            }
            let target = &targets[index];
            let reason = match &target.group {
                None => "target_group_missing",
                Some(g)
                    if g.input_stream_id != config.header.input_stream_id
                        || g.applied_prefix.input_stream_id != config.header.input_stream_id =>
                {
                    "input_stream_mismatch"
                }
                Some(_) => "exact_group_prefix_not_found",
            };
            sink(&unknown_row(target, None, reason))?;
            state.completed_targets += 1;
            progress(&state);
        }
    }
    progress(&state);
    Ok(state)
}
fn unknown_row(
    t: &HistoricalObservationTargetV1,
    matched_group: Option<PublicGroupReferenceV1>,
    reason: &str,
) -> HistoricalObservationExportRowV1 {
    HistoricalObservationExportRowV1 {
        schema_version: "historical_observation_export_v1",
        target: t.original.clone(),
        matched_group,
        observation: None,
        unknown_reason: Some(reason.into()),
    }
}
fn rows_at_applied(
    applied: &AppliedIngressV1,
    targets: &[HistoricalObservationTargetV1],
    pending: &std::collections::BTreeSet<usize>,
) -> Vec<(usize, HistoricalObservationExportRowV1)> {
    let mut rows = Vec::new();
    for group in &applied.closed_groups {
        let reference = PublicGroupReferenceV1 {
            input_stream_id: applied.applied_prefix.input_stream_id.clone(),
            applied_prefix: applied.applied_prefix.clone(),
            group_position: group.group_position,
            group_id: group.group_id.clone(),
            closed_at_input_position: group.closed_at_input_position,
        };
        for &index in pending {
            let target = &targets[index];
            if target.group.as_ref() != Some(&reference) {
                continue;
            }
            let row = match group.observations.get_market(&target.condition_id) {
                Some(o)
                    if o.visible_prefix == reference.applied_prefix
                        && o.group_id == reference.group_id
                        && o.group_position == reference.group_position =>
                {
                    HistoricalObservationExportRowV1 {
                        schema_version: "historical_observation_export_v1",
                        target: target.original.clone(),
                        matched_group: Some(reference.clone()),
                        observation: Some(o.clone()),
                        unknown_reason: None,
                    }
                }
                Some(_) => unknown_row(
                    target,
                    Some(reference.clone()),
                    "observation_prefix_mismatch",
                ),
                None => unknown_row(
                    target,
                    Some(reference.clone()),
                    "market_not_visible_at_group",
                ),
            };
            rows.push((index, row));
        }
    }
    rows
}
#[cfg(test)]
#[path = "observation_export_tests.rs"]
mod tests;
