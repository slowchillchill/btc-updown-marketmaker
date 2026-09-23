// 仅用于已有本地归档 ignored 测试。
const DATASET_LINEAGE_ROOT: &str = "/home/chillchill/new-datasets";
use std::path::Path;

use contracts::{
    BinanceDepthStateReferenceV1, CanonicalReferenceEventPayloadV1, CanonicalReferenceEventV1,
    Sha256Hex,
};
use data_pipeline_core::reduction::{
    BinanceDepthReducer, ReductionDispositionV1, ReductionOperationKindV1, ReductionReportEntryV1,
};
use data_pipeline_core::typed_raw::{
    BinanceDepthSnapshotRow, BinanceDepthStitchVerdict, BinanceDepthUpdateRow, TypedRawRow,
};
use data_pipeline_training::typed_raw::open_validated_typed_raw_parquet;
use data_pipeline_training::{
    ValidatedDirectSourceDailyInputV3, ValidatedDirectSourceParquetFileV3,
    validate_direct_source_daily_manifest_admission,
};

const EPOCH: &str = "e469e6da0debaff013008bdb95ebac25b913d1f4eb8e2bd45de0f30b9e5e328b";
const SNAPSHOT_FILE_SHA256: &str =
    "87ed05f5e9a2d757b55cd360af2824eac8dce34a9d7e3db3f30bcae63a634775";
const UPDATE_FILE_SHA256: &str = "dcbc05e742a1ca029c479b788c451337a286da82010e29896b2c4cbe1a6709c1";
const SNAPSHOT_RECEIVED_AT_UTC_MS: i64 = 1_785_867_277_605;

#[derive(Clone, Debug, PartialEq, Eq)]
enum FixedDepthInput {
    Snapshot(BinanceDepthSnapshotRow),
    Update(BinanceDepthUpdateRow),
}

impl FixedDepthInput {
    fn collector_admission_sequence(&self) -> u64 {
        match self {
            Self::Snapshot(row) => row.collector_admission_sequence,
            Self::Update(row) => row.collector_admission_sequence,
        }
    }

    fn source_sequence_epoch_id(&self) -> &str {
        match self {
            Self::Snapshot(row) => &row.source_sequence_epoch_id,
            Self::Update(row) => &row.source_sequence_epoch_id,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ReductionRun {
    events: Vec<CanonicalReferenceEventV1>,
    reports: Vec<ReductionReportEntryV1>,
    applied_anchors: Vec<u64>,
}

fn admitted_file<'a>(
    admitted: &'a ValidatedDirectSourceDailyInputV3,
    source_kind: &str,
    expected_relative_path: &str,
    expected_sha256: &str,
) -> &'a ValidatedDirectSourceParquetFileV3 {
    let file = admitted
        .files()
        .iter()
        .find(|file| file.sha256() == expected_sha256)
        .unwrap_or_else(|| panic!("缺少固定真实文件：{expected_relative_path}"));
    assert_eq!(file.source_kind(), source_kind);
    assert_eq!(
        file.manifest_relative_path(),
        Path::new(expected_relative_path)
    );
    file
}

fn read_prefix(
    file: &ValidatedDirectSourceParquetFileV3,
    batch_size: usize,
    row_count: usize,
) -> Vec<TypedRawRow> {
    let mut rows = Vec::with_capacity(row_count);
    for batch in open_validated_typed_raw_parquet(file, batch_size)
        .unwrap_or_else(|error| panic!("固定真实文件打开失败：{error}"))
    {
        for row in batch.unwrap_or_else(|error| panic!("固定真实批次解码失败：{error}"))
        {
            rows.push(row);
            if rows.len() == row_count {
                return rows;
            }
        }
    }
    panic!("固定真实文件不足 {row_count} 行");
}

fn read_fixed_chain(
    admitted: &ValidatedDirectSourceDailyInputV3,
    batch_size: usize,
) -> Vec<FixedDepthInput> {
    let snapshot_file = admitted_file(
        admitted,
        "binance_depth_snapshot",
        "source_kind=binance_depth_snapshot/binance_depth_snapshot_18.parquet",
        SNAPSHOT_FILE_SHA256,
    );
    let update_file = admitted_file(
        admitted,
        "binance_depth_update",
        "source_kind=binance_depth_update/binance_depth_update_18-part-1.parquet",
        UPDATE_FILE_SHA256,
    );

    let mut inputs = read_prefix(snapshot_file, batch_size, 1)
        .into_iter()
        .map(|row| match row {
            TypedRawRow::BinanceDepthSnapshot(row) => FixedDepthInput::Snapshot(row),
            _ => panic!("固定快照文件解码成了其他数据源类型"),
        })
        .collect::<Vec<_>>();
    inputs.extend(
        read_prefix(update_file, batch_size, 10)
            .into_iter()
            .map(|row| match row {
                TypedRawRow::BinanceDepthUpdate(row) => FixedDepthInput::Update(row),
                _ => panic!("固定更新文件解码成了其他数据源类型"),
            }),
    );

    inputs
}

fn sort_same_epoch_by_admission(inputs: &mut [FixedDepthInput]) {
    assert!(
        inputs
            .iter()
            .all(|input| input.source_sequence_epoch_id() == EPOCH)
    );
    inputs.sort_by_key(FixedDepthInput::collector_admission_sequence);
}

fn depth_state(event: &CanonicalReferenceEventV1) -> &BinanceDepthStateReferenceV1 {
    let CanonicalReferenceEventPayloadV1::BinanceDepthState(depth) = &event.payload else {
        panic!("Binance 深度归约产生了其他引用事件类型");
    };
    depth
}

fn reduce_fixed_chain(inputs: &[FixedDepthInput], manifest_sha256: &Sha256Hex) -> ReductionRun {
    let snapshot_file_sha256 = Sha256Hex(SNAPSHOT_FILE_SHA256.to_owned());
    let update_file_sha256 = Sha256Hex(UPDATE_FILE_SHA256.to_owned());
    let mut reducer = BinanceDepthReducer::new("binance-depth-reducer-v1")
        .expect("固定规范版本必须能初始化归约器");
    let mut events = Vec::new();
    let mut reports = Vec::new();
    let mut applied_anchors = Vec::new();

    for input in inputs {
        let outcome = match input {
            FixedDepthInput::Snapshot(row) => reducer
                .apply_snapshot(row, manifest_sha256, &snapshot_file_sha256)
                .expect("固定真实快照必须建立基准"),
            FixedDepthInput::Update(row) => reducer
                .apply_update(row, manifest_sha256, &update_file_sha256)
                .expect("固定真实更新必须按已记录的 stitch 语义归约"),
        };

        match input {
            FixedDepthInput::Snapshot(_) => {
                assert_eq!(
                    outcome.report.reason_code,
                    "BINANCE_DEPTH_SNAPSHOT_BASELINE"
                );
                assert_eq!(outcome.report.operation, ReductionOperationKindV1::Merge);
                assert!(matches!(
                    &outcome.report.disposition,
                    ReductionDispositionV1::Produced { .. }
                ));
                events.push(outcome.event.expect("快照基准必须产生事件"));
            }
            FixedDepthInput::Update(row)
                if row.stitch_verdict == BinanceDepthStitchVerdict::NotAppliedStaleDuplicate =>
            {
                assert!(outcome.event.is_none(), "stale 更新不得产生事件");
                assert_eq!(outcome.report.reason_code, "BINANCE_DEPTH_STALE_DUPLICATE");
                assert_eq!(outcome.report.operation, ReductionOperationKindV1::Suppress);
                assert!(matches!(
                    &outcome.report.disposition,
                    ReductionDispositionV1::Suppressed { .. }
                ));
            }
            FixedDepthInput::Update(row) => {
                assert_eq!(row.stitch_verdict, BinanceDepthStitchVerdict::Applied);
                assert_eq!(outcome.report.reason_code, "BINANCE_DEPTH_APPLIED_UPDATE");
                assert_eq!(
                    outcome.report.operation,
                    ReductionOperationKindV1::Overwrite
                );
                assert!(matches!(
                    &outcome.report.disposition,
                    ReductionDispositionV1::Produced { .. }
                ));
                let event = outcome.event.expect("applied 更新必须产生事件");
                applied_anchors.push(depth_state(&event).last_update_id);
                events.push(event);
            }
        }
        reports.push(outcome.report);
    }

    ReductionRun {
        events,
        reports,
        applied_anchors,
    }
}

#[test]
#[ignore = "需要完整准入本地已最终化的 2026-08-04 六类 typed_raw 数据"]
fn real_snapshot_pending_updates_stitch_deterministically_across_read_batches() {
    let manifest_path = Path::new(DATASET_LINEAGE_ROOT)
        .join("2026-08-04/typed_raw/utc_day=2026-08-04/manifest.json");
    let admitted = validate_direct_source_daily_manifest_admission(&manifest_path)
        .expect("2026-08-04 真实输入必须通过完整日清单准入");
    let manifest_sha256 = Sha256Hex(admitted.manifest_sha256().to_owned());

    let mut row_batches = read_fixed_chain(&admitted, 1);
    let mut wide_batches = read_fixed_chain(&admitted, 8_192);
    assert_eq!(row_batches, wide_batches);
    wide_batches.reverse();
    assert_eq!(
        wide_batches
            .iter()
            .map(FixedDepthInput::collector_admission_sequence)
            .collect::<Vec<_>>(),
        (1..=11).rev().collect::<Vec<_>>()
    );
    sort_same_epoch_by_admission(&mut row_batches);
    sort_same_epoch_by_admission(&mut wide_batches);
    assert_eq!(row_batches, wide_batches);
    assert_eq!(
        row_batches
            .iter()
            .map(FixedDepthInput::collector_admission_sequence)
            .collect::<Vec<_>>(),
        (1..=11).collect::<Vec<_>>()
    );

    let snapshot = match &row_batches[0] {
        FixedDepthInput::Snapshot(row) => row,
        FixedDepthInput::Update(_) => panic!("接纳序号 1 必须是固定快照"),
    };
    assert_eq!(snapshot.last_update_id, 98_229_803_565);
    assert_eq!(snapshot.timestamp_received, SNAPSHOT_RECEIVED_AT_UTC_MS);

    let update_signature = row_batches[1..]
        .iter()
        .map(|input| match input {
            FixedDepthInput::Update(row) => (
                row.collector_admission_sequence,
                row.first_update_id,
                row.final_update_id,
                row.local_u_before,
                row.stitch_verdict,
            ),
            FixedDepthInput::Snapshot(_) => panic!("更新切片中不得再次出现快照"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        update_signature,
        vec![
            (
                2,
                98_229_803_337,
                98_229_803_356,
                None,
                BinanceDepthStitchVerdict::NotAppliedStaleDuplicate
            ),
            (
                3,
                98_229_803_357,
                98_229_803_391,
                None,
                BinanceDepthStitchVerdict::NotAppliedStaleDuplicate
            ),
            (
                4,
                98_229_803_392,
                98_229_803_449,
                None,
                BinanceDepthStitchVerdict::NotAppliedStaleDuplicate
            ),
            (
                5,
                98_229_803_450,
                98_229_803_481,
                None,
                BinanceDepthStitchVerdict::NotAppliedStaleDuplicate
            ),
            (
                6,
                98_229_803_482,
                98_229_803_565,
                None,
                BinanceDepthStitchVerdict::NotAppliedStaleDuplicate
            ),
            (
                7,
                98_229_803_566,
                98_229_803_595,
                None,
                BinanceDepthStitchVerdict::Applied
            ),
            (
                8,
                98_229_803_596,
                98_229_803_609,
                Some(98_229_803_595),
                BinanceDepthStitchVerdict::Applied
            ),
            (
                9,
                98_229_803_610,
                98_229_803_626,
                Some(98_229_803_609),
                BinanceDepthStitchVerdict::Applied
            ),
            (
                10,
                98_229_803_627,
                98_229_803_657,
                Some(98_229_803_626),
                BinanceDepthStitchVerdict::Applied
            ),
            (
                11,
                98_229_803_658,
                98_229_803_678,
                Some(98_229_803_657),
                BinanceDepthStitchVerdict::Applied
            ),
        ]
    );

    let row_batch_run = reduce_fixed_chain(&row_batches, &manifest_sha256);
    let wide_batch_run = reduce_fixed_chain(&wide_batches, &manifest_sha256);
    assert_eq!(row_batch_run, wide_batch_run);
    assert_eq!(row_batch_run.events.len(), 6);
    assert_eq!(row_batch_run.reports.len(), 11);
    assert_eq!(
        row_batch_run.applied_anchors,
        vec![
            98_229_803_595,
            98_229_803_609,
            98_229_803_626,
            98_229_803_657,
            98_229_803_678,
        ]
    );
    assert_eq!(
        row_batch_run
            .events
            .iter()
            .map(|event| (
                event.time.source_event_at_utc_ms,
                event.time.received_at_utc_ms
            ))
            .collect::<Vec<_>>(),
        vec![
            (None, SNAPSHOT_RECEIVED_AT_UTC_MS),
            (Some(1_785_867_277_114), SNAPSHOT_RECEIVED_AT_UTC_MS),
            (Some(1_785_867_277_214), SNAPSHOT_RECEIVED_AT_UTC_MS),
            (Some(1_785_867_277_314), SNAPSHOT_RECEIVED_AT_UTC_MS),
            (Some(1_785_867_277_414), SNAPSHOT_RECEIVED_AT_UTC_MS),
            (Some(1_785_867_277_514), 1_785_867_277_617),
        ]
    );

    let final_depth = depth_state(row_batch_run.events.last().expect("必须存在最终盘口事件"));
    assert!(
        final_depth
            .bids
            .iter()
            .all(|level| level.price_usdt_per_btc.0 != "57766"),
        "stale row 0 独有的 bid 不得进入盘口"
    );
    assert!(final_depth.bids.iter().any(|level| {
        level.price_usdt_per_btc.0 == "63231.53" && level.quantity_btc.0 == "1.86241"
    }));
}
