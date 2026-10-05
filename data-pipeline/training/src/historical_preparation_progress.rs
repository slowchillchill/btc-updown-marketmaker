//! 单次历史准备的专用阶段/标量通知；不读取进程、不保存事件历史。
use crate::SortMetrics;
use serde::Serialize;
use std::cell::RefCell;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalPreparationStageV1 {
    Admission,
    InitializationSelection,
    EpochPass,
    SecondSupplyAndInitialSort,
    SourceSortFinish,
    PublicApplicationStatisticsRecording,
    PublicStabilizeAndCatalog,
    HiddenScanAndSupply,
    HiddenMessagesFinish,
    HiddenReduceAndWrite,
    HiddenFinalize,
    SourceProgressApplication,
}
impl HistoricalPreparationStageV1 {
    pub fn index(self) -> usize {
        self as usize
    }
}
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalRawPassV1 {
    InitializationSelection,
    Epoch,
    SecondSupply,
}
/// 每个通知只承载已完成工作的标量；原sort metrics最多六个来源。
pub enum HistoricalPreparationNoticeV1 {
    Stage(HistoricalPreparationStageV1),
    AdmissionFile {
        bytes: u64,
        rows: u64,
    },
    RawBatch {
        pass: HistoricalRawPassV1,
        initialization: bool,
        rows: u64,
    },
    RawFileComplete {
        pass: HistoricalRawPassV1,
        initialization: bool,
    },
    SelectedRow {
        pass: HistoricalRawPassV1,
        initialization: bool,
    },
    SourceSortFinished {
        index: usize,
        metrics: SortMetrics,
    },
    HiddenScanRow,
    HiddenMessageSupplied,
    HiddenMessageReduced,
    HiddenStepWritten,
    HiddenReportWritten,
    HiddenStepIndexed,
    HiddenReferenceIndexed,
    HiddenMessagesSorted(SortMetrics),
    HiddenIndexSorted(SortMetrics),
    Checkpoint,
}
/// 请求内同步通知，调用方负责可选输出；不是归约或业务选择接口。
pub struct HistoricalPreparationNotifierV1<'a> {
    callback: RefCell<&'a mut dyn FnMut(HistoricalPreparationNoticeV1)>,
}
impl<'a> HistoricalPreparationNotifierV1<'a> {
    pub fn new(callback: &'a mut dyn FnMut(HistoricalPreparationNoticeV1)) -> Self {
        Self {
            callback: RefCell::new(callback),
        }
    }
    pub(crate) fn notify(&self, notice: HistoricalPreparationNoticeV1) {
        (self.callback.borrow_mut())(notice);
    }
}
pub(crate) fn notify(
    notifier: Option<&HistoricalPreparationNotifierV1<'_>>,
    notice: HistoricalPreparationNoticeV1,
) {
    if let Some(notifier) = notifier {
        notifier.notify(notice);
    }
}
