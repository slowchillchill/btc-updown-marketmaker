//! 一个显式稳定前缀、一个顺序游标；只缓存最近响应以便网络重试。
use crate::dto::{PublicSourceConfig, exact_json};
use bytes::Bytes;
use contracts::CommittedIngressPrefixV1;
use data_pipeline_training::public_recording::PublicRecordingReaderV1;
use serde::{Serialize, Serializer};
use serde_json::{Value, json};
use std::time::Instant;
use std::{
    cell::Cell,
    io::{self, Write},
};

/// 已完成编码的测量；置于响应之外，避免为自身长度/计时重复编码。
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct PageEncodingCost {
    pub response_encoding_us: u128,
    pub response_bytes: usize,
}

// rows 必须先于 cost 编码，字节计数直接取同一次 writer 的位置差。
struct Rows<'a> {
    value: &'a Value,
    position: &'a Cell<usize>,
    bytes: &'a Cell<usize>,
}
impl Serialize for Rows<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let start = self.position.get();
        let result = self.value.serialize(serializer)?;
        self.bytes.set(self.position.get() - start);
        Ok(result)
    }
}
struct RowBytes<'a>(&'a Cell<usize>);
impl Serialize for RowBytes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0.get())
    }
}
struct PageWriter<'a> {
    buffer: Vec<u8>,
    position: &'a Cell<usize>,
}
impl Write for PageWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        self.position.set(self.buffer.len());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[derive(Serialize)]
struct PageResponse<'a> {
    page: String,
    row_start: String,
    returned_rows: String,
    total_returned_rows: String,
    at_end: bool,
    prefix: Value,
    rows: Rows<'a>,
    cost: PageCost<'a>,
}
#[derive(Serialize)]
struct PageCost<'a> {
    reader_open_count: &'static str,
    next_page_calls: String,
    read_elapsed_us: String,
    exact_json_us: String,
    rows_json_bytes: RowBytes<'a>,
    allocation_bytes: Option<u64>,
    underlying_read_bytes: Option<u64>,
}

pub struct PublicPages {
    reader: PublicRecordingReaderV1,
    page_rows: usize,
    next_page: u64,
    returned_rows: u64,
    last: Option<(u64, Bytes, PageEncodingCost)>,
    failure: Option<String>,
    exhausted: bool,
}
impl PublicPages {
    pub fn open(config: &PublicSourceConfig) -> Result<(Self, Value), String> {
        let started = Instant::now();
        // next_page(0) 无法推进；底层批次参数同样须能够工作。这是读取参数而非业务门。
        if config.page_rows == 0 || config.max_batch_bytes == 0 {
            return Err("公共记录 page_rows 与 max_batch_bytes 必须非零".into());
        }
        let prefix: CommittedIngressPrefixV1 = serde_json::from_slice(
            &std::fs::read(&config.committed_prefix).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        let reader = PublicRecordingReaderV1::open_committed(
            &config.root,
            Some(prefix.clone()),
            config.page_rows,
            config.max_batch_bytes,
        )
        .map_err(|e| e.to_string())?;
        let status = json!({"status":"ready", "root":config.root, "committed_prefix_source":config.committed_prefix,
            "prefix": exact_json(serde_json::to_value(prefix).map_err(|e|e.to_string())?),
            "header": exact_json(serde_json::to_value(&reader.header).map_err(|e|e.to_string())?),
            "page_rows":config.page_rows.to_string(), "reader_open_count":"1", "open_elapsed_us":started.elapsed().as_micros().to_string(),
            "cost_note":"打开时由所属接口枚举稳定分片并读取前缀元数据；分页沿同一游标解码。底层字节量与分配量未测量。"});
        Ok((
            Self {
                reader,
                page_rows: config.page_rows,
                next_page: 0,
                returned_rows: 0,
                last: None,
                failure: None,
                exhausted: false,
            },
            status,
        ))
    }
    pub fn last_encoding_cost(&self) -> Option<PageEncodingCost> {
        self.last.as_ref().map(|(_, _, cost)| *cost)
    }
    pub fn current(&mut self) -> Result<Bytes, String> {
        if let Some((_, response, _)) = &self.last {
            return Ok(response.clone());
        }
        self.page(0)
    }
    pub fn page(&mut self, requested: u64) -> Result<Bytes, String> {
        if let Some((page, response, _)) = &self.last
            && *page == requested
        {
            return Ok(response.clone());
        }
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.exhausted {
            return Err("已读到指定稳定前缀末端；最近页可重试".into());
        }
        if requested != self.next_page {
            return Err(format!(
                "顺序游标下一页为 {}；未跳转、未从头扫描",
                self.next_page
            ));
        }
        let started = Instant::now();
        let rows = match self.reader.next_page(self.page_rows) {
            Ok(rows) => rows,
            Err(e) => {
                let message = format!(
                    "读取页 {requested} 失败（已返回 {} 条）：{e}；游标停止，原产物未恢复",
                    self.returned_rows
                );
                self.failure = Some(message.clone());
                return Err(message);
            }
        };
        let read_elapsed_us = started.elapsed().as_micros().to_string();
        let row_start = self.returned_rows;
        self.returned_rows += rows.len() as u64;
        self.exhausted = rows.len() < self.page_rows;
        let started = Instant::now();
        let exact = exact_json(serde_json::to_value(&rows).map_err(|e| e.to_string())?);
        let prefix =
            exact_json(serde_json::to_value(&self.reader.prefix).map_err(|e| e.to_string())?);
        let exact_json_us = started.elapsed().as_micros().to_string();
        let position = Cell::new(0);
        let rows_bytes = Cell::new(0);
        let body = PageResponse {
            page: requested.to_string(),
            row_start: row_start.to_string(),
            returned_rows: rows.len().to_string(),
            total_returned_rows: self.returned_rows.to_string(),
            at_end: self.exhausted,
            prefix,
            rows: Rows {
                value: &exact,
                position: &position,
                bytes: &rows_bytes,
            },
            cost: PageCost {
                reader_open_count: "1",
                next_page_calls: (requested + 1).to_string(),
                read_elapsed_us,
                exact_json_us,
                rows_json_bytes: RowBytes(&rows_bytes),
                allocation_bytes: None,
                underlying_read_bytes: None,
            },
        };
        let started = Instant::now();
        let mut writer = PageWriter {
            buffer: Vec::new(),
            position: &position,
        };
        serde_json::to_writer(&mut writer, &body).map_err(|e| e.to_string())?;
        let response: Bytes = writer.buffer.into();
        let encoding_cost = PageEncodingCost {
            response_encoding_us: started.elapsed().as_micros(),
            response_bytes: response.len(),
        };
        self.next_page += 1;
        self.last = Some((requested, response.clone(), encoding_cost));
        Ok(response)
    }
}
