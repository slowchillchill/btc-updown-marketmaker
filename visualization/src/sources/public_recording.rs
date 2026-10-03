//! 一个显式稳定前缀、一个顺序游标；只缓存最近响应以便网络重试。
use crate::dto::{PublicSourceConfig, exact_json};
use bytes::Bytes;
use contracts::CommittedIngressPrefixV1;
use data_pipeline_training::public_recording::PublicRecordingReaderV1;
use serde_json::{Value, json};
use std::time::Instant;

pub struct PublicPages {
    reader: PublicRecordingReaderV1,
    page_rows: usize,
    next_page: u64,
    returned_rows: u64,
    last: Option<(u64, Bytes)>,
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
    pub fn current(&mut self) -> Result<Bytes, String> {
        if let Some((_, response)) = &self.last {
            return Ok(response.clone());
        }
        self.page(0)
    }
    pub fn page(&mut self, requested: u64) -> Result<Bytes, String> {
        if let Some((page, response)) = &self.last
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
        let rows_json = serde_json::to_string(&exact).map_err(|e| e.to_string())?;
        let encoding_us = started.elapsed().as_micros().to_string();
        let response: Bytes = serde_json::to_vec(&json!({"page":requested.to_string(), "row_start":row_start.to_string(),
            "returned_rows":rows.len().to_string(), "total_returned_rows":self.returned_rows.to_string(), "at_end":self.exhausted,
            "prefix":exact_json(serde_json::to_value(&self.reader.prefix).map_err(|e|e.to_string())?),
            "rows":exact,"cost":{"reader_open_count":"1","next_page_calls":(requested+1).to_string(),"read_elapsed_us":read_elapsed_us,
                "row_encoding_us":encoding_us,"rows_json_bytes":rows_json.len().to_string(),"allocation_bytes":null,"underlying_read_bytes":null}})).map_err(|e|e.to_string())?.into();
        self.next_page += 1;
        self.last = Some((requested, response.clone()));
        Ok(response)
    }
}
