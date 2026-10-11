//! #185 原PM早期窄读；使用原v3 Arrow解码器，不生成隐藏队列或归约小时。
use data_collect::polymarket_orderbook_v3::*;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector};
use serde_json::json;
fn main() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = std::env::args().collect::<Vec<_>>();
    let file = std::fs::File::open(args.get(1).ok_or("需要原PM首小时Parquet")?)?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let total = builder.metadata().file_metadata().num_rows() as usize;
    let rows = total.min(4096);
    builder = builder
        .with_row_selection(RowSelection::from(vec![
            RowSelector::select(rows),
            RowSelector::skip(total - rows),
        ]))
        .with_batch_size(256);
    let mut samples = Vec::new();
    let mut read = 0;
    for batch in builder.build()? {
        for message in polymarket_orderbook_v3_messages_from_batch(&batch?)? {
            let row = read;
            read += 1;
            let e = &message.envelope;
            if e.market != "0x094426c14d2742346e7e2b3f5f3c4620e5e140a8c2a32dd22f76a5e3d471d784"
                || !e
                    .timestamp
                    .is_some_and(|at| (1789171200000..=1789171205000).contains(&at))
            {
                continue;
            }
            let event = match &message.event {
                PolymarketOrderbookV3Event::PriceChange(c) => {
                    json!({"price_changes":c.price_changes.iter().map(|x|json!({"asset_id":x.asset_id,"price":x.price,"size":x.size,"side":x.side,"best_bid":x.best_bid,"best_ask":x.best_ask})).collect::<Vec<_>>()})
                }
                PolymarketOrderbookV3Event::LastTradePrice(c) => {
                    json!({"asset_id":c.asset_id,"price":c.price,"size":c.size,"side":c.side,"fee_rate_bps":c.fee_rate_bps})
                }
                _ => json!({"typed_event_debug":format!("{:?}", message.event)}),
            };
            samples.push(json!({"file_row":row,"timestamp_received":e.timestamp_received,"timestamp":e.timestamp,"collector_admission_time_utc_ms":e.collector_admission_time_utc_ms,"source_sequence_epoch_id":e.source_sequence_epoch_id,"collector_admission_sequence":e.collector_admission_sequence,"event_type":message.event_type(),"market":e.market,"event":event}));
        }
    }
    let output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(args.get(2).ok_or("需要新输出")?)?;
    serde_json::to_writer_pretty(
        output,
        &json!({"rows_read":read,"matched_market_rows":samples.len(),"samples":samples}),
    )?;
    println!(
        "readonly_pm_rows={read} selected_market_rows={}",
        samples.len()
    );
    Ok(())
}
