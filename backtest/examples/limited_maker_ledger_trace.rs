//! #185 只读早期 Ledger 前缀；复用所属物理编码，不恢复账户、不归约小时。
use execution_normalization::record_file;
use serde_json::json;
use std::{error::Error, path::PathBuf};
fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let path = PathBuf::from(args.get(1).ok_or("需要一个原 Ledger Parquet 段")?);
    let output = PathBuf::from(args.get(2).ok_or("需要新输出文件")?);
    let mut rows = Vec::new();
    let result = record_file::visit(&path, "ledger_arrow_parquet_v1", |(seq, kind, body)| {
        rows.push(json!({"record_sequence":seq,"variant":kind,"payload":serde_json::from_slice::<serde_json::Value>(&body)?}));
        if rows.len() == 128 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "有限前缀已读取",
            ));
        }
        Ok(())
    });
    match result {
        Err(e) if e.kind() == std::io::ErrorKind::Interrupted && rows.len() == 128 => (),
        other => {
            other?;
        }
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    serde_json::to_writer_pretty(&mut file, &rows)?;
    println!("readonly_ledger_prefix_records={}", rows.len());
    Ok(())
}
