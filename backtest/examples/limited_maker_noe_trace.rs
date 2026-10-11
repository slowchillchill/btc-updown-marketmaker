//! #185 显式早期引用的 V4 只读追溯；不枚举NOE历史或恢复账本。
use contracts::execution_normalization::NoeResultReferenceV1;
use execution_normalization::read_noe_result_v4;
use serde_json::json;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let references: Vec<NoeResultReferenceV1> = serde_json::from_reader(std::fs::File::open(
        args.get(1).ok_or("需要原引用JSON数组")?,
    )?)?;
    let mut results = Vec::new();
    for reference in &references {
        let result = read_noe_result_v4(reference)?;
        results.push(json!({"reference":reference,"context":result.context,"boundary":result.boundary,"record":result.record}));
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(args.get(2).ok_or("需要新结果路径")?)?;
    serde_json::to_writer_pretty(&mut file, &results)?;
    println!("readonly_v4_results={}", results.len());
    Ok(())
}
