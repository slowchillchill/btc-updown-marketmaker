//! #251 批准的固定合成对照；不读取研究原始数据。
use arrow_array::{Array, BinaryArray};
use data_pipeline_training::{SortConfig, SortRow, external_sort};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{fs::File, path::PathBuf, time::Instant};

const ROWS: u64 = 1_048_576;
fn payload(key: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(256);
    for word in 0..32u64 {
        let mut x = key.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(word);
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
        bytes.extend((x ^ (x >> 31)).to_be_bytes());
    }
    bytes
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let workers: usize = args.next().ok_or("需提供merge_workers")?.parse()?;
    let root = PathBuf::from(args.next().ok_or("需提供新输出目录")?);
    if args.next().is_some() {
        return Err("仅接受工作者数和输出目录".into());
    }
    std::fs::create_dir(&root)?;
    let config = SortConfig {
        max_run_rows: 16384,
        max_run_bytes: 64 << 20,
        max_temp_run_files: 256,
        merge_fan_in: 8,
        merge_workers: workers,
        max_open_files: 40,
        batch_rows: 1024,
        max_temp_bytes: 2 << 30,
    };
    let all_started = Instant::now();
    let input = (0..ROWS).map(|i| {
        // 奇数乘数在2的幂模数上构成固定排列，全部键恰好出现一次。
        let key = i.wrapping_mul(104729) ^ 0x5a5a5;
        let key = key & (ROWS - 1);
        SortRow::from_sort_key(key.to_be_bytes().to_vec(), payload(key))
    });
    let sort_started = Instant::now();
    let result = external_sort(input, &root, &config)?;
    let sort_wall_seconds = sort_started.elapsed().as_secs_f64();
    let mut expected = 0u64;
    let mut digest = Sha256::new();
    digest.update(b"data-pipeline-training/external-sort/logical-v1\0");
    for path in &result.output_run.parts {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
            .with_batch_size(1024)
            .build()?;
        for batch in reader {
            let batch = batch?;
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or("键列类型")?;
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or("载荷列类型")?;
            for i in 0..batch.num_rows() {
                if keys.is_null(i)
                    || values.is_null(i)
                    || keys.value(i) != expected.to_be_bytes()
                    || values.value(i) != payload(expected)
                {
                    return Err("完整独立预期对照失败".into());
                }
                for field in [keys.value(i), values.value(i)] {
                    digest.update((field.len() as u64).to_be_bytes());
                    digest.update(field);
                }
                expected += 1;
            }
        }
    }
    digest.update(expected.to_be_bytes());
    let expected_digest = format!("{:x}", digest.finalize());
    if expected != ROWS || expected_digest != result.logical_sha256 {
        return Err("完整行数/摘要对照失败".into());
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"config":config,"rows":expected,"payload_bytes":256,"sort_wall_seconds":sort_wall_seconds,"program_wall_seconds":all_started.elapsed().as_secs_f64(),"logical_sha256":result.logical_sha256,"independent_full_comparison":"passed","metrics":result.metrics})
        )?
    );
    Ok(())
}
