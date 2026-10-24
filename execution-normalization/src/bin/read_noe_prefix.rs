//! 只读输出调用方明确声明的 NOE 已发布批前缀。
use contracts::execution_normalization::{CancellationRequestCompletedV1, NoeResultReferenceV1};
use execution_normalization::{
    DurableErrorV1, NoeHistoryReadStatsV1, NoeMemoryRecord, NoeRecordPositionV1,
    visit_durable_noe_records_v2,
};
use serde::Serialize;
use std::{
    env,
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    process::ExitCode,
};

#[derive(Serialize)]
struct Begin {
    kind: &'static str,
    format: &'static str,
    published_prefix: u64,
}

#[derive(Serialize)]
struct Record<'a> {
    kind: &'static str,
    reference: NoeResultReferenceV1,
    position: NoeRecordPositionV1,
    record: &'a NoeMemoryRecord<CancellationRequestCompletedV1>,
}

#[derive(Serialize)]
struct Complete {
    kind: &'static str,
    stats: NoeHistoryReadStatsV1,
}

fn stdout_error(operation: &'static str, error: impl std::fmt::Display) -> DurableErrorV1 {
    DurableErrorV1::Io {
        operation,
        path: PathBuf::from("<stdout>"),
        message: error.to_string(),
    }
}

fn line(output: &mut impl Write, value: &impl Serialize) -> Result<(), DurableErrorV1> {
    serde_json::to_writer(&mut *output, value)
        .map_err(|error| stdout_error("serialize/write stdout JSONL", error))?;
    output
        .write_all(b"\n")
        .map_err(|error| stdout_error("write stdout JSONL newline", error))
}

fn read(directory: &Path, prefix: u64, output: &mut impl Write) -> Result<(), DurableErrorV1> {
    line(
        output,
        &Begin {
            kind: "begin",
            format: "noe-prefix-read-v1",
            published_prefix: prefix,
        },
    )?;
    let stats = visit_durable_noe_records_v2(directory, prefix, |reference, view| {
        line(
            output,
            &Record {
                kind: "record",
                reference,
                position: view.position(),
                record: view.record(),
            },
        )
    })?;
    line(
        output,
        &Complete {
            kind: "complete",
            stats,
        },
    )
}

fn diagnostic(message: impl std::fmt::Display) {
    let _ = writeln!(io::stderr().lock(), "{message}");
}

fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    let parsed = args
        .next()
        .zip(args.next())
        .and_then(|(directory, prefix)| {
            if args.next().is_some() {
                return None;
            }
            Some((
                PathBuf::from(directory),
                prefix.to_str()?.parse::<u64>().ok()?,
            ))
        });
    let Some((directory, prefix)) = parsed else {
        diagnostic("usage: read_noe_prefix <context-directory> <published-prefix:u64>");
        return ExitCode::from(2);
    };

    let mut output = BufWriter::new(io::stdout().lock());
    let result = read(&directory, prefix, &mut output);
    // 读取失败也尝试交出已写入缓冲的部分事实；额外输出错误不覆盖原始读取错误。
    let flushed = output
        .flush()
        .map_err(|error| stdout_error("flush stdout JSONL", error));
    // 已显式处理 flush，拆出缓冲，避免析构时再次尝试输出未成功交付的尾部。
    let _ = output.into_parts();
    match (result, flushed) {
        (Ok(()), Ok(())) => ExitCode::SUCCESS,
        (Err(error), flush) => {
            diagnostic(error);
            if let Err(error) = flush {
                diagnostic(format_args!("additional output failure: {error}"));
            }
            ExitCode::FAILURE
        }
        (Ok(()), Err(error)) => {
            diagnostic(error);
            ExitCode::FAILURE
        }
    }
}
