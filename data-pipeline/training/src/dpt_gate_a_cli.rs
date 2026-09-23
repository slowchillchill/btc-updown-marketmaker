use std::error::Error;
use std::path::PathBuf;

use crate::canonical_run::CanonicalRunScope;
use crate::freeze_direct_source_input_set;
use crate::gate_a_publication::{
    build_gate_a_publication, gate_a_formal_canonical_config, gate_a_formal_output_config,
    publish_gate_a_candidate,
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn run(args: impl IntoIterator<Item = String>) -> Result<()> {
    let mut args = args.into_iter();
    let command = args.next().ok_or("需要 build 或 publish 子命令")?;
    let mut manifests = Vec::new();
    let mut condition_id = None;
    let mut start = None;
    let mut end = None;
    let mut candidate = None;
    let mut output = None;
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} 缺少值"))?;
        match flag.as_str() {
            "--manifest" => manifests.push(PathBuf::from(value)),
            "--condition-id" => condition_id = Some(value),
            "--received-start-utc-ms" => start = Some(value.parse::<i64>()?),
            "--received-end-utc-ms" => end = Some(value.parse::<i64>()?),
            "--candidate-dir" => candidate = Some(PathBuf::from(value)),
            "--output-dir" => output = Some(PathBuf::from(value)),
            _ => return Err(format!("未知参数 {flag}").into()),
        }
    }
    let candidate = candidate.ok_or("缺少 --candidate-dir")?;
    match command.as_str() {
        "build" => {
            let scope = CanonicalRunScope::new(
                condition_id.ok_or("缺少 --condition-id")?,
                start.ok_or("缺少 --received-start-utc-ms")?,
                end.ok_or("缺少 --received-end-utc-ms")?,
            )?;
            let input = freeze_direct_source_input_set(&manifests)?;
            build_gate_a_publication(
                &input,
                &scope,
                &candidate,
                &gate_a_formal_canonical_config(),
                &gate_a_formal_output_config(),
            )?;
            println!("{}", candidate.display());
        }
        "publish" => {
            let output = output.ok_or("缺少 --output-dir")?;
            println!(
                "{}",
                publish_gate_a_candidate(&candidate, &output)?.display()
            );
        }
        _ => return Err(format!("未知子命令 {command}").into()),
    }
    Ok(())
}

pub fn main() {
    if let Err(error) = run(std::env::args().skip(1)) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
