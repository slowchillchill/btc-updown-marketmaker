//! 只读本轮有限输出的原NOE公开结果；不运行账户，不重做经济归约。
use contracts::execution_normalization::ExecutionNormalizationContextReferenceV1;
use contracts::execution_normalization::SourceNeutralControlResult;
use execution_normalization::{
    DurableContextStoreV4, NormalizationRecord, SourceNeutralEvidenceKind,
};
use model_train::ConsumerRunConfig;
use serde_json::{Value, json};
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 3 {
        return Err("用法: model_fixture_feedback <消费者配置.json> <新输出.json>".into());
    }
    let config: ConsumerRunConfig = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let mut accounts = Vec::new();
    for day in config.days {
        for candidate in day.candidates {
            let mut records = Vec::new();
            for entry in std::fs::read_dir(&candidate.noe_root)? {
                let path = entry?.path().join("context.json");
                if !path.is_file() {
                    continue;
                }
                let context: Value = serde_json::from_slice(&std::fs::read(path)?)?;
                let reference = ExecutionNormalizationContextReferenceV1(
                    context["payload"]["context"]["context_reference"]
                        .as_str()
                        .ok_or("NOE上下文缺身份")?
                        .into(),
                );
                let store = DurableContextStoreV4::open(&candidate.noe_root, &reference)?;
                for commit in store.commits() {
                    for record in &commit.records {
                        let value = serde_json::to_value(record)?;
                        if matches!(
                            record,
                            NormalizationRecord::Evidence {
                                evidence: SourceNeutralEvidenceKind::Control {
                                    result: SourceNeutralControlResult::Submission { .. }
                                },
                                ..
                            }
                        ) {
                            records.push(value);
                        }
                    }
                }
            }
            accounts
                .push(json!({"account":candidate.session.account_id,"submission_records":records}));
        }
    }
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&args[2])?;
    serde_json::to_writer_pretty(file, &accounts)?;
    println!("原NOE公开提交反馈已只读导出，账户数{}", accounts.len());
    Ok(())
}
