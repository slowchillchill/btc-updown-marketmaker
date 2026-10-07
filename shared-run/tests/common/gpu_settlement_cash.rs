//! 原实际Ledger材料的费用切片；before为输入，after仅作独立CPU预期。
use super::*;
use std::{path::Path, sync::Mutex};

type Materials = Arc<Mutex<Vec<LedgerRecordMaterialV2>>>;

pub fn bind(execution: &mut PublicSimExecutionV1, ledger: &mut LedgerWriterV1, root: &Path) -> Option<Materials> {
    if std::env::var_os("B_GPU_SETTLEMENT_CASH_VALIDATOR").is_none() {
        execution.bind_ledger_recording_v2(ledger, root).unwrap();
        return None;
    }
    let materials = Arc::new(Mutex::new(Vec::new()));
    let output = materials.clone();
    ledger.bind_record_sink_v2(Box::new(move |material| output.lock().unwrap().push(material)));
    Some(materials)
}

fn snapshot(ledger: &LedgerWriterV1) -> serde_json::Value {
    let read = ledger.economic_read().unwrap();
    serde_json::json!({"projection":read.base(),"cash":read.cash(),
        "directional_cash":read.residual_cash().directional_cash_pusd,
        "other_cash":read.residual_cash().other_cash_pusd,
        "markets":read.market_totals().collect::<Vec<_>>(),
        "costs":read.settlement_costs().map(|c|serde_json::json!({"fact":c.fact,"attribution":c.attribution})).collect::<Vec<_>>(),
        "resolutions":read.resolutions().map(|r|serde_json::json!({"resolved":r.resolved,"request":r.request,
            "attribution":r.settlement_cost_attribution,"paired_receivable":r.paired_receivable_pusd})).collect::<Vec<_>>(),
        "trades":read.trades().map(|t|serde_json::json!({"identity":t.trade_identity,"fact":t.fact,
            "economic_applied":t.economic_applied,"success_applied":t.success_applied,
            "actual_net_fee":t.actual_net_fee_pusd})).collect::<Vec<_>>(),
        "successful_trades":read.successful_trades().map(|t|serde_json::json!({"identity":t.trade_identity,"fact":t.terminal,
            "remaining_shares":t.remaining_success_shares,"remaining_principal":t.remaining_principal_cost_pusd})).collect::<Vec<_>>()})
}

fn selected(record: &::execution_normalization::NoeMemoryRecordV3) -> bool {
    use ::execution_normalization::{NormalizationRecord as N,SourceNeutralEvidenceKind as E};
    match record {
        N::Evidence { evidence:E::SettlementLifecycleV2 {result},.. } => matches!(result,
            SourceNeutralSettlementLifecycleResultV2::Cost(_) | SourceNeutralSettlementLifecycleResultV2::ExternalCash(_)),
        N::EconomicFactV2 {fact,..} => matches!(&fact.disposition,SourceNeutralEconomicDispositionV2::AccountTransition {
            transition:SourceNeutralAccountTransitionKindV2::SettlementCostRecognized {..}
                | SourceNeutralAccountTransitionKindV2::SettlementCostPaid {..}
                | SourceNeutralAccountTransitionKindV2::ExternalCashApplied {..},..}),
        _ => false,
    }
}

pub fn finish(materials: Option<Materials>,root: &Path,label: &str,seed: &str) {
    let Some(materials)=materials else {return};
    let mut writer=LedgerBackgroundWriterV2::new(root);
    let mut blocks=Vec::<serde_json::Value>::new();
    let mut source_records=Vec::new();
    let mut initials=Vec::new();
    for material in materials.lock().unwrap().iter() {
        if let Some(record)=material.consumed_record_v1() {
            if let ::execution_normalization::NormalizationRecord::Evidence {
                evidence: ::execution_normalization::SourceNeutralEvidenceKind::InitialMarket {result},..}=record.record() {
                initials.push(result.clone());
            }
        }
        let record=material.consumed_record_v1().filter(|r|selected(r.record()));
        let before=record.map(|_| {
            writer.finish().unwrap();
            snapshot(&LedgerWriterV1::open_durable(root).unwrap())
        });
        assert!(writer.write(material).unwrap());
        if let Some(record)=record {
            writer.finish().unwrap();
            let after=snapshot(&LedgerWriterV1::open_durable(root).unwrap());
            let row=serde_json::json!({"position":record.position(),"context":record.context(),
                "visibility":record.visibility(),"record":record.record(),"ledger_entry_seq":material.ledger_entry_seq().unwrap()});
            source_records.push(row.clone());
            let batch=serde_json::to_value(&record.position().source_watermark).unwrap();
            let feedback=serde_json::to_value(&record.record().source_order().sequence).unwrap();
            if let Some(block)=blocks.last_mut().filter(|b|b["batch"]==batch && b["feedback"]==feedback) {
                block["records"].as_array_mut().unwrap().push(row);block["after"]=after;
            } else {
                blocks.push(serde_json::json!({"batch":batch,"feedback":feedback,"before":before.unwrap(),"after":after,"records":[row]}));
            }
        }
    }
    writer.finish().unwrap();
    assert!(!blocks.is_empty());
    let source=serde_json::json!({"source_contract":"actual_NOE_Cost_Cash_slices_at_committed_CPU_prefixes",
        "label":label,"seed":seed,"initials":initials,"source_records":source_records,"blocks":blocks});
    let directory=std::path::PathBuf::from(std::env::var("B_GPU_ORDER_ARTIFACT_DIR").unwrap());
    std::fs::write(directory.join(format!("{label}.source.json")),serde_json::to_vec_pretty(&source).unwrap()).unwrap();
    let mut child=std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_SETTLEMENT_CASH_VALIDATOR").unwrap())
        .stdin(std::process::Stdio::piped()).stdout(std::fs::File::create(directory.join(format!("{label}.GPU.stdout"))).unwrap())
        .stderr(std::fs::File::create(directory.join(format!("{label}.GPU.stderr"))).unwrap()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(),&source).unwrap();
    let status=child.wait().unwrap();
    assert!(status.success(),"{label}: GPU费用切片失败，见专属stderr");
}
