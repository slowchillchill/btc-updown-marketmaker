//! #361：既有结果和持久证据的只读读出。这里不构造模型、SIM 或账户写入器。
use crate::learning::{confirmed_k, finite_j};
use contracts::ExactRationalV1;
use execution_normalization::{
    MemoryIdentityKeyV1, MemoryLogicalIdentityV1, NoeHistoryReadStatsV1, SourceNeutralLogicalKeyV1,
    visit_durable_noe_records_v2,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use shared_run::LearningAccountReadV1;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

type Result<T> = std::result::Result<T, String>;
fn load(path: &Path) -> Result<Value> {
    serde_json::from_reader(File::open(path).map_err(|e| format!("{}: {e}", path.display()))?)
        .map_err(|e| format!("{}: {e}", path.display()))
}
fn field<'a>(value: &'a Value, key: &str) -> Result<&'a Value> {
    value.get(key).ok_or_else(|| format!("缺少字段 {key}"))
}
fn unknown(reason: &str) -> Value {
    json!({"Unknown": reason})
}
fn objective(value: std::result::Result<contracts::economics::EconomicRational, String>) -> Value {
    match value {
        Ok(v) => json!({"Known": v.public()}),
        Err(e) => unknown(&e),
    }
}
/// 接受 consumer summary 与 learner completed-collection 两种原外层。
/// 账户标识来自原 candidate，不用数组下标假定普通/积压身份。
pub fn account_readouts(envelope: &Value) -> Result<Vec<Value>> {
    let summary = envelope.get("summary").unwrap_or(envelope);
    let mut output = Vec::new();
    for (day, item) in field(summary, "days")?
        .as_array()
        .ok_or("days不是数组")?
        .iter()
        .enumerate()
    {
        for candidate in field(item, "candidates")?
            .as_array()
            .ok_or("candidates不是数组")?
        {
            let report = field(candidate, "report")?;
            let memory = field(report, "learning_account_memory")?;
            let read: LearningAccountReadV1 =
                serde_json::from_value(memory.clone()).map_err(|e| e.to_string())?;
            output.push(json!({
                "day_index": day, "run_id": field(candidate,"run_id")?,
                "comparison_identity_sha256": candidate.get("comparison_identity_sha256"),
                "source_binding": envelope.get("binding"),
                "source_program_sha256": summary.get("program_sha256"),
                "source_consumer_version": summary.get("consumer_version"),
                "account_instance_key": report.pointer("/ledger_consumed_memory/account_instance_key"),
                "confirmed_k_pusd": objective(confirmed_k(&read)),
                "finite_j_pusd": objective(finite_j(&read)),
                "negative_costs_known": read.confirmed.unknown_negative_cost_identities.is_empty(),
                "learning_account_memory": memory,
                "account_economics_memory": report.get("account_economics_memory"),
                "durable_noe_prefix": report.get("durable_noe_prefix"),
                "observation_cutoff": report.get("observation_cutoff"),
                "ledger_entry_sequence_at_report": report.pointer("/ledger_consumed_memory/ledger_entry_seq"),
                "special": report.get("learning_result"),
                "semantics": "K成功不证明必要负成本已知；J包含未决本金代理，不是已实现损益。返佣Unavailable和pending_noe沿原值保留。"
            }));
        }
    }
    Ok(output)
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestReadoutV1 {
    pub request_id: String,
    pub origin: Value,
    pub decisions: Vec<Value>,
    pub sim_requests: Vec<Value>,
    pub durable_noe: Vec<Value>,
    pub ledger_confirmation_changes: Vec<Value>,
    pub report_directed_confirmation: Value,
    pub unknown: Vec<String>,
}
fn requests(account: &Value) -> Result<BTreeMap<String, RequestReadoutV1>> {
    let Some(origins) = account
        .pointer("/special/repair_requests")
        .and_then(Value::as_object)
    else {
        return Err("缺少原special.repair_requests".into());
    };
    let mut rows = BTreeMap::new();
    for (id, origin) in origins {
        let mapping = account.pointer("/learning_account_memory/directed_confirmations/requests");
        let read = match mapping.and_then(|m| m.get(id)) {
            None => unknown("原截止报告没有此request映射"),
            Some(Value::Null) => unknown("原截止报告映射为None"),
            Some(value) => value.clone(),
        };
        rows.insert(id.clone(), RequestReadoutV1 {
            request_id: id.clone(), origin: origin.clone(), decisions: Vec::new(),
                sim_requests: Vec::new(), durable_noe: Vec::new(),
            ledger_confirmation_changes: Vec::new(), report_directed_confirmation: read,
            unknown: vec!["空映射、未发现持久事实均不证明拒绝或未成交；后续持久事实不补造较早宿主观察/应用".into()],
        });
    }
    Ok(rows)
}
fn visit_json_lines(path: &Path, mut visit: impl FnMut(u64, Value) -> Result<()>) -> Result<u64> {
    let reader = BufReader::new(File::open(path).map_err(|e| format!("{}: {e}", path.display()))?);
    let mut count = 0;
    for (line, value) in reader.lines().enumerate() {
        let value = value.map_err(|e| e.to_string())?;
        let value = serde_json::from_str(&value)
            .map_err(|e| format!("{}:{}: {e}", path.display(), line + 1))?;
        visit(line as u64 + 1, value)?;
        count += 1;
    }
    Ok(count)
}
fn decisions(path: &Path, run: &str, rows: &mut BTreeMap<String, RequestReadoutV1>) -> Result<u64> {
    visit_json_lines(path, |line, value| {
        if value.get("run_id").and_then(Value::as_str) != Some(run) {
            return Ok(());
        }
        if let Some(intents) = value.get("intents").and_then(Value::as_array) {
            for intent in intents {
                if let Some(row) = intent
                    .pointer("/context/global_request_id")
                    .and_then(Value::as_str)
                    .and_then(|id| rows.get_mut(id))
                {
                    row.decisions.push(json!({"source_line":line,"run_id":run,"intent":intent,
                        "calculation_index":value.get("calculation_index"),"input_cutoff":value.get("input_cutoff"),
                        "started_at_utc_ms":value.get("started_at_utc_ms")}));
                }
            }
        }
        Ok(())
    })
}
// 只关联正式字段的完整身份。不得用字符串前缀/包含关系猜测订单或request归属。
fn identities(value: &Value, key: &str, output: &mut BTreeSet<MemoryLogicalIdentityV1>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                if k == key {
                    if let Ok(id) = serde_json::from_value(v.clone()) {
                        output.insert(id);
                    }
                } else {
                    identities(v, key, output);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                identities(value, key, output);
            }
        }
        _ => {}
    }
}
fn read_noe(
    directory: &Path,
    prefix: u64,
    run: &str,
    account_key: &Value,
    rows: &mut BTreeMap<String, RequestReadoutV1>,
) -> Result<NoeHistoryReadStatsV1> {
    let mut orders: BTreeMap<MemoryLogicalIdentityV1, BTreeSet<String>> = BTreeMap::new();
    // 按context保留待关联的订单/成交原事实，避免响应和反馈在同批内先后位置改变关联结果。
    // 只在本次读取中使用；不建立持久索引，不跨context复用身份。
    let mut related = Vec::new();
    let stats = visit_durable_noe_records_v2(directory, prefix, |reference, view| {
        if view.context().account_identity.run_scope != run
            || serde_json::to_value(&view.context().source.account_instance_key)
                .expect("正式身份可序列化")
                != *account_key
        {
            return Err(execution_normalization::DurableErrorV1::ContainerDecode {
                path: directory.into(),
                message: "context运行与原账户不符".into(),
            });
        }
        let record = view.record();
        let raw = serde_json::to_value(record).expect("正式NOE类型可序列化");
        let fact = json!({"reference":reference,"position":view.position(),"record":raw});
        if let MemoryIdentityKeyV1::Logical(
            SourceNeutralLogicalKeyV1::SubmissionItem {
                global_request_id, ..
            }
            | SourceNeutralLogicalKeyV1::DirectResponseRequest { global_request_id },
        ) = &record.logical_identity().key
            && let Some(row) = rows.get_mut(global_request_id)
        {
            let mut ids = BTreeSet::new();
            identities(&raw, "order_identity", &mut ids);
            for id in ids {
                orders
                    .entry(id)
                    .or_default()
                    .insert(global_request_id.clone());
            }
            row.durable_noe.push(fact.clone());
        }
        let mut ids = BTreeSet::new();
        identities(&raw, "order_identity", &mut ids);
        if !ids.is_empty() {
            related.push((ids, fact));
        }
        Ok(())
    })
    .map_err(|e| e.to_string())?;
    for (ids, fact) in related {
        let selected: BTreeSet<String> = ids
            .iter()
            .filter_map(|id| orders.get(id))
            .flatten()
            .cloned()
            .collect();
        for id in selected {
            let row = rows.get_mut(&id).unwrap();
            if !row
                .durable_noe
                .iter()
                .any(|prior| prior["reference"] == fact["reference"])
            {
                row.durable_noe.push(fact.clone());
            }
        }
    }
    Ok(stats)
}
fn read_ledger(
    directory: &Path,
    report_entry: Option<u64>,
    rows: &mut BTreeMap<String, RequestReadoutV1>,
) -> Result<()> {
    let mut trades = BTreeSet::new();
    let mut by_trade: BTreeMap<MemoryLogicalIdentityV1, BTreeSet<String>> = BTreeMap::new();
    for (id, row) in rows.iter() {
        for fact in &row.durable_noe {
            let mut selected = BTreeSet::new();
            identities(&fact["record"], "trade_identity", &mut selected);
            for trade in selected {
                trades.insert(trade.clone());
                by_trade.entry(trade).or_default().insert(id.clone());
            }
        }
    }
    ledger::visit_trade_confirmation_changes_v1(directory,&trades,|change| {
        if let Some(ids)=by_trade.get(&change.trade.trade_identity) {
            for id in ids {
                rows.get_mut(id).unwrap().ledger_confirmation_changes.push(json!({
                    "change":change,
                    "at_or_before_report_ledger_entry":report_entry.map(|seq|change.ledger_entry_sequence<=seq),
                    "semantics":"仅此已提交Ledger变化证明该trade应用；recorded_visibility是原消费可见性。未覆盖尾部保持未知。"
                }));
            }
        }
        Ok(())
    }).map_err(|e|e.to_string())?;
    for row in rows.values_mut() {
        if row.decisions.is_empty() {
            row.unknown
                .push("未找到原decisions中匹配run和完整request的intent".into());
        }
        if row.durable_noe.is_empty() {
            row.unknown
                .push("原耐久前缀未找到此request的规范化结果，不能区分未发生与未记录".into());
        }
        if row.ledger_confirmation_changes.is_empty() {
            row.unknown.push(
                "未找到此request成交的已提交Ledger确认变化；未封存尾部和截止前应用未知".into(),
            );
        }
    }
    Ok(())
}
fn read_sim(path: &Path, rows: &mut BTreeMap<String, RequestReadoutV1>) -> Result<Value> {
    let config = load(&path.join("run.json"))?;
    let max = field(field(&config, "config")?, "max_group_bytes")?
        .as_u64()
        .ok_or("max_group_bytes不是整数")?;
    let summary = sim_system::execution::visit_sim_requests_v1(
        &path.join("sim.jsonl"),
        max as usize,
        |sequence, request, coordinates| {
            if let Some(row) = rows.get_mut(&request.context().global_request_id) {
                row.sim_requests.push(
                    json!({"input_sequence":sequence,"request":request,"coordinates":coordinates}),
                );
            }
            Ok(())
        },
    )
    .map_err(|e| e.to_string())?;
    for row in rows.values_mut() {
        if row.sim_requests.is_empty() {
            row.unknown
                .push("原SIM完整帧中未找到该request；不能据此推断拒绝/未成交".into());
        }
    }
    serde_json::to_value(summary).map_err(|e| e.to_string())
}
fn contexts(root: &Path) -> Result<Vec<PathBuf>> {
    let mut output = Vec::new();
    for entry in fs::read_dir(root).map_err(|e| e.to_string())? {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.join("context.json").is_file() {
            output.push(path);
        }
    }
    output.sort();
    Ok(output)
}
fn save(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = File::create_new(path).map_err(|e| format!("{}: {e}", path.display()))?;
    serde_json::to_writer_pretty(&mut file, value).map_err(|e| e.to_string())?;
    file.write_all(b"\n").map_err(|e| e.to_string())
}
/// 本包只有四个积压context；独立context并行，内部保持原提交顺序。
/// 原件仅读取，输出采用独立目录。失败保留已完成context与partial清单，调用方不得自动重跑。
pub fn read_existing_package(root: &Path, output: &Path) -> Result<()> {
    fs::create_dir(output).map_err(|e| format!("创建独立输出目录: {e}"))?;
    let started = std::time::Instant::now();
    let mut tasks = Vec::new();
    let mut frozen = Vec::new();
    let mut training = Vec::new();
    for arm in ["control", "special"] {
        for stage in ["readout", "learn"] {
            let stage_root = root.join(format!("{stage}-{arm}"));
            let source = stage_root.join(if stage == "readout" {
                "summary.json"
            } else {
                "group/completed-collection.json"
            });
            let envelope = load(&source)?;
            let accounts = account_readouts(&envelope)?;
            for account in &accounts {
                if stage == "readout" {
                    frozen.push(json!({"source":source,"arm":arm,"account":account}));
                }
            }
            let account_root = stage_root.join(if stage == "readout" {
                "accounts/backlog"
            } else {
                "group/collection/day-0-account-1"
            });
            let metadata = load(&account_root.join("run.json"))?;
            let expected_run = metadata
                .pointer("/config/session/run_id")
                .and_then(Value::as_str)
                .ok_or("原run.json缺config.session.run_id")?;
            let account = accounts
                .iter()
                .find(|a| a["run_id"].as_str() == Some(expected_run))
                .ok_or("summary中未找到原积压账户run")?;
            tasks.push((format!("{stage}-{arm}"), account_root, account.clone()));
            if stage == "learn" {
                let path = stage_root.join("group/unit-objectives.json");
                let original = load(&path)?;
                for account in accounts {
                    let run = account["run_id"].as_str().ok_or("原账户缺run_id")?;
                    let j = field(field(&original, run)?, "j_end")?.clone();
                    let _: ExactRationalV1 =
                        serde_json::from_value(j.clone()).map_err(|e| e.to_string())?;
                    training.push(
                        json!({"arm":arm,"source":path,"run_id":run,"original_j_end":j,
                        "account_memory_source":source,"account":account,
                        "semantics":"原unit-objectives生产J对照；未用新计算替换原值"}),
                    );
                }
            }
        }
    }
    save(
        &output.join("accounts.json"),
        &json!({"frozen":frozen,"training_original_j":training}),
    )?;
    let results = std::thread::scope(|scope| {
        let handles:Vec<_>=tasks.into_iter().map(|(identity,path,account)| {
            scope.spawn(move || -> Result<Value> {
                let run=account["run_id"].as_str().ok_or("原账户缺run_id")?;
                let mut rows=requests(&account)?;
                eprintln!("context={identity} phase=SIM elapsed_seconds={:.3}",started.elapsed().as_secs_f64());
                let sim_read=read_sim(&path,&mut rows)?;
                if sim_read.pointer("/opened/run_id").and_then(Value::as_str)!=Some(run){return Err("SIM运行与原账户不符".into());}
                eprintln!("context={identity} phase=NOE elapsed_seconds={:.3}",started.elapsed().as_secs_f64());
                let decision_lines=decisions(&path.join("decisions.jsonl"),run,&mut rows)?;
                let prefix=account["durable_noe_prefix"].as_u64().ok_or("缺耐久前缀")?;
                let mut stats=Vec::new();
                for context in contexts(&path.join("noe"))? {stats.push(read_noe(&context,prefix,run,&account["account_instance_key"],&mut rows)?);}
                read_ledger(&path.join("ledger"),account["ledger_entry_sequence_at_report"].as_u64(),&mut rows)?;
                let result=json!({"identity":identity,"run_id":run,"account_instance_key":account["account_instance_key"],
                    "source_account_directory":path,"observation_cutoff":account["observation_cutoff"],
                    "durable_noe_prefix":prefix,"noe_read_stats":stats,"decision_lines":decision_lines,
                    "requests":rows,"sim_read":sim_read,
                    "sim_projection_coverage":"仅Request/PublicRequest；其他日志种类只计数，不解释其业务含义",
                    "ledger_coverage":"仅选择已关联成交的已提交确认变化；未封存尾部不读取/不修复"});
                save(&output.join(format!("{identity}.json")),&result)?;
                eprintln!("context={identity} requests={} elapsed_seconds={:.3}",rows.len(),started.elapsed().as_secs_f64());
                Ok(json!({"identity":identity,"requests":rows.len(),"status":"complete"}))
            })
        }).collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or_else(|_| Err("context读取线程异常".into()))
            })
            .collect::<Vec<_>>()
    });
    let complete = results.iter().all(std::result::Result::is_ok);
    save(
        &output.join("coverage.json"),
        &json!({"complete":complete,"contexts":results,
        "elapsed_seconds":started.elapsed().as_secs_f64(),"source_package":root,
        "semantics":"持久事件、截止前宿主观察和Ledger应用分别保留；不恢复/重放账户。原件读取失败不得自动重跑。"}),
    )?;
    if complete {
        Ok(())
    } else {
        Err("部分context读取失败，保留partial输出；不得自动重跑".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::{ExecutionReadValueV1, economics::EconomicRational as R};
    fn memory() -> LearningAccountReadV1 {
        LearningAccountReadV1 {
            confirmed: std::sync::Arc::new(ledger::ConfirmedEconomicsReadV1 {
                version: "confirmed_economics_v1".into(),
                components: vec![ledger::ConfirmedComponentReadV1 {
                    identity: "gross".into(),
                    kind: ledger::ConfirmedComponentKindV1::PairedGross,
                    amount_pusd: R::from_decimal(7.into()).public(),
                }],
                unresolved_position_principal_pusd: R::from_decimal(2.into()).public(),
                unknown_negative_cost_identities: vec![],
                covered_unapportioned_negative_cost_identities: vec![],
            }),
            obligations: vec![],
            pending_noe_record_count: 9,
            qualified_rebates: vec![],
            directed_confirmations: None,
        }
    }
    fn envelope(memory: &LearningAccountReadV1) -> Value {
        json!({"days":[{"candidates":[{"run_id":"ordinary","report":{"learning_account_memory":memory,"learning_result":{"repair_requests":{}}}},{"run_id":"backlog","report":{"learning_account_memory":memory,"learning_result":{"repair_requests":{"req":{"call":1}}}}}]}]})
    }
    #[test]
    fn readout_envelopes_keep_exact_accounts_original_errors_and_unavailable() {
        let mut m = memory();
        m.qualified_rebates.push(shared_run::QualifiedRebateReadV1 {
            identity: "rebate:Maker:0".into(),
            kind: contracts::economics::RebateKindV1::Maker,
            attribution_day_start_utc_ms: 0,
            result: ExecutionReadValueV1::Unavailable,
        });
        let mut original = envelope(&m);
        original["program_sha256"] = json!("a".repeat(64));
        original["consumer_version"] = json!("frozen-consumer-v1");
        let snapshot = original.clone();
        let a = account_readouts(&original).unwrap();
        let b = account_readouts(&json!({"summary":original})).unwrap();
        assert_eq!(a, b);
        for account in &a {
            assert_eq!(account["source_program_sha256"], json!("a".repeat(64)));
            assert_eq!(account["source_consumer_version"], "frozen-consumer-v1");
        }
        let binding = json!({"group_id":"learner-group-181", "consumer":{"version":"original-consumer-binding"},
            "parameters":{"special":{"version":"D-0375-special-v1","alpha":1}},"active_end_utc_ms":310000});
        let mut learner_summary = original.clone();
        learner_summary["program_sha256"] = json!("b".repeat(64));
        learner_summary["consumer_version"] = json!("learner-consumer-v1");
        let learner = json!({"summary":learner_summary,"binding":binding,
            "program_sha256":"c".repeat(64),"consumer_version":"outer-value-must-not-win"});
        let learner_snapshot = learner.clone();
        let read = account_readouts(&learner).unwrap();
        for account in &read {
            assert_eq!(account["source_binding"], binding);
            assert_eq!(account["source_binding"]["group_id"], "learner-group-181");
            assert_eq!(account["source_program_sha256"], json!("b".repeat(64)));
            assert_eq!(account["source_consumer_version"], "learner-consumer-v1");
        }
        assert_eq!(learner, learner_snapshot);
        assert_eq!(a.len(), 2);
        assert_eq!(a[0]["run_id"], "ordinary");
        assert_eq!(a[1]["run_id"], "backlog");
        assert_eq!(
            a[0]["confirmed_k_pusd"]["Known"],
            json!({"numerator":"7","denominator":"1"})
        );
        assert_eq!(
            a[0]["finite_j_pusd"]["Known"],
            json!({"numerator":"5","denominator":"1"})
        );
        assert_eq!(original, snapshot);
        std::sync::Arc::make_mut(&mut m.confirmed)
            .unknown_negative_cost_identities
            .push("fee".into());
        let a = account_readouts(&envelope(&m)).unwrap();
        assert_eq!(a[0]["negative_costs_known"], false);
        assert_eq!(a[0]["finite_j_pusd"]["Unknown"], finite_j(&m).unwrap_err());
        assert!(a[0]["confirmed_k_pusd"].get("Known").is_some());
        m.obligations.push(shared_run::ObligationPrincipalReadV1 {
            condition_id: contracts::ConditionId("market".into()),
            principal_upper_pusd: ExecutionReadValueV1::Unavailable,
        });
        assert_eq!(
            account_readouts(&envelope(&m)).unwrap()[0]["learning_account_memory"]["obligations"]
                [0]["principal_upper_pusd"],
            "Unavailable"
        );
    }
    #[test]
    fn readout_missing_none_empty_multi_and_same_request_in_another_run_are_distinct() {
        let mut account = account_readouts(&envelope(&memory())).unwrap().remove(1);
        let mut rows = requests(&account).unwrap();
        assert!(
            rows["req"]
                .report_directed_confirmation
                .get("Unknown")
                .is_some()
        );
        account["learning_account_memory"]["directed_confirmations"] =
            json!({"requests":{"req":null}});
        assert_eq!(
            requests(&account).unwrap()["req"].report_directed_confirmation,
            unknown("原截止报告映射为None")
        );
        account["learning_account_memory"]["directed_confirmations"]["requests"]["req"] = json!([]);
        assert_eq!(
            requests(&account).unwrap()["req"].report_directed_confirmation,
            json!([])
        );
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("decisions.jsonl");
        let values = [
            json!({"run_id":"another","intents":[{"context":{"global_request_id":"req"},"ordinal":9}]}),
            json!({"run_id":"backlog","intents":[{"context":{"global_request_id":"req"},"ordinal":0},{"context":{"global_request_id":"req"},"ordinal":1},{"context":{"global_request_id":"req-other"},"ordinal":2}]}),
        ];
        fs::write(
            &file,
            values.iter().map(|v| format!("{v}\n")).collect::<String>(),
        )
        .unwrap();
        decisions(&file, "backlog", &mut rows).unwrap();
        assert_eq!(rows["req"].decisions.len(), 2);
        assert_eq!(rows["req"].decisions[1]["intent"]["ordinal"], 1);
        assert!(rows["req"].durable_noe.is_empty());
    }
}
