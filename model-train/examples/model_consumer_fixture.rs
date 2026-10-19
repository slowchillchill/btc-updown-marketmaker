//! 合成Parquet只通过正式ConsumerRunConfig/原日factory进入同一真实模型消费者。
#[allow(dead_code)]
#[path = "../../backtest/examples/support/continuous_fixture.rs"]
mod reference_fixture;
use backtest::*;
use contracts::*;
use ledger::*;
use model_train::{worker::WorkerConfig, *};
use reference_fixture::common as fixture_material;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use sim_system::{platform_rules::*, public_consumption::*};
use std::{path::Path, time::Instant};
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    worker: WorkerConfig,
    implementation_identity: ImplementationIdentityV1,
    iterations: usize,
    accounts: usize,
    initial_cash_pusd: Vec<ExactDecimal>,
    sampling_seeds: Vec<u64>,
    progress_every: u64,
    timing: HistoricalTimingV1,
    public_statistics: PublicStatisticsConfigV1,
    finality_confirmation_ms: u64,
    #[serde(default)]
    received_duration_ms: Option<u64>,
    #[serde(default)]
    account_ordinals: Option<Vec<usize>>,
}
fn output_bytes(root: &Path) -> std::io::Result<u64> {
    let mut size = 0;
    for entry in std::fs::read_dir(root)? {
        let p = entry?.path();
        size += if p.is_dir() {
            output_bytes(&p)?
        } else {
            p.metadata()?.len()
        };
    }
    Ok(size)
}
fn main() -> Result<(), MaterialError> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 4 {
        return Err(
            "用法: model_consumer_fixture prepare|run <显式配置.json> <新输出根目录>".into(),
        );
    }
    let config: Config = serde_json::from_slice(&std::fs::read(&args[2])?)?;
    let root = Path::new(&args[3]);
    std::fs::create_dir(root)?;
    if config.accounts == 0
        || config.accounts > 2
        || config.initial_cash_pusd.len() != config.accounts
        || config.sampling_seeds.len() != config.accounts
    {
        return Err("合成夹具账户配置须显式匹配1或2账户".into());
    }
    let started = Instant::now();
    let mut days = Vec::new();
    let mut sealed = None;
    // 00:05后合成短窗，既不删短窗首尾，也不因第一市场尚未开始阻止公开允许交易。
    let base = fixture_material::BASE + 300_000;
    for iteration in 0..config.iterations {
        let path = root.join(format!("case-{iteration}"));
        std::fs::create_dir(&path)?;
        let (mut c, source) =
            fixture_material::setup_day_with_embedded_rules(&path, base, "2026-08-24", true);
        reference_fixture::append_reference_inputs_at(&path, base);
        reference_fixture::append_late_anchor_at(&path, base);
        // base是本合成短窗起点，日覆盖仍封口于声明UTC日的午夜边界。
        let manifest_path = &c.preparation.scope.manifest_paths[0];
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(manifest_path)?)?;
        manifest["collection_day_coverage"]["ended_utc_ms"] =
            (fixture_material::BASE + 86_400_000).into();
        std::fs::write(manifest_path, serde_json::to_vec(&manifest)?)?;
        std::fs::write(
            &c.finality_policy_path,
            serde_json::to_vec(
                &json!({"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10},{"state":"CONFIRMED","offset_ms":config.finality_confirmation_ms}],"economic_application_ordinal":1},"failure_overrides":[]}),
            )?,
        )?;
        let rules = derive_public_maker_snapshot_v3(
            HistoricalProcessSnapshotRequestV1 {
                snapshot_path: source.path().into(),
                expected_file_sha256: source.file_sha256().clone(),
                replay_causal_start_utc_ms: i64::MAX,
            },
            path.join("rules.json"),
            config.implementation_identity.clone(),
            &[SimCallerRoleV1::ModelTrain],
        )?;
        if iteration == 0 {
            sealed = Some(rules.clone());
        }
        if let Some(duration) = config.received_duration_ms {
            c.preparation.scope.received_end_utc_ms = base
                .checked_add(i64::try_from(duration)?)
                .ok_or("合成范围溢出")?;
        }
        c.implementation_identity = config.implementation_identity.clone();
        c.public_statistics = Some(config.public_statistics.clone());
        c.gamma_resolution_policy = GammaResolutionPolicyV1::QualifiedPublicConsumption;
        c.timing = config.timing.clone();
        c.session.caller_role = SimCallerRoleV1::ModelTrain;
        c.session.input_lifecycle_policy = InputLifecyclePolicyV1::FiniteInput;
        let mut candidates = Vec::new();
        for account in 0..config.accounts {
            let out = path.join(format!("account-{account}"));
            std::fs::create_dir(&out)?;
            let mut candidate = HistoricalPreparedRunConfigV1::from(&c);
            let ordinal = config
                .account_ordinals
                .as_ref()
                .map_or(account, |v| v[account]);
            let key = format!("synthetic-{iteration}-{ordinal}");
            candidate.session.account_id = key.clone();
            candidate.session.run_id = key.clone();
            candidate.session.session_id = key.clone();
            candidate.session.initial_account_state.available_pusd =
                config.initial_cash_pusd[account].clone();
            let consumer = ConsumerConfig {
                account_key: key.clone(),
                sampling_seed: config.sampling_seeds[account],
                request_namespace: key.clone(),
                progress_every: config.progress_every,
            };
            candidate.strategy = StrategyDescriptorV1 {
                strategy_id: "b-model-consumer".into(),
                strategy_rule_version: "b-gru-conditional-gtc-v1".into(),
                effective_parameters: json!({"model_consumer":consumer,"model":"由顶层worker显式指定","purpose":"合成工程接线，无学习"}),
            };
            candidate.sim_log = out.join("sim.jsonl");
            candidate.noe_root = out.join("noe");
            candidate.ledger_root = out.join("ledger");
            candidate.metadata_path = out.join("run.json");
            candidate.result_path = out.join("result.json");
            candidate.research_evidence_path = Some(out.join("decisions.jsonl"));
            candidates.push(candidate);
        }
        days.push(HistoricalDayConfigV1 {
            preparation: c.preparation,
            candidates,
            progress_interval_seconds: 1,
            producer_lead_groups: Some(2),
        });
    }
    let rules = sealed.ok_or("未指定合成运行数")?;
    let run = ConsumerRunConfig {
        worker: config.worker.clone(),
        rules: PublicExportRulesV1 {
            snapshot_path: rules.path().into(),
            expected_file_sha256: rules.file_sha256().clone(),
        },
        days,
        parallel_dates: 1,
    };
    std::fs::write(
        root.join("effective-fixture-config.json"),
        serde_json::to_vec_pretty(&config)?,
    )?;
    std::fs::write(
        root.join("consumer-run-config.json"),
        serde_json::to_vec_pretty(&run)?,
    )?;
    if args[1] == "prepare" {
        println!("{}", serde_json::to_string_pretty(&run)?);
        return Ok(());
    }
    if args[1] != "run" {
        return Err("未知夹具模式".into());
    }
    let mut summary = run_consumer(&run)?;
    let mut facts = Vec::new();
    for day in &run.days {
        for candidate in &day.candidates {
            let ledger = LedgerWriterV1::open_durable(&candidate.ledger_root)?;
            let read = ledger.economic_read().ok_or("没有真实账本")?;
            facts.push(json!({"account":candidate.session.account_id,"trades":read.trades().map(|t|json!({"fact":t.fact,"economic_applied":t.economic_applied,"success_applied":t.success_applied})).collect::<Vec<_>>()}));
        }
    }
    std::fs::write(
        root.join("trade-facts.json"),
        serde_json::to_vec_pretty(&facts)?,
    )?;
    summary["fixture_total_elapsed_seconds"] = started.elapsed().as_secs_f64().into();
    summary["output_bytes_before_summary"] = output_bytes(root)?.into();
    summary["program_sha256"] = format!(
        "{:x}",
        Sha256::digest(std::fs::read(std::env::current_exe()?)?)
    )
    .into();
    std::fs::write(
        root.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
