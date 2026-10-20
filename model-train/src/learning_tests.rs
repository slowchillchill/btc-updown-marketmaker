//! 零模型回归：走实际公共再生与完成采集读取入口，不启动worker。
use super::*;
use crate::learning_test_fixture as fixture;
use backtest::*;
use std::collections::BTreeMap;

fn material(root: &std::path::Path) -> (PublicExportRulesV1, HistoricalDayConfigV1) {
    let input = root.join("input");
    std::fs::create_dir(&input).unwrap();
    let export = fixture::build(&input, 0);
    let template = root.join("template");
    std::fs::create_dir(&template).unwrap();
    let (run, _) = fixture::common::setup(&template);
    let mut candidate = HistoricalPreparedRunConfigV1::from(&run);
    candidate.public_statistics = Some(export.statistics);
    candidate.timing.public_delivery_delay_ms = export.public_delivery_delay_ms;
    candidate.strategy.effective_parameters["model_consumer"] = json!({"account_key":"a","sampling_seed":1,"request_namespace":"a","progress_every":0,"learning":null});
    (
        export.rules,
        HistoricalDayConfigV1 {
            preparation: export.preparation,
            candidates: vec![candidate],
            progress_interval_seconds: 0,
            producer_lead_groups: Some(2),
        },
    )
}
#[test]
fn heterogeneous_statistics_and_delivery_replay_use_own_identity_and_equal_inputs_share() {
    let root = tempfile::tempdir().unwrap();
    let (rules, mut day) = material(root.path());
    let base = fixture::common::BASE;
    for (key, windows, delay) in [
        ("b", vec![17, 1000], 100),
        ("c", vec![250, 500, 1000, 2000, 5000], 500),
        ("d", vec![250, 500, 1000, 2000, 5000], 100),
    ] {
        let mut c = day.candidates[0].clone();
        c.public_statistics.as_mut().unwrap().momentum_windows_ms = windows;
        c.timing.public_delivery_delay_ms = DelayScheduleV1::Fixed(delay);
        c.strategy.effective_parameters["model_consumer"]["account_key"] = json!(key);
        day.candidates.push(c);
    }
    let queries = day
        .candidates
        .iter()
        .map(|c| {
            (
                c.strategy.effective_parameters["model_consumer"]["account_key"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
                [base + 2500, base + 4100].into_iter().collect(),
            )
        })
        .collect();
    let mut received = BTreeMap::new();
    let streams = visit_day_frames(&rules, None, &day, &queries, |key, at, group, frames| {
        let values = frames
            .iter()
            .map(|f| crate::encoding::market_projection(f, at))
            .collect::<Vec<_>>();
        received.insert((key.to_owned(), at), (serde_json::to_value(group)?, values));
        Ok(())
    })
    .unwrap();
    assert_eq!(streams, 3, "a/d同身份只运行一次公共流，异构b/c分别归属");
    for at in [base + 2500, base + 4100] {
        assert_eq!(received[&("a".into(), at)], received[&("d".into(), at)]);
        assert_eq!(
            received[&("a".into(), at)].0,
            received[&("b".into(), at)].0,
            "相同组引用不表示统计参数相同"
        );
        assert_ne!(received[&("a".into(), at)].1, received[&("b".into(), at)].1);
    }
    assert!(
        [base + 2500, base + 4100]
            .into_iter()
            .any(|at| received[&("a".into(), at)].0 != received[&("c".into(), at)].0),
        "不同公共交付时序具有各自可信组"
    );
    // 用相同原公共入口分别核对各身份的真实输出，而不是复制统计公式。
    for c in &day.candidates {
        let key = c.strategy.effective_parameters["model_consumer"]["account_key"]
            .as_str()
            .unwrap();
        let config = ContinuousPublicExportConfigV1 {
            rules: rules.clone(),
            preparation: day.preparation.clone(),
            markets: PublicObservationMarketsV1::All,
            statistics: c.public_statistics.clone().unwrap(),
            public_delivery_delay_ms: c.timing.public_delivery_delay_ms.clone(),
            observe_after_groups: false,
            query_times_utc_ms: vec![base + 2500, base + 4100],
            export_new_trades: false,
        };
        visit_continuous_public_frames_v1(
            &config,
            |at, group, frames| {
                assert_eq!(
                    received[&(key.to_owned(), at)],
                    (
                        serde_json::to_value(group)?,
                        frames
                            .iter()
                            .map(|f| crate::encoding::market_projection(f, at))
                            .collect()
                    )
                );
                Ok(())
            },
            |_| {},
        )
        .unwrap();
    }
}
fn zero_read() -> LearningAccountReadV1 {
    LearningAccountReadV1 {
        confirmed: std::sync::Arc::new(ledger::ConfirmedEconomicsReadV1 {
            version: "confirmed_economics_v1".into(),
            components: vec![],
            unresolved_position_principal_pusd: R::zero().public(),
            unknown_negative_cost_identities: vec![],
            covered_unapportioned_negative_cost_identities: vec![],
        }),
        obligations: vec![],
        pending_noe_record_count: 0,
        qualified_rebates: vec![],
    }
}
pub(super) fn collection(root: &std::path::Path, mixed: bool) -> (LearningJobConfig, Value) {
    let (rules, mut day) = material(root);
    let mut second = day.candidates[0].clone();
    second.strategy.effective_parameters["model_consumer"]["account_key"] = json!("b");
    day.candidates.push(second);
    let mut group = Value::Null;
    let q = [
        (
            "a".into(),
            [fixture::common::BASE + 2500].into_iter().collect(),
        ),
        ("b".into(), [].into_iter().collect()),
    ]
    .into_iter()
    .collect();
    visit_day_frames(&rules, None, &day, &q, |_, _, g, _| {
        group = serde_json::to_value(g)?;
        Ok(())
    })
    .unwrap();
    let mut reports = vec![];
    for (i, c) in day.candidates.iter_mut().enumerate() {
        let path = root.join(format!("trajectory-{i}.jsonl"));
        // 与消费者装配一样，调用前create_new空文件，不依赖首decide。
        drop(crate::strategy::open_collection_trajectory(&path).unwrap());
        let count = usize::from(mixed && i == 1);
        if count > 0 {
            let frame = json!({"version":"learning-trajectory-v1","processing_start_utc_ms":fixture::common::BASE+2500,"old_value":0.5,"old_joint_logp":-0.3,"k_exact":R::zero().public(),"market_frame":{"market_cutoff":{"decision_time":{"logical_time_utc_ms":fixture::common::BASE+2500},"receipt":{"group":group}}}});
            std::fs::write(&path, format!("{frame}\n")).unwrap();
        }
        c.strategy.effective_parameters["model_consumer"]["learning"] =
            json!({"trajectory_path":path});
        reports.push(json!({"report":{"calculation_count":count,"learning_account_start":zero_read(),"learning_account_memory":zero_read()}}));
    }
    let consumer=crate::ConsumerRunConfig {rules:rules.into(),days:vec![day],parallel_dates:1,worker:serde_json::from_value(json!({"python":"unused","script":"unused","model":"unused","device":"cuda:0","torch_threads":4,"max_batch":2,"encoding":{"field_buckets":4096,"category_buckets":4096}})).unwrap()};
    let parameters = LearningParameters {
        learning_rate: 0.0003,
        epsilon: 0.15,
        value_weight: 1.0,
        money_scale_pusd: 2000.0,
        tau_gae_s: 300.0,
        tbptt_steps: 4,
        ppo_epochs: 2,
        checkpoint_directory: root.join("checkpoint"),
        trajectory_page_rows: 4,
        trajectory_cached_pages: 2,
        training_sequence_batch: 2,
    };
    let config = LearningJobConfig {
        consumer: consumer.clone(),
        parameters,
        group_id: "zero-model-regression".into(),
        output_directory: root.to_owned(),
        active_end_utc_ms: fixture::common::BASE + 1000,
        active_end_by_day_utc_ms: None,
        audit_before_update: false,
        gradient_checks: false,
        fault: None,
        attempt_report: root.join("report.json"),
    };
    (
        config,
        json!({"consumer":consumer,"summary":{"days":[{"candidates":reports}]}}),
    )
}
#[test]
fn all_n0_completed_entry_reports_objectives_without_samples_or_commit() {
    let root = tempfile::tempdir().unwrap();
    let (config, complete) = collection(root.path(), false);
    let targets = prepare_collected_targets(&config, &complete).unwrap();
    let result = finish_without_actor(&config, &targets).unwrap().unwrap();
    assert_eq!(result["committed"], false);
    assert_eq!(result["no_actor_samples"], true);
    assert!(!config.parameters.checkpoint_directory.exists());
    let units: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("unit-objectives.json")).unwrap())
            .unwrap();
    for key in ["a", "b"] {
        assert_eq!(units[key]["n0"], true);
        assert_eq!(units[key]["actor_samples"], 0);
        assert_eq!(units[key]["p_start"], json!(R::zero().public()));
        assert_eq!(units[key]["j_end"], json!(R::zero().public()));
        assert!(units[key]["b_pre"].is_null());
    }
}
#[test]
fn mixed_n0_completed_entry_preserves_only_real_samples_and_missing_nonempty_is_error() {
    let root = tempfile::tempdir().unwrap();
    let (config, complete) = collection(root.path(), true);
    let targets = prepare_collected_targets(&config, &complete).unwrap();
    assert!(finish_without_actor(&config, &targets).unwrap().is_none());
    assert_eq!(targets.rows["b"].meta.len(), 1);
    assert!(targets.rows["a"].meta.is_empty());
    assert_eq!(targets.labels["b"].len(), 1);
    assert!(targets.labels["a"].is_empty());
    std::fs::remove_file(root.path().join("trajectory-1.jsonl")).unwrap();
    assert!(prepare_collected_targets(&config, &complete).is_err());
    std::fs::write(root.path().join("trajectory-1.jsonl"), b"").unwrap();
    assert!(
        prepare_collected_targets(&config, &complete)
            .err()
            .unwrap()
            .to_string()
            .contains("实际调用计数")
    );
}
