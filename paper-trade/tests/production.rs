#[path = "../../shared-run/tests/common/fixture_input.rs"]
mod fixture_input;
#[path = "../../shared-run/tests/common/fixture_policy.rs"]
mod fixture_policy;
#[path = "../../shared-run/tests/common/fixture_recording.rs"]
mod fixture_recording;
#[path = "../../shared-run/tests/common/fixture_reference.rs"]
mod fixture_reference;
mod common {
    pub use super::fixture_input::*;
    pub use super::fixture_policy::*;
    pub use super::fixture_recording::*;
}

use common::*;
use contracts::*;
use data_collect::handoff::{read_handoff_v2, write_handoff_v2};
use data_pipeline_core::{public_input::*, typed_raw::*};
use data_pipeline_training::public_online::*;
use shared_run::paper::*;
use sim_system::{execution::RequestContextV2, latency_profile::*, platform_rules::*};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::net::{UnixListener, UnixStream};

#[path = "../../shared-run/tests/common/fixture_paper.rs"]
mod fixture_paper;
use fixture_paper::*;

use paper_trade::*;
use shared_run::paper_strategy::*;
#[tokio::test(flavor = "current_thread")]
async fn formal_continuous_inventory_uses_same_production_host_without_account_or_strategy_copy() {
    let evidence = PaperHostEvidenceCase::new("formal-continuous-inventory");
    let (base, rules) = setup(&evidence.path);
    let mut strategy = baseline_mm::ContinuousInventoryConfigV1::parse_toml(include_str!(
        "../../baseline-mm/configs/strategies/continuous_inventory/adopted-v1.toml"
    ))
    .unwrap();
    strategy.initial_cash_pusd = rust_decimal::Decimal::from(100);
    let config = PaperTradeConfigV1 {
        runtime: PaperStrategyConfigV1 {
            runtime: base.runtime,
            statistics_input_set_identity: Sha256Hex("d".repeat(64)),
            statistics: PublicStatisticsConfigV1 {
                momentum_windows_ms: vec![],
                spot_variance_windows_ms: vec![],
                terminal_integral_windows_ms: vec![],
                blb_lookback_windows_ms: vec![],
            },
        },
        strategy,
        request_namespace: "formal-paper-ci".into(),
        progress_every_ms: 100.try_into().unwrap(),
    };
    std::fs::write(
        evidence.path.join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    let listener = UnixListener::bind(&config.runtime.runtime.socket_path).unwrap();
    let (computed_tx, computed_rx) = tokio::sync::oneshot::channel();
    let mut computed = Some(computed_tx);
    let observer = |p: PaperStrategyProgressV1<'_>| {
        if p.measurements.calculations_completed > 0 {
            if let Some(sender) = computed.take() {
                sender.send(()).unwrap();
            }
        }
    };
    let peer = async {
        let mut stream = accept(&listener).await;
        send(&mut stream, 1, metadata()).await;
        computed_rx.await.unwrap();
        // 无业务源健康/概率输入时正式策略保持NoTrade；不人为补齐行情或现金。
        write_handoff_v2(
            &mut stream,
            &DataCollectHandoffV2::End {
                last_confirmed_sequence: 1,
            },
        )
        .await
        .unwrap();
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            run_paper_trade_v1(&config, rules, std::future::pending(), observer),
            peer
        )
    })
    .await
    .unwrap();
    let report = run.unwrap();
    assert!(matches!(
        report.outcome,
        PaperStrategyEndV1::Input(PublicOnlineOutcomeV1::End(_))
    ));
    assert!(report.measurements.calculations_completed >= 1);
    assert_eq!(report.measurements.actual_sent_intents, 0);
    assert!(report.sim_noe_ledger_recording_drained);
    let accepted = inputs(&config.runtime.runtime.sim_log);
    assert_eq!(
        accepted
            .iter()
            .filter(|i| i["input"] == "CREATE_SESSION")
            .count(),
        1
    );
    assert_eq!(
        accepted
            .iter()
            .filter(|i| i["input"] == "ACTIVATE_SESSION")
            .count(),
        1
    );
    assert!(requests(&config.runtime.runtime.sim_log).is_empty());
    std::fs::write(
        evidence.path.join("report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn formal_strategy_submits_exact_paired_action_from_online_necessary_input() {
    let evidence = PaperHostEvidenceCase::new("formal-paired-action");
    let (base, rules) = setup(&evidence.path);
    let adopted = baseline_mm::ContinuousInventoryConfigV1::parse_toml(include_str!(
        "../../baseline-mm/configs/strategies/continuous_inventory/adopted-v1.toml"
    ))
    .unwrap();
    // 与已交付策略正式工程夹具采用相同有限健康配置；不改生产默认或策略公式。
    let mut value = serde_json::to_value(adopted).unwrap();
    for (key, n) in value["health"].as_object_mut().unwrap() {
        *n = serde_json::json!(if key.contains("normal") { 0 } else { 1_000_000 });
    }
    value["probability_reference"]["binance_innovation_weight"] = serde_json::json!(0.0);
    value["initial_cash_pusd"] = serde_json::json!("100");
    let config = PaperTradeConfigV1 {
        runtime: PaperStrategyConfigV1 {
            runtime: base.runtime,
            statistics_input_set_identity: Sha256Hex("d".repeat(64)),
            statistics: PublicStatisticsConfigV1 {
                momentum_windows_ms: vec![],
                spot_variance_windows_ms: vec![300_000],
                terminal_integral_windows_ms: vec![],
                blb_lookback_windows_ms: vec![],
            },
        },
        strategy: serde_json::from_value(value).unwrap(),
        request_namespace: "formal-action".into(),
        progress_every_ms: 100.try_into().unwrap(),
    };
    std::fs::write(
        evidence.path.join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    let listener = UnixListener::bind(&config.runtime.runtime.socket_path).unwrap();
    let (sent_tx, sent_rx) = tokio::sync::oneshot::channel();
    let mut sent_tx = Some(sent_tx);
    let progress = |p: PaperStrategyProgressV1<'_>| {
        if p.measurements.actual_sent_intents > 0 {
            if let Some(tx) = sent_tx.take() {
                tx.send(()).unwrap();
            }
        }
    };
    let peer = async {
        let mut stream = accept(&listener).await;
        let mut meta = metadata();
        let TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(m)) = &mut meta else {
            panic!()
        };
        let start = now() / 1000 * 1000;
        m.market_start_utc = start;
        m.market_end_utc = start + 300_000;
        m.slug = format!("btc-updown-5m-{}", start / 1000);
        send(&mut stream, 1, meta).await;
        send(&mut stream, 2, book_row(1)).await;
        let mut down = book_row(2);
        if let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)) = &mut down {
            if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut r.event {
                *asset_id = DOWN_TOKEN_ID.into();
            }
        }
        send(&mut stream, 3, down).await;
        let mut spot = fixture_reference::reference_after(now(), 1).common;
        spot.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
        spot.topic = "crypto_prices_chainlink".into();
        spot.source_native_payload_json = serde_json::json!({"topic":"crypto_prices_chainlink","type":"update","timestamp":spot.message_timestamp,"payload":{"symbol":"btc/usd","timestamp":spot.payload_timestamp,"value":"64950.01"}}).to_string();
        send(
            &mut stream,
            4,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot)),
        )
        .await;
        sent_rx.await.unwrap();
        write_handoff_v2(
            &mut stream,
            &DataCollectHandoffV2::End {
                last_confirmed_sequence: 4,
            },
        )
        .await
        .unwrap();
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            run_paper_trade_v1(&config, rules, std::future::pending(), progress),
            peer
        )
    })
    .await
    .expect("有限正式动作未提交，保留事实，不造策略有效");
    let report = run.unwrap();
    let requests = requests(&config.runtime.runtime.sim_log);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["method"], "POST_ORDERS");
    assert_eq!(requests[0]["request"]["items"].as_array().unwrap().len(), 2);
    assert_eq!(report.measurements.actual_sent_intents, 1);
    for (ordinal, token) in [(0usize, UP_TOKEN_ID), (1, DOWN_TOKEN_ID)] {
        let item = &requests[0]["request"]["items"][ordinal];
        assert_eq!(item["batch_item_ordinal"], ordinal);
        assert_eq!(
            item["item"]["item_id"],
            format!("formal-action:0:{ordinal}")
        );
        assert_eq!(item["item"]["order"]["token_id"], token);
        assert_eq!(
            item["item"]["order"]["kind"]["GtcLimit"]["limit_price_pusd_per_share"],
            "0.45"
        );
        assert_eq!(
            item["item"]["order"]["kind"]["GtcLimit"]["size_shares"]
                .as_str()
                .unwrap()
                .parse::<rust_decimal::Decimal>()
                .unwrap(),
            rust_decimal::Decimal::from(50)
        );
    }
    std::fs::write(
        evidence.path.join("report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    std::fs::write(
        evidence.path.join("requests.json"),
        serde_json::to_vec_pretty(&requests).unwrap(),
    )
    .unwrap();
}

fn persisted_noe_facts(root: &Path) -> (usize, bool) {
    use ::execution_normalization::{NormalizationRecord, SourceNeutralEvidenceKind};
    use contracts::execution_normalization::*;
    let mut accepted = 0;
    let mut success = false;
    if !root.exists() {
        return (0, false);
    }
    for entry in std::fs::read_dir(root).unwrap() {
        let context = entry.unwrap().path();
        let commits = context.join("commits");
        if !commits.is_dir() {
            continue;
        }
        let prefix = std::fs::read_dir(commits)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .filter(|n| n.to_str().is_some_and(|n| n.ends_with(".manifest.json")))
            .count() as u64;
        ::execution_normalization::visit_durable_noe_records_v2(&context, prefix, |_, view| {
            if let NormalizationRecord::Evidence { evidence, .. } = view.record() {
                match evidence {
                    SourceNeutralEvidenceKind::Control {
                        result:
                            SourceNeutralControlResult::Submission {
                                outcome: SourceNeutralSubmissionOutcomeV2::Accepted { .. },
                            },
                    } => accepted += 1,
                    SourceNeutralEvidenceKind::TradeLifecycle { result }
                        if result.economic_disposition
                            == SourceNeutralTradeEconomicDispositionV1::FinalSuccess =>
                    {
                        // 只控制独立测试对端；耐久身份不冒充策略已消费反馈。
                        if let SimV2SourceRecordReferenceV1::TradeLifecycle {
                            feedback_received_at_utc_ms,
                            ..
                        } = view.record().source_reference().record
                        {
                            success |= feedback_received_at_utc_ms <= now();
                        }
                    }
                    SourceNeutralEvidenceKind::TradeLifecycleV3 { result }
                        if result.result.economic_disposition
                            == SourceNeutralTradeEconomicDispositionV1::FinalSuccess =>
                    {
                        // V4原成交结果封装保留同一TradeLifecycle来源身份。
                        if let SimV2SourceRecordReferenceV1::TradeLifecycle {
                            feedback_received_at_utc_ms,
                            ..
                        } = view.record().source_reference().record
                        {
                            success |= feedback_received_at_utc_ms <= now();
                        }
                    }
                    _ => {}
                }
            }
            Ok(())
        })
        .unwrap();
    }
    (accepted, success)
}
async fn wait_persisted_fact(root: &Path, success: bool) {
    loop {
        let facts = persisted_noe_facts(root);
        if if success { facts.1 } else { facts.0 >= 2 } {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn adjacent_markets_share_one_account_and_preserve_old_filled_leg_responsibility() {
    const NEXT_CONDITION: &str =
        "0x2222222222222222222222222222222222222222222222222222222222222222";
    let evidence = PaperHostEvidenceCase::new("adjacent-old-risk");
    let (base, rules) = setup(&evidence.path);
    let adopted = baseline_mm::ContinuousInventoryConfigV1::parse_toml(include_str!(
        "../../baseline-mm/configs/strategies/continuous_inventory/adopted-v1.toml"
    ))
    .unwrap();
    let mut value = serde_json::to_value(adopted).unwrap();
    for (key, n) in value["health"].as_object_mut().unwrap() {
        *n = serde_json::json!(if key.contains("normal") { 0 } else { 1_000_000 });
    }
    value["probability_reference"]["binance_innovation_weight"] = serde_json::json!(0.0);
    value["initial_cash_pusd"] = serde_json::json!("100");
    let config = PaperTradeConfigV1 {
        runtime: PaperStrategyConfigV1 {
            runtime: base.runtime,
            statistics_input_set_identity: Sha256Hex("a".repeat(64)),
            statistics: PublicStatisticsConfigV1 {
                momentum_windows_ms: vec![],
                spot_variance_windows_ms: vec![300_000],
                terminal_integral_windows_ms: vec![],
                blb_lookback_windows_ms: vec![],
            },
        },
        strategy: serde_json::from_value(value).unwrap(),
        request_namespace: "adjacent".into(),
        progress_every_ms: 100.try_into().unwrap(),
    };
    let listener = UnixListener::bind(&config.runtime.runtime.socket_path).unwrap();
    let (new_read, read_new) = tokio::sync::oneshot::channel();
    let mut new_read = Some(new_read);
    let progress = |p: PaperStrategyProgressV1<'_>| {
        if p.measurements.applied_inputs >= 10
            && !p.calculation_in_flight
            && p.phase == "处理计算结果后等待"
        {
            if let Some(tx) = new_read.take() {
                let _ = tx.send(());
            }
        }
    };
    let end = (now() / 1000 + 4) * 1000;
    let spot = |seq| {
        let mut s = fixture_reference::reference_after(now(), seq).common;
        s.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
        s.topic = "crypto_prices_chainlink".into();
        s.source_native_payload_json = serde_json::json!({"topic":"crypto_prices_chainlink","type":"update","timestamp":s.message_timestamp,"payload":{"symbol":"btc/usd","timestamp":s.payload_timestamp,"value":"64950.01"}}).to_string();
        TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(s))
    };
    let peer = async {
        let mut stream = accept(&listener).await;
        let mut first = metadata();
        let TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(m)) = &mut first else {
            panic!()
        };
        m.market_start_utc = end - 300_000;
        m.market_end_utc = end;
        m.slug = format!("btc-updown-5m-{}", m.market_start_utc / 1000);
        send(&mut stream, 1, first).await;
        send(&mut stream, 2, book_row(1)).await;
        let mut down = book_row(2);
        if let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)) = &mut down {
            if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut r.event {
                *asset_id = DOWN_TOKEN_ID.into();
            }
        }
        send(&mut stream, 3, down).await;
        send(&mut stream, 4, spot(1)).await;
        wait_persisted_fact(&config.runtime.runtime.noe_root, false).await;
        assert!(now() < end, "原双BUY实际接纳在第一市场仍可撮合时");
        for (seq, size) in [(3, "50"), (4, "0")] {
            let mut row = book_row(seq);
            let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)) = &mut row else {
                panic!()
            };
            r.event = PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![PolymarketPriceChangeEntry {
                    asset_id: UP_TOKEN_ID.into(),
                    price: "0.45".into(),
                    size: Some(size.into()),
                    side: PolymarketSide::Buy,
                    best_bid: None,
                    best_ask: None,
                }],
            };
            send(&mut stream, seq + 2, row).await;
        }
        wait_persisted_fact(&config.runtime.runtime.noe_root, true).await;
        tokio::time::sleep(Duration::from_millis((end + 1 - now()).max(0) as u64)).await;
        let mut next = metadata();
        let TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(m)) = &mut next else {
            panic!()
        };
        m.condition_id = NEXT_CONDITION.into();
        m.clob_market_id = "3195577".into();
        m.asset_ids = r#"["40","5"]"#.into();
        m.market_start_utc = end;
        m.market_end_utc = end + 300_000;
        m.slug = format!("btc-updown-5m-{}", end / 1000);
        m.collector_admission_sequence = 2;
        send(&mut stream, 7, next).await;
        for (sequence, token) in [(8, "40"), (9, "5")] {
            let mut row = book_row(sequence);
            let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)) = &mut row else {
                panic!()
            };
            r.market = NEXT_CONDITION.into();
            if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut r.event {
                *asset_id = token.into();
            }
            send(&mut stream, sequence, row).await;
        }
        send(&mut stream, 10, spot(2)).await;
        read_new.await.unwrap();
        write_handoff_v2(
            &mut stream,
            &DataCollectHandoffV2::End {
                last_confirmed_sequence: 10,
            },
        )
        .await
        .unwrap();
    };
    let run = Box::pin(run_paper_trade_v1(
        &config,
        rules,
        std::future::pending(),
        progress,
    ));
    let (run, ()) =
        tokio::time::timeout(Duration::from_secs(40), async { tokio::join!(run, peer) })
            .await
            .expect("有限跨市场正式组合未完成，保留事实");
    let report = run.unwrap();
    let projection = report.ledger_consumed.as_ref().unwrap();
    let balance = projection
        .market_projections
        .iter()
        .find(|m| m.condition_id.0 == CONDITION_ID)
        .unwrap()
        .outcome_balances
        .iter()
        .find(|b| b.token_id.0 == UP_TOKEN_ID)
        .expect("旧市场真实持仓不能因换市场消失");
    assert_eq!(
        balance
            .balance_shares
            .0
            .parse::<rust_decimal::Decimal>()
            .unwrap(),
        rust_decimal::Decimal::from(50)
    );
    let accepted = inputs(&config.runtime.runtime.sim_log);
    assert_eq!(
        accepted
            .iter()
            .filter(|i| i["input"] == "CREATE_SESSION")
            .count(),
        1
    );
    assert_eq!(
        accepted
            .iter()
            .filter(|i| i["input"] == "ACTIVATE_SESSION")
            .count(),
        1
    );
    assert_eq!(
        accepted
            .iter()
            .filter(|i| i["input"] == "ENROLL_MARKET")
            .count(),
        2
    );
    let requests = requests(&config.runtime.runtime.sim_log);
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "POST_ORDERS"
                && r["request"]["context"]["global_request_id"] == "adjacent:0")
            .count(),
        1
    );
    std::fs::write(
        evidence.path.join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    std::fs::write(
        evidence.path.join("report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    std::fs::write(
        evidence.path.join("requests.json"),
        serde_json::to_vec_pretty(&requests).unwrap(),
    )
    .unwrap();
}
