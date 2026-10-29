//! #185 有限宿主计算测量；构造和成交装配不进入计时，不分派计时决定。
#[path = "../../shared-run/tests/common/fixture_clock.rs"]
mod fixture_clock;
#[path = "../../shared-run/tests/common/fixture_input.rs"]
mod fixture_input;
#[path = "../../shared-run/tests/common/fixture_recording.rs"]
mod fixture_recording;
#[path = "../../shared-run/tests/common/fixture_reference.rs"]
mod fixture_reference;
mod common {
    pub use super::fixture_clock::*;
    pub use super::fixture_input::*;
    pub use super::fixture_recording::*;
    pub use super::fixture_reference::*;
}

use baseline_mm::*;
use common::{TradingTimes, core, meta, pm, record};
use contracts::*;
use data_pipeline_core::{public_input::*, public_observation_core::*, typed_raw::*};
use data_pipeline_training::public_recording::*;
use ledger::*;
use rust_decimal::Decimal;
use serde_json::json;
use sha2::{Digest, Sha256};
use shared_run::*;
use sim_system::{
    execution::*, finality::*, latency_profile::*, platform_rules::*, public_consumption::*,
    scenario::*,
};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Instant};

// 当前探针使用V2合成工程配置；历史V1候选与旧测量结果保持原身份。
fn timing_engineering_config() -> ComplementaryPairConfigV3 {
    ComplementaryPairConfigV3::parse_toml(include_str!(
        "../configs/strategies/complementary_pair/synthetic-engineering-v3.toml"
    ))
    .unwrap()
}
#[test]
fn timing_entry_configuration_is_current_v3_engineering_material() {
    let config = timing_engineering_config();
    assert_eq!(RULE_VERSION, "candidate_v3");
    let value = serde_json::to_value(config).unwrap();
    assert!(value["repair"].get("maximum_overfill_shares").is_none());
    assert!(value["repair"].get("maximum_overfill_pusd").is_none());
}

const BASE: i64 = 1_787_614_800_000;
const CURRENT: usize = 865;
const RULE_SHA: &str = "4397d7b3a102616c93484892a1cdb5bfa1a6de23c9383e52a04bbb1d480ae422";
const FINALITY_SHA: &str = "b98102f86cbd2e0d635bdc5074dcb1d7c1fb1a84de61d419d07a475c2e8533ef";
type Driver = FinitePublicConsumptionV1<RecordedPublicGroupSourceV1<TradingTimes>>;
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn condition(i: usize) -> String {
    format!("0x{:064x}", i + 1)
}
fn token(i: usize, up: bool) -> String {
    (1000 + i * 2 + usize::from(!up)).to_string()
}

// 沿原 shared-run 测试装配，仅接受已封存的原规则与 finality；不修改费率或延迟。
fn execution(
    path: &Path,
    core: &PublicObservationCoreV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    finality: Arc<TradeFinalityPolicyV2>,
    run_id: &str,
) -> (PublicSimExecutionV1, MemoryNoeObservationV4) {
    let cash = "2000";
    let scenario = PlatformScenarioV1::try_new(
        core.fixed_markets()
            .map(|market| {
                let c = core.pm_context(&market.condition_id.0).unwrap();
                PlatformMarketV1::try_new(
                    c.market,
                    c.assets
                        .into_iter()
                        .map(|a| a.asset)
                        .collect::<Vec<_>>()
                        .try_into()
                        .unwrap(),
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let artifact = SimulationScenarioArtifactV1 {
        schema_version: PLATFORM_SCENARIO_SCHEMA_V1.into(),
        markets: scenario.market_set().markets().to_vec(),
    };
    let scenario = Arc::new(
        SealedPlatformScenarioV1::decode_and_seal(&serde_json::to_vec(&artifact).unwrap()).unwrap(),
    );
    let latency = load_embedded_proxy_latency_profile().unwrap();
    let opened = RunOpenedV2 {
        projection_version: BusinessProjectionVersionV1::V4,
        latency_sampler_version: "xoshiro256plusplus_jump_multiply_high_empirical_rank_v2".into(),
        run_id: run_id.into(),
        durability_role: RunDurabilityRoleV2::CleanRun,
        platform_rule_snapshot_file_sha256: rules.file_sha256().clone(),
        trade_finality_policy_file_sha256: finality.policy_id().clone(),
        platform_scenario_file_sha256: scenario.file_sha256().clone(),
        proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
        implementation_identity: ImplementationIdentityV1 {
            git_commit: std::env::var("ISSUE185_SOURCE_HEAD").unwrap(),
            source_identity_sha256: Sha256Hex(std::env::var("ISSUE185_SOURCE_SHA256").unwrap()),
            cargo_lock_sha256: Sha256Hex(hash(include_bytes!("../../Cargo.lock"))),
        },
        market_event_source_identity_sha256: Some(Sha256Hex("d".repeat(64))),
    };
    let reference = contracts::execution_normalization::DurableSourceReferenceV1 {
        source: contracts::execution_normalization::ImmutableSourceReferenceV1::Location(
            path.display().to_string(),
        ),
    };
    let port = MemoryNoeProjectionPort::<MeasuredCancellationEvidence>::new(
        path.parent().unwrap().join("noe"),
        opened.clone(),
        reference.clone(),
        reference,
        "create".into(),
        "shared-test".into(),
    );
    let observation = port.observation();
    let spec = PublicRunOpenedV1 {
        projection_version: opened.projection_version,
        run_id: opened.run_id.clone(),
        platform_rule_snapshot_file_sha256: opened.platform_rule_snapshot_file_sha256,
        trade_finality_policy_file_sha256: opened.trade_finality_policy_file_sha256,
        platform_scenario_file_sha256: opened.platform_scenario_file_sha256,
        proxy_latency_profile_file_sha256: opened.proxy_latency_profile_file_sha256,
        implementation_identity: opened.implementation_identity,
        market_event_source_identity_sha256: opened.market_event_source_identity_sha256,
    };
    let mut run = PublicSimExecutionV1::create_new(
        path,
        spec,
        rules.clone(),
        finality,
        scenario,
        latency,
        Box::new(port),
        16 << 20,
        128,
    )
    .unwrap();
    run.create_session(SessionSpecV3 {
        run_id: opened.run_id,
        session_id: "shared-session".into(),
        account_id: "shared-account".into(),
        caller_role: SimCallerRoleV1::Paper,
        caller_instance_id: "shared-caller".into(),
        creation_request_id: "create".into(),
        response_bridge_binding_id: "shared-bridge".into(),
        initial_account_state: InitialAccountStateV2 {
            available_pusd: ExactDecimal(cash.into()),
        },
        session_seed: Sha256Hex("e".repeat(64)),
    })
    .unwrap();
    run.activate_session("activate".into()).unwrap();
    (run, observation)
}
fn metadata(i: usize, at: i64, closed: bool, active: bool) -> TypedRawRowV2 {
    let DptIngressV1::Message {
        row: TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(mut m)),
        ..
    } = meta(0).ingress
    else {
        unreachable!()
    };
    m.condition_id = condition(i);
    m.clob_market_id = (4000000 + i).to_string();
    m.asset_ids = format!("[\"{}\",\"{}\"]", token(i, true), token(i, false));
    m.slug = format!(
        "btc-updown-5m-{}",
        if i == CURRENT {
            (BASE + 300000) / 1000
        } else {
            (BASE - 297000) / 1000
        }
    );
    m.market_start_utc = if i == CURRENT {
        BASE + 300000
    } else {
        BASE - 297000
    };
    m.market_end_utc = m.market_start_utc + 300000;
    m.timestamp_received = at;
    m.collector_admission_sequence = if at == BASE {
        i as u64 + 1
    } else {
        867 + i as u64
    };
    m.collector_admission_time_utc_ms = at + 1;
    m.fixed_identity_verified_at_utc = BASE - 1;
    m.metadata_fetch_start_utc = Some(at - 10);
    m.source_evidence_received_at_utc = Some(at);
    m.row_evidence_validation_completed_at_utc = Some(at + 1);
    m.market_lifecycle_state = if active {
        MetadataMarketLifecycleState::Active
    } else {
        MetadataMarketLifecycleState::Prefetched
    };
    m.market_runtime_role = if active {
        MetadataMarketRuntimeRole::Current
    } else {
        MetadataMarketRuntimeRole::Next
    };
    m.closed = Some(closed);
    m.accepting_orders = Some(!closed);
    TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(m))
}
fn book(i: usize, up: bool, at: i64, sequence: u64) -> TypedRawRowV2 {
    let event = PolymarketOrderbookEvent::Book {
        asset_id: token(i, up),
        bids: vec![PolymarketPriceLevel {
            price: "0.45".into(),
            size: "100".into(),
        }],
        asks: vec![PolymarketPriceLevel {
            price: "0.55".into(),
            size: "100".into(),
        }],
        last_trade_price: Some("0.50".into()),
    };
    let DptIngressV1::Message {
        row: TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(mut r)),
        ..
    } = pm(0, sequence, at, event).ingress
    else {
        unreachable!()
    };
    r.market = condition(i);
    r.timestamp_received = at;
    r.collector_admission_time_utc_ms = at + 1;
    TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r))
}
fn anchor(at: i64) -> TypedRawRowV2 {
    let a: MarketAnchorRuleUpdateInput = serde_json::from_value(json!({
        "schema_version":"polymarket_market_metadata_v6", "timestamp_received":at,
        "collector_admission_sequence":3000, "collector_admission_time_utc_ms":at+1,
        "metadata_origin":"gamma_event_by_slug", "collector_host_id":"host", "collector_region":"local",
        "slug":format!("btc-updown-5m-{}",(BASE+300000)/1000), "condition_id":condition(CURRENT), "clob_market_id":(4000000+CURRENT).to_string(), "asset_ids":format!("[\"{}\",\"{}\"]",token(CURRENT,true),token(CURRENT,false)),
        "market_start_utc":BASE+300000,"market_end_utc":BASE+600000,"fixed_identity_verified_at_utc":BASE-1,
        "market_lifecycle_state":"prefetched","market_runtime_role":"next","metadata_fetch_start_utc":at-10,
        "anchor_query_source_epoch_id":"host-timing-anchor","anchor_query_ordinal":1,"anchor_query_result":"observed",
        "price_to_beat_decimal":"65000","price_to_beat_source_field":"eventMetadata.priceToBeat",
        "twap_enabled":false,"http_received_at_utc":at,
        "http_request_url":"https://gamma-api.polymarket.com/events/slug/host-timing",
        "http_body_sha256":"a".repeat(64)
    })).unwrap();
    TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
        PolymarketMarketMetadataV6Input::AnchorRule(a),
    ))
}
fn sources(at: i64, n: u64) -> Vec<TypedRawRowV2> {
    let price = (65000_i64 + (n as i64 % 11) - 5).to_string();
    sources_with_price(at, n, price)
}
fn sources_with_price(at: i64, n: u64, price: String) -> Vec<TypedRawRowV2> {
    let mut spot = common::reference_after(at, n).common;
    spot.message_timestamp = at;
    spot.payload_timestamp = at;
    spot.age_ms = Some(0);
    spot.value = price.clone();
    spot.topic = "crypto_prices_chainlink".into();
    spot.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
    spot.source_native_payload_json =
        json!({"symbol":"btc/usd","value":price,"timestamp":at}).to_string();
    let trade=BinanceAggTradeRow {
        timestamp_received:at,rest_backfill_timestamp_received:None,ingest_source:"websocket".into(),rest_was_best_price_match:None,
        source_native_payload_json:json!({"e":"aggTrade","E":at,"T":at,"s":"BTCUSDT","a":n,"p":price,"q":"0.01","f":n*2,"l":n*2+1,"m":false}).to_string(),
        source_sequence_epoch_id:"host-timing-trade".into(),connection_generation:1,websocket_ingress_sequence:n,rest_response_ingress_sequence:None,rest_response_item_index:None,
        collector_admission_sequence:n,collector_admission_time_utc_ms:at+1,collector_host_id:"host".into(),collector_region:"local".into(),source_endpoint:"wss://stream.binance.com:9443/ws/btcusdt@aggTrade".into(),schema_version:BINANCE_AGGTRADE_SCHEMA.into(),
        record:BinanceAggTradeRecord::Trade {event_type:"aggTrade".into(),event_time:at,trade_time:at,symbol:"BTCUSDT".into(),aggregate_trade_id:n,price:price.clone(),quantity:"0.01".into(),first_trade_id:n*2,last_trade_id:n*2+1,buyer_is_maker:false,stream:"btcusdt@aggTrade".into(),ws_ignore:false}
    };
    vec![
        TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot)),
        TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(trade)),
    ]
}
struct Material {
    core: PublicObservationCoreV1,
    times: TradingTimes,
    early: u64,
    history: Vec<u64>,
    follow_up: Option<u64>,
}
fn material(root: &Path) -> Material {
    material_for_notrade(root, false, false)
}
fn material_for_notrade(root: &Path, missing_price: bool, follow_up: bool) -> Material {
    material_for_history(root, missing_price, follow_up, 863)
}
fn material_for_history(
    root: &Path,
    missing_price: bool,
    follow_up: bool,
    completed: usize,
) -> Material {
    let mut c = core();
    c.enable_source_progress();
    let mut writer =
        PublicRecordingWriterV1::create_historical(root, c.header(), common::config()).unwrap();
    let mut position = 0;
    let mut times = Vec::new();
    let mut push = |c: &mut PublicObservationCoreV1, row: TypedRawRowV2, at: i64| {
        let ingress = record(position, row);
        position += 1;
        let ids = BTreeMap::new();
        let prepared = prepare_public_ingress_v1(c, &ingress, &ids, &[]).unwrap();
        let applied = c.push(&prepared).unwrap().into_owned();
        let last = applied.applied.closed_groups.last().unwrap().group_position;
        times.extend(std::iter::repeat_n(
            at + 2,
            applied.applied.closed_groups.len(),
        ));
        writer.record(ingress, applied).unwrap();
        last
    };
    for i in (0..completed + 2).chain(std::iter::once(CURRENT)) {
        push(
            &mut c,
            metadata(i, BASE, i != CURRENT && i > 1, i != CURRENT),
            BASE,
        );
    }
    push(&mut c, anchor(BASE), BASE);
    let mut seq = 0;
    let mut early = 0;
    for i in 0..2 {
        for up in [true, false] {
            seq += 1;
            early = push(&mut c, book(i, up, BASE + 1000, seq), BASE + 1000);
        }
    }
    // 原经济路径在闭市前拥有足够时间完成请求及 finality。
    for i in 0..2 {
        for up in [true, false] {
            seq += 1;
            push(&mut c, book(i, up, BASE + 2000, seq), BASE + 2000);
        }
    }
    for i in 0..2 {
        push(&mut c, metadata(i, BASE + 3000, true, true), BASE + 3000);
    }
    let mut history = Vec::new();
    for n in if missing_price { 296..=296 } else { 0..=300 } {
        let at = BASE + 4000 + n * 1000;
        if n == 296 {
            push(&mut c, metadata(CURRENT, at, false, true), at);
        }
        let mut last = 0;
        for up in [true, false] {
            seq += 1;
            last = push(&mut c, book(CURRENT, up, at, seq), at);
        }
        if !missing_price {
            for row in sources(at, n as u64 + 1) {
                last = push(&mut c, row, at);
            }
        }
        history.push(last);
    }
    let follow_up = if follow_up {
        // 原请求出站+反馈入站各至多29697ms；一次固定后续前缀，不搜索完成时刻。
        let at = BASE + 364000;
        for up in [true, false] {
            seq += 1;
            push(&mut c, book(CURRENT, up, at, seq), at);
        }
        let mut last = 0;
        // 保持最后价格64999，正式新消息序号302；不更改已冻结输入。
        for row in sources_with_price(at, 302, "64999".into()) {
            last = push(&mut c, row, at);
        }
        Some(last)
    } else {
        None
    };
    writer.stabilize().unwrap();
    Material {
        core: c,
        times: TradingTimes(times),
        early,
        history,
        follow_up,
    }
}
fn buy(rules: &SealedProcessRuleSnapshotV3, i: usize, id: &str, maker: bool) -> ReservedIntentV1 {
    let price = if maker {
        Decimal::new(2, 1)
    } else {
        Decimal::new(6, 1)
    };
    let cost = rules
        .limit_buy_cost_v1(
            &500_u32.into(),
            if maker { 20 } else { 60 },
            &ExactDecimal("0.01".into()),
            maker,
        )
        .unwrap();
    ReservedIntentV1 {
        context: RequestContextV2 {
            global_request_id: id.into(),
            caller_correlation_id: None,
        },
        intent: Arc::new(ModelIntentV1::SubmitSingleBuy {
            condition_id: ConditionId(condition(i)),
            order: BuyLegIntentV1 {
                token_id: TokenId(token(i, true)),
                order: BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share: price,
                    size_shares: Decimal::from(5),
                    post_only: maker,
                },
            },
        }),
        reserves: vec![IntentReserveV1 {
            batch_item_ordinal: 0,
            reserve: ReserveIntentV1 {
                request_identity: LedgerRequestIdentityV1(format!("{id}-reserve")),
                condition_id: ConditionId(condition(i)),
                requested_principal_pusd: ExactDecimal(
                    (Decimal::from_str_exact(&cost.principal_micro.to_string()).unwrap()
                        / Decimal::from(1000000))
                    .to_string(),
                ),
                immediate_fee_reserve_pusd: ExactDecimal(
                    (Decimal::from_str_exact(&cost.fee_1e5_units.to_string()).unwrap()
                        / Decimal::from(100000))
                    .to_string(),
                ),
            },
        }],
    }
}
fn advance(driver: &mut Driver, group: u64) {
    loop {
        let r = driver.next_group().unwrap().unwrap();
        if r.0.group().group_position == group {
            break;
        }
    }
}
fn diagnostics(
    out: &StrategyOutputV1<ComplementaryPairDiagnosticV1>,
) -> (&ComplementaryPairDiagnosticV1, &[ReservedIntentV1]) {
    match out {
        StrategyOutputV1::Continue {
            diagnostics,
            intents,
            ..
        } => (diagnostics, intents),
        StrategyOutputV1::Exit { .. } => panic!("representative account must remain active"),
    }
}
/// 显式 opt-in，小构造测量；路径只由调用环境提供，不查真实历史文件。
#[tokio::test(flavor = "current_thread")]
#[ignore = "#185 sealed-rule representative timing; requires explicit input/output/source identities"]
async fn representative_host_calculation_timing() {
    let rules_path = std::env::var("ISSUE185_RULES").unwrap();
    let finality_bytes = std::fs::read(std::env::var("ISSUE185_FINALITY").unwrap()).unwrap();
    assert_eq!(hash(&finality_bytes), FINALITY_SHA);
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: rules_path.into(),
        expected_file_sha256: Sha256Hex(RULE_SHA.into()),
        replay_causal_start_utc_ms: BASE,
    })
    .unwrap();
    let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&finality_bytes).unwrap());
    let candidates =
        [json!({"identifier":"engineering-v2", "strategy":timing_engineering_config()})];
    let temp = tempfile::tempdir().unwrap();
    let dpt = temp.path().join("dpt");
    println!("setup: 866 markets, original rules and finality");
    let material = material(&dpt);
    let mut cells = Vec::new();
    for state in ["empty", "resting", "single_leg"] {
        for candidate in &candidates {
            let id = candidate["identifier"].as_str().unwrap();
            println!("setup/mature: {state}/{id}, 301 source points");
            let root = temp.path().join(format!("{state}-{id}"));
            std::fs::create_dir(&root).unwrap();
            let (mut execution, observation) = execution(
                &root.join("sim.jsonl"),
                &material.core,
                rules.clone(),
                finality.clone(),
                &format!("timing-{state}-{id}"),
            );
            let mut ledger = LedgerWriterV1::new_with_admission_v3(
                AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
            );
            execution
                .bind_ledger_recording_v2(&mut ledger, &root.join("ledger"))
                .unwrap();
            let mut shared = SharedExecutionV2::from_opening(ledger, observation)
                .unwrap()
                .unwrap();
            let mut source =
                RecordedPublicGroupSourceV1::open(&dpt, 128, 16 << 20, material.times.clone())
                    .unwrap();
            source.enable_source_progress();
            let mut driver =
                FinitePublicConsumptionV1::new(execution, source, format!("timing-{state}-{id}"));
            // 原有限宿主在未成功提交前交接回压；不重试复合驱动的已成功步骤。
            driver.enable_finite_recording_wait().unwrap();
            advance(&mut driver, material.early);
            shared
                .execute_batch_reserved(
                    &mut driver.decision().unwrap(),
                    &[
                        buy(&rules, 0, "closed-single-leg", false),
                        buy(&rules, 1, "closed-resting", true),
                    ],
                )
                .await
                .unwrap();
            let config: ComplementaryPairConfigV3 =
                serde_json::from_value(candidate["strategy"].clone()).unwrap();
            let mut strategy = ComplementaryPairV1::new(
                config,
                rules.clone(),
                format!("{state}-{id}"),
                StrategyContinuityV2::new_account(),
            )
            .unwrap();
            for (n, &g) in material.history.iter().enumerate() {
                advance(&mut driver, g);
                let input = shared
                    .strategy_observation_input_with_trade_details(
                        &mut driver,
                        strategy.requires_trade_economics_details(),
                    )
                    .unwrap()
                    .unwrap();
                let at = input
                    .action_before
                    .cutoff()
                    .decision_time()
                    .logical_time_utc_ms();
                input.calculate(&mut strategy, at).unwrap();
                if n == 299 && state != "empty" {
                    shared
                        .execute_batch_reserved(
                            &mut driver.decision().unwrap(),
                            &[buy(&rules, CURRENT, "current-state", state == "resting")],
                        )
                        .await
                        .unwrap();
                }
            }
            let before = shared
                .strategy_observation_input(&mut driver)
                .unwrap()
                .unwrap();
            assert_eq!(before.markets.len(), 866);
            assert_eq!(
                before
                    .markets
                    .iter()
                    .filter(|m| m.observation.closed.value == Some(true))
                    .count(),
                865
            );
            assert!(before.trade_economics.iter().any(|t| t.success_applied
                && t.economic_applied
                && t.fact.condition_id == ConditionId(condition(0))));
            let open_in = |i| {
                before
                    .action_before
                    .orders()
                    .filter(|o| {
                        o.condition_id == Some(&ConditionId(condition(i)))
                            && o.active_unfilled_shares
                                == ExecutionReadValueV1::Known(ExactDecimal("5".into()))
                    })
                    .count()
            };
            assert_eq!(
                open_in(1),
                1,
                "closed market retains actual resting-order responsibility"
            );
            assert_eq!(open_in(CURRENT), usize::from(state == "resting"));
            let current_fills = before
                .trade_economics
                .iter()
                .filter(|t| {
                    t.fact.condition_id == ConditionId(condition(CURRENT))
                        && t.success_applied
                        && t.economic_applied
                })
                .count();
            assert_eq!(current_fills, usize::from(state == "single_leg"));
            assert!(
                before
                    .trade_economics
                    .iter()
                    .all(|t| t.fact.condition_id == ConditionId(condition(0))
                        || t.fact.condition_id == ConditionId(condition(CURRENT)))
            );
            let old_fill = before
                .trade_economics
                .iter()
                .find(|t| t.fact.condition_id == ConditionId(condition(0)))
                .unwrap();
            assert_eq!(
                old_fill.fact.source_price_pusd_per_share,
                ExactDecimal("0.55".into())
            );
            assert_eq!(old_fill.fact.base_quantity_shares, ExactDecimal("5".into()));
            let current = before
                .markets
                .iter()
                .find(|m| m.observation.market.condition_id == ConditionId(condition(CURRENT)))
                .unwrap();
            assert_eq!(current.observation.closed.value, Some(false));
            let at = before
                .action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms();
            // 观察市场和Ledger经济市场分别计数；不以866推定经济规模。
            let economic_markets = &before.action_before.economics().markets;
            let positive_orphan_market_count = economic_markets
                .iter()
                .filter(|m| {
                    m.settlement_state == EconomicSettlementStateV1::Unresolved
                        && m.orphan_shares
                            .iter()
                            .any(|q| Decimal::from_str_exact(&q.0).unwrap() > Decimal::ZERO)
                })
                .count();
            let positive_orphan_leg_count = economic_markets
                .iter()
                .filter(|m| m.settlement_state == EconomicSettlementStateV1::Unresolved)
                .flat_map(|m| &m.orphan_shares)
                .filter(|q| Decimal::from_str_exact(&q.0).unwrap() > Decimal::ZERO)
                .count();
            let economic_market_count = economic_markets.len();
            let account_economics = serde_json::to_value(before.action_before.economics()).unwrap();
            let economics = format!("{:?}", before.action_before.economics());
            let trades = format!("{:?}", before.trade_economics);
            let mut assembly_samples = Vec::new();
            let mut decide_samples = Vec::new();
            let mut diagnostic_samples: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
            let mut output_counts: BTreeMap<&str, u64> = BTreeMap::new();
            let mut last = None;
            let requires_details = strategy.requires_trade_economics_details();
            for n in 0..=32 {
                // 独立 calculate 使用同一调用前策略状态；复制/恢复不计时，也不推进主实例。
                let mut diagnostic_strategy = ComplementaryPairV1::restore(
                    serde_json::from_value(candidate["strategy"].clone()).unwrap(),
                    rules.clone(),
                    strategy.checkpoint(),
                )
                .unwrap();
                let start = Instant::now();
                let input = shared
                    .strategy_observation_input_with_trade_details(&mut driver, requires_details)
                    .unwrap()
                    .unwrap();
                let assembly_ns = start.elapsed().as_nanos() as u64;
                let start = Instant::now();
                let output = strategy.decide(&input, at).unwrap();
                let decide_ns = start.elapsed().as_nanos() as u64;
                std::hint::black_box(&output);
                // 以下各项是同状态下的独立诊断调用，不能求和冒充完整装配分解。
                let frames = driver.frames_at_observation();
                let cutoff = driver.observation_cutoff().unwrap();
                let start = Instant::now();
                let frozen = shared.freeze_action_before(cutoff, &frames).unwrap();
                let freeze_ns = start.elapsed().as_nanos() as u64;
                let start = Instant::now();
                let projection = shared.ledger().funds_read_v1().projection().unwrap();
                let projection_ns = start.elapsed().as_nanos() as u64;
                let start = Instant::now();
                let economic_read = shared.ledger().economic_read().unwrap();
                let economic_read_ns = start.elapsed().as_nanos() as u64;
                let start = Instant::now();
                let market_totals: Vec<_> = economic_read.market_totals().collect();
                let market_totals_ns = start.elapsed().as_nanos() as u64;
                let start = Instant::now();
                let account_v1 = UnmarkedAccountEconomicsReadV3::from_ledger(&economic_read);
                let account_v1_ns = start.elapsed().as_nanos() as u64;
                let start = Instant::now();
                let account = AccountEconomicsReadV3::from_ledger_and_observations(
                    &economic_read,
                    frames.iter().map(|f| f.observation),
                );
                let account_ns = start.elapsed().as_nanos() as u64;
                let start = Instant::now();
                let cloned_markets = input.markets.clone();
                let clone_ns = start.elapsed().as_nanos() as u64;
                let start = Instant::now();
                let calculation = input.calculate(&mut diagnostic_strategy, at).unwrap();
                let calculate_ns = start.elapsed().as_nanos() as u64;
                // 原金额、截止、观察及策略结果在计时外核对；不分派诊断动作。
                assert_eq!(frozen.cutoff(), input.action_before.cutoff());
                assert_eq!(frozen.economics(), input.action_before.economics());
                assert_eq!(&projection, input.action_before.ledger());
                assert_eq!(&account, input.action_before.economics());
                assert_eq!(market_totals, account.markets);
                assert_eq!(account_v1.markets, account.markets);
                assert_eq!(
                    serde_json::to_value(&output).unwrap(),
                    serde_json::to_value(&calculation.output).unwrap()
                );
                assert_eq!(cloned_markets.len(), input.markets.len());
                if n == 0 || n == 32 {
                    for (original, cloned) in input.markets.iter().zip(&cloned_markets) {
                        assert_eq!(
                            serde_json::to_value(&original.observation).unwrap(),
                            serde_json::to_value(&cloned.observation).unwrap()
                        );
                        assert!(Arc::ptr_eq(&original.rule_context, &cloned.rule_context));
                    }
                }
                if n > 0 {
                    assembly_samples.push(assembly_ns);
                    decide_samples.push(decide_ns);
                    for (name, ns) in [
                        ("freeze_action_before", freeze_ns),
                        ("funds_projection", projection_ns),
                        ("economic_read_including_projection", economic_read_ns),
                        ("market_totals_collect", market_totals_ns),
                        ("account_economics_v1_from_existing_read", account_v1_ns),
                        ("account_economics_from_existing_read", account_ns),
                        ("frozen_market_vector_clone", clone_ns),
                        ("calculate_same_pre_decide_state", calculate_ns),
                    ] {
                        diagnostic_samples.entry(name).or_default().push(ns);
                    }
                    let kind = match &output {
                        StrategyOutputV1::Continue { intents, .. } if intents.is_empty() => {
                            "NoTrade"
                        }
                        StrategyOutputV1::Continue { .. } => "Actions",
                        StrategyOutputV1::Exit { .. } => "Exit",
                    };
                    *output_counts.entry(kind).or_default() += 1;
                }
                last = Some(output);
            }
            println!(
                "raw_samples_ns: {state}/{id} assembly={assembly_samples:?} decide={decide_samples:?}"
            );
            let after = shared
                .strategy_observation_input(&mut driver)
                .unwrap()
                .unwrap();
            assert_eq!(format!("{:?}", after.action_before.economics()), economics);
            assert_eq!(format!("{:?}", after.trade_economics), trades);
            let last = last.unwrap();
            let (d, intents) = diagnostics(&last);
            let current = d
                .markets
                .iter()
                .find(|m| m.condition_id == ConditionId(condition(CURRENT)))
                .unwrap();
            assert!(
                current.source_health.is_empty(),
                "current actual source path: {current:?}"
            );
            let ProbabilityEstimateV1::Known {
                sigma_usd_per_sqrt_second,
                ..
            } = current.probability
            else {
                panic!("current mature probability: {current:?}")
            };
            let expected = ((300.0 - 27.0 + 27.0 * 100.0) / 300.0_f64).sqrt();
            assert!(
                (sigma_usd_per_sqrt_second - expected).abs() < 1e-10,
                "real 300-pair variance: {sigma_usd_per_sqrt_second} vs {expected}"
            );
            let nearest_rank = |samples: &[u64]| {
                let mut sorted = samples.to_vec();
                sorted.sort_unstable();
                json!({"p50_ns":sorted[15],"p95_ns":sorted[30],"max_ns":sorted[31]})
            };
            let assembly_summary = nearest_rank(&assembly_samples);
            let decide_summary = nearest_rank(&decide_samples);
            println!(
                "measured: {state}/{id}, assembly={assembly_summary}, decide={decide_summary}, choice={:?}, intents={}",
                current.choice,
                intents.len()
            );
            let diagnostic_summaries: BTreeMap<_, _> = diagnostic_samples
                .iter()
                .map(|(name, samples)| (*name, nearest_rank(samples)))
                .collect();
            println!(
                "independent_diagnostics_ns: {state}/{id} {diagnostic_summaries:?} output_counts={output_counts:?}"
            );
            cells.push(json!({"state":state,"candidate":id,"economic_market_count":economic_market_count,"positive_unresolved_orphan_market_count":positive_orphan_market_count,"positive_unresolved_orphan_leg_count":positive_orphan_leg_count,"account_economics":account_economics,"requires_trade_economics_details":requires_details,"independent_diagnostic_samples_ns":diagnostic_samples,"independent_diagnostic_nearest_rank":diagnostic_summaries,"output_counts":output_counts,"assembly_samples_ns":assembly_samples,"decide_samples_ns":decide_samples,"assembly_nearest_rank":assembly_summary,"decide_nearest_rank":decide_summary,"current_diagnostic":current,"closed_diagnostics":d.markets.iter().filter(|m|m.condition_id==ConditionId(condition(0))||m.condition_id==ConditionId(condition(1))).collect::<Vec<_>>(),"intents":intents.iter().map(|i|format!("{:?}",i.intent)).collect::<Vec<_>>(),"account_economics_debug":economics,"trades_debug":trades}));
        }
    }
    assert_eq!(cells.len() * 32, 96);
    let output = json!({"format":"issue185-host-economics-composition-diagnostic-v1","strategy_rule_version":RULE_VERSION,"configuration_source":"synthetic-engineering-v3.toml","base_head":std::env::var("ISSUE185_SOURCE_HEAD").unwrap(),"source_identity_sha256":std::env::var("ISSUE185_SOURCE_SHA256").unwrap(),"rules_sha256":RULE_SHA,"finality_sha256":FINALITY_SHA,"configuration_sha256":hash(include_bytes!("../configs/strategies/complementary_pair/synthetic-engineering-v3.toml")),"markets":866,"history_points":301,"history_span_ms":300000,"warmup_per_cell":1,"samples_per_cell":32,"sample_count":96,"timed_path":"separate complete assembly and pure decide; eight independent same-state public diagnostics, not additive decomposition","cells":cells});
    std::fs::write(
        std::env::var("ISSUE185_TIMING_OUTPUT").unwrap(),
        serde_json::to_vec_pretty(&output).unwrap(),
    )
    .unwrap();
}

/// 两个真实NoTrade前缀；准备/公共分派/恢复均在测量之外，测量输出不发送。
#[tokio::test(flavor = "current_thread")]
#[ignore = "#185 NoTrade fixed-state timing; explicit sealed inputs/source/output required"]
async fn representative_notrade_host_calculation_timing() {
    let root = std::path::PathBuf::from(std::env::var("ISSUE185_NOTRADE_ROOT").unwrap());
    std::fs::create_dir(&root).unwrap();
    let finality_bytes = std::fs::read(std::env::var("ISSUE185_FINALITY").unwrap()).unwrap();
    assert_eq!(hash(&finality_bytes), FINALITY_SHA);
    let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&finality_bytes).unwrap());
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: std::env::var("ISSUE185_RULES").unwrap().into(),
        expected_file_sha256: Sha256Hex(RULE_SHA.into()),
        replay_causal_start_utc_ms: BASE,
    })
    .unwrap();
    let config = timing_engineering_config();
    let mut cells = Vec::new();
    for state in ["maintain", "missing_price_wait"] {
        let state_root = root.join(state);
        std::fs::create_dir(&state_root).unwrap();
        println!("prepare_notrade: {state}");
        let missing = state == "missing_price_wait";
        let material = material_for_notrade(&state_root.join("dpt"), missing, !missing);
        let (mut execution, observation) = execution(
            &state_root.join("sim.jsonl"),
            &material.core,
            rules.clone(),
            finality.clone(),
            &format!("notrade-{state}"),
        );
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        execution
            .bind_ledger_recording_v2(&mut ledger, &state_root.join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV2::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let mut source = RecordedPublicGroupSourceV1::open(
            &state_root.join("dpt"),
            128,
            16 << 20,
            material.times,
        )
        .unwrap();
        source.enable_source_progress();
        let mut driver =
            FinitePublicConsumptionV1::new(execution, source, format!("notrade-{state}"));
        driver.enable_finite_recording_wait().unwrap();
        advance(&mut driver, material.early);
        let mut setup_orders = vec![buy(&rules, 0, "closed-single-leg", false)];
        if !missing {
            setup_orders.push(buy(&rules, 1, "closed-resting", true));
        }
        shared
            .execute_batch_reserved(&mut driver.decision().unwrap(), &setup_orders)
            .await
            .unwrap();
        let mut strategy = ComplementaryPairV1::new(
            config.clone(),
            rules.clone(),
            format!("notrade-{state}"),
            StrategyContinuityV2::new_account(),
        )
        .unwrap();
        for &group in &material.history {
            advance(&mut driver, group);
            let input = shared
                .strategy_observation_input_with_trade_details(&mut driver, false)
                .unwrap()
                .unwrap();
            let at = input
                .action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms();
            input.calculate(&mut strategy, at).unwrap();
        }
        if !missing {
            let input = shared
                .strategy_observation_input_with_trade_details(&mut driver, false)
                .unwrap()
                .unwrap();
            let at = input
                .action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms();
            let calculation = input.calculate(&mut strategy, at).unwrap();
            let (diagnostic, intents) = diagnostics(&calculation.output);
            std::fs::write(
                state_root.join("before_dispatch.json"),
                serde_json::to_vec_pretty(
                    &json!({"decision_time":at,"diagnostic":diagnostic,"intents":intents}),
                )
                .unwrap(),
            )
            .unwrap();
            assert!(
                intents
                    .iter()
                    .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::CancelOrders { .. }))
            );
            assert!(
                intents
                    .iter()
                    .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::SubmitPairedBuy { .. }))
            );
            let mut decision = driver
                .send_decision_at(calculation.input_cutoff.clone(), at + 15)
                .unwrap();
            let dispatched = shared
                .dispatch_strategy_qualified(
                    &mut decision,
                    &mut strategy,
                    calculation.complete(at + 15),
                )
                .await
                .unwrap();
            assert!(matches!(
                dispatched,
                QualifiedStrategyDispatchV1::Dispatched { .. }
            ));
            drop(decision);
            advance(&mut driver, material.follow_up.unwrap());
        }
        let before = shared
            .strategy_observation_input_with_trade_details(&mut driver, false)
            .unwrap()
            .unwrap();
        let at = before
            .action_before
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        let initial = before.calculate(&mut strategy, at).unwrap();
        let (diagnostic, intents) = diagnostics(&initial.output);
        let current = diagnostic
            .markets
            .iter()
            .find(|m| m.condition_id == ConditionId(condition(CURRENT)))
            .unwrap();
        let requests = shared.registry().cumulative_request_totals();
        let cancellations = before.action_before.cancellation_requests(None);
        let orders: Vec<_> = before.action_before.orders().map(|o| json!({"condition":o.condition_id,"token":o.token_id,"accepted":o.accepted.is_some(),"quantity":o.active_unfilled_shares,"cancellable":o.cancellable})).collect();
        let signals = serde_json::to_value(strategy.checkpoint()).unwrap();
        let prefix = json!({"state":state,"decision_time":at,"current_diagnostic":current,"global_intent_count":intents.len(),"orders":orders,"request_totals":requests,"pending_noe":before.action_before.pending_noe_record_count(),"cancellation_pending":cancellations.pending_count,"cancellation_response_counts":cancellations.response_counts,"account_economics":before.action_before.economics(),"current_signal_samples":signals["signals"][condition(CURRENT)]["chainlink"].as_array().map(Vec::len),"observations":before.markets.len()});
        std::fs::write(
            state_root.join("prefix.json"),
            serde_json::to_vec_pretty(&prefix).unwrap(),
        )
        .unwrap();
        println!(
            "prepared_notrade: {state}, at={at}, choice={:?}, intents={}, requests={requests:?}",
            current.choice,
            intents.len()
        );
        assert!(intents.is_empty(), "真实全策略NoTrade前提: {current:?}");
        assert_eq!(before.action_before.pending_noe_record_count(), 0);
        assert_eq!(requests.registered_requests_without_receipt, 0);
        assert_eq!(cancellations.pending_count, ExecutionReadValueV1::Known(0));
        if missing {
            assert!(matches!(
                cancellations.latest_response,
                ExecutionReadValueV1::Known(None)
            ));
            assert_eq!(current.choice, MarketChoiceV1::Wait);
            assert!(matches!(
                current.probability,
                ProbabilityEstimateV1::Unavailable(_)
            ));
            let o = &before
                .markets
                .iter()
                .find(|m| m.observation.market.condition_id == ConditionId(condition(CURRENT)))
                .unwrap()
                .observation;
            assert!(o.chainlink_spot.value.is_none());
            assert!(before.action_before.orders().next().is_none());
        } else {
            assert_eq!(
                cancellations.response_counts[2],
                ExecutionReadValueV1::Known(0)
            );
            assert_eq!(current.choice, MarketChoiceV1::Maintain);
            assert!(current.source_health.is_empty());
            assert!(matches!(
                current.probability,
                ProbabilityEstimateV1::Known { .. }
            ));
            let current_orders: Vec<_> = before
                .action_before
                .orders()
                .filter(|o| o.condition_id == Some(&ConditionId(condition(CURRENT))))
                .collect();
            assert_eq!(current_orders.len(), 2);
            assert!(current_orders.iter().all(|o| o.accepted.is_some()
                && o.active_unfilled_shares
                    == ExecutionReadValueV1::Known(ExactDecimal("50".into()))));
            assert!(
                before
                    .action_before
                    .orders()
                    .all(|o| o.condition_id != Some(&ConditionId(condition(1))))
            );
            assert_eq!(requests.completed_receipted_cancellation_requests, 1);
            assert_eq!(requests.receipted_cancellation_response_counts, [1, 0, 0]);
        }
        let initial_economics = serde_json::to_value(before.action_before.economics()).unwrap();
        let initial_cutoff = before.action_before.cutoff().clone();
        let mut samples = Vec::new();
        for n in 0..=32 {
            let mut calculate_strategy =
                ComplementaryPairV1::restore(config.clone(), rules.clone(), strategy.checkpoint())
                    .unwrap();
            let mut decide_strategy =
                ComplementaryPairV1::restore(config.clone(), rules.clone(), strategy.checkpoint())
                    .unwrap();
            let combined_start = Instant::now();
            let start = Instant::now();
            let input = shared
                .strategy_observation_input_with_trade_details(&mut driver, false)
                .unwrap()
                .unwrap();
            let assembly_ns = start.elapsed().as_nanos() as u64;
            let start = Instant::now();
            let calculation = input.calculate(&mut calculate_strategy, at).unwrap();
            let calculate_ns = start.elapsed().as_nanos() as u64;
            let assembly_calculate_ns = combined_start.elapsed().as_nanos() as u64;
            let start = Instant::now();
            let decide = decide_strategy.decide(&input, at).unwrap();
            let decide_ns = start.elapsed().as_nanos() as u64;
            std::hint::black_box(&calculation);
            std::hint::black_box(&decide);
            assert_eq!(
                serde_json::to_value(&calculation.output).unwrap(),
                serde_json::to_value(&decide).unwrap()
            );
            assert_eq!(
                serde_json::to_value(&decide).unwrap(),
                serde_json::to_value(&initial.output).unwrap()
            );
            assert!(diagnostics(&calculation.output).1.is_empty());
            assert_eq!(input.action_before.cutoff(), &initial_cutoff);
            assert_eq!(
                serde_json::to_value(input.action_before.economics()).unwrap(),
                initial_economics
            );
            assert_eq!(
                serde_json::to_value(calculate_strategy.checkpoint()).unwrap(),
                serde_json::to_value(decide_strategy.checkpoint()).unwrap()
            );
            if n > 0 {
                samples.push(json!({"assembly_ns":assembly_ns,"calculate_ns":calculate_ns,"decide_ns":decide_ns,"assembly_calculate_ns":assembly_calculate_ns,"classification":"NoTrade"}));
            }
        }
        let after = shared
            .strategy_observation_input_with_trade_details(&mut driver, false)
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(after.action_before.economics()).unwrap(),
            initial_economics
        );
        assert_eq!(
            shared
                .registry()
                .cumulative_request_totals()
                .receipted_submission_requests,
            requests.receipted_submission_requests
        );
        cells.push(json!({"state":state,"prefix":prefix,"samples":samples,"setup_history_points":material.history.len(),"setup_extra_fresh_prefix":material.follow_up.is_some(),"warmup":1,"sample_count":32,"classification_counts":{"NoTrade":32,"Actions":0}}));
        driver.stop_and_drain().unwrap();
    }
    std::fs::write(std::env::var("ISSUE185_TIMING_OUTPUT").unwrap(), serde_json::to_vec_pretty(&json!({"format":"issue185-notrade-timing-v1","strategy_rule_version":RULE_VERSION,"configuration_source":"synthetic-engineering-v3.toml","base_head":std::env::var("ISSUE185_SOURCE_HEAD").unwrap(),"helper_sha256":std::env::var("ISSUE185_SOURCE_SHA256").unwrap(),"rule_sha256":RULE_SHA,"finality_sha256":FINALITY_SHA,"cells":cells,"sample_count":64,"limits":"fixed real NoTrade prefixes; setup/restore/serialization/comparison outside timing; combined assembly+calculate excludes feed/qualification/send and is not online end-to-end latency"})).unwrap()).unwrap();
}

/// 正常零目标撤单形成历史；保持866市场和当前两笔策略挂单，比较同版本两种表示。
#[tokio::test(flavor = "current_thread")]
#[ignore = "#185 D0342 finite cancellation-history probe; sealed inputs and output required"]
async fn completed_cancel_history_owned_and_borrowed_strategy_inputs() {
    let finality_bytes = std::fs::read(std::env::var("ISSUE185_FINALITY").unwrap()).unwrap();
    assert_eq!(hash(&finality_bytes), FINALITY_SHA);
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: std::env::var("ISSUE185_RULES").unwrap().into(),
        expected_file_sha256: Sha256Hex(RULE_SHA.into()),
        replay_causal_start_utc_ms: BASE,
    })
    .unwrap();
    let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&finality_bytes).unwrap());
    let config = timing_engineering_config();
    let temp = tempfile::tempdir().unwrap();
    let mut cells = Vec::new();
    for history in [1, 128, 512] {
        println!("prepare_cancel_history: {history}");
        let root = temp.path().join(format!("history-{history}"));
        std::fs::create_dir(&root).unwrap();
        let material = material_for_notrade(&root.join("dpt"), false, true);
        let (mut execution, observation) = execution(
            &root.join("sim.jsonl"),
            &material.core,
            rules.clone(),
            finality.clone(),
            &format!("cancel-history-{history}"),
        );
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        execution
            .bind_ledger_recording_v2(&mut ledger, &root.join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV2::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let mut source =
            RecordedPublicGroupSourceV1::open(&root.join("dpt"), 128, 16 << 20, material.times)
                .unwrap();
        source.enable_source_progress();
        let mut driver =
            FinitePublicConsumptionV1::new(execution, source, format!("cancel-history-{history}"));
        driver.enable_finite_recording_wait().unwrap();
        advance(&mut driver, material.early);
        // 注册过的旧市场，没有订单；真实平台完成事实仍由原SIM/NOE产生。
        for n in 0..history {
            driver.wait_for_recording_capacity().unwrap();
            shared
                .execute(
                    &mut driver.decision().unwrap(),
                    RequestContextV2 {
                        global_request_id: format!("history-cancel-{n}"),
                        caller_correlation_id: None,
                    },
                    Some(Arc::new(ModelIntentV1::CancelMarketOrders {
                        condition_id: ConditionId(condition(2)),
                    })),
                )
                .await
                .unwrap()
                .unwrap();
        }
        shared
            .execute_batch_reserved(
                &mut driver.decision().unwrap(),
                &[
                    buy(&rules, 0, "closed-single-leg", false),
                    buy(&rules, 1, "closed-resting", true),
                ],
            )
            .await
            .unwrap();
        let mut strategy = ComplementaryPairV1::new(
            config.clone(),
            rules.clone(),
            "cancel-history-strategy".into(),
            StrategyContinuityV2::new_account(),
        )
        .unwrap();
        for &group in &material.history {
            advance(&mut driver, group);
            let input = shared
                .strategy_borrowed_observation_input(&mut driver, false)
                .unwrap()
                .unwrap();
            let at = input
                .action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms();
            input.calculate_borrowed(&mut strategy, at).unwrap();
        }
        let totals = shared.registry().cumulative_request_totals();
        assert_eq!(totals.completed_receipted_cancellation_requests, history);
        let owned = shared
            .strategy_observation_input_with_trade_details(&mut driver, false)
            .unwrap()
            .unwrap();
        let at = owned
            .action_before
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        let saved = strategy.checkpoint();
        let mut owned_strategy =
            ComplementaryPairV1::restore(config.clone(), rules.clone(), saved.clone()).unwrap();
        let owned_calc = owned.calculate(&mut owned_strategy, at).unwrap();
        let mut borrowed_strategy =
            ComplementaryPairV1::restore(config.clone(), rules.clone(), saved).unwrap();
        let borrowed = shared
            .strategy_borrowed_observation_input(&mut driver, false)
            .unwrap()
            .unwrap();
        let borrowed_calc = borrowed
            .calculate_borrowed(&mut borrowed_strategy, at)
            .unwrap();
        assert_eq!(
            serde_json::to_value(&owned_calc).unwrap(),
            serde_json::to_value(&borrowed_calc).unwrap()
        );
        assert_eq!(
            serde_json::to_value(owned_strategy.checkpoint()).unwrap(),
            serde_json::to_value(borrowed_strategy.checkpoint()).unwrap()
        );
        assert_eq!(
            serde_json::to_value(owned.action_before.economics()).unwrap(),
            serde_json::to_value(borrowed.action_before.economics()).unwrap()
        );
        assert_eq!(
            owned.action_before.pending_cancellation_count(None),
            borrowed.action_before.pending_cancellation_count(None)
        );
        let (_, intents) = diagnostics(&owned_calc.output);
        assert!(!intents.is_empty());
        assert!(
            intents
                .iter()
                .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::SubmitPairedBuy { .. }))
        );
        drop(borrowed);
        strategy = borrowed_strategy;
        let mut decision = driver
            .send_decision_at(borrowed_calc.input_cutoff.clone(), at + 15)
            .unwrap();
        let sent = shared
            .dispatch_strategy_qualified(
                &mut decision,
                &mut strategy,
                borrowed_calc.complete(at + 15),
            )
            .await
            .unwrap();
        assert!(matches!(
            sent,
            QualifiedStrategyDispatchV1::Dispatched { .. }
        ));
        drop(decision);
        advance(&mut driver, material.follow_up.unwrap());
        let before = shared
            .strategy_observation_input_with_trade_details(&mut driver, false)
            .unwrap()
            .unwrap();
        let at = before
            .action_before
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        let initial_state = strategy.checkpoint();
        let initial_economics = serde_json::to_value(before.action_before.economics()).unwrap();
        let current_orders: Vec<_> = before
            .action_before
            .orders()
            .filter(|o| o.condition_id == Some(&ConditionId(condition(CURRENT))))
            .collect();
        assert_eq!(current_orders.len(), 2);
        assert!(current_orders.iter().all(|o| o.accepted.is_some()
            && o.active_unfilled_shares == ExecutionReadValueV1::Known(ExactDecimal("50".into()))));
        let initial = before.calculate(&mut strategy, at).unwrap();
        assert!(diagnostics(&initial.output).1.is_empty());
        let cutoff = before.action_before.cutoff().clone();
        let totals = shared.registry().cumulative_request_totals();
        let mut samples = Vec::new();
        for n in 0..=32 {
            let mut a =
                ComplementaryPairV1::restore(config.clone(), rules.clone(), initial_state.clone())
                    .unwrap();
            let mut b =
                ComplementaryPairV1::restore(config.clone(), rules.clone(), initial_state.clone())
                    .unwrap();
            let combined = Instant::now();
            let begin = Instant::now();
            let owned = shared
                .strategy_observation_input_with_trade_details(&mut driver, false)
                .unwrap()
                .unwrap();
            let owned_assembly_ns = begin.elapsed().as_nanos() as u64;
            let own = owned.calculate(&mut a, at).unwrap();
            let owned_combined_ns = combined.elapsed().as_nanos() as u64;
            let combined = Instant::now();
            let begin = Instant::now();
            let borrowed = shared
                .strategy_borrowed_observation_input(&mut driver, false)
                .unwrap()
                .unwrap();
            let borrowed_assembly_ns = begin.elapsed().as_nanos() as u64;
            let borrow = borrowed.calculate_borrowed(&mut b, at).unwrap();
            let borrowed_combined_ns = combined.elapsed().as_nanos() as u64;
            std::hint::black_box(&own);
            std::hint::black_box(&borrow);
            assert_eq!(
                serde_json::to_value(&own).unwrap(),
                serde_json::to_value(&borrow).unwrap()
            );
            assert_eq!(
                serde_json::to_value(a.checkpoint()).unwrap(),
                serde_json::to_value(b.checkpoint()).unwrap()
            );
            assert_eq!(borrowed.action_before.cutoff(), &cutoff);
            assert_eq!(
                serde_json::to_value(borrowed.action_before.economics()).unwrap(),
                initial_economics
            );
            assert_eq!(
                serde_json::to_value(owned.action_before.economics()).unwrap(),
                initial_economics
            );
            assert!(diagnostics(&own.output).1.is_empty());
            if n > 0 {
                samples.push(json!({"owned_assembly_ns":owned_assembly_ns,"owned_assembly_calculate_ns":owned_combined_ns,"borrowed_assembly_ns":borrowed_assembly_ns,"borrowed_assembly_calculate_ns":borrowed_combined_ns,"classification":"NoTrade"}));
            }
        }
        assert_eq!(
            shared
                .registry()
                .cumulative_request_totals()
                .completed_receipted_cancellation_requests,
            totals.completed_receipted_cancellation_requests
        );
        cells.push(json!({"zero_target_completed_cancels":history,"completed_cancellation_requests":totals.completed_receipted_cancellation_requests,"active_current_orders":2,"market_count":before.markets.len(),"source_points":material.history.len(),"extra_fresh_prefix":true,"warmup":1,"samples":samples,"actions_equivalence_checked":true,"NoTrade_samples":32}));
    }
    std::fs::write(std::env::var("ISSUE185_TIMING_OUTPUT").unwrap(),serde_json::to_vec_pretty(&json!({"format":"issue185-cancel-history-borrowed-v1","strategy_rule_version":RULE_VERSION,"configuration_source":"synthetic-engineering-v3.toml","base_head":std::env::var("ISSUE185_SOURCE_HEAD").unwrap(),"helper_sha256":std::env::var("ISSUE185_SOURCE_SHA256").unwrap(),"cells":cells,"limits":"同候选版本owning/borrowed路径对照；owning也复用投影，不是原main实测；准备/恢复/比较在计时外，不分派计时输出，无性能门槛"})).unwrap()).unwrap();
}

/// 固定当前市场与两处真实旧义务，仅增加已结束、无义务历史。
#[tokio::test(flavor = "current_thread")]
#[ignore = "D0344 finite history-growth probe; explicit sealed inputs/source/output required"]
async fn relevant_market_history_growth_timing() {
    let root = std::path::PathBuf::from(std::env::var("ISSUE185_NOTRADE_ROOT").unwrap());
    std::fs::create_dir_all(&root).unwrap();
    let finality_bytes = std::fs::read(std::env::var("ISSUE185_FINALITY").unwrap()).unwrap();
    assert_eq!(hash(&finality_bytes), FINALITY_SHA);
    let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&finality_bytes).unwrap());
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: std::env::var("ISSUE185_RULES").unwrap().into(),
        expected_file_sha256: Sha256Hex(RULE_SHA.into()),
        replay_causal_start_utc_ms: BASE,
    })
    .unwrap();
    let config = timing_engineering_config();
    let mut cells = Vec::new();
    for completed in [0, 48, 863] {
        let cell_root = root.join(completed.to_string());
        std::fs::create_dir(&cell_root).unwrap();
        let material = material_for_history(&cell_root.join("dpt"), false, false, completed);
        let (mut execution, observation) = execution(
            &cell_root.join("sim.jsonl"),
            &material.core,
            rules.clone(),
            finality.clone(),
            &format!("relevant-{completed}"),
        );
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        execution
            .bind_ledger_recording_v2(&mut ledger, &cell_root.join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV2::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let mut source = RecordedPublicGroupSourceV1::open(
            &cell_root.join("dpt"),
            128,
            16 << 20,
            material.times.clone(),
        )
        .unwrap();
        source.enable_source_progress();
        let mut driver =
            FinitePublicConsumptionV1::new(execution, source, format!("relevant-{completed}"));
        driver.enable_finite_recording_wait().unwrap();
        // SIM 规则已提前登记，公开组尚未消费时不能出现策略输入。
        assert!(
            driver
                .selected_frames_at_observation(std::iter::empty())
                .is_empty()
        );
        advance(&mut driver, material.early);
        let prestart = shared
            .strategy_selected_observation_input(&mut driver, false, &[])
            .unwrap()
            .unwrap();
        let future = prestart
            .markets
            .iter()
            .find(|f| f.observation.market.condition_id.0 == condition(CURRENT))
            .unwrap();
        assert!(
            prestart
                .action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms()
                < future.observation.market.window.start_at_utc_ms
        );
        drop(prestart);
        shared
            .execute_batch_reserved(
                &mut driver.decision().unwrap(),
                &[
                    buy(&rules, 0, "old-orphan", false),
                    buy(&rules, 1, "old-order", true),
                ],
            )
            .await
            .unwrap();
        let mut strategy = ComplementaryPairV1::new(
            config.clone(),
            rules.clone(),
            "history-growth".into(),
            StrategyContinuityV2::new_account(),
        )
        .unwrap();
        for &group in &material.history {
            advance(&mut driver, group);
            let input = shared
                .strategy_selected_observation_input(
                    &mut driver,
                    false,
                    &strategy.historical_market_requirements(),
                )
                .unwrap()
                .unwrap();
            let at = input
                .action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms();
            input.calculate_borrowed(&mut strategy, at).unwrap();
        }
        let owned = shared
            .strategy_observation_input_with_trade_details(&mut driver, false)
            .unwrap()
            .unwrap();
        let at = owned
            .action_before
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        let state_before = strategy.checkpoint();
        let mut owned_strategy =
            ComplementaryPairV1::restore(config.clone(), rules.clone(), state_before.clone())
                .unwrap();
        let owned_calculation = owned.calculate(&mut owned_strategy, at).unwrap();
        let historical = strategy.historical_market_requirements();
        let selected = shared
            .strategy_selected_observation_input(&mut driver, false, &historical)
            .unwrap()
            .unwrap();
        assert_eq!(owned.markets.len(), completed + 3);
        assert_eq!(selected.markets.len(), 3, "当前市场、旧孤腿、旧订单均保留");
        assert_eq!(
            selected.to_owned_input().markets.len(),
            owned.markets.len(),
            "显式转换仍完整冻结"
        );
        let actual = selected.calculate_borrowed(&mut strategy, at).unwrap();
        assert_eq!(
            serde_json::to_value(&actual).unwrap(),
            serde_json::to_value(&owned_calculation).unwrap()
        );
        assert_eq!(
            selected.action_before.profit_and_equity(),
            owned.action_before.profit_and_equity()
        );
        let selected_count = selected.markets.len();
        drop(selected);
        let expected_equity = owned.action_before.profit_and_equity().1;
        let mut probe = NecessaryReadProbe {
            expected: expected_equity,
        };
        let mut unordered = driver.frames_at_observation();
        unordered.reverse();
        assert!(
            shared
                .revalidate_calculation(&mut probe, &actual, &unordered, at)
                .unwrap()
                .0
                .is_none()
        );
        // 原账本保持不变；已构造的真实观察仅在测试中改变报价可用性。
        let mut missing: Vec<_> = unordered.iter().map(|f| f.observation.clone()).collect();
        for o in &mut missing {
            if o.market.condition_id.0 == condition(0) {
                for a in &mut o.assets {
                    std::sync::Arc::make_mut(&mut a.valuation_best_bid).quality =
                        MarketDataQuality::Stale;
                }
            }
        }
        let unavailable: Vec<_> = unordered
            .iter()
            .zip(&missing)
            .map(|(f, o)| PublicObservationFrameV1 {
                statistics: None,
                observation: o,
                rule_context: f.rule_context.clone(),
                receipt: f.receipt.clone(),
                decision_cutoff: f.decision_cutoff.clone(),
            })
            .collect();
        probe.expected = ExecutionReadValueV1::Unavailable;
        assert!(
            shared
                .revalidate_calculation(&mut probe, &actual, &unavailable, at)
                .unwrap()
                .0
                .is_none()
        );
        assert!(
            matches!(
                owned.action_before.profit_and_equity().1,
                ExecutionReadValueV1::Known(_)
            ),
            "旧拥有式值保持冻结"
        );
        drop(unavailable);
        drop(unordered);
        let mut samples = Vec::new();
        for representation in ["owned_full", "borrowed_relevant"] {
            for _ in 0..64 {
                // 每次都从相同策略记忆出发；恢复和后续业务比较不进入计时。
                let mut sample_strategy = ComplementaryPairV1::restore(
                    config.clone(),
                    rules.clone(),
                    state_before.clone(),
                )
                .unwrap();
                let assemble = Instant::now();
                let (assembly_ns, calculation_ns) = if representation == "borrowed_relevant" {
                    let input = shared
                        .strategy_selected_observation_input(&mut driver, false, &historical)
                        .unwrap()
                        .unwrap();
                    let assembly_ns = assemble.elapsed().as_nanos();
                    let calculate = Instant::now();
                    let result = input.calculate_borrowed(&mut sample_strategy, at).unwrap();
                    let calculation_ns = calculate.elapsed().as_nanos();
                    assert_eq!(
                        serde_json::to_value(result).unwrap(),
                        serde_json::to_value(&actual).unwrap()
                    );
                    (assembly_ns, calculation_ns)
                } else {
                    let input = shared
                        .strategy_observation_input_with_trade_details(&mut driver, false)
                        .unwrap()
                        .unwrap();
                    let assembly_ns = assemble.elapsed().as_nanos();
                    let calculate = Instant::now();
                    let result = input.calculate(&mut sample_strategy, at).unwrap();
                    let calculation_ns = calculate.elapsed().as_nanos();
                    assert_eq!(
                        serde_json::to_value(result).unwrap(),
                        serde_json::to_value(&actual).unwrap()
                    );
                    (assembly_ns, calculation_ns)
                };
                samples.push(json!({"representation":representation,"assembly_ns":assembly_ns,"calculation_ns":calculation_ns}));
            }
        }
        cells.push(json!({"completed_without_obligation":completed,"all_markets":owned.markets.len(),"selected_markets":selected_count,"samples":samples}));
        driver.stop_and_drain().unwrap();
    }
    std::fs::write(std::env::var("ISSUE185_TIMING_OUTPUT").unwrap(), serde_json::to_vec_pretty(&json!({"format":"issue185-relevant-history-timing-v1","strategy_rule_version":RULE_VERSION,"configuration_source":"synthetic-engineering-v3.toml","base_head":std::env::var("ISSUE185_SOURCE_HEAD").unwrap(),"source_identity":std::env::var("ISSUE185_SOURCE_SHA256").unwrap(),"rule_sha256":RULE_SHA,"finality_sha256":FINALITY_SHA,"cells":cells,"limits":"same fixed consumed prefixes; setup, serialization, assertions, qualification and feed outside assembly/calculation timers; full owned control uses current strategy diagnostic contract; no four-hour or online-capacity claim"})).unwrap()).unwrap();
}

struct NecessaryReadProbe {
    expected: ExecutionReadValueV1<contracts::economics::EconomicRational>,
}
impl StrategyV1 for NecessaryReadProbe {
    type Error = std::convert::Infallible;
    type Diagnostics = ComplementaryPairDiagnosticV1;
    fn uses_borrowed_input(&self) -> bool {
        true
    }
    fn requires_trade_economics_details(&self) -> bool {
        false
    }
    fn decide(
        &mut self,
        _: &StrategyInputV1,
        _: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, Self::Error> {
        unreachable!("只检查必要资格读取")
    }
    fn qualify_intent_borrowed(
        &mut self,
        input: &BorrowedStrategyCurrentReadV1<'_>,
        _: &StrategyCalculationV1<Self::Diagnostics>,
        _: usize,
        _: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        assert_eq!(input.action_before.profit_and_equity().1, self.expected);
        Ok(IntentQualificationV1::Qualified)
    }
}

// #384 M2独立成本入口：原例/原0-48-863矩阵不变；唯一执行内各构造C/D一份。
// 不调用历史prepare，不发送32次计算所得建议，所有准备/收尾计入外层。
fn pair_completion_cost_metadata(
    i: usize,
    at: i64,
    closed: bool,
    active: bool,
    sequence: u64,
) -> TypedRawRowV2 {
    let TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(mut m)) =
        metadata(i, at, closed, active)
    else {
        unreachable!()
    };
    m.collector_admission_sequence = sequence;
    // 原V5 source_evidence_payload_json是单市场对象；正式tick提取器按此解码。
    let payload = json!({
        "id":m.clob_market_id,"conditionId":m.condition_id,"slug":m.slug,
        "clobTokenIds":m.asset_ids,"market_start_utc":m.market_start_utc,
        "market_end_utc":m.market_end_utc,"closed":closed,
        "acceptingOrders":!closed,"orderPriceMinTickSize":"0.01"
    })
    .to_string();
    m.source_evidence_raw_sha256 = Some(hash(payload.as_bytes()));
    m.source_evidence_payload_json = Some(payload);
    TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(m))
}
fn pair_completion_cost_anchor(at: i64) -> TypedRawRowV2 {
    let TypedRawRowV2::PolymarketMarketMetadataV6(mut input) = anchor(at) else {
        unreachable!()
    };
    let PolymarketMarketMetadataV6Input::AnchorRule(a) = input.as_mut() else {
        unreachable!()
    };
    let body = json!({
        "slug":a.slug,"conditionId":a.condition_id,"clobTokenIds":a.asset_ids,
        "market_start_utc":a.market_start_utc,"market_end_utc":a.market_end_utc,
        "eventMetadata":{"priceToBeat":"65000"},"twapEnabled":false
    })
    .to_string();
    a.http_request_url = Some(format!(
        "https://gamma-api.polymarket.com/events/slug/{}",
        a.slug
    ));
    a.http_response_url = a.http_request_url.clone();
    a.http_status_code = Some(200);
    a.http_body_encoding = Some("utf8".into());
    a.http_body_sha256 = Some(hash(body.as_bytes()));
    a.http_body = Some(body);
    TypedRawRowV2::PolymarketMarketMetadataV6(input)
}
fn pair_completion_cost_material(root: &Path) -> (Material, Vec<u64>) {
    let mut c = core();
    c.enable_source_progress();
    let mut writer =
        PublicRecordingWriterV1::create_historical(root, c.header(), common::config()).unwrap();
    let mut position = 0_u64;
    let mut times = Vec::new();
    let mut push = |c: &mut PublicObservationCoreV1, row: TypedRawRowV2, at: i64| {
        let ingress = record(position, row);
        position += 1;
        let prepared = prepare_public_ingress_v1(c, &ingress, &BTreeMap::new(), &[]).unwrap();
        let applied = c.push(&prepared).unwrap().into_owned();
        let last = applied.applied.closed_groups.last().unwrap().group_position;
        times.extend(std::iter::repeat_n(
            at + 2,
            applied.applied.closed_groups.len(),
        ));
        writer.record(ingress, applied).unwrap();
        last
    };
    let mut metadata_sequence = 0;
    for i in [0, 1, CURRENT] {
        metadata_sequence += 1;
        push(
            &mut c,
            pair_completion_cost_metadata(i, BASE, false, i != CURRENT, metadata_sequence),
            BASE,
        );
    }
    push(&mut c, pair_completion_cost_anchor(BASE), BASE);
    let mut book_sequence = 0;
    let mut early = 0;
    for offset in [1000, 2000] {
        for i in 0..2 {
            for up in [true, false] {
                book_sequence += 1;
                let group = push(
                    &mut c,
                    book(i, up, BASE + offset, book_sequence),
                    BASE + offset,
                );
                if offset == 1000 {
                    early = group;
                }
            }
        }
    }
    for i in 0..2 {
        metadata_sequence += 1;
        push(
            &mut c,
            pair_completion_cost_metadata(i, BASE + 3000, true, true, metadata_sequence),
            BASE + 3000,
        );
    }
    let mut history = Vec::new();
    for n in 0..=300 {
        let at = BASE + 4000 + n * 1000;
        // 已结束且无责任的48个市场逐步出现；不塞入early首帧。
        if n < 48 {
            metadata_sequence += 1;
            push(
                &mut c,
                pair_completion_cost_metadata(n as usize + 2, at, true, true, metadata_sequence),
                at,
            );
        }
        if n == 296 {
            metadata_sequence += 1;
            push(
                &mut c,
                pair_completion_cost_metadata(CURRENT, at, false, true, metadata_sequence),
                at,
            );
        }
        for up in [true, false] {
            book_sequence += 1;
            push(&mut c, book(CURRENT, up, at, book_sequence), at);
        }
        let mut last = 0;
        for row in sources(at, n as u64 + 1) {
            last = push(&mut c, row, at);
        }
        history.push(last);
    }
    let mut late = Vec::new();
    for n in 0..32 {
        let at = BASE + 364000 + n * 1000;
        for up in [true, false] {
            book_sequence += 1;
            push(&mut c, book(CURRENT, up, at, book_sequence), at);
        }
        let mut last = 0;
        for row in sources_with_price(at, 302 + n as u64, "64999".into()) {
            last = push(&mut c, row, at);
        }
        late.push(last);
    }
    drop(push);
    assert_eq!(position, 1395);
    assert_eq!(metadata_sequence, 54);
    assert_eq!(book_sequence, 674);
    assert_eq!(c.fixed_markets().count(), 51);
    let ingress = fixture_recording::end(position);
    position += 1;
    let prepared = prepare_public_ingress_v1(&c, &ingress, &BTreeMap::new(), &[]).unwrap();
    let applied = c.push(&prepared).unwrap().into_owned();
    times.extend(std::iter::repeat_n(
        BASE + 395002,
        applied.applied.closed_groups.len(),
    ));
    writer.record(ingress, applied).unwrap();
    writer.stabilize().unwrap();
    assert_eq!(position, 1396);
    (
        Material {
            core: c,
            times: TradingTimes(times),
            early,
            history,
            follow_up: None,
        },
        late,
    )
}
fn pair_completion_cost_config(
    mode: ContinuousInventoryOperatingModeV1,
) -> ContinuousInventoryConfigV1 {
    let config = ContinuousInventoryConfigV1::parse_toml(include_str!(
        "../configs/strategies/continuous_inventory/adopted-v1.toml"
    ))
    .unwrap();
    let mut value = serde_json::to_value(config).unwrap();
    for (key, v) in value["health"].as_object_mut().unwrap() {
        *v = json!(if key.contains("normal") { 0 } else { 1_000_000 });
    }
    value["probability_reference"]["binance_innovation_weight"] = json!(0.0);
    let mut config: ContinuousInventoryConfigV1 = serde_json::from_value(value).unwrap();
    config.operating_mode = mode;
    config.extra_quote_space_ticks = 1;
    config.inventory_skew_ticks_per_capacity = Decimal::ZERO;
    config
}
fn pair_completion_cost_rss() -> serde_json::Value {
    // 每状态有限边界；没有持续采样。RSS/HWM只是当前测试进程口径。
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    json!(
        status
            .lines()
            .filter(|line| line.starts_with("VmRSS:") || line.starts_with("VmHWM:"))
            .collect::<Vec<_>>()
    )
}
fn pair_completion_cost_files(path: &Path) -> (u64, u64) {
    let mut files = 0;
    let mut bytes = 0;
    for item in std::fs::read_dir(path).unwrap() {
        let item = item.unwrap();
        let kind = item.file_type().unwrap();
        if kind.is_dir() {
            let (n, size) = pair_completion_cost_files(&item.path());
            files += n;
            bytes += size;
        } else {
            assert!(kind.is_file(), "仅本例自有普通产物");
            files += 1;
            bytes += item.metadata().unwrap().len();
        }
    }
    (files, bytes)
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "#384 M2一次有界release成本：C/D各51已见、32固定late；须明确来源/输出，禁止workspace重跑"]
async fn pair_completion_multimarket_selected_qualification_cost() {
    use std::io::Write as _;
    // 仅新M2复用#384已审M1输入；旧host常量和旧结果保持原身份。
    const M2_RULE_SHA: &str = "60e21a97c1d59fe1d33fdf777dbd2e448653539bc8c13114d3dd7785d833e519";
    const M2_FINALITY_SHA: &str =
        "65295a7a83cb2aa5ed612cd48d84675e9798b2da2f5037f4466e2bc5c2159b62";
    let whole = Instant::now();
    let root = std::path::PathBuf::from(std::env::var("ISSUE384_COST_ROOT").unwrap());
    std::fs::create_dir(&root).unwrap();
    let finality_path = std::env::var("ISSUE185_FINALITY").unwrap();
    let finality_bytes = std::fs::read(&finality_path).unwrap();
    assert_eq!(hash(&finality_bytes), M2_FINALITY_SHA);
    let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&finality_bytes).unwrap());
    assert_eq!(finality.policy_id(), &Sha256Hex(M2_FINALITY_SHA.into()));
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: std::env::var("ISSUE185_RULES").unwrap().into(),
        expected_file_sha256: Sha256Hex(M2_RULE_SHA.into()),
        replay_causal_start_utc_ms: BASE,
    })
    .unwrap();
    let mut cells = Vec::new();
    for (name, mode) in [
        ("C", ContinuousInventoryOperatingModeV1::PairCompletion),
        ("D", ContinuousInventoryOperatingModeV1::PairCompletionCost),
    ] {
        let state_start = Instant::now();
        let cell_root = root.join(name);
        std::fs::create_dir(&cell_root).unwrap();
        let rss_before = pair_completion_cost_rss();
        let preparation = Instant::now();
        let (material, late) = pair_completion_cost_material(&cell_root.join("dpt"));
        let preparation_ns = preparation.elapsed().as_nanos();
        let (mut execution, observation) = execution(
            &cell_root.join("sim.jsonl"),
            &material.core,
            rules.clone(),
            finality.clone(),
            &format!("issue384-m2-{name}"),
        );
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        execution
            .bind_ledger_recording_v2(&mut ledger, &cell_root.join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV2::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let mut source = RecordedPublicGroupSourceV1::open(
            &cell_root.join("dpt"),
            128,
            16 << 20,
            material.times.clone(),
        )
        .unwrap();
        source.enable_source_progress();
        let mut driver =
            FinitePublicConsumptionV1::new(execution, source, format!("issue384-m2-{name}"));
        driver.enable_finite_recording_wait().unwrap();
        advance(&mut driver, material.early);
        let early_start = Instant::now();
        let early = shared
            .strategy_selected_observation_input(&mut driver, false, &[])
            .unwrap()
            .unwrap();
        let early_ns = early_start.elapsed().as_nanos();
        let early_value = json!({"selected_markets":early.markets.iter().map(|m|m.observation.market.condition_id.clone()).collect::<Vec<_>>(),"cutoff":early.action_before.cutoff(),"cash":early.action_before.cash_available()});
        drop(early);
        let setup = shared
            .execute_batch_reserved(
                &mut driver.decision().unwrap(),
                &[
                    buy(&rules, 0, "m2-old-h", false),
                    buy(&rules, 1, "m2-old-r", true),
                ],
            )
            .await
            .unwrap();
        let mut setup_receipts = setup;
        for (n, &group) in material.history.iter().enumerate() {
            advance(&mut driver, group);
            shared.apply_closed().unwrap();
            if n == 299 {
                let receipts = shared
                    .execute_batch_reserved(
                        &mut driver.decision().unwrap(),
                        &[buy(&rules, CURRENT, "m2-current-h", false)],
                    )
                    .await
                    .unwrap();
                setup_receipts.extend(receipts);
            }
        }
        assert_eq!(setup_receipts.len(), 3);
        let config = pair_completion_cost_config(mode);
        let mut strategy = ContinuousInventoryV1::new(
            config.clone(),
            rules.clone(),
            format!("m2-{name}"),
            ContinuousInventoryContinuityV1::new_account_for_config(&config),
        )
        .unwrap();
        let mut output = std::io::BufWriter::new(
            std::fs::File::create(cell_root.join("calculations.jsonl")).unwrap(),
        );
        let mut samples = Vec::new();
        let mut qualification_items = 0_u64;
        let mut qualification_account_reads = 0_u64;
        let mut target_qualified = 0;
        let mut serialized_bytes = 0;
        for (n, &group) in late.iter().enumerate() {
            advance(&mut driver, group);
            let historical = strategy.historical_market_requirements();
            let selected_start = Instant::now();
            let input = shared
                .strategy_selected_observation_input(&mut driver, false, &historical)
                .unwrap()
                .unwrap();
            let selected_ns = selected_start.elapsed().as_nanos();
            let at = input
                .action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms();
            assert_eq!(at, BASE + 364002 + n as i64 * 1000);
            let ids = input
                .markets
                .iter()
                .map(|m| m.observation.market.condition_id.clone())
                .collect::<Vec<_>>();
            assert_eq!(ids.len(), 3);
            for i in [0, 1, CURRENT] {
                assert!(ids.contains(&ConditionId(condition(i))));
            }
            let old = input
                .action_before
                .market_economics(&ConditionId(condition(0)))
                .unwrap();
            let current = input
                .action_before
                .market_economics(&ConditionId(condition(CURRENT)))
                .unwrap();
            assert!(old.orphan_shares.iter().any(|q| *q > Decimal::ZERO));
            assert!(current.orphan_shares.iter().any(|q| *q > Decimal::ZERO));
            assert!(matches!(&old.orphan_fee, ExecutionReadValueV1::Known(_)));
            assert!(matches!(
                &current.orphan_fee,
                ExecutionReadValueV1::Known(_)
            ));
            let old_r_orders = input.action_before.orders().filter(|o|o.condition_id==Some(&ConditionId(condition(1))) && matches!(&o.active_unfilled_shares, ExecutionReadValueV1::Known(q) if Decimal::from_str_exact(&q.0).unwrap()>Decimal::ZERO)).map(|o|json!({"condition_id":o.condition_id,"token_id":o.token_id,"active_unfilled_shares":o.active_unfilled_shares})).collect::<Vec<_>>();
            let old_r = old_r_orders.len();
            assert_eq!(old_r, 1);
            let economic_value = json!({"old_h":old.orphan_shares,"old_principal":old.orphan_principal,"old_fee":old.orphan_fee,"current_h":current.orphan_shares,"current_principal":current.orphan_principal,"current_fee":current.orphan_fee,"old_r_order_count":old_r,"old_r_orders":old_r_orders});
            let calculate_start = Instant::now();
            let calculation = input.calculate_borrowed(&mut strategy, at).unwrap();
            let calculate_ns = calculate_start.elapsed().as_nanos();
            drop(input);
            let (diagnostic, intents) = match &calculation.output {
                StrategyOutputV1::Continue {
                    diagnostics,
                    intents,
                    ..
                } => (diagnostics, intents),
                _ => panic!("固定成本前缀不应退出"),
            };
            assert!(intents.len() <= 6);
            let current_d = diagnostic
                .markets
                .iter()
                .find(|m| m.condition_id == ConditionId(condition(CURRENT)))
                .unwrap();
            assert!(
                current_d
                    .h
                    .iter()
                    .any(|q| Decimal::from_str_exact(&q.0).unwrap() > Decimal::ZERO)
            );
            let target = intents.iter().position(|intent| matches!(intent.intent.as_ref(), ModelIntentV1::SubmitSingleBuy {condition_id,order} if *condition_id==ConditionId(condition(CURRENT)) && order.token_id==TokenId(token(CURRENT,false)) && matches!(&order.order, BuyOrderIntentV1::GtcLimit {size_shares,post_only:true,..} if *size_shares>Decimal::ZERO)));
            // 必须实际D正量；当前责任/原价原量和完整资格均在下方核，不用取消项替代。
            if name == "D" {
                assert!(target.is_some(), "32固定D目标须正量，不能换前缀补例");
            }
            let current_summary = json!({"h":current_d.h,"prices":current_d.prices,"quantities":current_d.quantities,"books":current_d.books,"unavailable":current_d.unavailable,"source_health":current_d.source_health});
            assert!(!current_d.initial);
            assert!(current_d.unavailable.is_empty());
            assert!(
                current_d
                    .source_health
                    .iter()
                    .all(|s| s.state.failure().is_none()),
                "按正式可用健康条件，允许Normal/Elevated"
            );
            if let Some(index) = target {
                let request = &intents[index].context.global_request_id;
                let dependency = diagnostic
                    .action_dependencies
                    .iter()
                    .find(|d| &d.request_id == request)
                    .unwrap();
                assert!(
                    dependency.independent_held_token.is_none(),
                    "目标须为普通补腿，不能以held修复替代"
                );
            }
            assert!(
                !diagnostic
                    .account_dependencies
                    .iter()
                    .find(|a| a.condition_id == ConditionId(condition(CURRENT)))
                    .unwrap()
                    .unknown
            );
            let intent_count = intents.len();
            let mut required = strategy.historical_market_requirements();
            required.extend(strategy.calculation_market_requirements(&calculation));
            let current_start = Instant::now();
            let frames = shared
                .strategy_selected_current_observations(&driver, &required)
                .unwrap();
            let current_read_ns = current_start.elapsed().as_nanos();
            let qualify_start = Instant::now();
            let (invalidated, calls) = shared
                .revalidate_calculation(&mut strategy, &calculation, &frames, at)
                .unwrap();
            let qualify_ns = qualify_start.elapsed().as_nanos();
            drop(frames);
            qualification_items += calls;
            // revalidate_calculation在首个待复核项惰性取得一次账户借用。
            qualification_account_reads += u64::from(calls != 0);
            // 先留原值，失败不回收唯一因果材料。
            let serialize_start = Instant::now();
            let bytes = serde_json::to_vec(&calculation).unwrap();
            let serialization_ns = serialize_start.elapsed().as_nanos();
            output.write_all(&bytes).unwrap();
            output.write_all(b"\n").unwrap();
            output.flush().unwrap();
            serialized_bytes += bytes.len() + 1;
            let sample = json!({"prefix_index":n,"group_position":group,"at":at,"seen_markets":51,"selected_markets":ids,"economics":economic_value,"current":current_summary,"target_intent_index":target,"intent_count":intent_count,"qualification_items":calls,"invalidated":invalidated.as_ref().map(|x|format!("{x:?}")),"selected_ns":selected_ns,"calculate_ns":calculate_ns,"current_read_ns":current_read_ns,"qualification_ns":qualify_ns,"serialization_ns":serialization_ns,"serialized_bytes":bytes.len()+1});
            println!("pair_completion_m2_sample {name} {sample}");
            assert!(invalidated.is_none(), "完整资格提前失效即未覆盖");
            assert_eq!(calls, intent_count as u64);
            if target.is_some() {
                target_qualified += 1;
            }
            let drop_start = Instant::now();
            drop((bytes, calculation));
            let drop_ns = drop_start.elapsed().as_nanos();
            let empty_start = Instant::now();
            std::hint::black_box(());
            let empty_timer_ns = empty_start.elapsed().as_nanos();
            samples.push(json!({"actual":sample,"output_drop_ns":drop_ns,"empty_timer_pair_ns":empty_timer_ns}));
        }
        assert_eq!(samples.len(), 32);
        if name == "D" {
            assert_eq!(target_qualified, 32);
        }
        assert!(qualification_items <= 192);
        output.flush().unwrap();
        drop(output);
        // 固定前缀后仅领取已有反馈，再真实stop/drain；不请求SIM完整公共EOF。
        // DPT材料仍包含已stabilize的End，不将它冒称已消费或经济终局。
        let final_apply = shared.apply_closed().unwrap();
        let cutoff = driver.observation_cutoff().unwrap();
        let final_read = shared.ledger().economic_read().unwrap();
        let final_economics =
            serde_json::to_value(final_read.market_totals().collect::<Vec<_>>()).unwrap();
        drop(final_read);
        let cleanup_start = Instant::now();
        let drained = driver.stop_and_drain();
        println!("pair_completion_m2_finish {name} {drained:?}");
        drained.unwrap();
        drop(strategy);
        drop(shared);
        drop(material);
        let cleanup_ns = cleanup_start.elapsed().as_nanos();
        let (files, bytes) = pair_completion_cost_files(&cell_root);
        cells.push(json!({"mode":name,"config":config,"recorded_typed_inputs":1396,"recorded_end_inputs":1,"seen_markets":51,"historical_without_responsibility":48,"early":early_value,"early_selected_ns":early_ns,"explicit_selected_reads":33,"current_frame_reads":32,"revalidation_calls":32,"qualification_account_read_calls":qualification_account_reads,"setup_request_count":3,"setup_receipts":setup_receipts,"calculations":32,"qualification_items":qualification_items,"target_qualified":target_qualified,"preparation_ns":preparation_ns,"samples":samples,"serialized_calculation_bytes":serialized_bytes,"final_apply_closed":final_apply,"last_consumed_cutoff":cutoff,"public_eof_requested":false,"final_economics":final_economics,"automatic_noe_record_count":"Unknown; delivered feedback sequence is separate","recording_drained":true,"economic_finality":"Unknown; old resting responsibility is intentionally retained","cleanup_ns":cleanup_ns,"state_through_cleanup_ns":state_start.elapsed().as_nanos(),"rss_before":rss_before,"rss_after_cleanup":pair_completion_cost_rss(),"output_files":files,"output_bytes":bytes}));
    }
    let value = json!({"format":"issue384-m2-selected-full-qualification-cost-v1","states":2,"recorded_typed_inputs":2792,"recorded_end_inputs":2,"explicit_calculations":64,"explicit_selected_reads":66,"setup_requests":6,"explicit_feedback_injections":0,"rules_sha256":rules.file_sha256(),"finality_sha256":finality.policy_id(),"rules_path":rules.path(),"finality_path":finality_path,"input_binding":"#384已审M1 sealed参数；不沿用旧host历史身份","source_head":std::env::var("ISSUE185_SOURCE_HEAD").unwrap(),"source_sha256":std::env::var("ISSUE185_SOURCE_SHA256").unwrap(),"through_states_ns":whole.elapsed().as_nanos(),"cells":cells,"limits":"组件输出而非完整Day；时钟/断言/文件与准备均在完整外层；内部阶段不相加，不证明真实多市场完整Day/65min上界；重复建议未发送"});
    std::fs::write(
        root.join("m2-result.json"),
        serde_json::to_vec_pretty(&value).unwrap(),
    )
    .unwrap();
    println!("pair_completion_m2_complete states=2 calculations=64 recorded_typed=2792");
}
