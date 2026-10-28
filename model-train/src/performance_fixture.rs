//! #377 F4：以真实公共类型构造的有限数值工程输入，不采样、不调用模型。
//!
//! 市场来自 typed raw -> 公共 Core -> 原记录/消费链。账户来自原开户、Ledger 和
//! Registry。Repair 的证据是实际发送回执所关联的单腿承诺，不反序列化私有账户。
//! 最后的工程选择器只选择生产 options/精确域内的值，不声称模型实际抽中过该动作。
use crate::{domain::*, encoding::EncodingConfig};
use backtest::MaterialError;
use contracts::{execution_normalization::*, *};
use data_pipeline_core::{
    public_input::*, public_observation_core::*, public_reference::*, typed_raw::*,
};
use data_pipeline_training::public_recording::PublicRecordingWriterV1;
use ledger::{AccountAdmissionPolicyV3, LedgerWriterV1};
use num_bigint::BigUint;
use num_traits::{One, Zero};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use shared_run::*;
use sim_system::{
    execution::*, finality::TradeFinalityPolicyV2,
    latency_profile::load_embedded_proxy_latency_profile, platform_rules::*, public_consumption::*,
    scenario::*,
};
use std::{collections::BTreeMap, path::Path, sync::Arc};

pub const F4_FIXTURE_VERSION: &str = "issue-377-f4-public-construction-v1";
const STREAM: &str = "issue-377-f4-synthetic-public-input";
const BASE: i64 = 1_788_912_000_000;
const MARKET_START: i64 = BASE + 600_000;
const MARKET_END: i64 = MARKET_START + 300_000;
const FIRST: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const SECOND: &str = "0x2222222222222222222222222222222222222222222222222222222222222222";
const MARKETS: [(&str, &str, [&str; 2]); 2] = [
    (FIRST, "3195576", ["20", "3"]),
    (SECOND, "3195577", ["21", "4"]),
];
const CASES: [(&str, &str, &str, &str); 4] = [
    ("F-N", "f4-notrade", "100", "NoTrade"),
    ("F-P", "f4-pair", "100", "Pair"),
    ("F-R", "f4-repair", "100", "Repair"),
    ("F-S", "f4-singleton", "0.0002", "Pair"),
];

/// 固定构造常量可在执行前封存；本函数不准备材料或启动任何进程。
pub fn fixture_constants() -> Value {
    json!({
        "version":F4_FIXTURE_VERSION,
        "scope":"synthetic_numerical_engineering_only",
        "base_utc_ms":BASE,"market_start_utc_ms":MARKET_START,
        "market_end_utc_ms":MARKET_END,"markets":MARKETS,
        "tick":"0.01","best_bid":"0.4","best_ask":"0.6",
        "first_market_bid_levels":1,"second_market_bid_levels":2,
        "cases":CASES,"repair_original_price":"0.2",
        "repair_original_size":"unchanged_rule_limit_min_quantity_shares",
        "selected_post_only":true,"multivalue_price_index":2,
        "multivalue_quantity_rank":"original_limit_min_quantity_in_centishares_minus_one",
        "singleton_price_index":1,
        "singleton_quantity_rank":"0",
        "singleton_legality":"original_model_exact_domain_only_not_platform_acceptance",
        "teacher_selection":"deterministic_valid_option_and_exact_domain_no_sampling"
    })
}

/// 一次创建有限 F4 材料。调用方负责批准的准备计时、准确版本、输出目录和 seed 绑定。
/// rules/finality 是已打开的同一封存公共材料；本入口不 GET、不改 MOS、费用或最终性。
pub async fn run_f4_fixture(
    root: &Path,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    finality: Arc<TradeFinalityPolicyV2>,
    implementation: ImplementationIdentityV1,
    encoding: &EncodingConfig,
    sampling_seed: u64,
) -> Result<Vec<Value>, MaterialError> {
    std::fs::create_dir(root)?;
    let public_root = root.join("public-input");
    let (core, source_sha, input_count) = record_public_input(&public_root)?;
    let scenario = scenario(&core)?;
    let mut result = Vec::with_capacity(CASES.len());
    for (case, account, cash, kind) in CASES {
        let case_root = root.join(case);
        std::fs::create_dir(&case_root)?;
        let (shared, execution) = execution(
            &case_root,
            case,
            account,
            cash,
            rules.clone(),
            finality.clone(),
            scenario.clone(),
            implementation.clone(),
            source_sha.clone(),
        )?;
        let source =
            RecordedPublicGroupSourceV1::open(&public_root, 32, 1 << 20, OriginalVisibleTime)?;
        let mut driver = FinitePublicConsumptionV1::new(
            execution,
            source,
            format!("{F4_FIXTURE_VERSION}-{case}"),
        );
        let mut shared = shared;
        // stop_and_drain 也在构造失败时执行；不把有限快照伪称完整经济终局。
        let constructed = build_case(
            &mut shared,
            &mut driver,
            case,
            account,
            kind,
            rules.as_ref(),
            encoding,
            sampling_seed,
            &source_sha,
            input_count,
        )
        .await;
        let drained = driver.stop_and_drain();
        let value = match (constructed, drained) {
            (Ok(value), Ok(())) => value,
            (Err(error), Ok(())) => return Err(error),
            (Ok(_), Err(error)) => return Err(error.into()),
            (Err(error), Err(drain)) => {
                return Err(format!("F4构造失败: {error}; 原后台排空失败: {drain}").into());
            }
        };
        result.push(value);
    }
    Ok(result)
}

struct OriginalVisibleTime;
impl PublicConsumptionTimeSourceV1 for OriginalVisibleTime {
    fn execution_time_for_group(
        &mut self,
        _group: &PublicGroupReferenceV1,
    ) -> Result<i64, SimRunActorErrorV2> {
        Err(SimRunActorErrorV2::PublicConsumptionSource(
            "F4要求原组的实际来源可见时间".into(),
        ))
    }
    fn execution_time_for_visible_group(
        &mut self,
        _group: &PublicGroupReferenceV1,
        visible_at_utc_ms: i64,
    ) -> Result<i64, SimRunActorErrorV2> {
        Ok(visible_at_utc_ms)
    }
}

fn record_public_input(
    root: &Path,
) -> Result<(PublicObservationCoreV1, Sha256Hex, u64), MaterialError> {
    let mut core = PublicObservationCoreV1::new(PublicInputStreamHeaderV1 {
        input_stream_id: STREAM.into(),
        canonical_rule_version: "rule-v1".into(),
        observation_version: "public-observation-v1".into(),
        ordering_version: "original-input-v1".into(),
        input_scope: "constructed-finite-engineering".into(),
        max_pending_ws_terminals: 2,
        initial_state: PublicInitialStateV1::Empty,
    })?;
    let mut config = data_pipeline_training::gate_a_formal_output_config();
    config.sort.batch_rows = 32;
    config.sort.max_run_rows = 32;
    config.sort.max_run_bytes = 1 << 20;
    config.max_artifact_payload_bytes = 1 << 19;
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config)?;
    let mut rows = Vec::new();
    for active in [false, true] {
        for (condition, clob, tokens) in &MARKETS {
            let at = MARKET_START + if active { 0 } else { -1_000 };
            rows.push(TypedRawRowV2::Existing(
                TypedRawRow::PolymarketMarketMetadata(Box::new(metadata(
                    condition,
                    clob,
                    *tokens,
                    at,
                    active,
                    rows.len() as u64 + 1,
                )?)),
            ));
        }
    }
    let mut sequence = 0;
    for (market_index, (condition, _, tokens)) in MARKETS.iter().enumerate() {
        for token in tokens {
            sequence += 1;
            rows.push(TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(
                book(condition, token, sequence, market_index == 1),
            )));
        }
    }
    let count = rows.len() as u64 + 1;
    let mut digest = Sha256::new();
    for (position, row) in rows.into_iter().enumerate() {
        let input = RecordedIngressV1 {
            input_stream_id: STREAM.into(),
            input_position: position as u64,
            ingress: DptIngressV1::Message {
                source_evidence: source_evidence_v3_from_typed_raw_v2(&row)?,
                row,
            },
            dependency_record_refs: vec![],
        };
        apply_record(&mut core, &mut writer, input, &mut digest)?;
    }
    apply_record(
        &mut core,
        &mut writer,
        RecordedIngressV1 {
            input_stream_id: STREAM.into(),
            input_position: count - 1,
            ingress: DptIngressV1::EndOfInput,
            dependency_record_refs: vec![],
        },
        &mut digest,
    )?;
    writer.stabilize()?;
    Ok((core, Sha256Hex(format!("{:x}", digest.finalize())), count))
}

fn apply_record(
    core: &mut PublicObservationCoreV1,
    writer: &mut PublicRecordingWriterV1,
    input: RecordedIngressV1,
    digest: &mut Sha256,
) -> Result<(), MaterialError> {
    let bytes = serde_json::to_vec(&input)?;
    digest.update((bytes.len() as u64).to_le_bytes());
    digest.update(bytes);
    let prepared = prepare_public_ingress_v1(core, &input, &BTreeMap::new(), &[])?;
    let applied = core.push(&prepared)?.into_owned();
    writer.record(input, applied)?;
    Ok(())
}

fn metadata(
    condition: &str,
    clob: &str,
    tokens: [&str; 2],
    at: i64,
    active: bool,
    sequence: u64,
) -> Result<PolymarketMarketMetadataRow, MaterialError> {
    let payload = r#"{"orderPriceMinTickSize":"0.01"}"#;
    Ok(serde_json::from_value(json!({
        "metadata_row_kind":PolymarketMetadataRowKind::MetadataSnapshot, "timestamp_received":at,
        "metadata_origin":PolymarketMetadataOrigin::GammaMarketsDiscovery, "collector_host_id":"issue-377-synthetic",
        "collector_region":"engineering-fixture", "schema_version":POLYMARKET_MARKET_METADATA_SCHEMA,
        "slug":"btc-updown-5m-engineering", "condition_id":condition,"clob_market_id":clob,
        "asset_ids":serde_json::to_string(&tokens)?, "market_start_utc":MARKET_START,
        "market_end_utc":MARKET_END, "fixed_identity_verified_at_utc":MARKET_START-1_001,
        "market_lifecycle_state":if active {MetadataMarketLifecycleState::Active} else {MetadataMarketLifecycleState::Prefetched},
        "market_runtime_role":if active {MetadataMarketRuntimeRole::Current} else {MetadataMarketRuntimeRole::Next},
        "source_endpoint":"https://gamma-api.polymarket.com/markets",
        "source_evidence_origin":MetadataSourceEvidenceOrigin::GammaMarketsDiscovery,
        "source_evidence_event_type":MetadataSourceEvidenceEventType::GammaMarketsDiscoveryResponse,
        "source_evidence_received_at_utc":at,"row_evidence_validation_completed_at_utc":at+1,
        "source_evidence_payload_json":payload,
        "source_evidence_raw_sha256":format!("{:x}",Sha256::digest(payload.as_bytes())),
        "metadata_fetch_start_utc":at-10, "closed":false,
        "outcomes_raw":"[\"UP\",\"DOWN\"]", "outcome_prices_raw":"[\"0.5\",\"0.5\"]",
        "active":true,"accepting_orders":true,"collector_admission_sequence":sequence,
        "collector_admission_time_utc_ms":at+1
    }))?)
}

fn book(condition: &str, token: &str, sequence: u64, extra_level: bool) -> PolymarketOrderbookRow {
    let mut bids = vec![PolymarketPriceLevel {
        price: "0.4".into(),
        size: "100".into(),
    }];
    if extra_level {
        bids.push(PolymarketPriceLevel {
            price: "0.3".into(),
            size: "80".into(),
        });
    }
    let at = MARKET_START + 100 + sequence as i64;
    PolymarketOrderbookRow {
        timestamp_received: at + 1,
        timestamp: Some(at),
        market: condition.into(),
        slug: Some("btc-updown-5m-engineering".into()),
        collector_host_id: "issue-377-synthetic".into(),
        collector_region: "engineering-fixture".into(),
        source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
        schema_version: POLYMARKET_ORDERBOOK_SCHEMA.into(),
        source_sequence_epoch_id: "issue-377-f4-book-epoch".into(),
        collector_admission_sequence: sequence,
        collector_admission_time_utc_ms: at + 2,
        event: PolymarketOrderbookEvent::Book {
            asset_id: token.into(),
            bids,
            asks: vec![PolymarketPriceLevel {
                price: "0.6".into(),
                size: "100".into(),
            }],
            last_trade_price: None,
        },
    }
}

fn scenario(
    core: &PublicObservationCoreV1,
) -> Result<Arc<SealedPlatformScenarioV1>, MaterialError> {
    let markets = core
        .fixed_markets()
        .map(|market| {
            let context = core
                .pm_context(&market.condition_id.0)
                .ok_or("F4公共Core缺固定市场上下文")?;
            let assets: [_; 2] = context
                .assets
                .into_iter()
                .map(|a| a.asset)
                .collect::<Vec<_>>()
                .try_into()
                .map_err(|_| "F4固定市场不是二元资产")?;
            PlatformMarketV1::try_new(context.market, assets).map_err(Into::into)
        })
        .collect::<Result<Vec<_>, MaterialError>>()?;
    let scenario = PlatformScenarioV1::try_new(markets)?;
    let artifact = SimulationScenarioArtifactV1 {
        schema_version: PLATFORM_SCENARIO_SCHEMA_V1.into(),
        markets: scenario.market_set().markets().to_vec(),
    };
    Ok(Arc::new(SealedPlatformScenarioV1::decode_and_seal(
        &serde_json::to_vec(&artifact)?,
    )?))
}

#[allow(clippy::too_many_arguments)]
fn execution(
    root: &Path,
    case: &str,
    account: &str,
    cash: &str,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    finality: Arc<TradeFinalityPolicyV2>,
    scenario: Arc<SealedPlatformScenarioV1>,
    implementation: ImplementationIdentityV1,
    source_sha: Sha256Hex,
) -> Result<(SharedExecutionV2, PublicSimExecutionV1), MaterialError> {
    let path = root.join("sim.jsonl");
    let latency = load_embedded_proxy_latency_profile()?;
    let opened = RunOpenedV2 {
        projection_version: BusinessProjectionVersionV1::V4,
        latency_sampler_version: sim_system::latency_profile::PROXY_LATENCY_SAMPLER_VERSION_V2
            .into(),
        run_id: format!("issue-377-{case}"),
        durability_role: RunDurabilityRoleV2::FiniteHistorical,
        platform_rule_snapshot_file_sha256: rules.file_sha256().clone(),
        trade_finality_policy_file_sha256: finality.policy_id().clone(),
        platform_scenario_file_sha256: scenario.file_sha256().clone(),
        proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
        implementation_identity: implementation,
        market_event_source_identity_sha256: Some(source_sha),
    };
    let source = DurableSourceReferenceV1 {
        source: ImmutableSourceReferenceV1::Location(path.display().to_string()),
    };
    let port = MemoryNoeProjectionPort::<MeasuredCancellationEvidence>::new(
        root.join("noe"),
        opened.clone(),
        source.clone(),
        source,
        format!("{case}-create"),
        "issue-377-f4".into(),
    );
    let observation = port.observation();
    let public = PublicRunOpenedV1 {
        projection_version: opened.projection_version,
        run_id: opened.run_id.clone(),
        platform_rule_snapshot_file_sha256: opened.platform_rule_snapshot_file_sha256,
        trade_finality_policy_file_sha256: opened.trade_finality_policy_file_sha256,
        platform_scenario_file_sha256: opened.platform_scenario_file_sha256,
        proxy_latency_profile_file_sha256: opened.proxy_latency_profile_file_sha256,
        implementation_identity: opened.implementation_identity,
        market_event_source_identity_sha256: opened.market_event_source_identity_sha256,
    };
    let mut execution = PublicSimExecutionV1::create_new_historical(
        &path,
        public,
        rules,
        finality,
        scenario,
        latency,
        Box::new(port),
        2 << 20,
        128,
    )?;
    execution.create_session(SessionSpecV3 {
        run_id: opened.run_id,
        session_id: format!("{case}-session"),
        account_id: account.into(),
        caller_role: SimCallerRoleV1::ModelTrain,
        caller_instance_id: "issue-377-f4".into(),
        creation_request_id: format!("{case}-create"),
        response_bridge_binding_id: format!("{case}-bridge"),
        initial_account_state: InitialAccountStateV2 {
            available_pusd: ExactDecimal(cash.into()),
        },
        session_seed: Sha256Hex("e".repeat(64)),
    })?;
    execution.configure_input_lifecycle_policy_v1(InputLifecyclePolicyV1::FiniteInput)?;
    execution.activate_session(format!("{case}-activate"))?;
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    execution.bind_ledger_recording_v2(&mut ledger, &root.join("ledger"))?;
    let shared = SharedExecutionV2::from_opening(ledger, observation)?
        .ok_or("F4公共开户未形成可领取原始账本事实")?;
    Ok((shared, execution))
}

#[allow(clippy::too_many_arguments)]
async fn build_case<I>(
    shared: &mut SharedExecutionV2,
    driver: &mut FinitePublicConsumptionV1<I>,
    case: &str,
    account: &str,
    kind: &str,
    rules: &SealedProcessRuleSnapshotV3,
    encoding: &EncodingConfig,
    seed: u64,
    source_sha: &Sha256Hex,
    input_count: u64,
) -> Result<Value, MaterialError>
where
    I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>,
{
    let mut last = None;
    // 查看EOF不关闭最后一个实际组；工程意图仍在原已交付且开放的组内发送。
    while driver.next_group_at()?.is_some() {
        let (_, cutoff) = driver.next_group()?.ok_or("F4已查看的公共组缺失")?;
        last = Some(cutoff);
    }
    let last = last.ok_or("F4未形成任何公共组")?;
    let at = last.decision_time().logical_time_utc_ms();
    driver.observe_at(at)?.ok_or("F4缺可信公共观察截止")?;
    let original_receipt = if case == "F-R" {
        let market = driver
            .frames_at_observation()
            .into_iter()
            .find(|frame| frame.observation.market.condition_id.0 == FIRST)
            .ok_or("F4 Repair缺第一市场观察")?;
        let minimum = Decimal::from_str_exact(&market.rule_context.limit_min_quantity_shares.0)?;
        let centishares = minimum * Decimal::from(100);
        let centishares = BigUint::parse_bytes(centishares.normalize().to_string().as_bytes(), 10)
            .ok_or("F4原MOS不能以本已固定GTC样本的完整centishares精确表达")?;
        let original_cost = costs(
            rules,
            &centishares,
            &[LimitLeg {
                price_index: 20,
                tick: ExactDecimal("0.01".into()),
                post_only: true,
            }],
        )?
        .into_iter()
        .fold(BigUint::zero(), |sum, cost| {
            sum + cost.principal_micro + cost.fee_1e5_units * 10u8
        });
        if original_cost > cash_micro(Decimal::from(100)) {
            return Err("F4已冻结工程现金不足以支持原MOS请求，不能降低MOS或自动增资".into());
        }
        let intent = ModelIntentV1::SubmitSingleBuy {
            condition_id: market.observation.market.condition_id.clone(),
            order: BuyLegIntentV1 {
                token_id: market
                    .observation
                    .assets
                    .first()
                    .ok_or("F4 Repair缺第一资产")?
                    .asset
                    .token_id
                    .clone(),
                order: BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share: Decimal::new(2, 1),
                    size_shares: minimum,
                    post_only: true,
                },
            },
        };
        let receipt = shared
            .execute(
                &mut driver.decision().ok_or("F4 Repair缺原发送能力")?,
                RequestContextV2 {
                    global_request_id: "issue-377-f4-repair-original".into(),
                    caller_correlation_id: None,
                },
                Some(Arc::new(intent)),
            )
            .await?
            .ok_or("F4 Repair原请求没有真实回执")?;
        Some(serde_json::to_value(receipt)?)
    } else {
        None
    };
    let cutoff = driver.observation_cutoff().ok_or("F4账户冻结缺观察截止")?;
    let frame = FrozenStrategyMarketFrameV1 {
        timing: MarketFrameTimingV1 {
            market_cutoff: cutoff,
            earliest_start_at_utc_ms: at,
        },
        markets: driver
            .frames_at_observation()
            .iter()
            .map(|frame| FrozenMarketReadV1 {
                statistics: frame.statistics.clone(),
                observation: frame.observation.clone(),
                rule_context: frame.rule_context.clone(),
            })
            .collect(),
    };
    let input = shared
        .strategy_input_for_market_frame_with_learning_requests(driver, frame, true, false, None)?
        .ok_or("F4未形成真实账户冻结输入")?;
    let scope = backtest::EntryMarketScopeV1::default().expand(BASE, MARKET_END)?;
    let frozen =
        crate::strategy::freeze_actor_input(&input, at, encoding, &scope, rules, None, false)?;
    let selected_market = input
        .markets
        .iter()
        .position(|market| market.observation.market.condition_id.0 == FIRST)
        .ok_or("F4生产冻结帧缺第一市场")?;
    let selected = frozen
        .options
        .iter()
        .position(|option| {
            option["kind"] == kind && (kind == "NoTrade" || option["market"] == selected_market)
        })
        .ok_or_else(|| format!("{case}生产options未提供指定合法动作"))?;
    if frozen.options.len() <= 1 {
        return Err(format!("{case}缺非退化option支持集").into());
    }
    let token_counts = frozen
        .market_tokens
        .iter()
        .map(Vec::len)
        .collect::<Vec<_>>();
    if case == "F-P" && (token_counts.len() < 2 || token_counts[0] == token_counts[1]) {
        return Err(format!("{case}原编码未形成两个不同长度市场，不能声称两级mean覆盖").into());
    }
    let selected_minimum = Decimal::from_str_exact(
        &input.markets[selected_market]
            .rule_context
            .limit_min_quantity_shares
            .0,
    )?;
    let (rpc, domains) = select_history(
        &frozen,
        selected,
        case == "F-S",
        rules,
        account,
        selected_minimum,
    )?;
    let minimums = input
        .markets
        .iter()
        .map(|m| m.rule_context.limit_min_quantity_shares.clone())
        .collect::<Vec<_>>();
    let market_ids = input
        .markets
        .iter()
        .map(|m| m.observation.market.condition_id.clone())
        .collect::<Vec<_>>();
    Ok(json!({
        "case":case,
        "frame":{
            "account":account,"call":1,"seed":seed,"processing_start_utc_ms":at,
            "global":frozen.global,"markets":frozen.market_tokens,"options":frozen.options,
            "selected_option":selected,"rpc":rpc,
            "domain_context":{"cash":frozen.cash.to_string(),"markets":frozen.domains.iter().map(|d|json!({"ticks":d.ticks,"max_prices":d.max_prices})).collect::<Vec<_>>()},
            "native_rules":{"snapshot_path":rules.path(),"expected_file_sha256":rules.file_sha256(),"implementation_identity":rules.implementation_identity()}
        },
        "construction":{
            "version":F4_FIXTURE_VERSION,"scope":"synthetic_numerical_engineering_only",
            "constants":fixture_constants(),"public_source_sha256":source_sha,"public_input_records":input_count,
            "caller_role":"ModelTrain","market_ids":market_ids,"market_token_counts":token_counts,
            "rules_sha256":rules.file_sha256(),"unchanged_limit_min_quantity_shares":minimums,
            "original_repair_receipt":original_receipt,"repair_evidence":frozen.repair_evidence,
            "domain_proof":domains,"account_cutoff":input.action_before.cutoff(),
            "market_frame":input.market_frame_timing,"global_private":frozen.global_projection,
            "market_private":frozen.private_markets,
            "teacher_choice_is_not_business_sampling":true,
            "singleton_is_not_platform_acceptance":case=="F-S"
        }
    }))
}

fn select_history(
    frozen: &crate::strategy::FrozenActorInput,
    selected: usize,
    singleton: bool,
    rules: &SealedProcessRuleSnapshotV3,
    account: &str,
    original_minimum: Decimal,
) -> Result<(Vec<Value>, Value), MaterialError> {
    let option = &frozen.options[selected];
    let kind = option["kind"].as_str().ok_or("F4选项缺kind")?;
    if kind == "NoTrade" {
        return Ok((vec![], json!({"kind":"NoTrade"})));
    }
    let market = usize::try_from(option["market"].as_u64().ok_or("F4选项缺market")?)?;
    let domain = frozen.domains.get(market).ok_or("F4选项market越界")?;
    let sides = if kind == "Pair" {
        vec![0usize, 1]
    } else {
        vec![usize::try_from(
            option["side"].as_u64().ok_or("F4 Repair缺side")?,
        )?]
    };
    let mut legs = Vec::new();
    let mut rpc = Vec::new();
    let mut proof = Vec::new();
    for (ordinal, side) in sides.iter().copied().enumerate() {
        let posts = supported_posts(rules, frozen.cash, &domain.ticks, &sides, &legs, ordinal)?;
        if !posts.contains(&true) {
            return Err("F4工程post-only选择不在原支持集中".into());
        }
        let maximum = maximum_affordable_price(
            rules,
            frozen.cash,
            &domain.ticks,
            &domain.max_prices,
            &sides,
            &legs,
            ordinal,
            true,
        )?;
        if (singleton && maximum != 1) || (!singleton && maximum <= 1) {
            return Err("F4原精确价格域未满足已声明单值/多值覆盖".into());
        }
        let index = if singleton { 1 } else { 2 };
        rpc.push(json!({"request":{"phase":"post","account":account,"call":1,"allowed":posts,"ordinal":ordinal,"side":side},"response":{"post_only":true,"index":null,"rank":null}}));
        rpc.push(json!({"request":{"phase":"price","account":account,"call":1,"maximum":maximum,"ordinal":ordinal,"side":side},"response":{"post_only":null,"index":index,"rank":null}}));
        proof.push(json!({"ordinal":ordinal,"side":side,"post_support":posts,"price_maximum":maximum,"selected_price_index":index}));
        legs.push(LimitLeg {
            price_index: index,
            tick: domain.ticks[side].clone(),
            post_only: true,
        });
    }
    let count = affordable_count(rules, frozen.cash, &legs)?;
    if count.is_zero()
        || (singleton && count != BigUint::one())
        || (!singleton && count <= BigUint::one())
    {
        return Err("F4原精确数量域未满足已声明单值/多值覆盖".into());
    }
    let rank = if singleton {
        BigUint::zero()
    } else {
        let centishares = original_minimum * Decimal::from(100);
        let quantity = BigUint::parse_bytes(centishares.normalize().to_string().as_bytes(), 10)
            .filter(|value| !value.is_zero())
            .ok_or("F4原MOS不在当前已冻结整数centishares选择器的表示范围")?;
        quantity - 1u8
    };
    if rank >= count {
        return Err("F4预定原MOS选择不在原完整资金域，不能自动换数量".into());
    }
    let quantity = shares(&rank_to_centishares(&rank)?)?;
    rpc.push(json!({"request":{"phase":"quantity","account":account,"call":1,"count":count.to_string()},"response":{"post_only":null,"index":null,"rank":rank.to_string()}}));
    Ok((
        rpc,
        json!({"kind":kind,"market":market,"legs":proof,"quantity_count":count.to_string(),"selected_rank":rank.to_string(),"selected_quantity_shares":quantity.to_string()}),
    ))
}

#[cfg(test)]
mod metadata_tests {
    use super::*;

    #[test]
    fn metadata_uses_formal_enum_values_for_both_fixture_states() {
        for (active, lifecycle, role) in [
            (
                false,
                MetadataMarketLifecycleState::Prefetched,
                MetadataMarketRuntimeRole::Next,
            ),
            (
                true,
                MetadataMarketLifecycleState::Active,
                MetadataMarketRuntimeRole::Current,
            ),
        ] {
            let row = metadata(
                FIRST,
                MARKETS[0].1,
                MARKETS[0].2,
                MARKET_START + if active { 0 } else { -1_000 },
                active,
                1,
            )
            .expect("F4 metadata must construct through the formal row serde");
            assert_eq!(
                row.metadata_row_kind,
                PolymarketMetadataRowKind::MetadataSnapshot
            );
            assert_eq!(
                row.metadata_origin,
                PolymarketMetadataOrigin::GammaMarketsDiscovery
            );
            assert_eq!(row.market_lifecycle_state, lifecycle);
            assert_eq!(row.market_runtime_role, role);
            assert_eq!(
                row.source_evidence_origin,
                Some(MetadataSourceEvidenceOrigin::GammaMarketsDiscovery)
            );
            assert_eq!(
                row.source_evidence_event_type,
                Some(MetadataSourceEvidenceEventType::GammaMarketsDiscoveryResponse)
            );
            if !active {
                // The raw-column spelling that caused the actual prepare failure is
                // not the JSON representation of this formal enum.
                let mut legacy = serde_json::to_value(&row).unwrap();
                legacy["market_lifecycle_state"] = json!("prefetched");
                let error = serde_json::from_value::<PolymarketMarketMetadataRow>(legacy)
                    .expect_err("legacy raw-column spelling must remain rejected");
                assert!(error.to_string().contains("unknown variant `prefetched`"));
            }
        }
    }
}
