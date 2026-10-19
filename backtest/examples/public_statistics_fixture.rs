//! 显式合成公共链夹具：不读取行情文件、不提交交易、不启动模型或研究。
#![allow(dead_code)]
#[path = "../../shared-run/tests/common/fixture_execution.rs"]
mod fixture_execution;
#[path = "../../shared-run/tests/common/fixture_input.rs"]
mod fixture_input;
#[path = "../../shared-run/tests/common/fixture_policy.rs"]
mod fixture_policy;
#[path = "support/statistics_sources.rs"]
mod sources;
use contracts::*;
use data_pipeline_core::{
    public_input::*, public_observation_core::*, public_statistics::*, typed_raw::*,
};
use ledger::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shared_run::*;
use sim_system::{execution::*, public_consumption::*};
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    io::Write,
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
    time::Instant,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub groups: u64,
    pub consumers: usize,
    pub progress_every: u64,
    pub output_every: u64,
    pub public_delivery_delay_ms: i64,
    pub statistics: PublicStatisticsConfigV1,
}
impl Config {
    pub fn small() -> Self {
        Self {
            groups: 100,
            consumers: 2,
            progress_every: 1000,
            output_every: 1000,
            public_delivery_delay_ms: 7,
            statistics: PublicStatisticsConfigV1 {
                momentum_windows_ms: vec![1000],
                spot_variance_windows_ms: vec![1000],
                terminal_integral_windows_ms: vec![10000],
            },
        }
    }
}
#[derive(Clone, Default)]
struct Source(Rc<RefCell<VecDeque<PublicGroupDeliveryV1>>>);
impl Iterator for Source {
    type Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.borrow_mut().pop_front().map(Ok)
    }
}
type Driver = FinitePublicConsumptionV1<Source>;
struct Consumer {
    driver: Driver,
    shared: SharedExecutionV2,
    source: Source,
}
/// 只有真实合法原始输入构造、公共 prepare/push 和 SIM from_day 接线；不复制归约。
pub struct Fixture {
    pub core: PublicObservationCoreV1,
    consumers: Vec<Consumer>,
    stream: Arc<PublicStatisticsStreamV1>,
    canonical_position: u64,
    input_position: u64,
    delivered_at: i64,
    delay: i64,
    pub updates: u64,
    pub reads: u64,
}
impl Fixture {
    pub fn new(root: &Path, config: &Config) -> Self {
        std::fs::create_dir_all(root).unwrap();
        let core = fixture_input::core();
        let identity = PublicStatisticsIdentityV1 {
            input_set_identity: Sha256Hex(format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&("synthetic-public-chain-v1", config)).unwrap())
            )),
            stream_header: core.header().clone(),
            rules_identity: Sha256Hex(
                "d63c0e7b09edc4b2c4c3f0295f806b25f6f5fd03663cdf60ff062117e53e4e54".into(),
            ),
            delivery_policy: format!(
                "synthetic visible+{}ms; monotonic-max; v1",
                config.public_delivery_delay_ms
            ),
        };
        let stream = PublicStatisticsStreamV1::new(identity, config.statistics.clone()).unwrap();
        let mut f = Self {
            core,
            consumers: vec![],
            stream,
            canonical_position: 0,
            input_position: 0,
            delivered_at: 0,
            delay: config.public_delivery_delay_ms,
            updates: 0,
            reads: 0,
        };
        let meta = fixture_input::meta(0);
        let initial = f.prepare(meta);
        for i in 0..config.consumers {
            let account = root.join(format!("account-{i}"));
            std::fs::create_dir_all(&account).unwrap();
            let (mut execution, observation, _) =
                fixture_execution::execution_with_evidence::<MeasuredCancellationEvidence>(
                    &account.join("sim.jsonl"),
                    &f.core,
                    fixture_policy::SUCCESS_POLICY,
                    None,
                    "e".repeat(64),
                    &(100 + i).to_string(),
                    &format!("statistics-fixture-{i}"),
                    BusinessProjectionVersionV1::V4,
                    None,
                    None,
                    SimCallerRoleV1::ModelTrain,
                );
            let mut ledger = LedgerWriterV1::new_with_admission_v3(
                AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
            );
            execution
                .bind_ledger_recording_v2(&mut ledger, &account.join("ledger"))
                .unwrap();
            let shared = SharedExecutionV2::from_opening(ledger, observation)
                .unwrap()
                .unwrap();
            let source = Source::default();
            let mut driver = FinitePublicConsumptionV1::new(
                execution,
                source.clone(),
                "synthetic-statistics".into(),
            );
            driver.enable_public_statistics(f.stream.clone()).unwrap();
            driver.enable_finite_recording_wait().unwrap();
            f.consumers.push(Consumer {
                driver,
                shared,
                source,
            });
        }
        f.deliver(initial);
        f
    }
    fn prepare(&mut self, input: RecordedIngressV1) -> Vec<PublicGroupDeliveryV1> {
        self.input_position = input.input_position + 1;
        let prepared =
            prepare_public_ingress_v1(&self.core, &input, &BTreeMap::new(), &[]).unwrap();
        let applied = self.core.push(&prepared).unwrap().into_owned().applied;
        let prepared_pm = match prepared.current {
            PreparedCanonicalInputV1::PolymarketMessage(pm) => Some(pm),
            _ => None,
        };
        let contexts = self
            .core
            .fixed_markets()
            .filter_map(|m| {
                self.core
                    .prepared_pm_context(&m.condition_id.0)
                    .map(|c| (m.condition_id.0.clone(), c))
            })
            .collect();
        let n = applied
            .closed_groups
            .iter()
            .map(|g| g.records.len() as u64)
            .sum::<u64>();
        let mut day = data_pipeline_training::day_producer::DayAppliedV1 {new_public_trades:vec![],
            input,
            applied,
            first_canonical_position: self.canonical_position,
            prepared_pm,
            contexts,
            initialization_binding: None,
            hidden: vec![],
        };
        self.canonical_position += n;
        let mut groups = PreparedPublicGroupV1::from_day(&mut day);
        for group in &mut groups {
            self.delivered_at = self
                .delivered_at
                .max(group.delivered_at_utc_ms + self.delay);
            group.delivered_at_utc_ms = self.delivered_at;
        }
        groups
    }
    fn deliver(&mut self, groups: Vec<PublicGroupDeliveryV1>) {
        for group in groups {
            let mut prefix = None;
            for c in &mut self.consumers {
                c.source.0.borrow_mut().push_back(group.clone());
                c.driver.next_group().unwrap().unwrap();
                if let Some(p) = c.driver.public_statistics_prefix() {
                    if let Some(first) = &prefix {
                        assert!(Arc::ptr_eq(first, p));
                    } else {
                        prefix = Some(p.clone());
                    }
                }
            }
            self.updates += 1;
        }
    }
    pub fn push(&mut self, row: TypedRawRowV2) {
        let groups = self.prepare(fixture_input::record(self.input_position, row));
        self.deliver(groups);
    }
    pub fn row(&mut self, row: TypedRawRow) {
        self.push(TypedRawRowV2::Existing(row));
    }
    pub fn freeze(&mut self) -> Vec<StrategyInputV1> {
        let mut inputs = vec![];
        let mut first = None;
        for c in &mut self.consumers {
            let input = c
                .shared
                .strategy_borrowed_observation_input(&mut c.driver, true)
                .unwrap()
                .unwrap()
                .to_owned_input();
            if let Some(s) = input.markets.first().and_then(|m| m.statistics.as_ref()) {
                if let Some(old) = &first {
                    assert!(Arc::ptr_eq(old, s));
                } else {
                    first = Some(s.clone());
                }
            }
            self.reads += 1;
            inputs.push(input);
        }
        inputs
    }
    pub fn observe(&mut self, t: i64) {
        for c in &mut self.consumers {
            c.driver.observe_at(t).unwrap();
        }
    }
    pub fn history_points(&self) -> (usize, usize) {
        self.consumers[0]
            .driver
            .public_statistics_prefix()
            .unwrap()
            .history_points()
    }
    pub fn warm(&mut self) {
        let mut depth = sources::snapshot("depth", 100, 1);
        depth.bids = vec![sources::level("99", "2"), sources::level("98", "3")];
        depth.asks = vec![sources::level("101", "3")];
        self.row(TypedRawRow::BinanceDepthSnapshot(depth));
        let mut trade = sources::trade(1, 1, "100");
        if let BinanceAggTradeRecord::Trade { quantity, .. } = &mut trade.record {
            *quantity = "2".into();
        }
        self.row(TypedRawRow::BinanceAggTrade(trade));
        self.spot(1, 21000, "100");
        self.push(TypedRawRowV2::PolymarketRtdsChainlinkTwap(
            sources::twap_input(30),
        ));
        self.push(TypedRawRowV2::PolymarketRtdsChainlinkTwap(
            sources::twap_input(60),
        ));
        self.push(
            fixture_input::pm(self.input_position, 1, 21000, fixture_input::book("5"))
                .ingress
                .into_message_row(),
        );
    }
    pub fn spot(&mut self, sequence: u64, at: i64, price: &str) {
        self.row(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot(
            sequence, at, price,
        )));
    }
}
trait MessageRow {
    fn into_message_row(self) -> TypedRawRowV2;
}
impl MessageRow for DptIngressV1 {
    fn into_message_row(self) -> TypedRawRowV2 {
        if let Self::Message { row, .. } = self {
            row
        } else {
            panic!("合成消息")
        }
    }
}
pub fn spot(sequence: u64, at: i64, price: &str) -> PolymarketRtdsChainlinkBtcUsdRow {
    let mut s = sources::twap_input(30).common;
    s.topic = "crypto_prices_chainlink".into();
    s.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
    s.value = price.into();
    s.timestamp_received = at;
    s.message_timestamp = at - 1;
    s.payload_timestamp = at - 2;
    s.source_native_payload_json = format!("{{\"synthetic_sequence\":{sequence}}}");
    s
}
pub fn frozen_json(input: &StrategyInputV1) -> serde_json::Value {
    serde_json::json!({"cutoff":input.action_before.cutoff(),"ledger":input.action_before.ledger(),"economics":input.action_before.economics(),"trade_economics":format!("{:?}",input.trade_economics),"markets":input.markets.iter().map(|m|serde_json::json!({"observation":m.observation,"rules":m.rule_context,"statistics":m.statistics})).collect::<Vec<_>>()})
}
fn output_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|e| {
            let p = e.unwrap().path();
            if p.is_dir() {
                output_bytes(&p)
            } else {
                std::fs::metadata(p).unwrap().len()
            }
        })
        .sum()
}
fn program_sha256() -> String {
    use std::io::Read;
    let mut input = std::fs::File::open(std::env::current_exe().unwrap()).unwrap();
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 65536];
    loop {
        let n = input.read(&mut buffer).unwrap();
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    format!("{hash:x}", hash = hash.finalize())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 3 {
        return Err("用法：public_statistics_fixture <配置.json> <输出目录>".into());
    }
    let config: Config = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    if config.consumers == 0 {
        return Err("consumers 必须大于零".into());
    }
    let root = PathBuf::from(&args[2]);
    std::fs::create_dir(&root)?;
    std::fs::write(
        root.join("effective-config.json"),
        serde_json::to_vec_pretty(&config)?,
    )?;
    let started = Instant::now();
    let program_hash = program_sha256();
    let mut fixture = Fixture::new(&root, &config);
    fixture.warm();
    let mut output = std::io::BufWriter::new(std::fs::File::create(root.join("samples.jsonl"))?);
    let mut outputs = 0;
    let mut max_points = (0, 0);
    for i in 0..config.groups {
        fixture.spot(
            i + 10,
            22010 + i as i64 * 2,
            if i % 2 == 0 { "103" } else { "102" },
        );
        let inputs = fixture.freeze();
        let n = fixture.history_points();
        max_points = (max_points.0.max(n.0), max_points.1.max(n.1));
        if config.output_every > 0 && (i % config.output_every == 0 || i + 1 == config.groups) {
            let bytes = serde_json::to_vec(&frozen_json(&inputs[0]))?;
            output.write_all(&bytes)?;
            output.write_all(b"\n")?;
            outputs += bytes.len() + 1;
        }
        if config.progress_every > 0 && (i + 1) % config.progress_every == 0 {
            eprintln!(
                "合成组 {}/{}；已耗时 {:.3}s",
                i + 1,
                config.groups,
                started.elapsed().as_secs_f64()
            );
        }
    }
    output.flush()?;
    for c in &mut fixture.consumers {
        c.driver.stabilize_recording()?;
        c.shared.apply_closed()?;
    }
    let elapsed = started.elapsed().as_secs_f64();
    let summary = serde_json::json!({"definition":PUBLIC_STATISTICS_VERSION_V1,"program_sha256":program_hash,"output_bytes_before_summary":output_bytes(&root),"fixture":"synthetic-public-chain-v1","effective_config":config,"public_updates":fixture.updates,"account_freezes":fixture.reads,"elapsed_seconds":elapsed,"groups_per_second":config.groups as f64/elapsed,"max_history_points":max_points,"final_history_points":fixture.history_points(),"sample_output_bytes":outputs,"cpu_gpu_transfer_bytes":0,"gpu":"N/A: CPU public statistics; no model","parallelism":"one causal public stream; independent account state; compiled with configured cargo jobs","input_identity":fixture.consumers[0].driver.public_statistics_prefix().unwrap().identity});
    std::fs::write(
        root.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    fn stat(input: &StrategyInputV1) -> &PublicMarketStatisticsV1 {
        input.markets[0].statistics.as_ref().unwrap()
    }
    #[test]
    fn hand_calculated_pressure_basis_channels_and_complete_input_freeze() {
        let temp = tempfile::tempdir().unwrap();
        let mut f = Fixture::new(temp.path(), &Config::small());
        f.warm();
        let old = f.freeze();
        let before = frozen_json(&old[0]);
        let s = stat(&old[0]);
        assert_eq!(s.depth.bid_quantity_btc.value, Some(Decimal::from(5)));
        assert_eq!(s.depth.ask_quantity_btc.value, Some(Decimal::from(3)));
        assert_eq!(s.depth.imbalance.value, Some(Decimal::new(25, 2)));
        assert_eq!(s.latest_trade_pressure.value, Some(Decimal::new(2, 1)));
        let pm = s
            .pm_quantities
            .iter()
            .find(|a| a.asset.token_id.0 == fixture_input::UP_TOKEN_ID)
            .unwrap();
        assert_eq!(pm.bid_shares.value, Some(Decimal::from(5)));
        assert_eq!(pm.ask_shares.value, Some(Decimal::from(80)));
        assert_eq!(old[0].action_before.cash_available(), Decimal::from(100));
        assert_eq!(old[1].action_before.cash_available(), Decimal::from(101));
        // 冻结后通过唯一 Ledger 增加本地预留，不推进隐藏平台或伪造已交付反馈。
        f.consumers[0]
            .shared
            .reserve_action_v2(&[ReserveIntentV1 {
                request_identity: LedgerRequestIdentityV1("synthetic-later-reserve".into()),
                condition_id: ConditionId(fixture_input::CONDITION_ID.into()),
                requested_principal_pusd: ExactDecimal("7".into()),
                immediate_fee_reserve_pusd: ExactDecimal("0".into()),
            }])
            .unwrap();
        f.row(TypedRawRow::BinanceAggTrade(sources::trade(2, 2, "103")));
        let after_b = f.freeze();
        assert_eq!(
            stat(&after_b[0]).basis[0]
                .as_ref()
                .unwrap()
                .innovation_usd_per_btc_numeric_proxy,
            Decimal::from(3)
        );
        f.spot(2, 23000, "102");
        for c in &mut f.consumers {
            c.driver.stabilize_recording().unwrap();
        }
        let after_r = f.freeze();
        assert_eq!(after_r[0].action_before.cash_available(), Decimal::from(93));
        assert_eq!(
            after_r[1].action_before.cash_available(),
            Decimal::from(101)
        );
        assert_eq!(old[0].action_before.cash_available(), Decimal::from(100));

        let saved = stat(&after_r[0]).basis[0].clone().unwrap();
        assert_eq!(saved.innovation_usd_per_btc_numeric_proxy, Decimal::ONE);
        let thirty = stat(&after_r[0]).basis[1].as_ref().unwrap();
        assert_eq!(
            thirty.related_group,
            stat(&after_b[0]).basis[1].as_ref().unwrap().related_group
        );
        let points = f.history_points();
        f.observe(23100);
        let silent = f.freeze();
        assert_eq!(stat(&silent[0]).basis[0].as_ref().unwrap(), &saved);
        assert_eq!(points, f.history_points());
        assert_eq!(before, frozen_json(&old[0]));
        assert_eq!(s.definition_version, PUBLIC_STATISTICS_VERSION_V1);
        assert_ne!(s.group, stat(&silent[0]).group);
    }
    #[test]
    fn same_millisecond_last_value_real_equal_retry_and_silent_operators() {
        let temp = tempfile::tempdir().unwrap();
        let mut cfg = Config::small();
        cfg.public_delivery_delay_ms = 0;
        cfg.statistics.terminal_integral_windows_ms = vec![8000];
        let mut f = Fixture::new(temp.path(), &cfg);
        f.warm();
        f.spot(2, 22000, "102");
        let first = f.freeze();
        let first_s = stat(&first[0]);
        let before = frozen_json(&first[0]);
        assert_eq!(
            first_s.spot_variance[0]
                .statistics
                .squared_change_sum_usd_squared,
            4.0
        );
        f.spot(3, 22000, "103");
        let last = f.freeze();
        assert_eq!(
            stat(&last[0]).spot_variance[0]
                .statistics
                .squared_change_sum_usd_squared,
            9.0
        );
        assert_eq!(before, frozen_json(&first[0]));
        f.spot(4, 23000, "103");
        let same = f.freeze();
        assert_eq!(
            stat(&same[0]).spot_variance[0]
                .statistics
                .positive_pair_count,
            2
        );
        assert_eq!(
            stat(&same[0]).spot_variance[0].statistics.elapsed_seconds,
            2.0
        );
        assert_eq!(
            stat(&same[0]).spot_variance[0].sigma_usd_per_btc_per_sqrt_second,
            Some((4.5_f64).sqrt())
        );
        assert_eq!(
            stat(&same[0]).spot_integrals[0]
                .integral_usd_per_btc_seconds
                .value,
            Some(102.997)
        );
        let points = f.history_points();
        let input = spot(4, 23000, "103");
        f.row(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(input));
        assert_eq!(points, f.history_points());
        // 有限源提前持有下一组，再推进静默截止；预取不构成应用。
        let mut depth = sources::snapshot("depth-next", 200, 1);
        depth.bids = vec![sources::level("97", "5")];
        depth.asks = vec![sources::level("101", "3")];
        depth.timestamp_received = 25000;
        depth.collector_admission_time_utc_ms = 25000;
        let groups = f.prepare(fixture_input::record(
            f.input_position,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(depth)),
        ));
        for c in &f.consumers {
            c.source.0.borrow_mut().extend(groups.iter().cloned());
        }

        f.observe(24001);
        let silent = f.freeze();
        assert_eq!(
            stat(&silent[0]).spot_variance[0].sigma_usd_per_btc_per_sqrt_second,
            None
        );
        assert_eq!(
            stat(&silent[0]).spot_integrals[0]
                .integral_usd_per_btc_seconds
                .value,
            Some(206.1)
        );
        assert_eq!(points, f.history_points());
        // 中点下降仍由时间动能表达；旧输入不读取预取组。
        for c in &mut f.consumers {
            c.driver.next_group().unwrap().unwrap();
        }
        let down = f.freeze();
        assert_eq!(
            stat(&down[0]).momentum[0].change_usdt_per_btc.value,
            Some(Decimal::from(-1))
        );
        f.observe(26001);
        let flat = f.freeze();
        assert_eq!(
            stat(&flat[0]).momentum[0].change_usdt_per_btc.value,
            Some(Decimal::ZERO)
        );
    }
    #[test]
    fn lagging_consumer_prefetched_future_and_configuration_identity() {
        let temp = tempfile::tempdir().unwrap();
        let mut f = Fixture::new(temp.path(), &Config::small());
        f.warm();
        let early = f
            .prepare(fixture_input::record(
                f.input_position,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot(
                    2, 23000, "102",
                ))),
            ))
            .remove(0);
        let late = f
            .prepare(fixture_input::record(
                f.input_position,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot(
                    3, 24000, "150",
                ))),
            ))
            .remove(0);
        f.consumers[0]
            .source
            .0
            .borrow_mut()
            .push_back(early.clone());
        f.consumers[0].driver.next_group().unwrap().unwrap();
        let first = f.consumers[0]
            .driver
            .public_statistics_prefix()
            .unwrap()
            .clone();
        let observation = f.consumers[0].driver.frames_at_observation()[0]
            .observation
            .clone();
        let time = first.applied_at_utc_ms;
        let frozen = first.read(&observation, time);
        f.consumers[0].source.0.borrow_mut().push_back(late.clone());
        f.consumers[0].driver.next_group().unwrap().unwrap();
        let latest = f.consumers[0]
            .driver
            .public_statistics_prefix()
            .unwrap()
            .clone();
        let slower = f.consumers[1].driver.frames_at_observation();
        assert_eq!(
            slower[0]
                .observation
                .chainlink_spot
                .value
                .as_ref()
                .unwrap()
                .price_usd_per_btc
                .0,
            "100"
        );
        f.consumers[1]
            .source
            .0
            .borrow_mut()
            .push_back(early.clone());
        f.consumers[1].driver.next_group().unwrap().unwrap();
        let recovered = f.consumers[1].driver.public_statistics_prefix().unwrap();
        assert!(Arc::ptr_eq(&first, recovered));
        assert!(Arc::ptr_eq(&frozen, &recovered.read(&observation, time)));
        assert!(
            early
                .prepared
                .statistics
                .consume(
                    &f.stream,
                    first.group.clone(),
                    time + 1,
                    &early.prepared.group
                )
                .is_err()
        );
        let mut alternate = frozen.config.as_ref().clone();
        alternate.spot_variance_windows_ms = vec![2000];
        let alternate_stream =
            PublicStatisticsStreamV1::new(frozen.identity.as_ref().clone(), alternate).unwrap();
        let alternate = early
            .prepared
            .statistics
            .consume(
                &alternate_stream,
                first.group.clone(),
                time,
                &early.prepared.group,
            )
            .unwrap()
            .unwrap();
        assert_ne!(alternate.read(&observation, time).config, frozen.config);
        assert!(!Arc::ptr_eq(&first, &alternate));
        f.consumers[1].source.0.borrow_mut().push_back(late);
        f.consumers[1].driver.next_group().unwrap().unwrap();
        assert!(Arc::ptr_eq(
            &latest,
            f.consumers[1].driver.public_statistics_prefix().unwrap()
        ));
        let mut bytes = vec![];
        ciborium::ser::into_writer(first.as_ref(), &mut bytes).unwrap();
        let restored: PublicStatisticsHistoryV1 =
            ciborium::de::from_reader(bytes.as_slice()).unwrap();
        assert_eq!(restored.read(&observation, time), frozen);
    }
    #[test]
    fn terminal_integral_survives_window_reclamation_and_stays_frozen() {
        let temp = tempfile::tempdir().unwrap();
        let mut cfg = Config::small();
        cfg.statistics.terminal_integral_windows_ms = vec![8000];
        let mut f = Fixture::new(temp.path(), &cfg);
        f.warm();
        f.spot(2, 23000, "100");
        f.spot(3, 30000, "200");
        let final_input = f.freeze();
        let integral = stat(&final_input[0]).spot_integrals[0].clone();
        assert_eq!(
            integral.integral_usd_per_btc_seconds.value,
            Some(800000.0 / 1000.0)
        );
        for i in 0..100 {
            f.spot(i + 4, 40000 + i as i64 * 1000, "300");
        }
        let later = f.freeze();
        assert_eq!(stat(&later[0]).spot_integrals[0], integral);
        assert!(f.history_points().1 <= 12);
    }
    #[test]
    fn legal_depth_delta_deleting_bid_side_preserves_known_ask() {
        let temp = tempfile::tempdir().unwrap();
        let mut f = Fixture::new(temp.path(), &Config::small());
        f.warm();
        let old = f.freeze();
        // Snapshot 100 -> first continuous update 101. Zero quantity removes
        // both existing bid levels while the unmodified ask at 101 remains.
        f.row(TypedRawRow::BinanceDepthUpdate(BinanceDepthUpdateRow {
            timestamp_received: 23000,
            event_type: "depthUpdate".to_owned(),
            event_time: 22999,
            symbol: "BTCUSDT".to_owned(),
            first_update_id: 101,
            final_update_id: 101,
            bids: vec![sources::level("99", "0"), sources::level("98", "0")],
            asks: Vec::new(),
            stream: "btcusdt@depth".to_owned(),
            snapshot_last_update_id: Some(100),
            local_u_before: None,
            stitch_verdict: BinanceDepthStitchVerdict::Applied,
            stitch_evidence: Some("continuous_depth_update".to_owned()),
            stitch_gap_reason: None,
            source_sequence_epoch_id: "depth".to_owned(),
            connection_generation: 1,
            depth_ingress_sequence: 2,
            collector_admission_sequence: 2,
            collector_admission_time_utc_ms: 23001,
            collector_host_id: "collector-1".to_owned(),
            collector_region: "eu-west-1".to_owned(),
            source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@depth".to_owned(),
            schema_version: "binance_depth_update_v4".to_owned(),
        }));
        let now = f.freeze();
        let depth = &stat(&now[0]).depth;
        assert_eq!(depth.best_ask_usdt_per_btc.value, Some(Decimal::from(101)));
        assert!(depth.best_bid_usdt_per_btc.value.is_none());
        assert!(depth.midpoint_usdt_per_btc.value.is_none());
        assert!(depth.spread_usdt_per_btc.value.is_none());
        assert_eq!(depth.bid_quantity_btc.value, Some(Decimal::ZERO));
        assert_eq!(depth.ask_quantity_btc.value, Some(Decimal::from(3)));
        assert_eq!(
            stat(&old[0]).depth.best_bid_usdt_per_btc.value,
            Some(Decimal::from(99))
        );
        assert_eq!(
            stat(&old[0]).depth.best_ask_usdt_per_btc.value,
            Some(Decimal::from(101))
        );
        assert!(Arc::ptr_eq(
            now[0].markets[0].statistics.as_ref().unwrap(),
            now[1].markets[0].statistics.as_ref().unwrap()
        ));
    }
    #[test]
    fn missing_depth_zero_empty_book_and_missing_left_support_are_distinct() {
        let temp = tempfile::tempdir().unwrap();
        let mut f = Fixture::new(temp.path(), &Config::small());
        f.spot(1, 21000, "100");
        f.row(TypedRawRow::BinanceAggTrade(sources::trade(1, 1, "100")));
        let missing = f.freeze();
        let s = stat(&missing[0]);
        assert!(s.latest_trade_pressure.value.is_none());
        assert!(s.depth.imbalance.value.is_none());
        assert!(
            s.spot_variance[0]
                .sigma_usd_per_btc_per_sqrt_second
                .is_none()
        );
        assert!(
            s.spot_integrals[0]
                .integral_usd_per_btc_seconds
                .value
                .is_none()
        );
        let mut event = fixture_input::book("5");
        if let PolymarketOrderbookEvent::Book { bids, asks, .. } = &mut event {
            bids.clear();
            asks.clear();
        }
        let row = fixture_input::pm(f.input_position, 1, 21000, event)
            .ingress
            .into_message_row();
        f.push(row);
        let empty = f.freeze();
        let pm = stat(&empty[0])
            .pm_quantities
            .iter()
            .find(|a| a.asset.token_id.0 == fixture_input::UP_TOKEN_ID)
            .unwrap();
        assert_eq!(pm.bid_shares.value, Some(Decimal::ZERO));
        assert_eq!(pm.ask_shares.value, Some(Decimal::ZERO));
        f.spot(2, 23000, "100");
        let zero = f.freeze();
        assert_eq!(
            stat(&zero[0]).spot_variance[0].sigma_usd_per_btc_per_sqrt_second,
            Some(0.0)
        );
    }
}
