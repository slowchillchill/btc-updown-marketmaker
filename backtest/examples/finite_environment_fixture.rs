//! D-0367 公共环境合成夹具；固定合法请求检验执行链，不是模型或学习入口。
#![allow(dead_code)]
#[path = "../tests/common/fixture_material.rs"]
mod fixture_material;
use backtest::*;
use contracts::*;
use ledger::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shared_run::*;
use sim_system::{execution::*, platform_rules::*, public_consumption::*};
use std::{io::Read, path::Path, sync::Arc, time::Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub caller_role: SimCallerRoleV1,
    pub implementation_identity: ImplementationIdentityV1,
    pub input_lifecycle_policy: InputLifecyclePolicyV1,
    pub gamma_resolution_policy: GammaResolutionPolicyV1,
    pub iterations: usize,
    pub progress_every: usize,
    pub public_delivery_delay_ms: u64,
    pub maker_trade: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub received_end_utc_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_market_end_utc_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rebate_entitlement_queries: Vec<RebateEntitlementQueryV1>,
}
struct Orders {
    rules: Arc<SealedProcessRuleSnapshotV3>,
    sent: [bool; 2],
    maker_trade: bool,
}
impl StrategyV1 for Orders {
    type Error = &'static str;
    type Diagnostics = ();
    fn decide(&mut self, input: &StrategyInputV1, _: i64) -> Result<StrategyOutputV1, Self::Error> {
        let mut intents = Vec::new();
        for (i, (condition, token, price, post_only)) in [
            (
                fixture_material::FIRST,
                "20",
                if self.maker_trade { "0.4" } else { "0.6" },
                self.maker_trade,
            ),
            (fixture_material::SECOND, "21", "0.3", true),
        ]
        .into_iter()
        .enumerate()
        {
            if self.sent[i]
                || !input.markets.iter().any(|m| {
                    m.observation.market.condition_id.0 == condition
                        && m.observation.assets.iter().all(|a| a.asks.value.is_some())
                })
            {
                continue;
            }
            self.sent[i] = true;
            let price: Decimal = price.parse().unwrap();
            let cost = self
                .rules
                .limit_buy_cost_v1(
                    &num_bigint::BigUint::from(1000u32),
                    (price * Decimal::from(100))
                        .normalize()
                        .to_string()
                        .parse()
                        .unwrap(),
                    &ExactDecimal("0.01".into()),
                    post_only,
                )
                .unwrap();
            intents.push(ReservedIntentV1 {
                context: RequestContextV2 {
                    global_request_id: format!("fixture-order-{i}"),
                    caller_correlation_id: None,
                },
                intent: Arc::new(ModelIntentV1::SubmitSingleBuy {
                    condition_id: ConditionId(condition.into()),
                    order: BuyLegIntentV1 {
                        token_id: TokenId(token.into()),
                        order: BuyOrderIntentV1::GtcLimit {
                            limit_price_pusd_per_share: price,
                            size_shares: Decimal::from(10),
                            post_only,
                        },
                    },
                }),
                reserves: vec![IntentReserveV1 {
                    batch_item_ordinal: 0,
                    reserve: ReserveIntentV1 {
                        request_identity: LedgerRequestIdentityV1(format!("fixture-reserve-{i}")),
                        condition_id: ConditionId(condition.into()),
                        requested_principal_pusd: ExactDecimal(
                            (Decimal::from_str_exact(&cost.principal_micro.to_string()).unwrap()
                                / Decimal::from(1_000_000))
                            .to_string(),
                        ),
                        immediate_fee_reserve_pusd: ExactDecimal(
                            (Decimal::from_str_exact(&cost.fee_1e5_units.to_string()).unwrap()
                                / Decimal::from(100_000))
                            .to_string(),
                        ),
                    },
                }],
            });
        }
        Ok(StrategyOutputV1::Continue {
            intents,
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        })
    }
}
fn bytes(root: &Path) -> u64 {
    std::fs::read_dir(root)
        .unwrap()
        .map(|e| {
            let p = e.unwrap().path();
            if p.is_dir() {
                bytes(&p)
            } else {
                p.metadata().unwrap().len()
            }
        })
        .sum()
}
/// 实际合成 Parquet → 原日供料 → 公共SIM/NOE/Ledger → 报告/记录输出。
pub fn run_fixture(
    root: &Path,
    config: &Config,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let started = Instant::now();
    let mut reports = Vec::new();
    let mut updates = 0;
    for i in 0..config.iterations {
        let path = root.join(format!("case-{i}"));
        std::fs::create_dir(&path)?;
        let (mut c, original) = fixture_material::setup_day_with_rebate_end(
            &path,
            fixture_material::BASE,
            "2026-08-24",
            config.maker_trade,
            config.first_market_end_utc_ms,
        );
        c.implementation_identity = config.implementation_identity.clone();
        c.strategy = StrategyDescriptorV1 {
            strategy_id: "finite-environment-fixed-orders-fixture".into(),
            strategy_rule_version: "synthetic-fixed-orders-v1".into(),
            effective_parameters: serde_json::json!({"purpose":"固定合法请求验证公共环境，不是模型或学习", "maker_trade":config.maker_trade, "first_market":{"token":"20", "size_shares":"10", "limit_price":if config.maker_trade {"0.4"} else {"0.6"}, "post_only":config.maker_trade}, "second_market":{"token":"21", "size_shares":"10", "limit_price":"0.3", "post_only":true}}),
        };
        let rules = derive_public_maker_snapshot_v3(
            HistoricalProcessSnapshotRequestV1 {
                snapshot_path: original.path().to_path_buf(),
                expected_file_sha256: original.file_sha256().clone(),
                replay_causal_start_utc_ms: i64::MAX,
            },
            path.join("explicit-rules.json"),
            c.implementation_identity.clone(),
            &[config.caller_role],
        )?;
        c.rebate_entitlement_queries = config.rebate_entitlement_queries.clone();
        if let Some(end) = config.received_end_utc_ms {
            c.preparation.scope.received_end_utc_ms = end;
        }
        c.session.caller_role = config.caller_role;
        c.session.input_lifecycle_policy = config.input_lifecycle_policy;
        c.gamma_resolution_policy = config.gamma_resolution_policy;
        c.timing.public_delivery_delay_ms = DelayScheduleV1::Fixed(config.public_delivery_delay_ms);
        std::fs::write(
            path.join("effective-backtest-config.json"),
            serde_json::to_vec_pretty(&c)?,
        )?;
        let day = HistoricalDayConfigV1 {
            preparation: c.preparation.clone(),
            candidates: vec![HistoricalPreparedRunConfigV1::from(&c)],
            progress_interval_seconds: 0,
            producer_lead_groups: Some(1),
        };
        let result = run_historical_days_v1(&[day], rules.clone(), &|_| {
            Ok(Orders {
                rules: rules.clone(),
                sent: [false; 2],
                maker_trade: config.maker_trade,
            })
        })?;
        updates += result[0].public_applied_inputs;
        let report = &result[0].candidates[0].report;
        assert_eq!(report["end_reason"], "RangeEnd");
        assert_eq!(report["recording_drained"], true);
        let metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&c.metadata_path)?)?;
        assert_eq!(
            metadata["caller_role"],
            serde_json::to_value(config.caller_role)?
        );
        assert_eq!(
            metadata["maker_model_version"],
            "PUBLIC_TRADE_CONSERVATIVE_FIFO_V1"
        );
        let ledger = LedgerWriterV1::open_durable(&c.ledger_root)?;
        let read = ledger.economic_read().unwrap();
        let trades = read.trades().map(|t| serde_json::json!({"fact":t.fact, "economic_applied":t.economic_applied, "success_applied":t.success_applied, "actual_net_fee_pusd":t.actual_net_fee_pusd})).collect::<Vec<_>>();
        std::fs::write(
            path.join("trade-facts.json"),
            serde_json::to_vec_pretty(&trades)?,
        )?;
        reports.push(report.clone());
        if config.progress_every > 0 && (i + 1) % config.progress_every == 0 {
            eprintln!(
                "合成环境 {}/{}，已耗时 {:.3}s",
                i + 1,
                config.iterations,
                started.elapsed().as_secs_f64()
            );
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    Ok(
        serde_json::json!({"fixture":"finite-public-environment-v1", "effective_config":config, "elapsed_seconds":elapsed, "public_applied_inputs":updates, "runs_per_second":config.iterations as f64/elapsed, "output_bytes_before_summary":bytes(root), "reports":reports, "gpu":"N/A: no model or GPU workload", "cpu_gpu_transfer_bytes":0}),
    )
}
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 3 {
        return Err("用法：finite_environment_fixture <显式配置.json> <新输出目录>".into());
    }
    let c: Config = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let root = Path::new(&args[2]);
    std::fs::create_dir(root)?;
    std::fs::write(
        root.join("effective-config.json"),
        serde_json::to_vec_pretty(&c)?,
    )?;
    let mut summary = run_fixture(root, &c)?;
    let mut file = std::fs::File::open(std::env::current_exe()?)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    summary["program_sha256"] = format!("{:x}", hash.finalize()).into();
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
    fn fixture_identity() -> ImplementationIdentityV1 {
        ImplementationIdentityV1 {
            git_commit: "b5dd31a0a4aab89486d251a07b84b582ed5a146f".into(),
            source_identity_sha256: Sha256Hex("b".repeat(64)),
            cargo_lock_sha256: Sha256Hex("c".repeat(64)),
        }
    }
    #[test]
    fn accurate_roles_stream_enrollment_and_finite_tail_preserve_real_economics() {
        for role in [
            SimCallerRoleV1::ModelTrain,
            SimCallerRoleV1::OfflineEvaluation,
            SimCallerRoleV1::HistoricalBacktest,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let c = Config {
                caller_role: role,
                implementation_identity: fixture_identity(),
                input_lifecycle_policy: InputLifecyclePolicyV1::FiniteInput,
                gamma_resolution_policy: GammaResolutionPolicyV1::QualifiedPublicConsumption,
                iterations: 1,
                progress_every: 1,
                public_delivery_delay_ms: 100,
                maker_trade: false,
                received_end_utc_ms: None,
                first_market_end_utc_ms: None,
                rebate_entitlement_queries: Vec::new(),
            };
            let result = run_fixture(temp.path(), &c).unwrap();
            let report = &result["reports"][0];
            // 冻结规则rate=0.07、exponent=1：第一市场10股×0.6=6，费用10×0.07×0.6×0.4=0.168。
            // 第二市场挂单另预留3；范围末可用现金=2000-6-0.168-3=1990.832。
            assert_eq!(
                report["account_economics_memory"]["cash"]["actual_available_pusd"],
                "1990.832"
            );
        }
    }
    #[test]
    fn conservative_maker_trade_reaches_noe_ledger_for_accurate_roles() {
        for role in [
            SimCallerRoleV1::ModelTrain,
            SimCallerRoleV1::OfflineEvaluation,
            SimCallerRoleV1::HistoricalBacktest,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let c = Config {
                caller_role: role,
                implementation_identity: fixture_identity(),
                input_lifecycle_policy: InputLifecyclePolicyV1::FiniteInput,
                gamma_resolution_policy: GammaResolutionPolicyV1::QualifiedPublicConsumption,
                iterations: 1,
                progress_every: 1,
                public_delivery_delay_ms: 100,
                maker_trade: true,
                received_end_utc_ms: None,
                first_market_end_utc_ms: None,
                rebate_entitlement_queries: Vec::new(),
            };
            let result = run_fixture(temp.path(), &c).unwrap();
            let facts: Vec<serde_json::Value> = serde_json::from_slice(
                &std::fs::read(temp.path().join("case-0/trade-facts.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(facts.len(), 1, "净减不造额外成交；规范成交只分配一笔");
            let t = &facts[0];
            assert_eq!(t["fact"]["liquidity_role"], "MakerInferred");
            // 初始外部排位100保留，SELL105只余5股给本账户，价0.4，本金2、Maker费0。
            assert_eq!(t["fact"]["base_quantity_shares"], "5");
            assert_eq!(t["fact"]["source_price_pusd_per_share"], "0.4");
            assert_eq!(t["fact"]["quote_amount_pusd"], "2");
            assert_eq!(t["fact"]["immediate_taker_fee_pusd"], "0");
            assert_eq!(
                t["fact"]["matched_at_utc_ms"],
                fixture_material::BASE + 4000
            );
            assert_eq!(t["economic_applied"], true);
            assert_eq!(t["success_applied"], true);
            let cash = &result["reports"][0]["account_economics_memory"]["cash"];
            let available: Decimal = cash["actual_available_pusd"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            let reserved: Decimal = cash["actual_reserved_pusd"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(available + reserved, Decimal::from(1998));
            assert!(reserved >= Decimal::from(3), "第二市场未闭义务不因EOF释放");
            assert!(
                result["reports"][0]["unclosed_commitments"]
                    .as_u64()
                    .unwrap()
                    > 0
            );
        }
    }
    #[test]
    fn rebate_entitlement_export_requires_real_day_frontier_and_preserves_cash() {
        use contracts::economics::RebateKindV1;
        for (end, market_end, known) in [
            (fixture_material::BASE + 12000, None, false),
            (fixture_material::BASE + 86_400_000, None, true),
            // 真实隐藏前沿已闭到日末，但市场end在D+1零点，尚未达到。
            (
                fixture_material::BASE + 86_400_000,
                Some(fixture_material::BASE + 86_400_000),
                false,
            ),
            // 只在明确新范围真正消费到市场end后闭合原市场屏障。
            (
                fixture_material::BASE + 86_400_001,
                Some(fixture_material::BASE + 86_400_000),
                true,
            ),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let c = Config {
                caller_role: SimCallerRoleV1::ModelTrain,
                implementation_identity: fixture_identity(),
                input_lifecycle_policy: InputLifecyclePolicyV1::FiniteInput,
                gamma_resolution_policy: GammaResolutionPolicyV1::QualifiedPublicConsumption,
                iterations: 1,
                progress_every: 1,
                public_delivery_delay_ms: 100,
                maker_trade: true,
                received_end_utc_ms: Some(end),
                first_market_end_utc_ms: market_end,
                rebate_entitlement_queries: vec![
                    RebateEntitlementQueryV1 {
                        kind: RebateKindV1::Maker,
                        attribution_day_start_utc_ms: fixture_material::BASE,
                    },
                    RebateEntitlementQueryV1 {
                        kind: RebateKindV1::Taker,
                        attribution_day_start_utc_ms: fixture_material::BASE,
                    },
                ],
            };
            let result = run_fixture(temp.path(), &c).unwrap();
            let report = &result["reports"][0];
            let read = &report["rebate_entitlements"][0]["result"];
            if known {
                // 5股 * .07 * .4 * .6 * .20 = .0168，低于原1pUSD门槛，不结转。
                assert_eq!(read["Known"]["day_result"]["accrued_pusd"], "0.0168");
                assert_eq!(read["Known"]["day_result"]["payable_pusd"], "0");
                assert_eq!(read["Known"]["day_result"]["locked_remainder_pusd"], "0");
                assert_eq!(read["Known"]["paid_pusd"], "0");
            } else {
                assert_eq!(read, "Unavailable");
            }
            // 无taker日桶不能确认为完整零；查询/日末不会造返佣现金。
            assert_eq!(report["rebate_entitlements"][1]["result"], "Unavailable");
            let cash = &report["account_economics_memory"]["cash"];
            let available: Decimal = cash["actual_available_pusd"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            let reserved: Decimal = cash["actual_reserved_pusd"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(available + reserved, Decimal::from(1998));
        }
    }
}
