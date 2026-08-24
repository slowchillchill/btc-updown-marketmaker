use std::fs;
use std::time::Instant;

use contracts::execution_normalization::{
    DurableSourceReferenceV1, ImmutableSourceReferenceV1, NoeResultReferenceV1,
    SimV2SourceRecordReferenceV1, SimV2SourceReferenceV1, SimV2SourceWatermarkV1,
    SourceNeutralAccountBalanceDeltaV1, SourceNeutralAccountTransitionKindV1,
    SourceNeutralOrderLifecycleEventV1, SourceNeutralOrderLifecycleResultV1,
    SourceNeutralOutcomeBalanceDeltaV1, SourceNeutralTradeEconomicDispositionV1,
    SourceNeutralTradeLifecycleResultV1, SourceNeutralTradeStateV1, StableLogicalIdentityV1,
};
use contracts::{
    AccountInstanceKeyV1, ConditionId, ExactDecimal, PlatformSourceV1, Sha256Hex, SimCallerRoleV1,
    TokenId,
};
use execution_normalization::{
    DurableContextStoreV1, NormalizationBatchInputV1, SimV2ContextSeedV1,
    SourceNeutralEconomicEvidenceV1, SourceNeutralEvidenceKindV1, SourceNeutralEvidenceV1,
    SourceNeutralLogicalKeyV1,
};
use serde::Serialize;
use tempfile::TempDir;

use crate::{
    AccountAdmissionPolicyV1, LedgerLocalEventIdentityV1, LedgerRequestIdentityV1, LedgerWriterV1,
};
use crate::{LedgerSubmissionItemIdentityV1, core::LedgerHotIndexCountsV1};

const ACCOUNT_COUNT: usize = 4;
const OVERLAPPING_MARKET_COUNT: usize = 3;
const SETTLEMENT_CYCLES_PER_ACCOUNT: usize = 24;

struct BaselineNoeFixture {
    _temporary: TempDir,
    store: DurableContextStoreV1,
    run_id: String,
    opening_evidence_identity: String,
    next_sequence: u64,
}

impl BaselineNoeFixture {
    fn new(account_ordinal: usize) -> Self {
        let temporary = TempDir::new().unwrap();
        let run_id = format!("stage4-baseline-{account_ordinal}");
        let opening_evidence_identity = format!("opening-{account_ordinal}");
        let store = DurableContextStoreV1::open_or_create_sim_v2(
            temporary.path(),
            SimV2ContextSeedV1 {
                run_id: run_id.clone(),
                account_instance_key: AccountInstanceKeyV1 {
                    run_id: run_id.clone(),
                    caller_role: SimCallerRoleV1::ModelTrain,
                    caller_instance_id: format!("caller-{account_ordinal}"),
                    account_id: format!("account-{account_ordinal}"),
                    platform_source: PlatformSourceV1::Simulated,
                },
                run_opened: immutable_reference('1'),
                account_opening: immutable_reference('2'),
                account_opening_evidence_identity: opening_evidence_identity.clone(),
                implementation_identity: "ledger-stage4-resource-baseline".to_owned(),
            },
        )
        .unwrap();
        Self {
            _temporary: temporary,
            store,
            run_id,
            opening_evidence_identity,
            next_sequence: 1,
        }
    }

    fn opening(&mut self) -> NoeResultReferenceV1 {
        self.commit(SourceNeutralEconomicEvidenceV1::AccountOpening {
            opening_evidence_identity: self.opening_evidence_identity.clone(),
            available_pusd: exact("10000"),
        })
    }

    fn transition(
        &mut self,
        condition_id: &ConditionId,
        account_transition_seq: u64,
        transition: SourceNeutralAccountTransitionKindV1,
        balance_delta: SourceNeutralAccountBalanceDeltaV1,
    ) -> NoeResultReferenceV1 {
        self.commit(SourceNeutralEconomicEvidenceV1::AccountTransition {
            account_transition_seq,
            economic_effective_at_utc_ms: 10_000 + account_transition_seq as i64,
            condition_id: Some(condition_id.clone()),
            transition,
            balance_delta: Box::new(balance_delta),
        })
    }

    fn commit(&mut self, evidence: SourceNeutralEconomicEvidenceV1) -> NoeResultReferenceV1 {
        let sequence = self.next_sequence;
        let record = if sequence == 1 {
            SimV2SourceRecordReferenceV1::AccountOpening
        } else {
            SimV2SourceRecordReferenceV1::OrderLifecycle {
                private_feedback_sequence: sequence,
                platform_event_at_utc_ms: 20_000 + sequence as i64,
                feedback_received_at_utc_ms: 21_000 + sequence as i64,
            }
        };
        self.commit_kind(
            record,
            SourceNeutralLogicalKeyV1::EconomicEvidence {
                evidence_identity: format!("baseline-{sequence}"),
            },
            SourceNeutralEvidenceKindV1::Economic { evidence },
        )
    }

    fn trade(
        &mut self,
        condition_id: &ConditionId,
        token_id: &str,
        account_ordinal: usize,
        market_ordinal: usize,
        disposition: SourceNeutralTradeEconomicDispositionV1,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_sequence;
        let trade_identity = identity("trade", account_ordinal, market_ordinal);
        self.commit_kind(
            SimV2SourceRecordReferenceV1::TradeLifecycle {
                private_feedback_sequence: sequence,
                platform_event_at_utc_ms: 20_000 + sequence as i64,
                feedback_received_at_utc_ms: 21_000 + sequence as i64,
            },
            SourceNeutralLogicalKeyV1::TradeLifecycle {
                trade_identity: trade_identity.0.clone(),
                occurrence: sequence,
            },
            SourceNeutralEvidenceKindV1::TradeLifecycle {
                result: SourceNeutralTradeLifecycleResultV1 {
                    trade_identity,
                    order_identity: identity("order", account_ordinal, market_ordinal),
                    condition_id: condition_id.clone(),
                    token_id: TokenId(token_id.to_owned()),
                    state: SourceNeutralTradeStateV1::Matched,
                    economic_disposition: disposition,
                },
            },
        )
    }

    fn close_order(
        &mut self,
        condition_id: &ConditionId,
        token_id: &str,
        account_ordinal: usize,
        market_ordinal: usize,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_sequence;
        let order_identity = identity("order", account_ordinal, market_ordinal);
        self.commit_kind(
            SimV2SourceRecordReferenceV1::OrderLifecycle {
                private_feedback_sequence: sequence,
                platform_event_at_utc_ms: 20_000 + sequence as i64,
                feedback_received_at_utc_ms: 21_000 + sequence as i64,
            },
            SourceNeutralLogicalKeyV1::OrderLifecycle {
                order_identity: order_identity.0.clone(),
                occurrence: sequence,
            },
            SourceNeutralEvidenceKindV1::OrderLifecycle {
                result: SourceNeutralOrderLifecycleResultV1 {
                    order_identity,
                    condition_id: condition_id.clone(),
                    token_id: TokenId(token_id.to_owned()),
                    event: SourceNeutralOrderLifecycleEventV1::FakUnspentReleased,
                },
            },
        )
    }

    fn commit_kind(
        &mut self,
        record: SimV2SourceRecordReferenceV1,
        logical_key: SourceNeutralLogicalKeyV1,
        kind: SourceNeutralEvidenceKindV1,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_sequence;
        let watermark = SimV2SourceWatermarkV1 {
            run_id: self.run_id.clone(),
            projection_batch_sequence: sequence,
        };
        self.store
            .commit_batch(
                NormalizationBatchInputV1 {
                    source_watermark: watermark.clone(),
                    evidence: vec![SourceNeutralEvidenceV1 {
                        source_reference: SimV2SourceReferenceV1 {
                            context_reference: self.store.context().context_reference.clone(),
                            watermark,
                            record,
                        },
                        source_ordinal: 0,
                        logical_key,
                        kind,
                    }],
                },
                30_000 + sequence as i64,
            )
            .unwrap();
        self.next_sequence += 1;
        self.store
            .commits()
            .last()
            .unwrap()
            .result_reference(0)
            .unwrap()
    }
}

#[derive(Serialize)]
struct BaselineOutputV1 {
    git_head: String,
    hardware: HardwareV1,
    build_mode: &'static str,
    account_count: usize,
    overlapping_market_count: usize,
    settlement_cycles_per_account: usize,
    noe_input_count: usize,
    local_tail_input_count: usize,
    append_elapsed_ns: u64,
    append_throughput_per_second: f64,
    append_latency_p50_ns: u64,
    append_latency_p95_ns: u64,
    full_replay_elapsed_ns: u64,
    sealed_replay_elapsed_ns: u64,
    active_tail_scan_apply_elapsed_ns: u64,
    rss_kib: Option<u64>,
    hwm_kib: Option<u64>,
    hot_containers: HotContainersV1,
}

#[derive(Serialize)]
struct HardwareV1 {
    architecture: &'static str,
    logical_parallelism: usize,
    cpu_model: Option<String>,
}

#[derive(Serialize)]
struct HotContainersV1 {
    account: AccountHotContainersV1,
    reservations: ReservationHotContainersV1,
    lifecycle: LifecycleHotContainersV1,
    applied_noe_references: usize,
    durable_history: DurableHistoryV1,
    active_tail: ActiveTailV1,
}

#[derive(Serialize)]
struct AccountHotContainersV1 {
    markets: usize,
    global_outcome_balances: usize,
    per_market_outcome_balances: usize,
}

#[derive(Serialize)]
struct ReservationHotContainersV1 {
    requests: usize,
    applied_local_events: usize,
    markets: usize,
}

#[derive(Serialize)]
struct LifecycleHotContainersV1 {
    submission_requests: usize,
    query_requests: usize,
    orders: usize,
    trades: usize,
    fills: usize,
    global_outcome_costs: usize,
    markets: usize,
    per_market_outcome_costs: usize,
}

#[derive(Serialize)]
struct DurableHistoryV1 {
    sealed_segment_count: usize,
    sealed_record_count: usize,
    sealed_segment_bytes: u64,
}

#[derive(Serialize)]
struct ActiveTailV1 {
    record_count: usize,
    bytes: u64,
}

#[test]
#[ignore = "阶段 4 资源与重放原始基线按需显式运行"]
fn durable_replay_resource_baseline() {
    let durable_roots = (0..ACCOUNT_COUNT)
        .map(|_| TempDir::new().unwrap())
        .collect::<Vec<_>>();
    let markets = (0..SETTLEMENT_CYCLES_PER_ACCOUNT)
        .map(|ordinal| ConditionId(format!("shared-market-cycle-{ordinal}")))
        .collect::<Vec<_>>();
    let current_markets = (0..OVERLAPPING_MARKET_COUNT)
        .map(|ordinal| ConditionId(format!("current-market-{ordinal}")))
        .collect::<Vec<_>>();
    let mut latencies_ns = Vec::new();
    let append_started = Instant::now();
    let mut sealed_segment_bytes = 0_u64;

    for (account_ordinal, durable_root) in durable_roots.iter().enumerate() {
        let mut fixture = BaselineNoeFixture::new(account_ordinal);
        let mut writer = LedgerWriterV1::create_durable(
            AccountAdmissionPolicyV1 {
                per_market_all_in_commitment_limit_pusd: exact("5000"),
            },
            durable_root.path(),
        )
        .unwrap();
        apply_timed(&mut writer, &fixture.opening(), &mut latencies_ns);
        let mut account_transition_seq = 1_u64;
        for wave in markets.chunks(OVERLAPPING_MARKET_COUNT) {
            for condition_id in wave {
                let market_ordinal = market_ordinal(condition_id);
                apply_timed(
                    &mut writer,
                    &fixture.transition(
                        condition_id,
                        account_transition_seq,
                        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
                            order_identity: identity("order", account_ordinal, market_ordinal),
                        },
                        delta("0", "-10", "10", None),
                    ),
                    &mut latencies_ns,
                );
                account_transition_seq += 1;
            }
            for condition_id in wave {
                let market_ordinal = market_ordinal(condition_id);
                let token_id = format!("token-cycle-{market_ordinal}");
                apply_timed(
                    &mut writer,
                    &fixture.trade(
                        condition_id,
                        &token_id,
                        account_ordinal,
                        market_ordinal,
                        SourceNeutralTradeEconomicDispositionV1::PendingApplied,
                    ),
                    &mut latencies_ns,
                );
                apply_timed(
                    &mut writer,
                    &fixture.transition(
                        condition_id,
                        account_transition_seq,
                        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
                            trade_identity: identity("trade", account_ordinal, market_ordinal),
                            path_ordinal: 0,
                        },
                        delta("-10", "0", "-10", Some((&token_id, "20"))),
                    ),
                    &mut latencies_ns,
                );
                account_transition_seq += 1;
                apply_timed(
                    &mut writer,
                    &fixture.trade(
                        condition_id,
                        &token_id,
                        account_ordinal,
                        market_ordinal,
                        SourceNeutralTradeEconomicDispositionV1::FinalSuccess,
                    ),
                    &mut latencies_ns,
                );
                apply_timed(
                    &mut writer,
                    &fixture.close_order(condition_id, &token_id, account_ordinal, market_ordinal),
                    &mut latencies_ns,
                );
                apply_timed(
                    &mut writer,
                    &fixture.transition(
                        condition_id,
                        account_transition_seq,
                        SourceNeutralAccountTransitionKindV1::AutoRedeemApplied {
                            resolution_identity: identity(
                                "resolution",
                                account_ordinal,
                                market_ordinal,
                            ),
                        },
                        delta("12", "12", "0", Some((&token_id, "-20"))),
                    ),
                    &mut latencies_ns,
                );
                account_transition_seq += 1;
            }
        }
        let sealed = writer.seal_active_segment().unwrap().unwrap();
        sealed_segment_bytes += fs::metadata(&sealed.location).unwrap().len();
    }
    let append_elapsed = append_started.elapsed();

    let full_replay_started = Instant::now();
    let mut writers = durable_roots
        .iter()
        .map(|root| LedgerWriterV1::open_durable(root.path()).unwrap())
        .collect::<Vec<_>>();
    let full_replay_elapsed = full_replay_started.elapsed();

    let mut local_tail_input_count = 0_usize;
    for (account_ordinal, writer) in writers.iter_mut().enumerate() {
        for (market_ordinal, condition_id) in current_markets.iter().enumerate() {
            let request =
                LedgerRequestIdentityV1(format!("tail-request-{account_ordinal}-{market_ordinal}"));
            writer
                .try_reserve_intent(&request, condition_id, &exact("1"), &exact("0.01"))
                .unwrap();
            writer
                .record_submission_started(
                    &LedgerLocalEventIdentityV1(format!(
                        "tail-submission-{account_ordinal}-{market_ordinal}"
                    )),
                    &request,
                    &LedgerSubmissionItemIdentityV1 {
                        global_request_id: format!(
                            "tail-submission-request-{account_ordinal}-{market_ordinal}"
                        ),
                        batch_item_ordinal: 0,
                    },
                )
                .unwrap();
            writer
                .record_order_query_association(
                    &LedgerLocalEventIdentityV1(format!(
                        "tail-query-{account_ordinal}-{market_ordinal}"
                    )),
                    &format!("tail-query-request-{account_ordinal}-{market_ordinal}"),
                    &request,
                )
                .unwrap();
            local_tail_input_count += 3;
        }
    }
    drop(writers);
    let active_tail_bytes = durable_roots
        .iter()
        .map(|root| {
            fs::metadata(root.path().join("ledger-active-v1.jsonl"))
                .unwrap()
                .len()
        })
        .sum();

    let mut sealed_replay_elapsed = std::time::Duration::ZERO;
    let mut active_tail_scan_apply_elapsed = std::time::Duration::ZERO;
    let recovered = durable_roots
        .iter()
        .map(|root| {
            let (writer, durations) = LedgerWriterV1::open_durable_measured(root.path()).unwrap();
            sealed_replay_elapsed += durations.sealed_replay;
            active_tail_scan_apply_elapsed += durations.active_replay;
            writer
        })
        .collect::<Vec<_>>();
    let mut hot = LedgerHotIndexCountsV1::default();
    for writer in &recovered {
        hot.add_assign(writer.baseline_hot_index_counts());
    }
    let noe_input_count = latencies_ns.len();
    latencies_ns.sort_unstable();
    let (rss_kib, hwm_kib) = process_memory_kib();
    let output = BaselineOutputV1 {
        git_head: std::env::var("LEDGER_BASELINE_GIT_HEAD").unwrap_or_else(|_| "UNSET".to_owned()),
        hardware: HardwareV1 {
            architecture: std::env::consts::ARCH,
            logical_parallelism: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(0),
            cpu_model: cpu_model(),
        },
        build_mode: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        account_count: ACCOUNT_COUNT,
        overlapping_market_count: OVERLAPPING_MARKET_COUNT,
        settlement_cycles_per_account: SETTLEMENT_CYCLES_PER_ACCOUNT,
        noe_input_count,
        local_tail_input_count,
        append_elapsed_ns: nanos(append_elapsed),
        append_throughput_per_second: noe_input_count as f64 / append_elapsed.as_secs_f64(),
        append_latency_p50_ns: percentile(&latencies_ns, 50),
        append_latency_p95_ns: percentile(&latencies_ns, 95),
        full_replay_elapsed_ns: nanos(full_replay_elapsed),
        sealed_replay_elapsed_ns: nanos(sealed_replay_elapsed),
        active_tail_scan_apply_elapsed_ns: nanos(active_tail_scan_apply_elapsed),
        rss_kib,
        hwm_kib,
        hot_containers: HotContainersV1 {
            account: AccountHotContainersV1 {
                markets: hot.account_markets,
                global_outcome_balances: hot.account_global_outcome_balances,
                per_market_outcome_balances: hot.account_market_outcome_balances,
            },
            reservations: ReservationHotContainersV1 {
                requests: hot.reservation_requests,
                applied_local_events: hot.reservation_applied_local_events,
                markets: hot.reservation_markets,
            },
            lifecycle: LifecycleHotContainersV1 {
                submission_requests: hot.lifecycle_submission_requests,
                query_requests: hot.lifecycle_query_requests,
                orders: hot.lifecycle_orders,
                trades: hot.lifecycle_trades,
                fills: hot.lifecycle_fills,
                global_outcome_costs: hot.lifecycle_global_outcome_costs,
                markets: hot.lifecycle_markets,
                per_market_outcome_costs: hot.lifecycle_market_outcome_costs,
            },
            applied_noe_references: hot.applied_noe_references,
            durable_history: DurableHistoryV1 {
                sealed_segment_count: ACCOUNT_COUNT,
                sealed_record_count: ACCOUNT_COUNT + noe_input_count,
                sealed_segment_bytes,
            },
            active_tail: ActiveTailV1 {
                record_count: local_tail_input_count,
                bytes: active_tail_bytes,
            },
        },
    };
    println!("{}", serde_json::to_string(&output).unwrap());
}

fn apply_timed(
    writer: &mut LedgerWriterV1,
    reference: &NoeResultReferenceV1,
    latencies_ns: &mut Vec<u64>,
) {
    let started = Instant::now();
    writer.apply_noe_result(reference).unwrap();
    latencies_ns.push(nanos(started.elapsed()));
}

fn immutable_reference(value: char) -> DurableSourceReferenceV1 {
    DurableSourceReferenceV1 {
        source: ImmutableSourceReferenceV1::Sha256(Sha256Hex(value.to_string().repeat(64))),
    }
}

fn identity(kind: &str, account: usize, cycle: usize) -> StableLogicalIdentityV1 {
    StableLogicalIdentityV1(format!("{kind}-{account}-{cycle}"))
}

fn exact(value: &str) -> ExactDecimal {
    ExactDecimal(value.to_owned())
}

fn delta(
    cash: &str,
    available: &str,
    principal: &str,
    outcome: Option<(&str, &str)>,
) -> SourceNeutralAccountBalanceDeltaV1 {
    SourceNeutralAccountBalanceDeltaV1 {
        cash_pusd_delta: exact(cash),
        available_pusd_delta: exact(available),
        principal_encumbrance_pusd_delta: exact(principal),
        immediate_taker_fee_encumbrance_pusd_delta: exact("0"),
        outcome_balance_deltas: outcome
            .into_iter()
            .map(|(token_id, balance)| SourceNeutralOutcomeBalanceDeltaV1 {
                token_id: TokenId(token_id.to_owned()),
                balance_shares_delta: exact(balance),
            })
            .collect(),
        maker_rebate_receivable_pusd_delta: exact("0"),
        taker_rebate_receivable_pusd_delta: exact("0"),
        maker_rebate_locked_remainder_pusd_delta: exact("0"),
        taker_rebate_locked_remainder_pusd_delta: exact("0"),
    }
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    values[(values.len() - 1) * percentile / 100]
}

fn nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn process_memory_kib() -> (Option<u64>, Option<u64>) {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    (
        status_value_kib(&status, "VmRSS:"),
        status_value_kib(&status, "VmHWM:"),
    )
}

fn status_value_kib(status: &str, key: &str) -> Option<u64> {
    status
        .lines()
        .find(|line| line.starts_with(key))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
}

fn cpu_model() -> Option<String> {
    fs::read_to_string("/proc/cpuinfo")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("model name\t: ").map(str::to_owned))
}

fn market_ordinal(condition_id: &ConditionId) -> usize {
    condition_id.0.rsplit('-').next().unwrap().parse().unwrap()
}
