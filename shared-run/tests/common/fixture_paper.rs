//! 同一正式PAPER装配的受控socket/合法规则夹具，供两组合入口复用。
use super::*;
pub(super) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
pub(super) fn context(id: &str) -> RequestContextV2 {
    RequestContextV2 {
        global_request_id: id.into(),
        caller_correlation_id: None,
    }
}
pub(super) fn setup(root: &Path) -> (PaperConfigV1, Arc<SealedProcessRuleSnapshotV3>) {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: fixture,
        expected_file_sha256: Sha256Hex(
            "d63c0e7b09edc4b2c4c3f0295f806b25f6f5fd03663cdf60ff062117e53e4e54".into(),
        ),
        replay_causal_start_utc_ms: i64::MAX,
    })
    .unwrap();
    let finality = root.join("finality.json");
    std::fs::write(&finality, SUCCESS_POLICY).unwrap();
    // 原采样器选定可短时覆盖的测试 seed；不替换生产时钟/延迟 profile。
    let latency = load_embedded_proxy_latency_profile().unwrap();
    // 固定新版完整32字节种子；保留原出站200..300ms及短响应的真实窗口。
    let seed = "28158a348a1d6c7c487c652331cc7457d85a359488a13aaf18e487127a33e45f".to_owned();
    let mut sampler = sim_system::latency_profile::SessionDelaySamplerV2::from_hex(&seed).unwrap();
    assert_eq!(
        sampler.sample(&latency, DelaySampleDomainV1::RequestOutbound),
        250
    );
    assert_eq!(
        sampler.sample(&latency, DelaySampleDomainV1::DirectResponseInbound),
        10
    );
    assert_eq!(
        sampler.sample(&latency, DelaySampleDomainV1::RequestOutbound),
        10
    );
    assert_eq!(
        sampler.sample(&latency, DelaySampleDomainV1::DirectResponseInbound),
        11
    );
    let config = PaperConfigV1 {
        runtime: PaperRuntimeConfigV1 {
            socket_path: root.join("hot.sock"),
            dpt_root: root.join("dpt"),
            sim_log: root.join("sim.jsonl"),
            noe_root: root.join("noe"),
            ledger_root: root.join("ledger"),
            public_online: PublicOnlineConfigV1 {
                source_progress: false,
                header: core().header().clone(),
                recording: config(),
                recording_in_flight_inputs: 1.try_into().unwrap(),
                stabilize_every_inputs: 1.try_into().unwrap(),
                read_page_rows: 1.try_into().unwrap(),
                max_read_batch_bytes: (1 << 20).try_into().unwrap(),
            },
            reference_market: ReferenceMarketSpecV1 {
                condition_id: ConditionId(CONDITION_ID.into()),
                clob_market_id: ClobMarketId("3195576".into()),
                slug: "btc-updown-5m-20".into(),
                outcomes: vec![
                    ReferenceOutcomeSpecV1 {
                        token_id: TokenId(UP_TOKEN_ID.into()),
                        outcome: "UP".into(),
                    },
                    ReferenceOutcomeSpecV1 {
                        token_id: TokenId(DOWN_TOKEN_ID.into()),
                        outcome: "DOWN".into(),
                    },
                ],
            },
            rule_snapshot_path: root.join("rules.json"),
            implementation_identity: ImplementationIdentityV1 {
                git_commit: "a".repeat(40),
                source_identity_sha256: Sha256Hex("b".repeat(64)),
                cargo_lock_sha256: Sha256Hex("c".repeat(64)),
            },
            finality_policy_path: finality,
            session: PaperSessionV1 {
                run_id: "paper-test".into(),
                session_id: "paper-session".into(),
                account_id: "paper-account".into(),
                caller_instance_id: "paper-host".into(),
                creation_request_id: "paper-create".into(),
                response_bridge_binding_id: "paper-bridge".into(),
                initial_account_state: InitialAccountStateV2 {
                    available_pusd: ExactDecimal("100".into()),
                },
                session_seed: Sha256Hex(seed),
            },
            activation_id: "paper-active".into(),
            consumption_sequence_ref: "paper-inputs".into(),
            noe_implementation_identity: "paper-test".into(),
            admission_policy: ledger::AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
            max_group_bytes: 2 << 20,
            finite_in_flight_groups: 1,
        },
        script: PaperScriptConfigV1::SingleBuyThenCancel {
            condition_id: ConditionId(CONDITION_ID.into()),
            order: PaperGtcLegV1 {
                token_id: TokenId(UP_TOKEN_ID.into()),
                limit_price_pusd_per_share: rust_decimal::Decimal::new(2, 1),
                size_shares: rust_decimal::Decimal::from(5),
                post_only: true,
            },
            submit_context: context("paper-buy"),
            cancel_context: context("paper-cancel"),
        },
    };
    (config, rules)
}
pub(super) async fn accept(listener: &UnixListener) -> UnixStream {
    let (mut stream, _) = listener.accept().await.unwrap();
    write_handoff_v2(
        &mut stream,
        &DataCollectHandoffV2::Session(DataCollectHandoffSessionV2 {
            frame_schema_version: DATA_COLLECT_HANDOFF_SCHEMA_V2.into(),
            environment: "paper".into(),
            run_id: "peer".into(),
            git_commit: "test".into(),
            binary_sha256: "test".into(),
            config_digest: "test".into(),
            collector_host_id: "test".into(),
            collector_region: "test".into(),
        }),
    )
    .await
    .unwrap();
    confirmed(&mut stream, 0).await;
    stream
}
pub(super) async fn confirmed(stream: &mut UnixStream, sequence: u64) {
    assert_eq!(
        read_handoff_v2(stream).await.unwrap(),
        DataCollectHandoffV2::Received {
            transport_sequence: sequence
        }
    );
}
pub(super) async fn send(stream: &mut UnixStream, sequence: u64, row: TypedRawRowV2) {
    write_handoff_v2(
        stream,
        &DataCollectHandoffV2::Message {
            transport_sequence: sequence,
            row,
        },
    )
    .await
    .unwrap();
    confirmed(stream, sequence).await;
}
pub(super) fn raw(input: RecordedIngressV1) -> TypedRawRowV2 {
    let DptIngressV1::Message { row, .. } = input.ingress else {
        panic!()
    };
    row
}
pub(super) fn metadata() -> TypedRawRowV2 {
    let mut row = raw(meta(0));
    let TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(m)) = &mut row else {
        panic!()
    };
    let at = now();
    let start = (at / 1000 - 60) * 1000;
    m.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    m.market_runtime_role = MetadataMarketRuntimeRole::Current;
    m.timestamp_received = at;
    m.collector_admission_time_utc_ms = at + 1;
    m.slug = format!("btc-updown-5m-{}", start / 1000);
    m.market_start_utc = start;
    m.market_end_utc = start + 300_000;
    m.fixed_identity_verified_at_utc = at - 1;
    m.source_evidence_received_at_utc = Some(at);
    m.row_evidence_validation_completed_at_utc = Some(at + 1);
    m.metadata_fetch_start_utc = Some(at - 10);
    row
}
pub(super) fn book_row(seq: u64) -> TypedRawRowV2 {
    let at = now();
    let mut row = raw(pm(
        0,
        seq,
        at,
        PolymarketOrderbookEvent::Book {
            asset_id: UP_TOKEN_ID.into(),
            bids: vec![PolymarketPriceLevel {
                price: "0.5".into(),
                size: "100".into(),
            }],
            asks: vec![PolymarketPriceLevel {
                price: "0.6".into(),
                size: "80".into(),
            }],
            last_trade_price: Some("0.55".into()),
        },
    ));
    let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)) = &mut row else {
        panic!()
    };
    r.timestamp_received = at;
    r.collector_admission_time_utc_ms = at + 1;
    r.slug = None;
    row
}
pub(super) fn inputs(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter_map(|line| match line["kind"].as_str() {
            Some("ACCEPTED_INPUTS") => Some(line["body"].as_array().unwrap().clone()),
            Some("ACCEPTED_INPUTS_V3") => Some(line["body"]["inputs"].as_array().unwrap().clone()),
            _ => None,
        })
        .flatten()
        .collect()
}
pub(super) fn requests(path: &Path) -> Vec<serde_json::Value> {
    let mut requests = vec![];
    // 正式读取接口读取原请求；只说明耐久输入，平台接受另核 NOE/Ledger。
    sim_system::execution::visit_sim_requests_v1(path, 2 << 20, |_, request, _| {
        requests.push(serde_json::to_value(request).unwrap());
        Ok(())
    })
    .unwrap();
    requests
}

pub(super) struct PaperHostEvidenceCase {
    pub(super) path: std::path::PathBuf,
    temporary: Option<tempfile::TempDir>,
}
impl PaperHostEvidenceCase {
    pub(super) fn new(name: &str) -> Self {
        if let Some(root) = std::env::var_os("PAPER_HOST_TEST_OUTPUT_ROOT") {
            let path = std::path::PathBuf::from(root).join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::create_dir(&path).unwrap();
            Self {
                path,
                temporary: None,
            }
        } else {
            let temporary = tempfile::tempdir().unwrap();
            Self {
                path: temporary.path().to_owned(),
                temporary: Some(temporary),
            }
        }
    }
}
impl Drop for PaperHostEvidenceCase {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Some(temporary) = self.temporary.take() {
                eprintln!("失败用例原始材料保留：{}", temporary.keep().display());
            }
        }
    }
}
