use super::*;
use crate::prefix::*;
use data_pipeline_core::{public_input::*, typed_raw::*};
use data_pipeline_training::public_recording::*;
use num_bigint::BigUint;
fn segment_for(interval: &QuantityInterval, n: &BigUint) -> usize {
    (0..16)
        .find(|&j| {
            crate::quantity_segment(interval, j).is_some_and(|(lo, hi)| &lo <= n && n <= &hi)
        })
        .unwrap()
}
fn selected(family: Family, variant: Variant, n: &str) -> PrefixState {
    PrefixState {
        family: Some(family),
        single_leg: Some(Leg::B),
        quantity_hundredths: Some(n.parse().unwrap()),
        legs: std::array::from_fn(|i| LegSelection {
            variant: Some(variant),
            post_only: Some(true),
            lifetime: Some(Lifetime::Seconds300),
            price_index: Some(BigUint::from(if i == 0 { 5u32 } else { 10u32 })),
        }),
        ..Default::default()
    }
}
fn fixture() -> Harness {
    fixture_cash("2000")
}
fn fixture_cash(cash: &str) -> Harness {
    fixture_config(cash, ["0.01", "0.01"])
}
fn fixture_config(cash: &str, ticks: [&str; 2]) -> Harness {
    fixture_config_at(cash, ticks, 24_999)
}
fn fixture_config_at(cash: &str, ticks: [&str; 2], intermediate_at: i64) -> Harness {
    fixture_config_times(cash, ticks, intermediate_at, &[35_000, 45_000, 55_000])
}
fn fixture_config_times(
    cash: &str,
    ticks: [&str; 2],
    intermediate_at: i64,
    followups: &[i64],
) -> Harness {
    fixture_config_depths(cash, ticks, intermediate_at, followups, ["100", "100"])
}
fn fixture_config_depths(
    cash: &str,
    ticks: [&str; 2],
    intermediate_at: i64,
    followups: &[i64],
    asks: [&str; 2],
) -> Harness {
    fixture_with_minimum(cash, ticks, intermediate_at, followups, asks, "5")
}
fn fixture_with_minimum(
    cash: &str,
    ticks: [&str; 2],
    intermediate_at: i64,
    followups: &[i64],
    asks: [&str; 2],
    minimum_shares: &str,
) -> Harness {
    fixture_with_midpoints(
        cash,
        ticks,
        intermediate_at,
        followups,
        asks,
        minimum_shares,
        &[],
    )
}
fn fixture_with_midpoints(
    cash: &str,
    ticks: [&str; 2],
    intermediate_at: i64,
    followups: &[i64],
    asks: [&str; 2],
    minimum_shares: &str,
    midpoints: &[(i64, &str, &str)],
) -> Harness {
    fixture_with_execution(cash,ticks,intermediate_at,followups,asks,minimum_shares,midpoints,None).0
}

fn fixture_with_execution(
    cash:&str,ticks:[&str;2],intermediate_at:i64,followups:&[i64],asks:[&str;2],
    minimum_shares:&str,midpoints:&[(i64,&str,&str)],configured:Option<(&[u8],String)>,
) -> (Harness,Option<crate::public_input_tests::nav_gpu::Materials>,MemoryNoeObservationV4) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let mut rows = Vec::new();
    let mut times = Vec::new();
    for (at, active) in [(10_000, false), (20_000, true)] {
        let DptIngressV1::Message {
            row: TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(mut m)),
            ..
        } = meta(0).ingress
        else {
            unreachable!()
        };
        m.market_end_utc = 320_000;
        m.timestamp_received = at;
        m.collector_admission_sequence = rows.len() as u64 + 1;
        m.collector_admission_time_utc_ms = at + 1;
        // 同一固定身份的两次真实输入使用不同Gamma源顺序；DPT负责投影，夹具不重排原字段。
        let source_tokens = if active {
            [UP_TOKEN_ID, DOWN_TOKEN_ID]
        } else {
            [DOWN_TOKEN_ID, UP_TOKEN_ID]
        };
        m.outcomes_raw = Some(
            if active {
                r#"["UP","DOWN"]"#
            } else {
                r#"["DOWN","UP"]"#
            }
            .into(),
        );
        m.outcome_prices_raw = Some(
            if active {
                r#"["0.6","0.4"]"#
            } else {
                r#"["0.4","0.6"]"#
            }
            .into(),
        );
        let initial_tick = if ticks == ["0.001", "0.001"] {
            "0.001"
        } else {
            "0.01"
        };
        let payload = serde_json::json!({"clobTokenIds":serde_json::to_string(&source_tokens).unwrap(), "orderPriceMinTickSize":initial_tick}).to_string();
        use sha2::{Digest, Sha256};
        m.source_evidence_raw_sha256 = Some(format!("{:x}", Sha256::digest(payload.as_bytes())));
        m.source_evidence_payload_json = Some(payload);
        m.metadata_fetch_start_utc = Some(at - 10);
        m.source_evidence_received_at_utc = Some(at);
        m.row_evidence_validation_completed_at_utc = Some(at + 1);
        if active {
            m.market_lifecycle_state = MetadataMarketLifecycleState::Active;
            m.market_runtime_role = MetadataMarketRuntimeRole::Current;
        }
        rows.push(TypedRawRowV2::Existing(
            TypedRawRow::PolymarketMarketMetadata(m),
        ));
        times.push(at);
    }
    let mut events = Vec::new();
    for (token, ask_size) in [UP_TOKEN_ID, DOWN_TOKEN_ID].into_iter().zip(asks) {
        events.push((
            21_000,
            22_000,
            PolymarketOrderbookEvent::Book {
                asset_id: token.into(),
                bids: vec![PolymarketPriceLevel {
                    price: "0.5".into(),
                    size: "100".into(),
                }],
                asks: vec![PolymarketPriceLevel {
                    price: "0.6".into(),
                    size: ask_size.into(),
                }],
                last_trade_price: None,
            },
        ));
    }
    for (at, visible, token, old, new) in [
        (23_000, 23_900, UP_TOKEN_ID, "0.01", ticks[0]),
        (23_001, 23_900, DOWN_TOKEN_ID, "0.01", ticks[1]),
        (
            24_000,
            intermediate_at,
            DOWN_TOKEN_ID,
            ticks[1],
            if ticks[1] == "0.01" {
                "0.001"
            } else {
                ticks[1]
            },
        ),
    ] {
        events.push((
            at,
            visible,
            PolymarketOrderbookEvent::TickSizeChange {
                asset_id: token.into(),
                old_tick_size: Some(old.into()),
                new_tick_size: new.into(),
            },
        ));
    }
    for &(at, bid, ask) in midpoints {
        events.push((
            at,
            at + 100,
            PolymarketOrderbookEvent::BestBidAsk {
                asset_id: UP_TOKEN_ID.into(),
                best_bid: Some(bid.into()),
                best_ask: Some(ask.into()),
                spread: Some("0.2".into()),
            },
        ));
    }
    for &at in followups {
        events.push((
            at,
            at + 1000,
            PolymarketOrderbookEvent::LastTradePrice {
                asset_id: UP_TOKEN_ID.into(),
                price: "0.6".into(),
                size: None,
                side: None,
                fee_rate_bps: None,
            },
        ));
    }
    events.sort_by_key(|(_, visible, _)| *visible);
    for (at, visible, event) in events {
        let p = rows.len() as u64;
        let DptIngressV1::Message {
            row: TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(mut r)),
            ..
        } = pm(p, p, at, event).ingress
        else {
            unreachable!()
        };
        r.timestamp_received = at + 1;
        r.collector_admission_time_utc_ms = at + 2;
        rows.push(TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)));
        times.push(visible);
    }
    let end_position=rows.len() as u64;
    for (i, row) in rows.into_iter().enumerate() {
        let input = record(i as u64, row);
        let prepared = prepare_public_ingress_v1(&core, &input, &Default::default(), &[]).unwrap();
        let applied = core.push(&prepared).unwrap().into_owned();
        writer.record(input, applied).unwrap();
    }
    if configured.is_some() {
        // 此有限来源确已结束；静默observe的peek必须读到原EndOfInput，而非未完稳定前缀。
        let input=RecordedIngressV1 {input_stream_id:core.header().input_stream_id.clone(),
            input_position:end_position,ingress:DptIngressV1::EndOfInput,dependency_record_refs:vec![]};
        let prepared=prepare_public_ingress_v1(&core,&input,&Default::default(),&[]).unwrap();
        let applied=core.push(&prepared).unwrap().into_owned();
        writer.record(input,applied).unwrap();
    }
    writer.stabilize().unwrap();
    let (mut execution, observation, rules) = match configured.as_ref() {
        Some((policy,seed))=>execution_with_rules_v4(&temp.path().join("sim.jsonl"),&core,policy,None,seed.clone(),cash),
        None=>execution_with_minimum_v4(&temp.path().join("sim.jsonl"),&core,cash,minimum_shares),
    };
    let mut ledger =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    let materials=if configured.is_some() || std::env::var_os("B_GPU_ACTOR_SELECTION_REPORT").is_some() {
        crate::public_input_tests::nav_gpu::bind(&mut execution,&mut ledger,&temp.path().join("ledger"))
    } else {
        execution.bind_ledger_recording_v2(&mut ledger,&temp.path().join("ledger")).unwrap();None
    };
    let noe_context = observation
        .opening_record()
        .unwrap()
        .context()
        .context_reference
        .clone();
    let retained_observation=observation.clone();
    let shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let account = shared
        .ledger()
        .projection_v2()
        .unwrap()
        .account_instance_key;
    let native = NativeInputRoot::new("buy".into(), account, -10_000, rules.clone());
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, TradingTimes(times)).unwrap();
    (Harness {
        _temp: temp,
        shared,
        native,
        driver: FinitePublicConsumptionV1::new(execution, source, "buy".into()),
        groups: vec![],
        noe_context,
        buy_receipts: vec![],
        cancel_receipts: vec![],
        materials: materials.clone(),
    },materials,retained_observation)
}

#[cfg(feature = "python")]
#[path = "projection_sensitivity_tests.rs"]
mod projection_sensitivity_tests;
fn ready(h: &mut Harness) -> NativeInputGroup {
    for _ in 0..5 {
        h.next().unwrap();
    }
    h.next().unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn accepted_only_zero_gap_keeps_original_actor_support() {
    use crate::public_input_tests::{nav_gpu,account_registry_sources_for_tests};
    use sim_system::latency_profile::{load_embedded_proxy_latency_profile,DelaySampleDomainV1};
    let policy=br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10000},{"state":"CONFIRMED","offset_ms":20000}],"economic_application_ordinal":1},"failure_overrides":[]}"#;
    let latency=load_embedded_proxy_latency_profile().unwrap();
    const CANDIDATE_LIMIT:u64=65_536;
    // 仅选择有限工程seed，仍由同一真实SIM的NOE证明时序；预测不作为支持集预期。
    // 当前首单无成交，后来单的初始成交预计占private4；下面的完整来源断言保持独立。
    let seed=(0..CANDIDATE_LIMIT).map(|value|format!("{value:064x}")).find(|seed| {
        let sample=|identity,domain|latency.sample_delay_ms(seed.as_bytes(),identity,domain);
        sample("4",DelaySampleDomainV1::LifecycleFeedbackInbound)>=1000
            && ["1","3"].iter().all(|identity|sample(identity,DelaySampleDomainV1::LifecycleFeedbackInbound)<100)
            && ["first-a","later-b"].iter().all(|identity|
                sample(identity,DelaySampleDomainV1::RequestOutbound)<100
                    && sample(identity,DelaySampleDomainV1::DirectResponseInbound)<100)
    }).expect("有限工程seed候选中未建立来源，不能签发Accepted-only能力");
    eprintln!("accepted_only engineering_seed={seed} candidate_limit={CANDIDATE_LIMIT} latency_profile={}",latency.file_sha256().0);
    let (mut h,materials,observation)=fixture_with_execution("2000",["0.01","0.01"],24_999,
        &[55_000],["100","100"],"5",&[],Some((policy,seed.clone())));
    let artifact=std::env::var_os("B_GPU_ACTOR_CANCEL_REFERENCE").map(|directory| {
        let path=std::path::Path::new(&directory).join("accepted-only");std::fs::create_dir(&path).unwrap();
        assert!(h._temp.path().starts_with("/root/autodl-tmp"));
        h._temp.disable_cleanup(true);
        std::fs::write(path.join("producer-path.txt"),h._temp.path().display().to_string()).unwrap();path
    });
    ready(&mut h);
    let mut receipts=Vec::new();
    for (request,token,price) in [("first-a",UP_TOKEN_ID,Decimal::new(1,2)),
                                ("later-b",DOWN_TOKEN_ID,Decimal::new(6,1))] {
        if request=="later-b" {
            loop {
                let group=h.next().unwrap();
                if group.action_before.as_ref().unwrap().cutoff().decision_time().logical_time_utc_ms()==56_000 {break;}
            }
        }
        let intent=Arc::new(ModelIntentV1::SubmitSingleBuy {
            condition_id:ConditionId(CONDITION_ID.into()),order:BuyLegIntentV1 {
                token_id:TokenId(token.into()),order:BuyOrderIntentV1::GtcLimit {
                    size_shares:Decimal::from(5),limit_price_pusd_per_share:price,post_only:false,
                },
            },
        });
        let receipt=h.shared.execute(&mut h.driver.decision().unwrap(),context(request),Some(intent.clone()))
            .await.unwrap().unwrap();
        receipts.push(serde_json::json!({"request":request,"receipt":{
            "input_sequence":receipt.input_sequence(),"platform_arrived_at_utc_ms":receipt.platform_arrived_at_utc_ms(),
            "scheduled_action_count":receipt.scheduled_action_count()},"intent":crate::support::gpu_intent_reference(&intent)}));
        if request=="later-b" {
            // 只处理真实到达，随后从实际已物化NOE读取时钟；不提前领取或注回引用。
            h.driver.observe_at(receipt.platform_arrived_at_utc_ms().checked_add(1).unwrap()).unwrap().unwrap();
        }
    }
    h.driver.stabilize_recording().unwrap();
    let result_references=observation.result_references();
    let raw=result_references.iter().map(|reference| {
        serde_json::to_value(::execution_normalization::read_noe_result_v4(reference).unwrap().record).unwrap()
    }).collect::<Vec<_>>();
    if let Some(path)=artifact.as_ref() {
        std::fs::write(path.join("actual-published-noe.json"),serde_json::to_vec_pretty(&serde_json::json!({
            "references":result_references,"records":raw,"receipts":receipts,
            "engineering_seed":seed,"candidate_limit":CANDIDATE_LIMIT,
            "latency_profile":latency.file_sha256()})).unwrap()).unwrap();
    }
    let accepted=raw.iter().find_map(|record| {
        let body=record.get("Evidence")?;
        let source=body["source_reference"]["record"].get("DirectResponse")?;
        (source["association"]["SubmissionItem"]["global_request_id"]=="later-b"
            && body["evidence"]["Control"]["result"].get("Submission").is_some())
            .then(||source["response_received_at_utc_ms"].as_i64().unwrap())
    }).expect("真实later-b Accepted已物化");
    let first_trade=raw.iter().filter_map(|record| {
        let body=record.get("Evidence")?;
        body["evidence"].as_object()?.keys().any(|name|name.starts_with("TradeLifecycle"))
            .then(||body["source_reference"]["record"]["TradeLifecycle"]["feedback_received_at_utc_ms"].as_i64().unwrap())
    }).min().expect("真实Matched成交已有规范来源");
    let cutoff=accepted.checked_add(100).unwrap().div_euclid(100).checked_mul(100).unwrap();
    eprintln!("accepted_only actual_received={accepted} first_trade_received={first_trade} cutoff={cutoff}");
    assert!(cutoff<=first_trade,"本次实际来源无Accepted-only的100ms间隙: Accepted={accepted}, Trade={first_trade}, cutoff={cutoff}");
    h.driver.observe_at(cutoff).unwrap().unwrap();
    h.shared.apply_closed().unwrap();
    let decision=h.driver.decision().unwrap();
    let at=decision.cutoff().clone();let frames=decision.frames();
    h.shared.observe_group(&frames);
    let before=h.shared.freeze_action_before(at,&frames).unwrap();
    let group=Arc::new(h.native.encode(&frames,before).unwrap());
    let read=group.action_before.as_ref().unwrap();
    assert!(h.shared.registry().orders().all(|order|h.shared.registry().order_trades(order.order_identity).next().is_none()));
    let matched=read.orders().find(|order|order.token_id==Some(&TokenId(DOWN_TOKEN_ID.into()))).unwrap();
    assert!(matched.accepted.is_some());
    assert_eq!(matched.matched_shares,ExecutionReadValueV1::Known(ExactDecimal("5".into())));
    assert!(read.commitments().any(|value|value.remaining_target_shares==ExecutionReadValueV1::Unavailable));
    assert!(read.economics().markets.iter().all(|market|market.repair_gap_shares.iter().all(|value|value.0=="0")));
    let actor=NativeActor::new(group.clone(),0).unwrap();
    let support=actor.support().unwrap();assert!(support[0]&&!support[1]&&support[2]);
    if let Some(artifact)=artifact {
        let compact=artifact.join("public-compact");nav_gpu::build_compact(&h._temp.path().join("dpt"),&compact);
        let checkpoint=nav_gpu::checkpoint(&h.shared,read,Some(&group),&materials);
        let report=serde_json::json!({"compact":compact,"checkpoint":checkpoint,
            "public_order_sources":account_registry_sources_for_tests(h.shared.registry()),"receipts":receipts,
            "accepted_received":accepted,"first_trade_received":first_trade,"cutoff":cutoff,
            "engineering_seed":seed,"candidate_limit":CANDIDATE_LIMIT,"latency_profile":latency.file_sha256(),
            "actor_context":actor.gpu_context_reference(),"actor_paths":gpu_actor_paths(group.clone(),&[0,1,2,3,4,5])});
        std::fs::write(artifact.join("reference.json"),serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
    h.driver.stop_and_drain().unwrap();
}

#[cfg(feature = "python")]
#[test]
#[ignore = "requires locked project Python; finite real joint probability and singleton AdamW validation"]
fn small_domain_joint_probability_entropy_and_singleton_optimizer() {
    use pyo3::prelude::*;
    // 真实现金约束自然产生三个数量与19个双腿GTC动作，不能用测试掩码裁剪生产支持。
    let mut h = fixture_cash("0.0006");
    let group = Arc::new(ready(&mut h));
    let actor = NativeActor::new(group.clone(), 0).unwrap();
    assert_eq!(
        actor.root_upper(Family::SubmitPairedBuy).unwrap(),
        3u32.into()
    );
    assert_eq!(
        actor.support().unwrap(),
        [true, false, true, false, false, true]
    );
    let report: String = Python::attach(|py| {
        let module = py.import("model_train.probability_validation").unwrap();
        let native = Py::new(py, crate::python::PythonGroup { group }).unwrap();
        let history = crate::python::value(
            py,
            serde_json::Value::Array(h.groups[..h.groups.len() - 1].to_vec()),
        )
        .unwrap();
        module
            .getattr("run")
            .unwrap()
            .call1((native, history))
            .unwrap()
            .extract()
            .unwrap()
    });
    if let Ok(path) = std::env::var("NATIVE_ACTION_PROBABILITY_OUTPUT") {
        std::fs::write(path, &report).unwrap();
    }
    eprintln!("{report}");
    h.driver.stop_and_drain().unwrap();
}
#[cfg(feature = "python")]
#[test]
#[ignore = "requires locked GPU Python; full device paths and original joint/AdamW reference"]
fn device_full_actor_path_matches_original_probability_and_optimizer() {
    use pyo3::prelude::*;
    let mut h = fixture_cash("0.0006");
    let group = Arc::new(ready(&mut h));
    let actor = NativeActor::new(group.clone(), 0).unwrap();
    assert_eq!(actor.root_upper(Family::SubmitPairedBuy).unwrap(), 3u32.into());
    let context = actor.gpu_context_reference();
    let report: String = Python::attach(|py| {
        let module = py.import("model_train.device_actor_validation").unwrap();
        let native = Py::new(py, crate::python::PythonGroup { group }).unwrap();
        let source = crate::python::value(py, serde_json::json!({"actor_context":context})).unwrap();
        let history = crate::python::value(py,
            serde_json::Value::Array(h.groups[..h.groups.len() - 1].to_vec())).unwrap();
        module.getattr("run").unwrap().call1((native, source, history)).unwrap().extract().unwrap()
    });
    let path = std::env::var("B_GPU_ACTOR_PATH_REPORT").unwrap();
    std::fs::write(path, &report).unwrap();
    eprintln!("{report}");
    h.driver.stop_and_drain().unwrap();
}

#[cfg(feature="python")]
pub(crate) fn validate_actor_selection(group:Arc<NativeInputGroup>,source:serde_json::Value,entry:&str,suffix:&str) {
    use pyo3::prelude::*;
    let base=std::path::PathBuf::from(std::env::var("B_GPU_ACTOR_SELECTION_REPORT").unwrap());
    let path=base.with_file_name(format!("{}-{suffix}.json",base.file_stem().unwrap().to_string_lossy()));
    std::fs::write(path.with_extension("source.json"),serde_json::to_vec_pretty(&source).unwrap()).unwrap();
    let report:String=Python::attach(|py|{
        let module=py.import("model_train.actor_selection_validation").unwrap();
        let native=Py::new(py,crate::python::PythonGroup {group}).unwrap();
        module.getattr(entry).unwrap().call1((native,crate::python::value(py,source).unwrap())).unwrap().extract().unwrap()
    });
    std::fs::write(&path,&report).unwrap();eprintln!("actor_selection_{suffix}={report}");
}

#[cfg(feature = "python")]
#[test]
#[ignore = "requires locked GPU Python; real Actor selection publication and public Ledger reservation"]
fn device_actor_selection_publishes_original_and_reserves_current_ledger() {
    use pyo3::prelude::*;
    let (mut h,materials,_)=fixture_with_execution("10",["0.01","0.01"],24_999,
        &[35_000,45_000,55_000],["100","100"],"5",&[],None);
    let group=Arc::new(ready(&mut h));
    let read=h.shared.ledger().economic_read().unwrap();
    assert_eq!(read.base().platform_source_prefix,0);
    assert_eq!(read.successful_trades().count(),0);
    let action=group.rows[0].action_context.as_ref().unwrap();
    let mut source=serde_json::json!({"actor_context":NativeActor::new(group.clone(),0).unwrap().gpu_context_reference(),
        "cutoff":group.cutoff.decision_time().logical_time_utc_ms(),
        "condition_id":group.rows[0].hidden_state_key.condition_id,
        "tokens":action.tokens,
        "checkpoint":{"projection":read.base(),"cash":read.cash(),"records":[],"successful_trades":[],
            "resolutions":[],"costs":[],"rebate_days":[],
            "residual":{"other_cash_pusd":read.residual_cash().other_cash_pusd,
                "directional_cash_pusd":read.residual_cash().directional_cash_pusd}}});
    let mut cash_steps=Vec::new();
    let mut cursor=materials.as_ref().unwrap().lock().unwrap().len();
    for (identity,amount,correction,at) in [("actor-debit","-9.99",None,34_000),
        ("actor-deposit","20",None,44_000),("actor-correction","9.99",Some("actor-debit"),54_000)] {
        h.driver.decision().unwrap().apply_external_cash_fact_v1(contracts::economics::ExternalCashFactV1 {
            identity:identity.into(),signed_delta_pusd:ExactDecimal(amount.into()),
            attribution:contracts::economics::ExternalCashAttributionV1::UnknownActivity,
            correction_of:correction.map(str::to_owned)}).unwrap();
        while h.driver.next_group_at().unwrap().is_some_and(|next|next<at) {
            h.driver.next_group().unwrap().unwrap();h.shared.apply_closed().unwrap();
        }
        h.driver.observe_at(at).unwrap().unwrap();h.shared.apply_closed().unwrap();
        let stored=materials.as_ref().unwrap().lock().unwrap();
        let rows=stored[cursor..].iter().filter_map(|material|material.consumed_record_v2().map(|row|
            serde_json::json!({"record":row.record(),"visibility":row.visibility(),"position":row.position()}))).collect::<Vec<_>>();
        cursor=stored.len();
        assert_eq!(rows.len(),2,"本有限阶段只有原ExternalCash Evidence/Economic");
        let read=h.shared.ledger().economic_read().unwrap();
        cash_steps.push(serde_json::json!({"rows":rows,"cash":read.cash(),"projection":read.base(),"cutoff":at}));
    }
    source["cash_steps"]=serde_json::json!(cash_steps);
    std::fs::write(std::path::Path::new(&std::env::var("B_GPU_ACTOR_SELECTION_REPORT").unwrap()).with_extension("source.json"),
        serde_json::to_vec_pretty(&source).unwrap()).unwrap();
    let report:String=Python::attach(|py| {
        let module=py.import("model_train.actor_selection_validation").unwrap();
        let native=Py::new(py,crate::python::PythonGroup {group}).unwrap();
        module.getattr("run").unwrap().call1((native,crate::python::value(py,source).unwrap())).unwrap().extract().unwrap()
    });
    std::fs::write(std::env::var("B_GPU_ACTOR_SELECTION_REPORT").unwrap(),&report).unwrap();
    eprintln!("{report}");
    h.driver.stop_and_drain().unwrap();
}
fn leg_values(order: &BuyOrderIntentV1) -> (Decimal, Decimal, Decimal) {
    match order {
        BuyOrderIntentV1::GtcLimit {
            limit_price_pusd_per_share: p,
            size_shares: q,
            ..
        }
        | BuyOrderIntentV1::GtdLimit {
            limit_price_pusd_per_share: p,
            size_shares: q,
            ..
        } => (*q, *p, *q * *p),
        BuyOrderIntentV1::FokMarket {
            spend_pusd: p,
            worst_price_pusd_per_share: price,
        }
        | BuyOrderIntentV1::FakMarket {
            spend_pusd: p,
            worst_price_pusd_per_share: price,
        } => (*p / *price, *price, *p),
    }
}
#[test]
fn four_variants_keep_big_integers_ticks_and_frozen_identity() {
    let mut h = fixture();
    let mut g = ready(&mut h);
    let mut observation = h.driver.frames()[0].observation.clone();
    observation.assets.reverse();
    g.rows[0].action_context = Some(NativeActionContext::from_observation(
        &observation,
        h.driver.frames()[0].rule_context.clone(),
        g.rows[0].action_context.as_ref().unwrap().snapshot.clone(),
    ));
    for variant in [
        Variant::GtcLimit,
        Variant::GtdLimit,
        Variant::FokMarket,
        Variant::FakMarket,
    ] {
        for n in ["9007199254740993", "9007199254740994"] {
            let mut state = selected(Family::SubmitPairedBuy, variant, n);
            state.legs[0].post_only = Some(false);
            if matches!(variant, Variant::FokMarket | Variant::FakMarket) {
                for leg in &mut state.legs {
                    leg.post_only = None;
                    leg.lifetime = None;
                }
            }
            let ModelIntentV1::SubmitPairedBuy { orders, .. } = g.buy_intent(0, &state).unwrap()
            else {
                panic!()
            };
            assert_eq!(
                [orders[0].token_id.0.as_str(), orders[1].token_id.0.as_str()],
                [UP_TOKEN_ID, DOWN_TOKEN_ID]
            );
            match &orders[0].order {
                BuyOrderIntentV1::GtcLimit { post_only, .. }
                | BuyOrderIntentV1::GtdLimit { post_only, .. } => assert!(!post_only),
                _ => {}
            }
            let q = if n.ends_with('3') {
                Decimal::from_str_exact("90071992547409.93").unwrap()
            } else {
                Decimal::from_str_exact("90071992547409.94").unwrap()
            };
            assert_eq!(
                leg_values(&orders[0].order),
                (q, Decimal::new(5, 2), q * Decimal::new(5, 2))
            );
            assert_eq!(
                leg_values(&orders[1].order),
                (q, Decimal::new(1, 1), q * Decimal::new(1, 1))
            );
        }
    }
    let mut state = selected(Family::SubmitSingleBuy, Variant::GtcLimit, "500");
    let original = g.buy_intent(0, &state).unwrap();
    let next = h.next().unwrap();
    let ModelIntentV1::SubmitSingleBuy { order, .. } = next.buy_intent(0, &state).unwrap() else {
        panic!()
    };
    assert_eq!(order.token_id.0, DOWN_TOKEN_ID);
    assert_eq!(leg_values(&order.order).1, Decimal::new(1, 2));
    assert_eq!(g.buy_intent(0, &state).unwrap(), original);
    state.quantity_hundredths = None;
    assert!(matches!(
        g.buy_intent(0, &state),
        Err(InputError::IncompleteBuyPrefix)
    ));
    h.driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn exact_buy_assembly_reaches_original_requests_and_receipts() {
    let mut h = fixture();
    let g = ready(&mut h);
    for (i, lifetime, seconds) in [
        (0, Lifetime::Seconds180, 264),
        (1, Lifetime::Seconds240, 324),
        (2, Lifetime::Seconds300, 384),
    ] {
        let mut state = selected(Family::SubmitPairedBuy, Variant::GtdLimit, "500");
        for leg in &mut state.legs {
            leg.lifetime = Some(lifetime);
        }
        if i == 2 {
            state.legs[0].variant = Some(Variant::GtcLimit);
        }
        let intent = g.buy_intent(0, &state).unwrap();
        if let ModelIntentV1::SubmitPairedBuy { orders, .. } = &intent {
            for o in orders {
                if let BuyOrderIntentV1::GtdLimit {
                    expires_at_utc_sec, ..
                } = o.order
                {
                    assert_eq!(expires_at_utc_sec, seconds);
                }
            }
        }
        g.execute_selected(
            0,
            SelectedAction::Buy(&state),
            &mut h.shared,
            &mut h.driver.decision().unwrap(),
            context(&format!("limit-{i}")),
            |_, _, _| -> Result<Vec<CancelScoreBlock>, ()> { panic!("已选参数不评分") },
        )
        .await
        .unwrap()
        .unwrap();
    }
    for (id, variant) in [("fok", Variant::FokMarket), ("fak", Variant::FakMarket)] {
        let mut state = selected(Family::SubmitSingleBuy, variant, "500");
        state.legs[1].price_index = Some(BigUint::from(60u32));
        g.execute_selected(
            0,
            SelectedAction::Buy(&state),
            &mut h.shared,
            &mut h.driver.decision().unwrap(),
            context(id),
            |_, _, _| -> Result<Vec<CancelScoreBlock>, ()> { panic!() },
        )
        .await
        .unwrap()
        .unwrap();
    }
    while let Some(group) = h.next() {
        if group.cutoff.decision_time().logical_time_utc_ms() == 56_000 {
            break;
        }
    }
    let orders = h.shared.registry().orders().collect::<Vec<_>>();
    assert_eq!(orders.len(), 8);
    for order in &orders {
        let origin = order.origin.as_ref().unwrap();
        match origin.intent.intent() {
            ModelIntentV1::SubmitPairedBuy { orders, .. } => assert_eq!(
                orders[origin.batch_item_ordinal as usize].token_id.0,
                if origin.batch_item_ordinal == 0 {
                    UP_TOKEN_ID
                } else {
                    DOWN_TOKEN_ID
                }
            ),
            ModelIntentV1::SubmitSingleBuy { order, .. } => {
                assert_eq!(order.token_id.0, DOWN_TOKEN_ID);
                assert_eq!(origin.batch_item_ordinal, 0);
            }
            _ => panic!(),
        };
        assert!(order.accepted.is_some());
        if matches!(
            origin.intent.intent(),
            ModelIntentV1::SubmitSingleBuy { .. }
        ) {
            assert!(
                matches!(&order.matched_shares, ExecutionReadValueV1::Known(q) if Decimal::from_str_exact(&q.0).unwrap() >= Decimal::from(5))
            );
        }
    }
    h.driver.stop_and_drain().unwrap();
    let log = std::fs::read_to_string(h._temp.path().join("sim.jsonl")).unwrap();
    for expiration in [264000, 324000, 384000] {
        assert!(log.contains(&format!("\"expires_at_utc_ms\":{expiration}")));
    }
}

#[test]
fn actor_complete_support_prices_and_shared_budget_match_independent_enumeration() {
    let mut h = fixture_cash("2.02");
    let g = ready(&mut h);
    let actor = NativeActor::new(Arc::new(g), 0).unwrap();
    let mut state = PrefixState {
        family: Some(Family::SubmitPairedBuy),
        ..Default::default()
    };
    // 未固定属性时，小数量有真实GTC续项；固定市价后每腿仍至少1 pUSD。
    for n in [1u32, 112, 113, 125, 499] {
        assert!(actor.fixed_completion(&n.into(), &state).unwrap());
    }
    for leg in &mut state.legs {
        leg.variant = Some(Variant::FokMarket);
    }
    // 两腿 0.01；独立枚举原本金，不使用完成性函数作 oracle。
    for n in 1u32..500 {
        let mut feasible = false;
        for ka in 1u32..100 {
            let a = n * ka * 100;
            if a < 1_000_000 {
                continue;
            }
            for kb in 1u32..100 {
                let b = n * kb * 100;
                if b >= 1_000_000 && a + b <= 2_020_000 {
                    feasible = true;
                    break;
                }
            }
            if feasible {
                break;
            }
        }
        assert_eq!(
            actor.fixed_completion(&n.into(), &state).unwrap(),
            feasible,
            "n={n}"
        );
    }
    state.quantity_hundredths = Some(500u32.into());
    state.legs[0].variant = Some(Variant::FokMarket);
    state.legs[1].variant = Some(Variant::GtcLimit);
    state.legs[1].post_only = Some(true);
    let mut actor = actor;
    actor.state = state;
    actor.state.stage = Stage::Price(Leg::A);
    let mask = actor.support().unwrap();
    for (at, &valid) in mask.iter().enumerate() {
        let k = at as u32 + 1;
        assert_eq!(
            valid,
            k < 100 && 500 * k * 100 >= 1_000_000 && 500 * k * 100 + 500 * 100 <= 2_020_000
        );
    }
    let fee = actor.price_fee_features().unwrap();
    let snapshot = &actor.group.rows[0]
        .action_context
        .as_ref()
        .unwrap()
        .snapshot;
    let expected = snapshot
        .market_buy_fee_estimate_v1(&500u32.into(), 20, &ExactDecimal("0.01".into()))
        .unwrap();
    assert_eq!(
        fee[0],
        crate::exact::signed_log_f32(&crate::exact::ratio(
            num_bigint::BigInt::from(expected.fee_1e5_units),
            100_000
        ))
    );
    assert_eq!(fee.len(), actor.price_indices().unwrap().len());
    actor.state.stage = Stage::Price(Leg::B);
    assert!(actor.price_fee_features().unwrap().iter().all(|x| *x == 0.));
}

#[tokio::test(flavor = "current_thread")]
async fn full_actor_retains_exact_pair_and_original_execution_reservations() {
    let mut h = fixture();
    let g = Arc::new(ready(&mut h));
    let mut actor = NativeActor::new(g.clone(), 0).unwrap();
    assert_eq!(actor.support().unwrap()[..3], [true, false, true]);
    actor.choose(2).unwrap();
    let n = BigUint::from(500u32);
    while actor.state.stage == Stage::QuantityBranch {
        let interval = actor.state.interval.as_ref().unwrap();
        let mid = interval;
        actor.choose(segment_for(mid, &n)).unwrap();
    }
    actor.choose(0).unwrap();
    assert_eq!(actor.state.stage, Stage::Variant(Leg::B));
    actor.choose(0).unwrap();
    assert_eq!(actor.state.stage, Stage::Price(Leg::A));
    actor.choose(4).unwrap();
    actor.choose(9).unwrap();
    let intent = actor.intent().unwrap().unwrap();
    let ModelIntentV1::SubmitPairedBuy { orders, .. } = &intent else {
        panic!()
    };
    assert_eq!(leg_values(&orders[0].order).2, Decimal::new(25, 2));
    assert_eq!(leg_values(&orders[1].order).2, Decimal::new(5, 1));
    let mut decision = h.driver.decision().unwrap();
    actor
        .execute(&mut h.shared, &mut decision, context("full-actor-pair"))
        .await
        .unwrap()
        .unwrap();
    let read = h.shared.ledger().economic_read().unwrap();
    assert_eq!(read.cash().actual_available_pusd.0, "1999.25");
    assert_eq!(
        g.action_before
            .as_ref()
            .unwrap()
            .economics()
            .cash
            .actual_available_pusd
            .0,
        "2000"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_market_repair_has_nonmonotone_quantity_holes_but_limit_does_not() {
    let mut h = fixture_config("2.038", ["0.01", "0.01"]);
    let _ = ready(&mut h);
    let receipt = h
        .shared
        .execute(
            &mut h.driver.decision().unwrap(),
            context("bootstrap-real-leg"),
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                order: BuyLegIntentV1 {
                    token_id: TokenId(UP_TOKEN_ID.into()),
                    order: BuyOrderIntentV1::FokMarket {
                        spend_pusd: Decimal::ONE,
                        worst_price_pusd_per_share: Decimal::new(9, 1),
                    },
                },
            })),
        )
        .await
        .unwrap();
    eprintln!(
        "bootstrap arrival={}",
        receipt.unwrap().platform_arrived_at_utc_ms()
    );
    let mut last = None;
    for _ in 0..4 {
        last = Some(h.next().unwrap());
    }
    let actor = NativeActor::new(Arc::new(last.unwrap()), 0).unwrap();
    h.driver.wait_for_recording_capacity().unwrap();
    assert_eq!(
        actor
            .group
            .action_before
            .as_ref()
            .unwrap()
            .economics()
            .cash
            .maker_available_pusd
            .0,
        "1.01"
    );
    let mut state = PrefixState {
        family: Some(Family::SubmitSingleBuy),
        single_leg: Some(Leg::B),
        ..Default::default()
    };
    for n in [1u32, 111, 112, 113, 125] {
        assert!(actor.fixed_completion(&n.into(), &state).unwrap());
    }
    state.legs[1].variant = Some(Variant::FokMarket);
    for n in [111u32, 112, 113, 125] {
        let want = (1..1000).any(|k| n * k * 10 >= 1_000_000 && n * k * 10 <= 1_010_000);
        assert_eq!(
            actor.fixed_completion(&n.into(), &state).unwrap(),
            want,
            "n={n}"
        );
    }
    assert!(actor.root_upper(Family::SubmitSingleBuy).unwrap() <= BigUint::from(166u32));
}

#[cfg(all(feature = "python", feature = "ort"))]
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires explicitly supplied exported graphs and matching Python wheel ORT library"]
async fn same_process_python_and_rust_ort_share_native_actor_and_final_intent() {
    for case in 0..3 {
        three_runtime_buy_case(case).await;
    }
}

#[cfg(all(feature = "python", feature = "ort"))]
async fn three_runtime_buy_case(case: u8) {
    use pyo3::prelude::*;
    let directory = std::env::var("ACTOR_GRAPH_DIRECTORY").unwrap();
    let library = std::env::var("ORT_DYLIB_PATH").unwrap();
    ort::init_from(&library).unwrap().commit();
    let mut h = match case {
        0 => fixture(),
        1 => fixture_with_minimum(
            "2000",
            ["0.01", "0.01"],
            24_999,
            &[35_000, 45_000, 55_000],
            ["100", "100"],
            "10",
        ),
        2 => fixture_config_times(
            "2000",
            ["0.01", "0.01"],
            24_999,
            &[202_999, 204_000, 205_000],
        ),
        _ => unreachable!(),
    };
    let history_native = (0..5)
        .map(|_| Arc::new(h.next().unwrap()))
        .collect::<Vec<_>>();
    let g = Arc::new(h.next().unwrap());
    let mut probe = NativeActor::new(g.clone(), 0).unwrap();
    probe.choose(2).unwrap();
    let quantity = if case == 1 { 1u32 } else { 500u32 };
    while probe.state.stage == Stage::QuantityBranch {
        let interval = probe.state.interval.as_ref().unwrap();
        let mid = interval;
        probe
            .choose(segment_for(mid, &BigUint::from(quantity)))
            .unwrap();
    }
    if case == 0 {
        probe.choose(1).unwrap();
        probe.choose(1).unwrap();
        probe.choose(79).unwrap();
        probe.choose(79).unwrap();
    } else {
        probe.choose(0).unwrap();
        probe.choose(0).unwrap();
        probe.choose(4).unwrap();
        probe.choose(9).unwrap();
    }
    let replay = probe.path.clone();
    type Step = (Vec<bool>, String, Vec<f32>, Vec<f32>);
    type Trace = (Vec<f32>, Vec<f32>);
    type PythonEvidence = (
        Vec<Trace>,
        Vec<usize>,
        Vec<Step>,
        Vec<Step>,
        Vec<Vec<f32>>,
        usize,
        usize,
        Py<PyAny>,
    );
    let (traces, path, actual_scores, scores, mixed_scores, layers, hidden, continuation): PythonEvidence =
        Python::attach(|py| {
            let module = py.import("model_train.actor_validation").unwrap();
            let group = Py::new(py, crate::python::PythonGroup { group: g.clone() }).unwrap();
            let history = crate::python::value(
                py,
                serde_json::Value::Array(h.groups[..h.groups.len() - 1].to_vec()),
            )
            .unwrap();
            let directory = py
                .import("pathlib")
                .unwrap()
                .getattr("Path")
                .unwrap()
                .call1((&directory,))
                .unwrap();
            module
                .getattr("validate_python")
                .unwrap()
                .call1((group, history, directory, replay.clone(), case == 0))
                .unwrap()
                .extract()
                .unwrap()
        });
    let mut state_graph = ort::session::Session::builder()
        .unwrap()
        .with_intra_threads(1)
        .unwrap()
        .with_inter_threads(1)
        .unwrap()
        .with_parallel_execution(false)
        .unwrap()
        .with_memory_pattern(false)
        .unwrap()
        .with_intra_op_spinning(false)
        .unwrap()
        .with_inter_op_spinning(false)
        .unwrap()
        .commit_from_file(std::path::Path::new(&directory).join("state_encoder.onnx"))
        .unwrap();
    let mut hidden_state = vec![0.; layers * hidden];
    let mut z = vec![];
    let mut frame = 0;
    for group in history_native.iter().chain(std::iter::once(&g)) {
        for row in &group.rows {
            assert_eq!(
                row.hidden_state_key.condition_id,
                g.rows[0].hidden_state_key.condition_id
            );
            let (depth, book, book_mask) = crate::pad_books([&row.book_levels]);
            let outputs=state_graph.run(ort::inputs![
                "fast_features"=>ort::value::Tensor::from_array(([1,596],row.fast.clone())).unwrap(),
                "slow_features"=>ort::value::Tensor::from_array(([1,433],row.slow.clone())).unwrap(),
                "book_levels" => ort::value::Tensor::from_array(([1,6,depth,9],book)).unwrap(),
                "book_mask" => ort::value::Tensor::from_array(([1,6,depth],book_mask)).unwrap(),
                "h_prev"=>ort::value::Tensor::from_array(([layers,1,hidden],hidden_state)).unwrap()
            ]).unwrap();
            z = outputs["z_t"]
                .try_extract_tensor::<f32>()
                .unwrap()
                .1
                .to_vec();
            hidden_state = outputs["h_next"]
                .try_extract_tensor::<f32>()
                .unwrap()
                .1
                .to_vec();
            for (actual, expected) in [(&z, &traces[frame].0), (&hidden_state, &traces[frame].1)] {
                assert_eq!(actual.len(), expected.len());
                for (&a, &b) in actual.iter().zip(expected) {
                    assert!(finite_close(a, b), "frame={frame}: {a} != {b}");
                }
            }
            frame += 1;
        }
    }
    assert_eq!(frame, traces.len());
    let mut scorer =
        crate::ort_actor::OrtActorScorer::new(std::path::Path::new(&directory), hidden, 1).unwrap();
    let mut actor = NativeActor::new(g.clone(), 0).unwrap();
    scorer.greedy(&mut actor, &z).unwrap();
    assert_eq!(actor.path, path);
    let frozen_hidden = hidden_state.clone();
    for (selected, records) in [(&path, &actual_scores), (&replay, &scores)] {
        let mut reference = NativeActor::new(g.clone(), 0).unwrap();
        let reduced = scorer.reduced(&reference, &z).unwrap();
        assert_eq!(selected.len(), records.len());
        for (&chosen, (support, state, prefix, want)) in selected.iter().zip(records) {
            assert_eq!(&reference.support().unwrap(), support);
            assert_eq!(&format!("{:?}", reference.state), state);
            assert_eq!(reference.prefix_features().as_slice(), prefix);
            let got = scorer.scores(&reference, &z, &reduced).unwrap();
            assert_eq!(got.len(), want.len());
            for (&a, &b) in got.iter().zip(want) {
                assert!(finite_close(a, b), "{a} != {b}");
            }
            reference.choose(chosen).unwrap();
        }
        assert_eq!(
            reference.intent().unwrap(),
            if selected == &path {
                actor.intent().unwrap()
            } else {
                probe.intent().unwrap()
            }
        );
    }
    // 不同阶段/合法区间在同一活动批次分组，原坐标稳定回填。
    let mut mixture = NativeActor::new(g.clone(), 0).unwrap();
    let mut actors = Vec::new();
    for &choice in &replay {
        if actors.is_empty() || mixture.head_index() == 3 {
            let mut copy = NativeActor::new(g.clone(), 0).unwrap();
            for &picked in &mixture.path {
                copy.choose(picked).unwrap();
            }
            actors.push(copy);
        }
        mixture.choose(choice).unwrap();
    }
    let prepared = scorer.reduced(&actors[0], &z).unwrap();
    let entries = actors
        .iter()
        .map(|a| (a, z.as_slice(), &prepared))
        .collect::<Vec<_>>();
    let actual = scorer.scores_batch(&entries).unwrap();
    for (values, expected) in actual.iter().zip(&mixed_scores) {
        assert_eq!(values.len(), expected.len());
        for (&a, &b) in values.iter().zip(expected) {
            assert!(finite_close(a, b));
        }
    }
    assert_eq!(hidden_state, frozen_hidden);
    // 原市场案例执行实际贪心；新增反例执行已逐前缀比对的低量/GTD原意图。
    let executing = if case == 0 { &actor } else { &probe };
    let intent = executing.intent().unwrap();
    let receipt = executing
        .execute(
            &mut h.shared,
            &mut h.driver.decision().unwrap(),
            context("three-runtime-actual-intent"),
        )
        .await
        .unwrap();
    assert_eq!(receipt.is_some(), intent.is_some());
    if case == 1 {
        let known = selected(Family::SubmitPairedBuy, Variant::GtcLimit, "1000");
        g.execute_selected(
            0,
            SelectedAction::Buy(&known),
            &mut h.shared,
            &mut h.driver.decision().unwrap(),
            context("three-runtime-known-before-unknown"),
            |_, _, _| -> Result<Vec<CancelScoreBlock>, ()> { panic!() },
        )
        .await
        .unwrap()
        .unwrap();
        h.shared
            .execute(
                &mut h.driver.decision().unwrap(),
                context("three-runtime-unknown-cancel"),
                Some(Arc::new(ModelIntentV1::CancelOrder {
                    platform_order_id: "sim-999".into(),
                })),
            )
            .await
            .unwrap()
            .unwrap();
    }
    let mut saw_unknown = false;

    for _ in 0..4 {
        let successor = Arc::new(h.next().unwrap());
        if case == 1
            && successor
                .action_before
                .as_ref()
                .unwrap()
                .orders()
                .any(|o| o.condition_id.is_none())
        {
            for offset in [382, 385, 388, 391, 406] {
                assert_eq!(&successor.rows[0].slow[offset..offset + 3], &[0., 0., 1.]);
            }
            saw_unknown = true;
        }
        continue_three_runtime(
            successor,
            &continuation,
            &mut state_graph,
            &mut hidden_state,
            &mut scorer,
            layers,
            hidden,
        );
    }
    if intent.is_none() {
        assert_eq!(h.shared.registry().orders().count(), 0);
        assert_eq!(
            h.shared
                .ledger()
                .economic_read()
                .unwrap()
                .cash()
                .actual_available_pusd
                .0,
            "2000"
        );
    }
    if case == 1 {
        assert!(saw_unknown);
        assert_eq!(
            h.shared
                .registry()
                .orders()
                .filter(|o| o.condition_id.is_some())
                .count(),
            2
        );
        // 只有10份已知订单占用本金；0.01份原请求已被SIM拒绝并全额释放。
        assert_eq!(
            h.shared
                .ledger()
                .economic_read()
                .unwrap()
                .cash()
                .actual_reserved_pusd
                .0,
            "1.5"
        );
    }
    eprintln!(
        "case={case}, actual graph path={path:?}, executed intent={intent:?}, successor_orders={}",
        h.shared.registry().orders().count()
    );
    h.driver.stop_and_drain().unwrap();
    if case != 0 {
        return;
    }
    // 独立真实前史上的 FOK/FAK 定向探针；不冒充模型选择。
    let mut h = fixture();
    let mut tested = NativeActor::new(Arc::new(ready(&mut h)), 0).unwrap();
    for chosen in replay {
        tested.choose(chosen).unwrap();
    }
    tested
        .execute(
            &mut h.shared,
            &mut h.driver.decision().unwrap(),
            context("three-runtime-original-intent"),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        h.shared
            .ledger()
            .economic_read()
            .unwrap()
            .cash()
            .actual_available_pusd
            .0,
        "1992"
    );
    for _ in 0..4 {
        h.next().unwrap();
    }
    assert_eq!(h.shared.registry().orders().count(), 2);
    assert_eq!(
        h.shared
            .ledger()
            .economic_read()
            .unwrap()
            .cash()
            .actual_available_pusd
            .0,
        "1991.776"
    );
}

#[test]
fn cash_root_is_not_replaced_by_encoding_limit() {
    let mut h = fixture_cash("10000000000000000000000");
    let mut actor = NativeActor::new(Arc::new(ready(&mut h)), 0).unwrap();
    let original = actor.root_upper(Family::SubmitPairedBuy).unwrap();
    let limit = encoding_limit();
    assert!(original > limit);
    actor.choose(2).unwrap();
    assert_eq!(actor.state.interval.as_ref().unwrap().upper, original);
    assert!(actor.has_completion(&limit, &limit, &actor.state).unwrap());
    assert!(
        !actor
            .has_completion(&(&limit + 1u32), &original, &actor.state)
            .unwrap()
    );
    assert!(
        actor
            .has_completion(&(&limit - 1u32), &(&limit + 1u32), &actor.state)
            .unwrap()
    );
    assert_eq!(actor.state.interval.as_ref().unwrap().hi, original);
    if let Some(directory)=std::env::var_os("B_GPU_ACTOR_CANCEL_REFERENCE") {
        let report=serde_json::json!({"actor_context":actor.gpu_context_reference(),
            "root_upper":original.to_string(),"quantity_support":actor.support().unwrap(),
            "prefix_features":actor.prefix_features().to_vec()});
        std::fs::write(std::path::Path::new(&directory).join("encoding.json"),
            serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actor_pair_reserves_once_and_rejected_leg_releases_without_erasing_other_fill() {
    // 原源事件 24000，Matched received=24171，首成交 received=24173，
    // Accepted received=24250。24172 截点只闭合到 24171。
    // 在真实两者之间读取，不能依赖旧驱动把已接收关联拖到下一组。
    let mut h = fixture_config_at("2000", ["0.01", "0.01"], 24_172);
    let g = Arc::new(ready(&mut h));
    let mut actor = NativeActor::new(g, 0).unwrap();
    actor.choose(2).unwrap();
    while actor.state.stage == Stage::QuantityBranch {
        let interval = actor.state.interval.as_ref().unwrap();
        let mid = interval;
        actor
            .choose(segment_for(mid, &BigUint::from(500u32)))
            .unwrap();
    }
    actor.choose(1).unwrap();
    actor.choose(1).unwrap();
    actor.choose(79).unwrap();
    actor.choose(19).unwrap();
    for _ in 0..2 {
        actor
            .execute(
                &mut h.shared,
                &mut h.driver.decision().unwrap(),
                context("mixed-actor-feedback"),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            h.shared
                .ledger()
                .economic_read()
                .unwrap()
                .cash()
                .actual_available_pusd
                .0,
            "1995"
        );
    }
    let original_commitments: Vec<_> = h
        .shared
        .registry()
        .all_commitments()
        .map(|c| (c.origin.batch_item_ordinal, c.remaining_target_shares))
        .collect();
    assert_eq!(original_commitments.len(), 2);
    let intermediate = h.next().unwrap();
    assert_eq!(
        intermediate.cutoff.decision_time().logical_time_utc_ms(),
        24_172
    );
    assert_eq!(intermediate.cancel.len(), 1, "真实空市场仍有 Market 候选");
    assert_eq!(intermediate.cancel[0][2], 1.);
    let view = intermediate.action_before.as_ref().unwrap();
    let orders: Vec<_> = view.orders().collect();
    assert_eq!(orders.len(), 1, "不可撤但关联未齐的订单仍在动作快照中");
    let order = &orders[0];
    assert!(order.origin.is_none());
    assert!(order.accepted.is_none());
    assert!(order.formed.is_some());
    assert_eq!(order.cancellable, ExecutionReadValueV1::Known(false));
    assert_eq!(order.matched_shares, ExecutionReadValueV1::Unavailable);
    assert_eq!(
        order.active_unfilled_shares,
        ExecutionReadValueV1::Unavailable
    );
    assert_eq!(
        view.commitments()
            .map(|c| (c.origin.batch_item_ordinal, c.remaining_target_shares))
            .collect::<Vec<_>>(),
        original_commitments
    );
    let later = h.next().unwrap();
    assert_eq!(h.shared.registry().orders().count(), 1);
    assert_eq!(
        h.shared
            .ledger()
            .economic_read()
            .unwrap()
            .cash()
            .actual_available_pusd
            .0,
        "1995.888"
    );
    let mut follow = NativeActor::new(Arc::new(later), 0).unwrap();
    follow.choose(1).unwrap();
    assert_eq!(follow.state.single_leg, Some(Leg::B));
    assert_eq!(
        follow.state.interval.as_ref().unwrap().upper,
        BigUint::from(666u32)
    );
    while follow.state.stage == Stage::QuantityBranch {
        let interval = follow.state.interval.as_ref().unwrap();
        let mid = interval;
        follow
            .choose(segment_for(mid, &BigUint::from(500u32)))
            .unwrap();
    }
    follow.choose(0).unwrap();
    follow.choose(99).unwrap();
    follow
        .execute(
            &mut h.shared,
            &mut h.driver.decision().unwrap(),
            context("actual-repair-commitment"),
        )
        .await
        .unwrap()
        .unwrap();
    let next = NativeActor::new(Arc::new(h.next().unwrap()), 0).unwrap();
    assert_eq!(
        next.root_upper(Family::SubmitSingleBuy).unwrap(),
        BigUint::from(166u32)
    );
}

#[test]
fn fixed_prefix_variants_and_common_budget_match_small_full_action_oracle() {
    let mut h = fixture_config("2.02", ["0.01", "0.01"]);
    let actor = NativeActor::new(Arc::new(ready(&mut h)), 0).unwrap();
    let attributes = [(Variant::GtcLimit, false), (Variant::FokMarket, false)];
    let oracle = |n: u64, k: u64, variant: Variant, post: bool| -> Option<u64> {
        let principal = n * k * 100;
        match variant {
            Variant::FokMarket | Variant::FakMarket => {
                (principal >= 1_000_000).then_some(principal)
            }
            _ => {
                let m = k.min(50);
                let fee = if post || principal < 1_000_000 {
                    0
                } else {
                    (n * 7 * m * (100 - m) + 500) / 1000
                };
                Some(principal + 10 * fee)
            }
        }
    };
    for n in [1u64, 100, 112, 113, 125, 499, 500, 501, 1000] {
        for &(a, pa) in &attributes {
            for &(b, pb) in &attributes {
                for ka in [1u64, 10, 20, 30, 49, 50, 51, 80, 99] {
                    for kb in [1u64, 10, 20, 30, 49, 50, 51, 80, 99] {
                        let want = oracle(n, ka, a, pa)
                            .zip(oracle(n, kb, b, pb))
                            .is_some_and(|(ca, cb)| ca + cb <= 2_020_000);
                        let state = PrefixState {
                            family: Some(Family::SubmitPairedBuy),
                            quantity_hundredths: Some(n.into()),
                            legs: [
                                LegSelection {
                                    variant: Some(a),
                                    post_only: Some(pa),
                                    lifetime: Some(Lifetime::Seconds180),
                                    price_index: Some(ka.into()),
                                },
                                LegSelection {
                                    variant: Some(b),
                                    post_only: Some(pb),
                                    lifetime: Some(Lifetime::Seconds300),
                                    price_index: Some(kb.into()),
                                },
                            ],
                            ..Default::default()
                        };
                        assert_eq!(
                            actor.fixed_completion(&n.into(), &state).unwrap(),
                            want,
                            "n={n} {a:?}/{pa}@{ka} {b:?}/{pb}@{kb}"
                        );
                    }
                }
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_single_repair_cap_counts_visible_success_and_nonterminal_commitment() {
    let times = [22_942, 24_017, 24_019]
        .into_iter()
        .chain((24_001..28_002).step_by(100))
        .collect::<Vec<_>>();
    let mut h = fixture_config_times("2000", ["0.01", "0.01"], 24_999, &times);
    ready(&mut h);
    async fn buy(h: &mut Harness, id: &str, token: &str, spend: &str) {
        h.driver.wait_for_recording_capacity().unwrap();
        h.shared
            .execute(
                &mut h.driver.decision().unwrap(),
                context(id),
                Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                    condition_id: ConditionId(CONDITION_ID.into()),
                    order: BuyLegIntentV1 {
                        token_id: TokenId(token.into()),
                        order: BuyOrderIntentV1::GtcLimit {
                            size_shares: spend.parse().unwrap(),
                            limit_price_pusd_per_share: Decimal::new(6, 1),
                            post_only: false,
                        },
                    },
                })),
            )
            .await
            .unwrap()
            .unwrap();
    }
    buy(&mut h, "repair-a-ten", UP_TOKEN_ID, "10").await;
    let mut sent = false;

    let mut saw_pending = false;
    let mut saw_terminal = false;
    let mut saw_economic_pending = false;
    let mut early_order_group = None;
    while let Some(group) = h.next() {
        let group = Arc::new(group);
        let read = group.action_before.as_ref().unwrap();
        let at = read.cutoff().decision_time().logical_time_utc_ms();
        let Some(market) = read
            .economics()
            .markets
            .iter()
            .find(|m| m.condition_id.0 == CONDITION_ID)
        else {
            continue;
        };
        let assets = market.assets.as_ref().unwrap();
        let a = assets
            .iter()
            .position(|a| a.token_id.0 == UP_TOKEN_ID)
            .unwrap();
        let b = 1 - a;
        if at == 25_017 {
            let order = read
                .orders()
                .find(|o| o.token_id == Some(&TokenId(DOWN_TOKEN_ID.into())))
                .unwrap();
            assert!(order.origin.is_none() && order.accepted.is_none() && order.formed.is_none());
            assert_eq!(order.cancellable, ExecutionReadValueV1::Unavailable);
            let single = group
                .cancel
                .iter()
                .find(|v| v[0] == 1.)
                .expect("实际成交已证明可寻址 ID");
            assert_eq!(single[7], 1., "已到成交证明 B 腿");
            assert_eq!(&single[10..14], &[0.; 4], "原订单变体仍未知");
            assert_eq!(&single[56..60], &[0.; 4], "四个条件字段适用性仍未知");
            assert_eq!(&single[60..63], &[0., 0., 1.], "可撤性未知仍保留 Single");
            early_order_group = Some(group.clone());
        }
        if at == 25_019 {
            let order = read
                .orders()
                .find(|o| o.token_id == Some(&TokenId(DOWN_TOKEN_ID.into())))
                .unwrap();
            assert!(order.origin.is_some() && order.accepted.is_some() && order.formed.is_some());
            assert_eq!(order.cancellable, ExecutionReadValueV1::Known(false));
            assert!(
                early_order_group
                    .as_ref()
                    .unwrap()
                    .action_before
                    .as_ref()
                    .unwrap()
                    .orders()
                    .any(|o| o.token_id == Some(&TokenId(DOWN_TOKEN_ID.into()))
                        && o.origin.is_none())
            );
        }
        if at == 23_942 {
            assert_eq!(market.success_shares[a].0, "0", "原经济前驱尚未到达");
            assert_eq!(
                market.repair_gap_shares[b].0, "10",
                "实际 FinalSuccess 已公开"
            );
            let actor = NativeActor::new(group.clone(), 0).unwrap();
            assert_eq!(
                actor.root_upper(Family::SubmitSingleBuy).unwrap(),
                BigUint::from(1000u32)
            );
            assert!(read.pending_noe_record_count() > 0);
            if let Some(directory)=std::env::var_os("B_GPU_ACTOR_CANCEL_REFERENCE") {
                let report=serde_json::json!({"cutoff":at,"actor_context":actor.gpu_context_reference(),
                    "actor_paths":gpu_actor_paths(group.clone(),&[0,1,2,3,4,5]),
                    "rule_features":group.rows[0].rule.as_slice(),"cancel_features":group.cancel,
                    "fok_trace":gpu_actor_trace(group.clone(),1,true)});
                std::fs::write(std::path::Path::new(&directory).join("repair-before-economic.json"),
                    serde_json::to_vec_pretty(&report).unwrap()).unwrap();
            }
            #[cfg(feature="python")]
            if std::env::var_os("B_GPU_ACTOR_SELECTION_REPORT").is_some() {
                let source=serde_json::json!({"cutoff":at,"actor_context":actor.gpu_context_reference(),
                    "tokens":group.rows[0].action_context.as_ref().unwrap().tokens,
                    "condition_id":group.rows[0].hidden_state_key.condition_id,"cancel_features":group.cancel,
                    "checkpoint":crate::public_input_tests::nav_gpu::checkpoint(&h.shared,read,Some(&group),&h.materials)});
                validate_actor_selection(group.clone(),source,"run_single","single");
            }
            saw_economic_pending = true;
        }
        if !sent && market.success_shares[a].0.parse::<Decimal>().unwrap() == Decimal::from(10) {
            buy(&mut h, "repair-b-four", DOWN_TOKEN_ID, "5").await;
            eprintln!("repair send B cutoff={at}");
            sent = true;
            continue;
        }
        if sent {
            let actor = NativeActor::new(group.clone(), 0).unwrap();
            let gap = market.repair_gap_shares[b].0.parse::<Decimal>().unwrap();
            let commitment = h.shared.registry().committed_target_shares(
                &ConditionId(CONDITION_ID.into()),
                &TokenId(DOWN_TOKEN_ID.into()),
            );
            if commitment == ExecutionReadValueV1::Known(ExactDecimal("5".into())) {
                assert_eq!(gap, Decimal::from(10));
                assert_eq!(
                    actor.root_upper(Family::SubmitSingleBuy).unwrap(),
                    BigUint::from(500u32)
                );
                if !saw_pending {
                    eprintln!("repair pending cutoff={at}");
                    if let Some(directory)=std::env::var_os("B_GPU_ACTOR_CANCEL_REFERENCE") {
                        let report=serde_json::json!({"cutoff":at,"actor_context":actor.gpu_context_reference(),
                            "actor_paths":gpu_actor_paths(group.clone(),&[0,1,2,3,4,5])});
                        std::fs::write(std::path::Path::new(&directory).join("repair-pending.json"),
                            serde_json::to_vec_pretty(&report).unwrap()).unwrap();
                    }
                }
                saw_pending = true;
            } else if commitment == ExecutionReadValueV1::Known(ExactDecimal("0".into())) {
                assert_eq!(gap, Decimal::from(5));
                assert_eq!(
                    actor.root_upper(Family::SubmitSingleBuy).unwrap(),
                    BigUint::from(500u32)
                );
                if !saw_terminal {
                    eprintln!("repair terminal cutoff={at}");
                }
                saw_terminal = true;
                if market.success_shares[b].0.parse::<Decimal>().unwrap() == Decimal::ZERO {
                    saw_economic_pending = true;
                    eprintln!(
                        "repair final-before-economic cutoff={at} pending={} gap={gap} cap=500",
                        read.pending_noe_record_count()
                    );
                }
            }
        }
        if at == 29_001 {
            break;
        }
    }
    assert!(
        sent && saw_pending && saw_terminal && early_order_group.is_some(),
        "真实前史与终局须实际覆盖"
    );
    let evidence = h._temp.keep();
    eprintln!("repair evidence {}", evidence.display());
    assert!(saw_economic_pending, "必须覆盖终局已公开但经济尚未应用");
}

#[cfg(feature = "python")]
#[test]
fn python_features_consumes_real_native_groups_with_the_original_public_shape() {
    use pyo3::prelude::*;
    let mut h = fixture_config_times(
        "2000",
        ["0.01", "0.01"],
        24_999,
        &[35_000, 319_000, 320_000],
    );
    let mut groups = 0;
    loop {
        let group = h.next().expect("有限公开来源含到期后的组");
        let retired = group
            .post_frame_hidden_ops
            .iter()
            .any(|op| matches!(op, HiddenOp::Retire { .. }));
        let expected = serde_json::to_value(&group).unwrap();
        Python::attach(|py| {
            let expected = crate::python::value(py, expected).unwrap();
            let native = Py::new(
                py,
                crate::python::PythonGroup {
                    group: Arc::new(group),
                },
            )
            .unwrap();
            let actual = native.call_method0(py, "features").unwrap();
            assert!(actual.bind(py).eq(expected.bind(py)).unwrap());
            let rows = actual.bind(py).get_item("rows").unwrap();
            for row in rows.try_iter().unwrap() {
                let row = row.unwrap();
                assert!(!row.contains("action_context").unwrap());
                assert_eq!(row.get_item("slow").unwrap().len().unwrap(), 433);
                assert_eq!(row.get_item("fast").unwrap().len().unwrap(), 596);
            }
        });
        groups += 1;
        if retired {
            break;
        }
    }
    assert!(groups >= 6);
    h.driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_cancel_clears_known_account_aggregates() {
    let mut h = fixture();
    let g = ready(&mut h);
    let s = selected(Family::SubmitPairedBuy, Variant::GtcLimit, "500");
    g.execute_selected(
        0,
        SelectedAction::Buy(&s),
        &mut h.shared,
        &mut h.driver.decision().unwrap(),
        context("known-buy-before-unknown-cancel"),
        |_, _, _| -> Result<Vec<CancelScoreBlock>, ()> { panic!() },
    )
    .await
    .unwrap()
    .unwrap();
    h.shared
        .execute(
            &mut h.driver.decision().unwrap(),
            context("unknown-cancel"),
            Some(Arc::new(ModelIntentV1::CancelOrder {
                platform_order_id: "sim-999".into(),
            })),
        )
        .await
        .unwrap()
        .unwrap();
    let mut found = false;
    while let Some(g) = h.next() {
        let read = g.action_before.as_ref().unwrap();
        let unknown = read.orders().filter(|o| o.condition_id.is_none()).count();
        let known = read.orders().filter(|o| o.condition_id.is_some()).count();
        if unknown != 0 && known != 0 {
            let slow = &g.rows[0].slow;
            assert_eq!(unknown, 1);
            assert_eq!(known, 2);
            for offset in [382, 385, 388, 391, 406] {
                assert_eq!(&slow[offset..offset + 3], &[0., 0., 1.], "slot {offset}");
            }
            found = true;
            break;
        }
    }
    assert!(
        found,
        "formal unknown cancellation must appear in actual frozen account read"
    );
    h.driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn model_selects_low_limit_quantity_and_platform_refusal_releases_original_reservation() {
    for minimum in ["5", "10"] {
        for n in [1u32, 499, 500, 999, 1000, 1001] {
            let mut h = fixture_with_minimum(
                "2000",
                ["0.01", "0.01"],
                24_999,
                &[35_000, 45_000, 55_000],
                ["100", "100"],
                minimum,
            );
            let group = Arc::new(ready(&mut h));
            assert_eq!(
                group.rows[0].rule[14],
                crate::exact::signed_log_f32(&crate::exact::integer(
                    minimum.parse::<u32>().unwrap()
                ))
            );
            assert_eq!(group.rows[0].rule[15], 1.);
            assert_eq!(&group.rows[0].fast[566..596], &group.rows[0].rule);
            let mut actor = NativeActor::new(group, 0).unwrap();
            actor.choose(2).unwrap();
            while actor.state.stage == Stage::QuantityBranch {
                let interval = actor.state.interval.as_ref().unwrap();
                let midpoint = interval;
                actor
                    .choose(segment_for(midpoint, &BigUint::from(n)))
                    .unwrap();
            }
            assert_eq!(actor.state.quantity_hundredths, Some(n.into()));
            for _ in 0..2 {
                actor.choose(0).unwrap(); // 普通 GTC；不增加 PostOnly 选择。
            }
            actor.choose(0).unwrap();
            actor.choose(0).unwrap();
            let original = actor.intent().unwrap().unwrap();
            let ModelIntentV1::SubmitPairedBuy { orders, .. } = &original else {
                panic!()
            };
            for order in orders {
                assert_eq!(leg_values(&order.order).0, Decimal::new(i64::from(n), 2));
            }
            actor
                .execute(
                    &mut h.shared,
                    &mut h.driver.decision().unwrap(),
                    context("model-low-limit"),
                )
                .await
                .unwrap()
                .unwrap();
            let expected_reserve = Decimal::new(i64::from(n) * 2, 4);
            assert_eq!(
                Decimal::from_str_exact(
                    &h.shared
                        .ledger()
                        .economic_read()
                        .unwrap()
                        .cash()
                        .actual_reserved_pusd
                        .0
                )
                .unwrap(),
                expected_reserve
            );
            let next = h.next().unwrap();
            let below = n < minimum.parse::<u32>().unwrap() * 100;
            let cash = next
                .action_before
                .as_ref()
                .unwrap()
                .economics()
                .cash
                .clone();
            assert_eq!(
                Decimal::from_str_exact(&cash.actual_reserved_pusd.0).unwrap(),
                if below {
                    Decimal::ZERO
                } else {
                    expected_reserve
                }
            );
            assert_eq!(cash.actual_reserved_pusd, cash.maker_reserved_pusd);
            let registry = h.shared.registry();
            assert_eq!(registry.orders().count(), if below { 0 } else { 2 });
            for order in registry.orders() {
                assert_eq!(order.origin.as_ref().unwrap().intent.intent(), &original);
                assert!(order.accepted.is_some());
            }
            h.driver.stop_and_drain().unwrap();
            let noe = ::execution_normalization::DurableContextStoreV4::open(
                h._temp.path().join("noe"),
                &h.noe_context,
            )
            .unwrap();
            let rejected = noe.commits().iter().flat_map(|commit| &commit.records).filter(|record| matches!(record,
                ::execution_normalization::NormalizationRecord::Evidence {
                    evidence: ::execution_normalization::SourceNeutralEvidenceKind::Control {
                        result: contracts::execution_normalization::SourceNeutralControlResult::Submission {
                            outcome: contracts::execution_normalization::SourceNeutralSubmissionOutcomeV2::RejectedNoEffect,
                        },
                    }, ..
                })).count();
            assert_eq!(
                rejected,
                if below { 2 } else { 0 },
                "minimum={minimum}, n={n}"
            );
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn current_gtd_expires_at_effective_deadline_through_noe_ledger_and_native() {
    for (lifetime, seconds) in [
        (Lifetime::Seconds180, 180i64),
        (Lifetime::Seconds240, 240),
        (Lifetime::Seconds300, 300),
    ] {
        let deadline = (24 + seconds) * 1000;
        let mut h = fixture_config_times(
            "2000",
            ["0.01", "0.01"],
            24_999,
            &[deadline - 1001, deadline, deadline + 1000],
        );
        let group = ready(&mut h);
        assert_eq!(group.cutoff.decision_time().logical_time_utc_ms(), 23_900);
        let mut selection = selected(Family::SubmitPairedBuy, Variant::GtdLimit, "500");
        for leg in &mut selection.legs {
            leg.lifetime = Some(lifetime);
        }
        group
            .execute_selected(
                0,
                SelectedAction::Buy(&selection),
                &mut h.shared,
                &mut h.driver.decision().unwrap(),
                context("effective-gtd"),
                |_, _, _| -> Result<Vec<CancelScoreBlock>, ()> { panic!() },
            )
            .await
            .unwrap()
            .unwrap();
        let mut before = false;
        let mut after = false;
        while let Some(group) = h.next() {
            let at = group.cutoff.decision_time().logical_time_utc_ms();
            if at == deadline - 1 {
                if !group.rows.is_empty() {
                    assert_eq!(group.cancel.iter().filter(|v| v[0] == 1.).count(), 2);
                }
                assert_eq!(
                    group
                        .action_before
                        .as_ref()
                        .unwrap()
                        .economics()
                        .cash
                        .actual_reserved_pusd
                        .0,
                    "0.75"
                );
                before = true;
            }
            if at >= deadline + 1000 {
                assert!(group.cancel.iter().all(|v| v[0] != 1.));
                let cash = &group.action_before.as_ref().unwrap().economics().cash;
                assert_eq!(cash.actual_reserved_pusd.0, "0");
                assert_eq!(cash.maker_reserved_pusd.0, "0");
                for order in h.shared.registry().orders() {
                    let record = order.latest_lifecycle.unwrap();
                    let (source, received) = crate::values::times(Some(record)).unwrap();
                    assert_eq!(source, deadline);
                    assert!(received > source && received <= at);
                    let (_, original) = crate::values::leg(order.origin.as_ref().unwrap()).unwrap();
                    let BuyOrderIntentV1::GtdLimit {
                        expires_at_utc_sec, ..
                    } = original.order
                    else {
                        panic!()
                    };
                    assert_eq!(expires_at_utc_sec, deadline / 1000 + 60);
                }
                after = true;
                break;
            }
        }
        assert!(before && after);
        h.driver.stop_and_drain().unwrap();
    }
}

#[test]
fn radix_sixteen_partitions_cover_adjacent_exact_integers_without_renumbering() {
    let base = BigUint::from(1u8) << 200usize;
    for size in [1u32, 2, 15, 16, 17, 255, 256, 257, 5_000_000] {
        let q = QuantityInterval {
            upper: &base + size,
            lo: base.clone(),
            hi: &base + size - 1u32,
        };
        let mut next = q.lo.clone();
        let mut count = 0;
        for j in 0..16 {
            if let Some((lo, hi)) = crate::quantity_segment(&q, j) {
                assert_eq!(lo, next);
                assert!(hi >= lo);
                next = hi + 1u32;
                count += 1;
            }
        }
        assert_eq!(next, &q.hi + 1u32);
        assert_eq!(count, size.min(16));
    }
    let q = QuantityInterval {
        upper: 2u32.into(),
        lo: 1u32.into(),
        hi: 2u32.into(),
    };
    assert_eq!(
        (0..16)
            .filter(|&j| crate::quantity_segment(&q, j).is_some())
            .collect::<Vec<_>>(),
        [7, 15]
    );
}

#[test]
fn unsupported_tick_stops_native_run_and_preserves_original_public_fact() {
    let mut h = fixture_config("2000", ["0.0001", "0.01"]);
    loop {
        h.driver.next_group().unwrap().unwrap();
        match h.native.consume(&mut h.shared, &mut h.driver) {
            Err(InputError::UnsupportedTick(tick)) => {
                assert_eq!(tick, crate::exact::ratio(1, 10000));
                assert!(h.driver.frames().iter().any(|f| {
                    f.observation
                        .assets
                        .iter()
                        .any(|a| a.tick.value.as_ref().is_some_and(|v| v.0 == "0.0001"))
                }));
                break;
            }
            Ok(_) => {}
            other => panic!("unexpected native result {:?}", other.err()),
        }
    }
    h.driver.stop_and_drain().unwrap();
}

#[test]
fn ordinary_gtc_price_support_reserves_fee_and_keeps_mixed_fok_completion() {
    let mut h = fixture_config("2", ["0.01", "0.01"]);
    let group = Arc::new(ready(&mut h));
    let mut actor = NativeActor::new(group, 0).unwrap();
    actor.choose(2).unwrap();
    while actor.state.stage == Stage::QuantityBranch {
        let choice = segment_for(actor.state.interval.as_ref().unwrap(), &500u32.into());
        actor.choose(choice).unwrap();
    }
    actor.choose(0).unwrap();
    actor.choose(1).unwrap();
    // B 的 FOK 最小本金为1；A@.20 的本金为1，但即时费用使总额超过2。
    assert!(actor.support().unwrap()[18]);
    assert!(!actor.support().unwrap()[19]);
    assert!(actor.intent().is_err(), "未完成两腿不形成任何提交意图");
    actor.choose(18).unwrap();
    let first = actor.price_indices().unwrap()[0];
    assert_eq!(first, 19);
    actor.choose(first as usize).unwrap();
    let ModelIntentV1::SubmitPairedBuy { orders, .. } = actor.intent().unwrap().unwrap() else {
        panic!()
    };
    assert!(matches!(
        orders[0].order,
        BuyOrderIntentV1::GtcLimit {
            post_only: false,
            ..
        }
    ));
    assert!(matches!(
        orders[1].order,
        BuyOrderIntentV1::FokMarket { .. }
    ));
}

#[test]
#[ignore = "显式导出公共 CPU 费用与完整价格支持，供 GPU 精确对照"]
fn export_gpu_exact_buy_reference_v1() {
    let mut h = fixture_config("2", ["0.01", "0.01"]);
    let group = Arc::new(ready(&mut h));
    let context = group.rows[0].action_context.as_ref().unwrap();
    let snapshot = context.snapshot.clone();
    let rules = context.rules.clone();
    assert_eq!(rules.fee_rate.0, "0.07");
    assert_eq!(rules.fee_exponent, 1);
    let n = BigUint::from(500u32);
    let tick = ExactDecimal("0.01".into());
    let mut costs = Vec::new();
    // 直接读取公共 LIMIT 成本；不在诊断中重写费用或资金可负担公式。
    for (index, price, principal, fee) in [
        (19u16, "0.19", "950000", "0"),
        (20, "0.20", "1000000", "5600"),
        (25, "0.25", "1250000", "6563"),
    ] {
        let cost = snapshot.limit_buy_cost_v1(&n, index, &tick, false).unwrap();
        assert_eq!(cost.principal_micro.to_string(), principal);
        assert_eq!(cost.fee_1e5_units.to_string(), fee);
        costs.push(serde_json::json!({
            "price_index": index,
            "price_pusd_per_share": price,
            "principal_micro": cost.principal_micro.to_string(),
            "fee_1e5_units": cost.fee_1e5_units.to_string(),
        }));
    }
    let context_reference = NativeActor::new(group.clone(),0).unwrap().gpu_context_reference();
    let full_reference = gpu_actor_paths(group.clone(), &[2]);
    let mut actor = NativeActor::new(group, 0).unwrap();
    actor.choose(2).unwrap();
    while actor.state.stage == Stage::QuantityBranch {
        let choice = segment_for(actor.state.interval.as_ref().unwrap(), &n);
        actor.choose(choice).unwrap();
    }
    assert_eq!(actor.state.quantity_hundredths.as_ref(), Some(&n));
    actor.choose(0).unwrap(); // A 普通 GTC。
    actor.choose(1).unwrap(); // B FOK。
    assert_eq!(actor.state.stage, Stage::Price(Leg::A));
    let mask = actor.support().unwrap();
    assert_eq!(mask.len(), 999);
    assert!(mask[18]);
    assert!(!mask[19]);
    let a_price_coordinates = actor.price_indices().unwrap();
    let a_price_prefix_path = actor.path.clone();
    let fee_case = |value: &NativeActor| serde_json::json!({
        "prefix_path":value.path,"prefix_features":value.prefix_features().to_vec(),
        "price_indices":value.price_indices().unwrap(),
        "price_fee_features":value.price_fee_features().unwrap(),
        "price_support":value.support().unwrap()});
    let mut price_fee_cases = vec![fee_case(&actor)];
    let mut alternative = NativeActor::new(actor.group.clone(),0).unwrap();
    for &choice in &a_price_prefix_path { alternative.choose(choice).unwrap(); }
    alternative.choose(0).unwrap();
    price_fee_cases.push(fee_case(&alternative));
    actor.choose(18).unwrap();
    price_fee_cases.push(fee_case(&actor));
    let b_price_prefix_features = actor.prefix_features();
    let b_price_coordinate = actor.price_indices().unwrap()[0];
    assert_eq!(b_price_coordinate, 19);
    actor.choose(b_price_coordinate as usize).unwrap();
    let ModelIntentV1::SubmitPairedBuy {
        condition_id,
        orders,
    } = actor.intent().unwrap().unwrap()
    else {
        panic!("诊断选择应形成双腿买入意图")
    };
    let BuyOrderIntentV1::GtcLimit {
        limit_price_pusd_per_share,
        size_shares,
        post_only: false,
    } = &orders[0].order
    else {
        panic!("A 腿应为普通 GTC")
    };
    let BuyOrderIntentV1::FokMarket {
        spend_pusd,
        worst_price_pusd_per_share,
    } = &orders[1].order
    else {
        panic!("B 腿应为 FOK")
    };
    let report = serde_json::json!({
        "schema": "gpu_exact_buy_reference_v1",
        "actor_context": context_reference,
        "actor_paths": full_reference,
        "fixture": {
            "source": "model-train/native-inputs/src/buy_tests.rs::fixture_config/ready",
            "reference_test": "ordinary_gtc_price_support_reserves_fee_and_keeps_mixed_fok_completion",
            "cost_reference_test": "sim-system/src/buy_cost.rs::tests::limit_cost_branches_round_each_order_and_preserve_snapshot",
            "rule_fixture": "sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json",
            "rule_loader": "shared-run/tests/common/mod.rs::execution_with_minimum_v4",
            "rule_snapshot_sha256": snapshot.file_sha256().0,
        },
        "input": {
            "cash_pusd": "2",
            "quantity_centishares": n.to_string(),
            "ticks_pusd_per_share": ["0.01", "0.01"],
            "fee_rate": rules.fee_rate.0,
            "fee_exponent": rules.fee_exponent,
            "limit_min_quantity_shares": rules.limit_min_quantity_shares.0,
            "market_buy_min_spend_pusd": rules.market_buy_min_spend_pusd.0,
            "post_only": false,
            "family": "SubmitPairedBuy",
            "variants": ["GtcLimit", "FokMarket"],
        },
        "limit_buy_cost_v1": costs,
        "a_price_support": {
            "axis": "zero-based coordinate c; price_index=c+1; price=price_index*tick",
            "mask": mask,
            "legal_coordinates": a_price_coordinates,
            "prefix_path": a_price_prefix_path,
        },
        "b_price_prefix_features": b_price_prefix_features.to_vec(),
        "price_fee_cases": price_fee_cases,
        "selected_path": actor.path,
        "selected_intent": {
            "condition_id": condition_id.0,
            "orders": [
                {
                    "token_id": orders[0].token_id.0,
                    "type": "GtcLimit",
                    "limit_price_pusd_per_share": limit_price_pusd_per_share.to_string(),
                    "size_shares": size_shares.to_string(),
                    "post_only": false,
                },
                {
                    "token_id": orders[1].token_id.0,
                    "type": "FokMarket",
                    "spend_pusd": spend_pusd.to_string(),
                    "worst_price_pusd_per_share": worst_price_pusd_per_share.to_string(),
                },
            ],
        },
    });
    h.driver.stop_and_drain().unwrap();
    println!("BTC_GPU_EXACT_REFERENCE_V1={report}");
}

/// 原NativeActor每一步支持与坐标，供设备实现核对，不在Python重建合法域。
pub(crate) fn gpu_actor_paths(group: Arc<NativeInputGroup>, families: &[usize]) -> serde_json::Value {
    let family_support=NativeActor::new(group.clone(),0).unwrap().support().unwrap();
    let mut traces=Vec::new();
    for &family in families {
        if !family_support[family] { continue; }
        let mut actor=NativeActor::new(group.clone(),0).unwrap();
        let mut steps=Vec::new();
        let mut choice=family;
        loop {
            let mask=actor.support().unwrap();
            steps.push(serde_json::json!({"stage":format!("{:?}",actor.state.stage),"mask":mask,
                "choice":choice,"prefix_features":actor.prefix_features().to_vec(),
                "price_indices":if matches!(actor.state.stage,Stage::Price(_)) {actor.price_indices().unwrap()} else {vec![]},
                "price_fee_features":if matches!(actor.state.stage,Stage::Price(_)) {actor.price_fee_features().unwrap()} else {vec![]}}));
            actor.choose(choice).unwrap();
            if actor.is_complete() { break; }
            choice=actor.support().unwrap().iter().position(|enabled|*enabled).unwrap();
        }
        traces.push(serde_json::json!({"family":family,"steps":steps,"path":actor.path,"intent":actor.intent().unwrap().as_ref().map(crate::support::gpu_intent_reference),
            "quantity_centishares":actor.state.quantity_hundredths.as_ref().map(ToString::to_string),
            "variants":actor.state.legs.iter().map(|leg|leg.variant.map(|v|format!("{v:?}"))).collect::<Vec<_>>(),
            "price_indices":actor.state.legs.iter().map(|leg|leg.price_index.as_ref().map(ToString::to_string)).collect::<Vec<_>>() }));
    }
    serde_json::json!({"family_support":family_support,"traces":traces})
}

/// 有限参考仅选已有合法最高数量/FOK，完整支持仍来自原NativeActor。
fn gpu_actor_trace(group:Arc<NativeInputGroup>,family:usize,prefer_fok:bool)->serde_json::Value {
    let mut actor=NativeActor::new(group,0).unwrap();
    let mut steps=Vec::new();let mut choice=family;
    loop {
        let mask=actor.support().unwrap();
        steps.push(serde_json::json!({"stage":format!("{:?}",actor.state.stage),"mask":mask,
            "choice":choice,"prefix_features":actor.prefix_features().to_vec(),
            "price_indices":if matches!(actor.state.stage,Stage::Price(_)){actor.price_indices().unwrap()}else{vec![]},
            "price_fee_features":if matches!(actor.state.stage,Stage::Price(_)){actor.price_fee_features().unwrap()}else{vec![]}}));
        actor.choose(choice).unwrap();
        if actor.is_complete(){break;}
        let support=actor.support().unwrap();
        choice=match actor.state.stage {
            Stage::QuantityBranch if prefer_fok=>support.iter().rposition(|v|*v).unwrap(),
            Stage::Variant(_) if prefer_fok&&support[1]=>1,
            _=>support.iter().position(|v|*v).unwrap(),
        };
    }
    serde_json::json!({"family":family,"steps":steps,"path":actor.path,
        "variants":actor.state.legs.iter().map(|v|v.variant.map(|v|format!("{v:?}"))).collect::<Vec<_>>()})
}

#[tokio::test(flavor = "current_thread")]
async fn paired_new_types_submit_together_gtc_fills_then_posts_remainder_and_fok_fails() {
    let mut h = fixture();
    let group = Arc::new(ready(&mut h));
    let mut actor = NativeActor::new(group, 0).unwrap();
    actor.choose(2).unwrap();
    while actor.state.stage == Stage::QuantityBranch {
        let choice = segment_for(actor.state.interval.as_ref().unwrap(), &15000u32.into());
        actor.choose(choice).unwrap();
    }
    actor.choose(0).unwrap();
    actor.choose(1).unwrap();
    actor.choose(79).unwrap();
    assert!(actor.intent().is_err());
    assert_eq!(h.shared.registry().all_commitments().count(), 0);
    actor.choose(19).unwrap();
    let original = actor.intent().unwrap().unwrap();
    let ModelIntentV1::SubmitPairedBuy { orders, .. } = &original else {
        panic!()
    };
    assert!(matches!(
        orders[0].order,
        BuyOrderIntentV1::GtcLimit {
            post_only: false,
            ..
        }
    ));
    assert!(
        matches!(orders[1].order,BuyOrderIntentV1::FokMarket {spend_pusd,..} if spend_pusd==Decimal::from(30))
    );
    actor
        .execute(
            &mut h.shared,
            &mut h.driver.decision().unwrap(),
            context("paired-ordinary-and-fok"),
        )
        .await
        .unwrap()
        .unwrap();
    let commits = h.shared.registry().all_commitments().collect::<Vec<_>>();
    assert_eq!(commits.len(), 2);
    assert_eq!(
        commits[0].origin.intent.global_request_id(),
        commits[1].origin.intent.global_request_id()
    );
    for _ in 0..4 {
        h.next().unwrap();
    }
    let orders = h.shared.registry().orders().collect::<Vec<_>>();
    assert_eq!(orders.len(), 1);
    assert_eq!(
        orders[0].active_unfilled_shares,
        ExecutionReadValueV1::Known(ExactDecimal("50".into()))
    );
    let cash = h.shared.ledger().economic_read().unwrap();
    assert_eq!(cash.cash().actual_available_pusd.0, "1898.32");
    h.driver.stop_and_drain().unwrap();
}

#[test]
fn preopen_initial_milli_tick_and_old_actor_keep_their_own_exact_grid() {
    let mut initial = fixture_config("2000", ["0.001", "0.001"]);
    initial.next().unwrap();
    let first = Arc::new(initial.next().unwrap());
    let actor = NativeActor::new(first, 0).unwrap();
    assert_eq!(
        actor.root_upper(Family::SubmitPairedBuy).unwrap(),
        BigUint::from(100_000_000u32)
    );
    initial.driver.stop_and_drain().unwrap();
    let mut h = fixture();
    let old = Arc::new(ready(&mut h));
    let later = Arc::new(h.next().unwrap());
    let state = selected(Family::SubmitSingleBuy, Variant::GtcLimit, "500");
    let price = |g: &NativeInputGroup| {
        let ModelIntentV1::SubmitSingleBuy { order, .. } = g.buy_intent(0, &state).unwrap() else {
            panic!()
        };
        leg_values(&order.order).1
    };
    assert_eq!(price(&old), Decimal::new(1, 1));
    assert_eq!(price(&later), Decimal::new(1, 2));
    assert_eq!(price(&old), Decimal::new(1, 1));
    h.driver.stop_and_drain().unwrap();
}

#[test]
#[ignore = "#176 单进程 release 首次网格/真实tick切换定向计时；非闭环性能矩阵"]
fn tick_grid_transition_support_measurement() {
    use std::time::Instant;
    fn price_actor(group: Arc<NativeInputGroup>) -> NativeActor {
        let mut actor = NativeActor::new(group, 0).unwrap();
        actor.choose(2).unwrap();
        while actor.state.stage == Stage::QuantityBranch {
            let choice = segment_for(
                actor.state.interval.as_ref().unwrap(),
                &BigUint::from(500u32),
            );
            actor.choose(choice).unwrap();
        }
        actor.choose(0).unwrap();
        actor.choose(0).unwrap();
        actor.choose(0).unwrap();
        actor
    }
    let mut h = fixture();
    let old = price_actor(Arc::new(ready(&mut h)));
    let later = price_actor(Arc::new(h.next().unwrap()));
    let mut results = Vec::new();
    for (name, actor, expected) in [("cent", &old, 99), ("milli_after_tick_event", &later, 999)] {
        let start = Instant::now();
        let indices = actor.price_indices().unwrap();
        let first_indices_ns = start.elapsed().as_nanos();
        assert_eq!(indices.len(), expected);
        let mut samples = Vec::new();
        for i in 0..40 {
            let start = Instant::now();
            let support = actor.support().unwrap();
            let support_ns = start.elapsed().as_nanos();
            let start = Instant::now();
            let indices = actor.price_indices().unwrap();
            let indices_ns = start.elapsed().as_nanos();
            let start = Instant::now();
            let fees = actor.price_fee_features().unwrap();
            let fee_features_ns = start.elapsed().as_nanos();
            assert_eq!(support.iter().filter(|v| **v).count(), expected);
            assert_eq!(indices.len(), fees.len());
            if i >= 8 {
                samples.push(serde_json::json!({"support_ns":support_ns,"indices_ns":indices_ns,"fee_features_ns":fee_features_ns}));
            }
        }
        results.push(serde_json::json!({"case":name,"valid_prices":expected,"first_indices_ns":first_indices_ns,"samples":samples}));
    }
    assert_eq!(old.price_indices().unwrap().len(), 99);
    eprintln!("tick_support_measurement={}", serde_json::json!(results));
    h.driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn paired_commitment_snapshot_retains_closed_leg_without_reopening_order_observations() {
    let times = [22_942]
        .into_iter()
        .chain(23_001..23_401)
        .collect::<Vec<_>>();
    let mut h = fixture_config_times("2000", ["0.01", "0.01"], 24_999, &times);
    ready(&mut h);
    h.shared
        .execute(
            &mut h.driver.decision().unwrap(),
            context("snapshot-success"),
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                order: BuyLegIntentV1 {
                    token_id: TokenId(DOWN_TOKEN_ID.into()),
                    order: BuyOrderIntentV1::GtcLimit {
                        size_shares: Decimal::from(5),
                        limit_price_pusd_per_share: Decimal::new(6, 1),
                        post_only: false,
                    },
                },
            })),
        )
        .await
        .unwrap()
        .unwrap();
    let mut submitted = false;
    let mut canceled_up = false;
    let mut frozen = None;
    let mut closed_pair = false;
    while let Some(group) = h.next() {
        let group = Arc::new(group);
        let read = group.action_before.as_ref().unwrap();
        let at = read.cutoff().decision_time().logical_time_utc_ms();
        let market = read
            .economics()
            .markets
            .iter()
            .find(|m| m.condition_id.0 == CONDITION_ID)
            .unwrap();
        let assets = market.assets.as_ref().unwrap();
        let down = assets
            .iter()
            .position(|a| a.token_id.0 == DOWN_TOKEN_ID)
            .unwrap();
        if !submitted
            && market.success_shares[down].0.parse::<Decimal>().unwrap() == Decimal::from(5)
        {
            h.shared
                .execute(
                    &mut h.driver.decision().unwrap(),
                    context("snapshot-pair"),
                    Some(Arc::new(ModelIntentV1::SubmitPairedBuy {
                        condition_id: ConditionId(CONDITION_ID.into()),
                        orders: [
                            leg(UP_TOKEN_ID, Decimal::new(1, 2)),
                            leg(DOWN_TOKEN_ID, Decimal::new(1, 2)),
                        ],
                    })),
                )
                .await
                .unwrap()
                .unwrap();
            submitted = true;
        } else if submitted && !canceled_up {
            let pair = h
                .shared
                .registry()
                .orders()
                .filter(|o| {
                    o.origin
                        .as_ref()
                        .is_some_and(|v| v.intent.global_request_id() == "snapshot-pair")
                })
                .collect::<Vec<_>>();
            if pair.len() == 2
                && pair.iter().all(|o| {
                    o.accepted.is_some()
                        && o.matched_shares == ExecutionReadValueV1::Known(ExactDecimal("0".into()))
                })
            {
                let up = pair
                    .iter()
                    .find(|o| o.token_id.is_some_and(|v| v.0 == UP_TOKEN_ID))
                    .unwrap();
                let id = platform(up);
                h.shared
                    .execute(
                        &mut h.driver.decision().unwrap(), // 原时延采样给此请求 35ms 响应回程；观察真实 CallerCanceled 先于直接响应。
                        context("snapshot-cancel-up-0"),
                        Some(Arc::new(ModelIntentV1::CancelOrder {
                            platform_order_id: id,
                        })),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                canceled_up = true;
            }
        } else if canceled_up && frozen.is_none() {
            let pair = h
                .shared
                .registry()
                .orders()
                .filter(|o| {
                    o.origin
                        .as_ref()
                        .is_some_and(|v| v.intent.global_request_id() == "snapshot-pair")
                })
                .collect::<Vec<_>>();
            let up = pair
                .iter()
                .find(|o| o.token_id.is_some_and(|v| v.0 == UP_TOKEN_ID))
                .unwrap();
            if up.cancellable == ExecutionReadValueV1::Known(false)
                && up.latest_cancel_feedback.is_none()
            {
                let full = h
                    .shared
                    .registry()
                    .all_commitments()
                    .filter(|c| c.origin.intent.global_request_id() == "snapshot-pair")
                    .map(|c| (c.origin.batch_item_ordinal, c.remaining_target_shares))
                    .collect::<Vec<_>>();
                let projected = read
                    .commitments()
                    .filter(|c| c.origin.intent.global_request_id() == "snapshot-pair")
                    .map(|c| (c.origin.batch_item_ordinal, c.remaining_target_shares))
                    .collect::<Vec<_>>();
                assert_eq!(
                    full,
                    vec![(1, ExecutionReadValueV1::Known(ExactDecimal("5".into())))]
                );
                assert_eq!(
                    projected, full,
                    "closed leg still referenced by the paired request must retain its original zero evidence"
                );
                assert_eq!(market.repair_gap_shares[1 - down].0, "5");
                NativeActor::new(group.clone(), 0)
                    .unwrap()
                    .support()
                    .unwrap();
                let observed = read.orders().collect::<Vec<_>>();
                assert_eq!(observed.len(), 1);
                assert_eq!(observed[0].token_id.unwrap().0, DOWN_TOKEN_ID);
                assert_eq!(
                    read.order_funds()
                        .map(|f| f.order_identity)
                        .collect::<Vec<_>>(),
                    vec![observed[0].order_identity]
                );
                assert_eq!(
                    read.request_funds().count(),
                    0,
                    "this paper account has no live reservations"
                );
                assert_eq!(&group.rows[0].slow[406..409], &[1_f32.ln_1p(), 1., 1.]);
                assert_eq!(
                    &group.rows[0].slow[382..385],
                    &group.rows[0].slow[385..388],
                    "one open order supplies both age extrema"
                );
                let ExecutionReadValueV1::Known(objects) = read.cancel_objects() else {
                    panic!("known accepted open leg")
                };
                assert_eq!(
                    objects
                        .iter()
                        .filter(|o| matches!(o, CancelObjectRead::Single { .. }))
                        .count(),
                    1
                );
                assert!(
                    !objects
                        .iter()
                        .any(|o| matches!(o, CancelObjectRead::Paired { .. }))
                );
                let id = platform(&observed[0]);
                frozen = Some(group.clone());
                h.shared
                    .execute(
                        &mut h.driver.decision().unwrap(),
                        context("snapshot-cancel-down"),
                        Some(Arc::new(ModelIntentV1::CancelOrder {
                            platform_order_id: id,
                        })),
                    )
                    .await
                    .unwrap()
                    .unwrap();
            }
        } else if frozen.is_some() && read.orders().count() == 0 {
            assert!(
                !read
                    .commitments()
                    .any(|c| c.origin.intent.global_request_id() == "snapshot-pair")
            );
            assert_eq!(read.order_funds().count(), 0);
            let old = frozen.as_ref().unwrap().action_before.as_ref().unwrap();
            assert_eq!(old.orders().count(), 1);
            assert_eq!(old.order_funds().count(), 1);
            assert_eq!(
                h.shared.registry().orders().count(),
                3,
                "the full registry still retains original history"
            );
            assert_eq!(
                old.commitments()
                    .filter(|c| c.origin.intent.global_request_id() == "snapshot-pair")
                    .map(|c| (c.origin.batch_item_ordinal, c.remaining_target_shares))
                    .collect::<Vec<_>>(),
                vec![(1, ExecutionReadValueV1::Known(ExactDecimal("5".into())))]
            );
            closed_pair = true;
            break;
        }
        if at == 24_400 {
            break;
        }
    }
    assert!(
        submitted && canceled_up && frozen.is_some() && closed_pair,
        "real cancellation lifecycle before direct response must be observed"
    );
    h.driver.stop_and_drain().unwrap();
}
