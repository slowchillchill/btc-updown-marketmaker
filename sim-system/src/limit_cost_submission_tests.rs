#[tokio::test(flavor = "current_thread")]
async fn limit_cost_matches_real_submission_admission_before_price_improvement() {
    use num_bigint::BigUint;
    for gtd in [false, true] {
        for cash in ["2.5875", "4.58749", "4.5875"] {
            let directory = tempfile::tempdir().unwrap();
            let rules = successful_snapshot_v3_for_sim_tests().await;
            let finality = successful_policy_v2_for_tests();
            let scenario = sealed_scenario();
            let latency = load_embedded_proxy_latency_profile().unwrap();
            let run_opened = opened("limit-cost", &rules, &finality, &scenario, &latency);
            let mut actor = SimRunActorV2::create_new(
                &directory.path().join(SIM_EVENT_LOG_FILE_NAME_V2),
                run_opened,
                rules.clone(),
                finality,
                scenario,
                latency,
                accepting_projection_port(),
                MAX_GROUP_BYTES,
            )
            .unwrap();
            let mut session = spec("limit-cost", "create-limit-cost");
            session.initial_account_state.available_pusd = ExactDecimal(cash.into());
            actor.create_session(session).unwrap();
            actor
                .activate_session("activate-limit-cost".into())
                .unwrap();

            // 原公开市场事件的编码／解码入口建立足量 0.5 卖盘，未手拼内部队列。
            let mut initial = decoded_initial_market_events("10");
            let event = &mut initial[0];
            let ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) = &mut event.payload
            else {
                unreachable!()
            };
            snapshot.asks[0].price_pusd_per_share = ExactDecimal("0.5".into());
            event.event_id =
                data_pipeline_core::derive_execution_market_event_v3_id(event).unwrap();
            *event = data_pipeline_core::decode_execution_market_event_v3(
                &data_pipeline_core::encode_execution_market_event_v3(event).unwrap(),
            )
            .unwrap();
            let condition_id = initial[0].market.condition_id.clone();
            let state = actor.state.take().unwrap();
            let (state, _) = reduce_market_event_slice_v2(
                state,
                MarketEventPathV1::HiddenPlatform,
                1_100,
                &initial,
                rules.snapshot(),
                actor._finality_policy.as_ref(),
            )
            .unwrap_or_else(|_| panic!("合法初始盘口必须可读取"));

            let state_before_cost = format!("{state:?}");
            let cost = rules
                .limit_buy_cost_v1(
                    &BigUint::from(500_u32),
                    90,
                    &ExactDecimal("0.01".into()),
                    false,
                )
                .unwrap();
            assert_eq!(cost.principal_micro, BigUint::from(4_500_000_u32));
            assert_eq!(cost.fee_1e5_units, BigUint::from(8_750_u32));
            assert_eq!(format!("{state:?}"), state_before_cost);
            let kind = if gtd {
                SimBuyOrderKindV1::GtdLimit {
                    limit_price_pusd_per_share: "0.9".parse().unwrap(),
                    size_shares: Decimal::from(5),
                    post_only: false,
                    expires_at_utc_ms: 302_000,
                }
            } else {
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: "0.9".parse().unwrap(),
                    size_shares: Decimal::from(5),
                    post_only: false,
                }
            };
            let request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
                context: RequestContextV2 {
                    global_request_id: "limit-cost-buy".into(),
                    caller_correlation_id: None,
                },
                item: post_item("buy", &condition_id, &TokenId("1".into()), kind),
            });
            let (state, receipt) = register_request_candidate(
                &actor.opened,
                actor.scenario.initial_market_directory(),
                state,
                &request_cutoff(1_200),
                &request,
                11,
                actor._latency_profile.as_ref(),
                rules.snapshot(),
            );
            let (_, effect) = advance_mutation_candidate(
                &actor.opened,
                actor.scenario.initial_market_directory(),
                state,
                receipt.platform_arrived_at_utc_ms,
                rules.snapshot(),
                actor._finality_policy.as_ref(),
            );
            let Some(MutationActionBusinessEffectV2::Post(effect)) = effect else {
                panic!("真实提交必须形成处理结果")
            };
            if cash != "4.5875" {
                assert!(
                    matches!(
                        effect.outcome,
                        PostOrderItemOutcomeV2::RejectedNoEffect(
                            SubmissionRejectedReasonV2::InsufficientAvailablePusd
                        )
                    ),
                    "原准入本金不得预扣价格改善：{:?}",
                    effect.outcome
                );
            } else {
                let PostOrderItemOutcomeV2::Accepted(accepted) = &effect.outcome else {
                    panic!("恰好最大成本应通过资金准入：{:?}", effect.outcome)
                };
                let contracts::SubmissionMeasurementV2::Base {
                    accepted_quote_amount_pusd,
                    matched_quote_amount_pusd,
                    released_price_improvement_quote_amount_pusd,
                    immediate_taker_fee_encumbered_pusd,
                    ..
                } = &accepted.measurement
                else {
                    panic!("LIMIT 使用 Base 计量")
                };
                assert_eq!(accepted_quote_amount_pusd.0, "4.5");
                assert_eq!(matched_quote_amount_pusd.0, "2.5");
                assert_eq!(released_price_improvement_quote_amount_pusd.0, "2");
                assert_eq!(immediate_taker_fee_encumbered_pusd.0, "0.0875");
            }
        }
    }
}
