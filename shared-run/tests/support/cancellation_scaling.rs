use super::*;

#[tokio::test(flavor = "current_thread")]
async fn actual_freeze_with_active_pair_and_unrelated_cancel_history_has_constant_visits() {
    let mut expected = None;
    let mut expected_visits = None;
    for history in [0, 128] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (core, times) = super::fixture_multi_market::recorded_equal_start_markets(&root);
        let (mut execution, observation, _) = execution_with_rules_v4(
            &temp.path().join("sim.jsonl"),
            &core,
            SUCCESS_POLICY,
            None,
            "e".repeat(64),
            "1000",
        );
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        execution
            .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV2::from_opening(ledger, observation.clone())
            .unwrap()
            .unwrap();
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let mut driver =
            FinitePublicConsumptionV1::new(execution, source, "active-cancel-history".into());
        driver.enable_finite_recording_wait().unwrap();
        for _ in 0..8 {
            driver.next_group().unwrap().unwrap();
        }
        for i in 0..=history {
            driver.wait_for_recording_capacity().unwrap();
            shared
                .execute(
                    &mut driver.decision().unwrap(),
                    request(&format!("unrelated-{i:03}")),
                    Some(Arc::new(ModelIntentV1::CancelMarketOrders {
                        condition_id: ConditionId(
                            super::fixture_market_identity::OTHER_CONDITION_ID.into(),
                        ),
                    })),
                )
                .await
                .unwrap()
                .unwrap();
        }
        let pair = Arc::new(ModelIntentV1::SubmitPairedBuy {
            condition_id: ConditionId(CONDITION_ID.into()),
            orders: [UP_TOKEN_ID, DOWN_TOKEN_ID].map(|token| BuyLegIntentV1 {
                token_id: TokenId(token.into()),
                order: BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share: rust_decimal::Decimal::new(2, 1),
                    size_shares: 5.into(),
                    post_only: true,
                },
            }),
        });
        driver.wait_for_recording_capacity().unwrap();
        shared
            .execute(
                &mut driver.decision().unwrap(),
                request("active-pair"),
                Some(pair),
            )
            .await
            .unwrap()
            .unwrap();
        for _ in 0..2 {
            driver.next_group().unwrap().unwrap();
            shared.apply_closed().unwrap();
        }
        let condition = ConditionId(CONDITION_ID.into());
        let scope = CancellationScopeV1::Market(&condition);
        assert!(matches!(
            shared
                .registry()
                .cancellation_requests(Some(&scope))
                .latest_response,
            ExecutionReadValueV1::Known(None)
        ));
        fixture_registry::assert_cancellation_requests_reference(shared.registry(), Some(&scope));
        fixture_registry::assert_cancel_objects_reference(shared.registry());
        let cutoff = driver.observation_cutoff().unwrap();
        let frames = driver.frames_at_observation();
        let actual = shared
            .freeze_action_before(cutoff.clone(), &frames)
            .unwrap();
        let original = shared
            .freeze_action_before_full_history_for_measurement(cutoff.clone(), &frames)
            .unwrap();
        assert_eq!(actual.cancel_objects(), original.cancel_objects());
        assert_eq!(
            actual.cancellation_requests(Some(&scope)),
            original.cancellation_requests(Some(&scope))
        );
        assert_eq!(
            actual.cancellation_requests(None),
            original.cancellation_requests(None)
        );
        let objects = actual.cancel_objects();
        assert!(
            matches!(&objects,ExecutionReadValueV1::Known(objects) if objects.iter().any(|object|matches!(object,StrategyCancelObjectReadV1::Paired{..})))
        );
        let current = serde_json::json!({"cash":shared.ledger().economic_read().unwrap().cash(),"public_group":serde_json::to_value(&cutoff).unwrap()["receipt"]["group"],
            "visible_at":serde_json::to_value(&cutoff).unwrap()["decision_time"]["logical_time_utc_ms"],
            "orders":actual.orders().map(|o|serde_json::json!({"origin":o.origin.as_ref().map(|v|v.intent.global_request_id()),"active":o.active_unfilled_shares,"cancellable":o.cancellable})).collect::<Vec<_>>()});
        if let Some(expected) = &expected {
            assert_eq!(&current, expected);
        } else {
            expected = Some(current);
        }
        let before = account_history_visits_v1();
        let started = std::time::Instant::now();
        for _ in 0..32 {
            let _ = shared
                .freeze_action_before(cutoff.clone(), &frames)
                .unwrap();
        }
        let elapsed = started.elapsed().as_nanos();
        let after = account_history_visits_v1();
        let original_before = account_history_visits_v1();
        let original_started = std::time::Instant::now();
        for _ in 0..32 {
            let _ = shared
                .freeze_action_before_full_history_for_measurement(cutoff.clone(), &frames)
                .unwrap();
        }
        let original_elapsed = original_started.elapsed().as_nanos();
        let original_after = account_history_visits_v1();
        let visits = (
            after.cancellation_source_visits - before.cancellation_source_visits,
            after.latest_cancel_request_visits - before.latest_cancel_request_visits,
            after.paired_intent_visits - before.paired_intent_visits,
            after.cancel_order_visits - before.cancel_order_visits,
        );
        if let Some(expected) = expected_visits {
            assert_eq!(visits, expected, "同当前责任时真实冻结访问不随无关历史增长");
        } else {
            expected_visits = Some(visits);
        }
        println!(
            "freeze_cancel_history_cost={}",
            serde_json::json!({"history":history,"active_orders":2,"active_pair":1,"frames":32,
            "wall_ns":elapsed,"original_wall_ns":original_elapsed,"visits":{"source":after.cancellation_source_visits-before.cancellation_source_visits,
            "latest":after.latest_cancel_request_visits-before.latest_cancel_request_visits,"paired":after.paired_intent_visits-before.paired_intent_visits,
            "orders":after.cancel_order_visits-before.cancel_order_visits},"original_visits":{"source":original_after.cancellation_source_visits-original_before.cancellation_source_visits,
            "latest":original_after.latest_cancel_request_visits-original_before.latest_cancel_request_visits,"paired":original_after.paired_intent_visits-original_before.paired_intent_visits,
            "orders":original_after.cancel_order_visits-original_before.cancel_order_visits}})
        );
        // 从原owner检查点移除新增可恢复字段，沿正式restore入口一次重建。
        let mut checkpoint = Vec::new();
        shared.write_checkpoint(&mut checkpoint).unwrap();
        let mut checkpoint_value: ciborium::Value =
            ciborium::de::from_reader(checkpoint.as_slice()).unwrap();
        let ciborium::Value::Map(fields) = &mut checkpoint_value else {
            panic!("checkpoint")
        };
        let (_, ciborium::Value::Map(registry)) = fields
            .iter_mut()
            .find(|(k, _)| k.as_text() == Some("registry"))
            .unwrap()
        else {
            panic!("registry")
        };
        let (_, ciborium::Value::Map(population)) = registry
            .iter_mut()
            .find(|(k, _)| k.as_text() == Some("action_population"))
            .unwrap()
        else {
            panic!("population")
        };
        population
            .retain(|(k, _)| !matches!(k.as_text(), Some("cancellation_reads" | "pairs_by_order")));
        let mut old = Vec::new();
        ciborium::ser::into_writer(&checkpoint_value, &mut old).unwrap();
        let mut ledger_bytes = Vec::new();
        shared.ledger().write_checkpoint(&mut ledger_bytes).unwrap();
        let restored = SharedExecutionV2::restore_checkpoint(
            LedgerWriterV1::restore_checkpoint(ledger_bytes.as_slice()).unwrap(),
            observation.clone(),
            old.as_slice(),
        )
        .unwrap();
        fixture_registry::assert_cancel_objects_reference(restored.registry());
        fixture_registry::assert_cancellation_requests_reference(restored.registry(), Some(&scope));
        let restored_before = account_history_visits_v1();
        let restored_frozen = restored
            .freeze_action_before(cutoff.clone(), &frames)
            .unwrap();
        let restored_after = account_history_visits_v1();
        assert_eq!(restored_frozen.cancel_objects(), actual.cancel_objects());
        assert_eq!(
            restored_after.paired_intent_visits - restored_before.paired_intent_visits,
            1
        );
        assert_eq!(
            restored_after.latest_cancel_request_visits
                - restored_before.latest_cancel_request_visits,
            1
        );
        // 真实完成引用的实际交付键另记是否出现tie；强制同键在内部索引合同测试覆盖。
        let raw = serde_json::to_value(shared.registry()).unwrap();
        let intents = raw["intents"].as_object().unwrap();
        let mut keys = std::collections::BTreeMap::new();
        for (id, intent) in intents {
            let complete = &intent["completed"];
            if !complete.is_null() {
                let key = (
                    complete["visibility"]["Feedback"]["delivery_sequence"]
                        .as_u64()
                        .unwrap(),
                    complete["ordinal"].as_u64().unwrap(),
                );
                keys.entry(key).or_insert_with(Vec::new).push(id);
            }
        }
        println!(
            "completion_key_groups={}",
            serde_json::json!({"history":history,"completed":history+1,"tied_groups":keys.values().filter(|ids|ids.len()>1).count()})
        );
        drop(frames);
        if history == 0 {
            // 沿真实CancelOrders及原反馈入口取得引用，再验证不完整登记的Unknown→已定位转换。
            let order = shared.registry().orders().next().unwrap();
            let accepted = order.accepted.unwrap().clone();
            let ::execution_normalization::MemoryIdentityKeyV1::Entity(
                ::execution_normalization::SourceNeutralEntityKeyV1::Order { source_order_id },
            ) = &order.order_identity.key
            else {
                panic!("real platform order")
            };
            let platform_id = source_order_id.clone();
            let cancel = Arc::new(ModelIntentV1::CancelOrders {
                platform_order_ids: vec![platform_id.clone()],
            });
            let receipt = shared
                .execute(
                    &mut driver.decision().unwrap(),
                    request("locate-cancel"),
                    Some(cancel.clone()),
                )
                .await
                .unwrap()
                .unwrap();
            for _ in 0..2 {
                driver.next_group().unwrap().unwrap();
                shared.apply_closed().unwrap();
            }
            driver.stop_and_drain().unwrap();
            shared.apply_closed().unwrap();
            let exact = CancellationScopeV1::Orders(vec![&platform_id]);
            fixture_registry::assert_cancellation_requests_reference(
                shared.registry(),
                Some(&exact),
            );
            let ExecutionReadValueV1::Known(Some(completed)) = shared
                .registry()
                .cancellation_requests(Some(&exact))
                .latest_response
            else {
                panic!("real completion")
            };
            let mut partial = SharedExecutionRegistryV2::new(Arc::new(completed.context().clone()));
            let frozen = partial.freeze("locate-cancel".into(), cancel);
            partial.record_sim_receipt(&frozen, &receipt);
            partial.observe(completed);
            let other = ConditionId(super::fixture_market_identity::OTHER_CONDITION_ID.into());
            for condition in [&condition, &other] {
                let market = CancellationScopeV1::Market(condition);
                fixture_registry::assert_cancellation_requests_reference(&partial, Some(&market));
                assert!(matches!(
                    partial.cancellation_requests(Some(&market)).latest_response,
                    ExecutionReadValueV1::Unavailable
                ));
            }
            partial.observe(&accepted);
            for condition in [&condition, &other] {
                fixture_registry::assert_cancellation_requests_reference(
                    &partial,
                    Some(&CancellationScopeV1::Market(condition)),
                );
            }
            assert!(matches!(
                partial.cancellation_requests(Some(&scope)).latest_response,
                ExecutionReadValueV1::Known(Some(_))
            ));
            assert!(matches!(
                partial
                    .cancellation_requests(Some(&CancellationScopeV1::Market(&other)))
                    .latest_response,
                ExecutionReadValueV1::Known(None)
            ));
        } else {
            for _ in 0..2 {
                driver.next_group().unwrap().unwrap();
                shared.apply_closed().unwrap();
            }
            driver.stop_and_drain().unwrap();
            shared.apply_closed().unwrap();
        }
    }
}
