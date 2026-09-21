use std::future::{Ready, ready};

use contracts::{BuyLegIntentV1, BuyOrderIntentV1, ConditionId, ModelIntentV1};
use sim_system::execution::{
    CancelMarketOrdersRequestV2, CancelOrderBatchItemV2, CancelOrderItemV1, CancelOrderRequestV2,
    CancelOrdersRequestV2, MutationRequestReceiptV2, PostOrderBatchItemV2, PostOrderItemV1,
    PostOrderRequestV2, PostOrdersRequestV2, RequestContextV2, SimBuyOrderKindV1, SimBuyOrderV1,
    SimExecution, SimRunActorErrorV2, TrustedDecisionCutoffV2,
};

use crate::{BridgeBackendV1, private};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimIntentCallContextV1 {
    pub request_context: RequestContextV2,
    pub trusted_decision_cutoff: TrustedDecisionCutoffV2,
}

pub type SimIntentImmediateSuccessV1 = MutationRequestReceiptV2;
pub type SimIntentErrorV1 = SimRunActorErrorV2;

pub struct SimAdapterV1<'execution> {
    execution: &'execution mut SimExecution,
}

impl<'execution> SimAdapterV1<'execution> {
    pub(crate) fn bind(execution: &'execution mut SimExecution) -> Self {
        Self { execution }
    }

    fn dispatch_intent(
        &mut self,
        context: SimIntentCallContextV1,
        intent: &ModelIntentV1,
    ) -> Result<MutationRequestReceiptV2, SimRunActorErrorV2> {
        let SimIntentCallContextV1 {
            request_context,
            trusted_decision_cutoff,
        } = context;
        match intent {
            ModelIntentV1::SubmitSingleBuy {
                condition_id,
                order,
            } => self.execution.post_order(
                trusted_decision_cutoff,
                single_buy_request(request_context, condition_id, order)?,
            ),
            ModelIntentV1::SubmitPairedBuy {
                condition_id,
                orders,
            } => self.execution.post_orders(
                trusted_decision_cutoff,
                paired_buy_request(request_context, condition_id, orders)?,
            ),
            ModelIntentV1::CancelOrder { platform_order_id } => self.execution.cancel_order(
                trusted_decision_cutoff,
                cancel_order_request(request_context, platform_order_id),
            ),
            ModelIntentV1::CancelOrders { platform_order_ids } => self.execution.cancel_orders(
                trusted_decision_cutoff,
                cancel_orders_request(request_context, platform_order_ids)?,
            ),
            ModelIntentV1::CancelMarketOrders { condition_id } => {
                self.execution.cancel_market_orders(
                    trusted_decision_cutoff,
                    cancel_market_orders_request(request_context, condition_id),
                )
            }
        }
    }
}

impl<'execution> private::Sealed for SimAdapterV1<'execution> {
    type DispatchFuture<'a>
        = Ready<Result<MutationRequestReceiptV2, SimRunActorErrorV2>>
    where
        Self: 'a;

    fn dispatch<'a, K>(
        &'a mut self,
        context: <Self as BridgeBackendV1>::CallContext,
        intent: &'a ModelIntentV1,
    ) -> Self::DispatchFuture<'a>
    where
        'execution: 'a,
        K: private::DispatchKey,
    {
        ready(self.dispatch_intent(context, intent))
    }
}

impl<'execution> BridgeBackendV1 for SimAdapterV1<'execution> {
    type CallContext = SimIntentCallContextV1;
    type ImmediateSuccess = MutationRequestReceiptV2;
    type Error = SimRunActorErrorV2;
}

fn sim_item_id(global_request_id: &str, batch_item_ordinal: u16) -> String {
    format!("{global_request_id}:{batch_item_ordinal}")
}

fn single_buy_request(
    context: RequestContextV2,
    condition_id: &ConditionId,
    order: &BuyLegIntentV1,
) -> Result<PostOrderRequestV2, SimRunActorErrorV2> {
    let item = post_order_item(&context.global_request_id, 0, condition_id, order)?;
    Ok(PostOrderRequestV2 { context, item })
}

fn paired_buy_request(
    context: RequestContextV2,
    condition_id: &ConditionId,
    orders: &[BuyLegIntentV1; 2],
) -> Result<PostOrdersRequestV2, SimRunActorErrorV2> {
    let first = post_order_item(&context.global_request_id, 0, condition_id, &orders[0])?;
    let second = post_order_item(&context.global_request_id, 1, condition_id, &orders[1])?;
    Ok(PostOrdersRequestV2 {
        context,
        items: vec![
            PostOrderBatchItemV2 {
                batch_item_ordinal: 0,
                item: first,
            },
            PostOrderBatchItemV2 {
                batch_item_ordinal: 1,
                item: second,
            },
        ],
    })
}

fn cancel_order_request(
    context: RequestContextV2,
    platform_order_id: &str,
) -> CancelOrderRequestV2 {
    CancelOrderRequestV2 {
        item: CancelOrderItemV1 {
            item_id: sim_item_id(&context.global_request_id, 0),
            platform_order_id: platform_order_id.to_owned(),
        },
        context,
    }
}

fn cancel_orders_request(
    context: RequestContextV2,
    platform_order_ids: &[String],
) -> Result<CancelOrdersRequestV2, SimRunActorErrorV2> {
    let items = platform_order_ids
        .iter()
        .enumerate()
        .map(|(index, platform_order_id)| {
            let batch_item_ordinal = u16::try_from(index).map_err(|_| {
                SimRunActorErrorV2::InvalidBusinessRequest("批量撤单 ordinal 超出 u16 表示范围")
            })?;
            Ok(CancelOrderBatchItemV2 {
                batch_item_ordinal,
                item: CancelOrderItemV1 {
                    item_id: sim_item_id(&context.global_request_id, batch_item_ordinal),
                    platform_order_id: platform_order_id.clone(),
                },
            })
        })
        .collect::<Result<Vec<_>, SimRunActorErrorV2>>()?;
    Ok(CancelOrdersRequestV2 { context, items })
}

fn cancel_market_orders_request(
    context: RequestContextV2,
    condition_id: &ConditionId,
) -> CancelMarketOrdersRequestV2 {
    CancelMarketOrdersRequestV2 {
        context,
        condition_id: condition_id.clone(),
    }
}

fn post_order_item(
    global_request_id: &str,
    batch_item_ordinal: u16,
    condition_id: &ConditionId,
    leg: &BuyLegIntentV1,
) -> Result<PostOrderItemV1, SimRunActorErrorV2> {
    Ok(PostOrderItemV1 {
        item_id: sim_item_id(global_request_id, batch_item_ordinal),
        order: SimBuyOrderV1 {
            condition_id: condition_id.clone(),
            token_id: leg.token_id.clone(),
            kind: sim_order_kind(&leg.order)?,
        },
    })
}

fn sim_order_kind(order: &BuyOrderIntentV1) -> Result<SimBuyOrderKindV1, SimRunActorErrorV2> {
    Ok(match order {
        BuyOrderIntentV1::GtcLimit {
            limit_price_pusd_per_share,
            size_shares,
            post_only,
        } => SimBuyOrderKindV1::GtcLimit {
            limit_price_pusd_per_share: *limit_price_pusd_per_share,
            size_shares: *size_shares,
            post_only: *post_only,
        },
        BuyOrderIntentV1::GtdLimit {
            limit_price_pusd_per_share,
            size_shares,
            post_only,
            expires_at_utc_sec,
        } => SimBuyOrderKindV1::GtdLimit {
            limit_price_pusd_per_share: *limit_price_pusd_per_share,
            size_shares: *size_shares,
            post_only: *post_only,
            expires_at_utc_ms: expires_at_utc_sec
                .checked_mul(1_000)
                .ok_or(SimRunActorErrorV2::RequestTimeOverflow)?,
        },
        BuyOrderIntentV1::FokMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => SimBuyOrderKindV1::FokMarket {
            spend_pusd: *spend_pusd,
            worst_price_pusd_per_share: *worst_price_pusd_per_share,
        },
        BuyOrderIntentV1::FakMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => SimBuyOrderKindV1::FakMarket {
            spend_pusd: *spend_pusd,
            worst_price_pusd_per_share: *worst_price_pusd_per_share,
        },
    })
}

/// 仅借用当前组的 sealed 后端；不接受调用方替换 cutoff。
pub struct SimGroupAdapterV1<'d, 'run> {
    decision: &'d mut sim_system::execution::PublicGroupDecisionV1<'run>,
}
impl<'d, 'run> crate::IntentBridge<SimGroupAdapterV1<'d, 'run>> {
    pub fn bind_sim_group(
        decision: &'d mut sim_system::execution::PublicGroupDecisionV1<'run>,
    ) -> Self {
        Self::new(SimGroupAdapterV1 { decision })
    }
}
impl BridgeBackendV1 for SimGroupAdapterV1<'_, '_> {
    type CallContext = RequestContextV2;
    type ImmediateSuccess = MutationRequestReceiptV2;
    type Error = SimRunActorErrorV2;
}
impl private::Sealed for SimGroupAdapterV1<'_, '_> {
    type DispatchFuture<'a>
        = Ready<Result<MutationRequestReceiptV2, SimRunActorErrorV2>>
    where
        Self: 'a;
    fn dispatch<'a, K>(
        &'a mut self,
        context: <Self as BridgeBackendV1>::CallContext,
        intent: &'a ModelIntentV1,
    ) -> Self::DispatchFuture<'a>
    where
        Self: 'a,
        K: private::DispatchKey,
    {
        use sim_system::execution::BusinessRequestV2;
        let request = (|| {
            Ok(match intent {
                ModelIntentV1::SubmitSingleBuy {
                    condition_id,
                    order,
                } => {
                    BusinessRequestV2::PostOrder(single_buy_request(context, condition_id, order)?)
                }
                ModelIntentV1::SubmitPairedBuy {
                    condition_id,
                    orders,
                } => BusinessRequestV2::PostOrders(paired_buy_request(
                    context,
                    condition_id,
                    orders,
                )?),
                ModelIntentV1::CancelOrder { platform_order_id } => {
                    BusinessRequestV2::CancelOrder(cancel_order_request(context, platform_order_id))
                }
                ModelIntentV1::CancelOrders { platform_order_ids } => {
                    BusinessRequestV2::CancelOrders(cancel_orders_request(
                        context,
                        platform_order_ids,
                    )?)
                }
                ModelIntentV1::CancelMarketOrders { condition_id } => {
                    BusinessRequestV2::CancelMarketOrders(cancel_market_orders_request(
                        context,
                        condition_id,
                    ))
                }
            })
        })();
        ready(request.and_then(|request| self.decision.request(request)))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::future::Future;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    use contracts::{
        BuyLegIntentV1, BuyOrderIntentV1, ConditionId, ExactDecimal, ExecutionMethodV2,
        InitialAccountStateV2, ModelIntentV1, Sha256Hex, SimCallerRoleV1, TokenId,
    };
    use sim_system::execution::{
        BusinessDirectResponseV2, BusinessProjectionBatchV2, BusinessProjectionPortV2,
        CancelMarketOrdersRequestV2, CancelOrderBatchItemV2, CancelOrderItemV1,
        CancelOrderRequestV2, CancelOrdersRequestV2, Decimal, MutationRequestReceiptV2,
        PlatformPrefixAdvanceV2, PostOrderBatchItemV2, PostOrderItemV1, PostOrderRequestV2,
        PostOrdersRequestV2, PublicVisibilityCursorV1, RequestContextV2, RunDurabilityRoleV2,
        RunOpenedV2, SessionSpecV3, SimBuyOrderKindV1, SimBuyOrderV1, SimExecution,
        SimRunActorErrorV2, TrustedDecisionCutoffV2,
    };
    use sim_system::finality::TradeFinalityPolicyV2;
    use sim_system::latency_profile::load_embedded_proxy_latency_profile;
    use sim_system::platform_rules::{
        HistoricalProcessSnapshotRequestV1, ImplementationIdentityV1,
        load_historical_process_snapshot_v3,
    };
    use sim_system::scenario::SealedPlatformScenarioV1;

    use crate::IntentBridge;

    use super::{
        SimIntentCallContextV1, cancel_market_orders_request, cancel_order_request,
        cancel_orders_request, paired_buy_request, sim_order_kind, single_buy_request,
    };

    const CONDITION_ID: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
    const RULE_SNAPSHOT_SHA256: &str =
        "d63c0e7b09edc4b2c4c3f0295f806b25f6f5fd03663cdf60ff062117e53e4e54";
    const MAX_GROUP_BYTES: usize = 2 * 1024 * 1024;

    fn decimal(value: &str) -> Decimal {
        value.parse().expect("测试十进制定点数字面值必须合法")
    }

    fn request_context(global_request_id: &str) -> RequestContextV2 {
        RequestContextV2 {
            global_request_id: global_request_id.to_owned(),
            caller_correlation_id: Some(format!("correlation-{global_request_id}")),
        }
    }

    fn call_context(global_request_id: &str) -> SimIntentCallContextV1 {
        SimIntentCallContextV1 {
            request_context: request_context(global_request_id),
            trusted_decision_cutoff: TrustedDecisionCutoffV2 {
                logical_time_utc_ms: 1_200,
                public_visibility: PublicVisibilityCursorV1::default(),
                private_feedback_sequence: 0,
            },
        }
    }

    fn leg(token_id: &str, order: BuyOrderIntentV1) -> BuyLegIntentV1 {
        BuyLegIntentV1 {
            token_id: TokenId(token_id.to_owned()),
            order,
        }
    }

    fn resolve_ready<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let mut context = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("Sim Adapter 必须直接返回现有同步 SimExecution 结果"),
        }
    }

    #[derive(Default)]
    struct CapturedProjectionsV1 {
        durable_through: u64,
        batches: Vec<Arc<BusinessProjectionBatchV2>>,
    }

    struct CapturingProjectionPortV1 {
        captured: Rc<RefCell<CapturedProjectionsV1>>,
    }

    impl BusinessProjectionPortV2 for CapturingProjectionPortV1 {
        fn try_accept(&mut self, batch: Arc<BusinessProjectionBatchV2>) -> bool {
            let mut captured = self.captured.borrow_mut();
            captured.durable_through = batch.projection_batch_sequence;
            captured.batches.push(batch);
            true
        }

        fn durable_through(&self, _run_id: &str) -> u64 {
            self.captured.borrow().durable_through
        }
    }

    struct SimFixtureV1 {
        execution: SimExecution,
        captured: Rc<RefCell<CapturedProjectionsV1>>,
        _directory: tempfile::TempDir,
    }

    impl SimFixtureV1 {
        fn new(run_id: &str) -> Self {
            let rule_snapshot =
                load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
                    snapshot_path: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                        .join("../sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json"),
                    expected_file_sha256: Sha256Hex(RULE_SNAPSHOT_SHA256.to_owned()),
                    replay_causal_start_utc_ms: i64::MAX,
                })
                .expect("加载仓库锁定的规则快照夹具");
            let finality_policy = Arc::new(
                TradeFinalityPolicyV2::decode_and_seal(
                    br#"{
                        "schema_version":"trade_finality_policy_v2",
                        "normal_path":{
                            "steps":[
                                {"state":"MATCHED","offset_ms":0},
                                {"state":"MINED","offset_ms":10},
                                {"state":"CONFIRMED","offset_ms":20}
                            ],
                            "economic_application_ordinal":1
                        },
                        "failure_overrides":[]
                    }"#,
                )
                .expect("构造成功最终性策略夹具"),
            );
            let scenario = Arc::new(
                SealedPlatformScenarioV1::decode_and_seal(
                    br#"{
                        "schema_version":"platform_scenario_v1",
                        "scenario_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "market_set_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "markets":[{
                            "market":{
                                "condition_id":"0x1111111111111111111111111111111111111111111111111111111111111111",
                                "clob_market_id":"123",
                                "window":{"start_at_utc_ms":1000,"end_at_utc_ms":1000000}
                            },
                            "assets":[
                                {"token_id":"1","outcome":{"index":0,"label":"Up"}},
                                {"token_id":"2","outcome":{"index":1,"label":"Down"}}
                            ]
                        }]
                    }"#,
                )
                .expect("构造单市场场景夹具"),
            );
            let latency_profile =
                load_embedded_proxy_latency_profile().expect("加载锁定的代理时延配置档");
            let directory = tempfile::tempdir().expect("创建 SimExecution 临时目录");
            let captured = Rc::new(RefCell::new(CapturedProjectionsV1::default()));
            let projection_port = Box::new(CapturingProjectionPortV1 {
                captured: Rc::clone(&captured),
            });
            let opened = RunOpenedV2 {
                projection_version: Default::default(),
                run_id: run_id.to_owned(),
                durability_role: RunDurabilityRoleV2::CleanRun,
                platform_rule_snapshot_file_sha256: rule_snapshot.file_sha256().clone(),
                trade_finality_policy_file_sha256: finality_policy.policy_id().clone(),
                platform_scenario_file_sha256: scenario.file_sha256().clone(),
                proxy_latency_profile_file_sha256: latency_profile.file_sha256().clone(),
                implementation_identity: ImplementationIdentityV1 {
                    git_commit: "a".repeat(40),
                    source_identity_sha256: Sha256Hex("b".repeat(64)),
                    cargo_lock_sha256: Sha256Hex("c".repeat(64)),
                },
                market_event_source_identity_sha256: Some(Sha256Hex("d".repeat(64))),
            };
            let mut execution = SimExecution::create_new(
                &directory.path().join("sim-event-log.jsonl"),
                opened,
                rule_snapshot,
                finality_policy,
                scenario,
                latency_profile,
                projection_port,
                MAX_GROUP_BYTES,
            )
            .expect("创建真实 SimExecution 夹具");
            execution
                .create_session(SessionSpecV3 {
                    run_id: run_id.to_owned(),
                    session_id: "session-intent-bridge".to_owned(),
                    account_id: "account-intent-bridge".to_owned(),
                    caller_role: SimCallerRoleV1::ModelTrain,
                    caller_instance_id: "caller-intent-bridge".to_owned(),
                    creation_request_id: "create-intent-bridge".to_owned(),
                    response_bridge_binding_id: "response-intent-bridge".to_owned(),
                    initial_account_state: InitialAccountStateV2 {
                        available_pusd: ExactDecimal("1000".to_owned()),
                    },
                    session_seed: Sha256Hex("e".repeat(64)),
                })
                .expect("创建真实 SimExecution 会话夹具");
            execution
                .activate_session("activate-intent-bridge".to_owned())
                .expect("激活真实 SimExecution 会话夹具");
            Self {
                execution,
                captured,
                _directory: directory,
            }
        }

        fn execute(
            &mut self,
            global_request_id: &str,
            intent: &ModelIntentV1,
        ) -> Result<MutationRequestReceiptV2, SimRunActorErrorV2> {
            let mut bridge = IntentBridge::bind_sim(&mut self.execution);
            resolve_ready(bridge.execute(call_context(global_request_id), intent))
        }

        fn drive_until_responses(&mut self, request_ids: &[&str]) {
            for _ in 0..64 {
                let all_seen = {
                    let captured = self.captured.borrow();
                    request_ids.iter().all(|request_id| {
                        captured
                            .batches
                            .iter()
                            .flat_map(|batch| batch.direct_responses())
                            .any(|response| response_request_id(response) == *request_id)
                    })
                };
                if all_seen {
                    return;
                }
                let required = self
                    .execution
                    .next_required_logical_drive()
                    .expect("未完成请求必须存在必要逻辑推进");
                self.execution
                    .drive_platform_prefix(PlatformPrefixAdvanceV2 {
                        path: required.path,
                        closed_through_utc_ms: required.required_at_utc_ms,
                        next_index_position: 0,
                    })
                    .expect("推进真实 SimExecution 到直接响应");
            }
            panic!("固定逻辑推进次数内未收到全部直接响应");
        }
    }

    fn response_request_id(response: &BusinessDirectResponseV2) -> &str {
        match response {
            BusinessDirectResponseV2::PostOrder(response) => &response.metadata.global_request_id,
            BusinessDirectResponseV2::PostOrders(response) => &response.metadata.global_request_id,
            BusinessDirectResponseV2::CancelOrder(response) => &response.metadata.global_request_id,
            BusinessDirectResponseV2::CancelOrders(response) => {
                &response.metadata.global_request_id
            }
            BusinessDirectResponseV2::CancelMarketOrders(response) => {
                &response.metadata.global_request_id
            }
            BusinessDirectResponseV2::CancelAllOrders(response) => {
                &response.metadata.global_request_id
            }
            BusinessDirectResponseV2::Order(response) => &response.metadata.global_request_id,
            BusinessDirectResponseV2::Orders(response) => &response.metadata.global_request_id,
            BusinessDirectResponseV2::Trades(response) => &response.metadata.global_request_id,
        }
    }

    #[test]
    fn maps_four_buy_kinds_with_literal_fields_and_checked_gtd_units() {
        assert_eq!(
            sim_order_kind(&BuyOrderIntentV1::GtcLimit {
                limit_price_pusd_per_share: decimal("0.31"),
                size_shares: decimal("5.25"),
                post_only: true,
            }),
            Ok(SimBuyOrderKindV1::GtcLimit {
                limit_price_pusd_per_share: decimal("0.31"),
                size_shares: decimal("5.25"),
                post_only: true,
            })
        );
        assert_eq!(
            sim_order_kind(&BuyOrderIntentV1::GtdLimit {
                limit_price_pusd_per_share: decimal("0.42"),
                size_shares: decimal("6.5"),
                post_only: false,
                expires_at_utc_sec: 401,
            }),
            Ok(SimBuyOrderKindV1::GtdLimit {
                limit_price_pusd_per_share: decimal("0.42"),
                size_shares: decimal("6.5"),
                post_only: false,
                expires_at_utc_ms: 401_000,
            })
        );
        assert_eq!(
            sim_order_kind(&BuyOrderIntentV1::FokMarket {
                spend_pusd: decimal("7.75"),
                worst_price_pusd_per_share: decimal("0.53"),
            }),
            Ok(SimBuyOrderKindV1::FokMarket {
                spend_pusd: decimal("7.75"),
                worst_price_pusd_per_share: decimal("0.53"),
            })
        );
        assert_eq!(
            sim_order_kind(&BuyOrderIntentV1::FakMarket {
                spend_pusd: decimal("8.125"),
                worst_price_pusd_per_share: decimal("0.64"),
            }),
            Ok(SimBuyOrderKindV1::FakMarket {
                spend_pusd: decimal("8.125"),
                worst_price_pusd_per_share: decimal("0.64"),
            })
        );
        assert_eq!(
            sim_order_kind(&BuyOrderIntentV1::GtdLimit {
                limit_price_pusd_per_share: decimal("0.5"),
                size_shares: decimal("5"),
                post_only: false,
                expires_at_utc_sec: i64::MAX,
            }),
            Err(SimRunActorErrorV2::RequestTimeOverflow)
        );
    }

    #[test]
    fn builds_five_native_request_shapes_with_stable_order_and_scope() {
        let condition_id = ConditionId(CONDITION_ID.to_owned());
        let single_context = request_context("single-literal");
        let single_leg = leg(
            "1",
            BuyOrderIntentV1::GtcLimit {
                limit_price_pusd_per_share: decimal("0.21"),
                size_shares: decimal("9"),
                post_only: true,
            },
        );
        assert_eq!(
            single_buy_request(single_context.clone(), &condition_id, &single_leg),
            Ok(PostOrderRequestV2 {
                context: single_context,
                item: PostOrderItemV1 {
                    item_id: "single-literal:0".to_owned(),
                    order: SimBuyOrderV1 {
                        condition_id: ConditionId(CONDITION_ID.to_owned()),
                        token_id: TokenId("1".to_owned()),
                        kind: SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: decimal("0.21"),
                            size_shares: decimal("9"),
                            post_only: true,
                        },
                    },
                },
            })
        );

        let paired_context = request_context("paired-literal");
        let paired_legs = [
            leg(
                "2",
                BuyOrderIntentV1::FokMarket {
                    spend_pusd: decimal("11"),
                    worst_price_pusd_per_share: decimal("0.71"),
                },
            ),
            leg(
                "1",
                BuyOrderIntentV1::FakMarket {
                    spend_pusd: decimal("12"),
                    worst_price_pusd_per_share: decimal("0.72"),
                },
            ),
        ];
        assert_eq!(
            paired_buy_request(paired_context.clone(), &condition_id, &paired_legs),
            Ok(PostOrdersRequestV2 {
                context: paired_context,
                items: vec![
                    PostOrderBatchItemV2 {
                        batch_item_ordinal: 0,
                        item: PostOrderItemV1 {
                            item_id: "paired-literal:0".to_owned(),
                            order: SimBuyOrderV1 {
                                condition_id: ConditionId(CONDITION_ID.to_owned()),
                                token_id: TokenId("2".to_owned()),
                                kind: SimBuyOrderKindV1::FokMarket {
                                    spend_pusd: decimal("11"),
                                    worst_price_pusd_per_share: decimal("0.71"),
                                },
                            },
                        },
                    },
                    PostOrderBatchItemV2 {
                        batch_item_ordinal: 1,
                        item: PostOrderItemV1 {
                            item_id: "paired-literal:1".to_owned(),
                            order: SimBuyOrderV1 {
                                condition_id: ConditionId(CONDITION_ID.to_owned()),
                                token_id: TokenId("1".to_owned()),
                                kind: SimBuyOrderKindV1::FakMarket {
                                    spend_pusd: decimal("12"),
                                    worst_price_pusd_per_share: decimal("0.72"),
                                },
                            },
                        },
                    },
                ],
            })
        );

        let cancel_context = request_context("cancel-one-literal");
        assert_eq!(
            cancel_order_request(cancel_context.clone(), "sim-9"),
            CancelOrderRequestV2 {
                context: cancel_context,
                item: CancelOrderItemV1 {
                    item_id: "cancel-one-literal:0".to_owned(),
                    platform_order_id: "sim-9".to_owned(),
                },
            }
        );

        let cancel_many_context = request_context("cancel-many-literal");
        assert_eq!(
            cancel_orders_request(
                cancel_many_context.clone(),
                &["sim-9".to_owned(), "sim-2".to_owned(), "sim-9".to_owned()],
            ),
            Ok(CancelOrdersRequestV2 {
                context: cancel_many_context,
                items: vec![
                    CancelOrderBatchItemV2 {
                        batch_item_ordinal: 0,
                        item: CancelOrderItemV1 {
                            item_id: "cancel-many-literal:0".to_owned(),
                            platform_order_id: "sim-9".to_owned(),
                        },
                    },
                    CancelOrderBatchItemV2 {
                        batch_item_ordinal: 1,
                        item: CancelOrderItemV1 {
                            item_id: "cancel-many-literal:1".to_owned(),
                            platform_order_id: "sim-2".to_owned(),
                        },
                    },
                    CancelOrderBatchItemV2 {
                        batch_item_ordinal: 2,
                        item: CancelOrderItemV1 {
                            item_id: "cancel-many-literal:2".to_owned(),
                            platform_order_id: "sim-9".to_owned(),
                        },
                    },
                ],
            })
        );

        let market_context = request_context("cancel-market-literal");
        assert_eq!(
            cancel_market_orders_request(market_context.clone(), &condition_id),
            CancelMarketOrdersRequestV2 {
                context: market_context,
                condition_id: ConditionId(CONDITION_ID.to_owned()),
            }
        );
    }

    #[test]
    fn real_sim_execution_observes_each_exact_method_once_and_preserves_order() {
        let mut fixture = SimFixtureV1::new("run-intent-bridge-five-methods");
        let condition_id = ConditionId(CONDITION_ID.to_owned());
        let intents = [
            ModelIntentV1::SubmitSingleBuy {
                condition_id: condition_id.clone(),
                order: leg(
                    "1",
                    BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share: decimal("0.31"),
                        size_shares: decimal("5"),
                        post_only: false,
                    },
                ),
            },
            ModelIntentV1::SubmitPairedBuy {
                condition_id: condition_id.clone(),
                orders: [
                    leg(
                        "2",
                        BuyOrderIntentV1::FokMarket {
                            spend_pusd: decimal("6"),
                            worst_price_pusd_per_share: decimal("0.61"),
                        },
                    ),
                    leg(
                        "1",
                        BuyOrderIntentV1::GtdLimit {
                            limit_price_pusd_per_share: decimal("0.41"),
                            size_shares: decimal("7"),
                            post_only: true,
                            expires_at_utc_sec: 401,
                        },
                    ),
                ],
            },
            ModelIntentV1::CancelOrder {
                platform_order_id: "sim-7".to_owned(),
            },
            ModelIntentV1::CancelOrders {
                platform_order_ids: vec![
                    "sim-9".to_owned(),
                    "sim-2".to_owned(),
                    "sim-9".to_owned(),
                ],
            },
            ModelIntentV1::CancelMarketOrders {
                condition_id: condition_id.clone(),
            },
        ];
        let request_ids = [
            "sim-single",
            "sim-paired",
            "sim-cancel-one",
            "sim-cancel-many",
            "sim-cancel-market",
        ];
        let receipts = request_ids
            .iter()
            .zip(intents.iter())
            .map(|(request_id, intent)| {
                fixture
                    .execute(request_id, intent)
                    .expect("真实 SimExecution 接纳适配请求")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            receipts
                .iter()
                .map(MutationRequestReceiptV2::input_sequence)
                .collect::<Vec<_>>(),
            vec![3, 4, 5, 6, 7]
        );
        assert_eq!(
            receipts
                .iter()
                .map(MutationRequestReceiptV2::scheduled_action_count)
                .collect::<Vec<_>>(),
            vec![1, 2, 1, 2, 1]
        );

        fixture.drive_until_responses(&request_ids);
        let captured = fixture.captured.borrow();
        let responses = request_ids
            .iter()
            .map(|request_id| {
                captured
                    .batches
                    .iter()
                    .flat_map(|batch| batch.direct_responses())
                    .find(|response| response_request_id(response) == *request_id)
                    .expect("捕获指定请求的直接响应")
            })
            .collect::<Vec<_>>();

        let BusinessDirectResponseV2::PostOrder(single) = responses[0] else {
            panic!("SubmitSingleBuy 必须形成 PostOrder 直接响应")
        };
        assert_eq!(single.metadata.method, ExecutionMethodV2::PostOrder);
        assert_eq!(single.item.item_id, "sim-single:0");
        assert_eq!(single.item.batch_item_ordinal, 0);
        assert_eq!(single.item.condition_id, condition_id);
        assert_eq!(single.item.token_id, TokenId("1".to_owned()));

        let BusinessDirectResponseV2::PostOrders(paired) = responses[1] else {
            panic!("SubmitPairedBuy 必须形成 PostOrders 直接响应")
        };
        assert_eq!(paired.metadata.method, ExecutionMethodV2::PostOrders);
        assert_eq!(paired.items.len(), 2);
        assert_eq!(paired.items[0].item_id, "sim-paired:0");
        assert_eq!(paired.items[0].batch_item_ordinal, 0);
        assert_eq!(paired.items[0].token_id, TokenId("2".to_owned()));
        assert_eq!(paired.items[1].item_id, "sim-paired:1");
        assert_eq!(paired.items[1].batch_item_ordinal, 1);
        assert_eq!(paired.items[1].token_id, TokenId("1".to_owned()));

        let BusinessDirectResponseV2::CancelOrder(cancel_one) = responses[2] else {
            panic!("CancelOrder 必须形成 CancelOrder 直接响应")
        };
        assert_eq!(cancel_one.metadata.method, ExecutionMethodV2::CancelOrder);
        assert_eq!(cancel_one.target.platform_order_id, "sim-7");
        assert_eq!(cancel_one.target.input_item_refs.len(), 1);
        assert_eq!(
            cancel_one.target.input_item_refs[0].item_id,
            "sim-cancel-one:0"
        );
        assert_eq!(cancel_one.target.input_item_refs[0].batch_item_ordinal, 0);

        let BusinessDirectResponseV2::CancelOrders(cancel_many) = responses[3] else {
            panic!("CancelOrders 必须形成 CancelOrders 直接响应")
        };
        assert_eq!(cancel_many.metadata.method, ExecutionMethodV2::CancelOrders);
        assert_eq!(cancel_many.targets.len(), 2);
        assert_eq!(cancel_many.targets[0].platform_order_id, "sim-9");
        assert_eq!(cancel_many.targets[0].input_item_refs.len(), 2);
        assert_eq!(
            cancel_many.targets[0]
                .input_item_refs
                .iter()
                .map(|item| (item.item_id.as_str(), item.batch_item_ordinal))
                .collect::<Vec<_>>(),
            vec![("sim-cancel-many:0", 0), ("sim-cancel-many:2", 2)]
        );
        assert_eq!(cancel_many.targets[1].platform_order_id, "sim-2");
        assert_eq!(cancel_many.targets[1].input_item_refs.len(), 1);
        assert_eq!(
            cancel_many.targets[1].input_item_refs[0].batch_item_ordinal,
            1
        );

        let BusinessDirectResponseV2::CancelMarketOrders(cancel_market) = responses[4] else {
            panic!("CancelMarketOrders 必须形成市场级响应，不能成为会话级 CancelAllOrders")
        };
        assert_eq!(
            cancel_market.metadata.method,
            ExecutionMethodV2::CancelMarketOrders
        );
    }

    #[test]
    fn propagates_sim_errors_and_accepts_large_cancel_batches() {
        let mut fixture = SimFixtureV1::new("run-intent-bridge-errors");
        let condition_id = ConditionId(CONDITION_ID.to_owned());

        assert_eq!(
            fixture.execute(
                "invalid-cancel",
                &ModelIntentV1::CancelOrder {
                    platform_order_id: "not-a-sim-order".to_owned(),
                },
            ),
            Err(SimRunActorErrorV2::InvalidBusinessRequest(
                "撤单项身份或 V2 平台订单身份非法"
            ))
        );
        assert_eq!(
            fixture.execute(
                "overflowing-gtd",
                &ModelIntentV1::SubmitSingleBuy {
                    condition_id: condition_id.clone(),
                    order: leg(
                        "1",
                        BuyOrderIntentV1::GtdLimit {
                            limit_price_pusd_per_share: decimal("0.5"),
                            size_shares: decimal("5"),
                            post_only: false,
                            expires_at_utc_sec: i64::MAX,
                        },
                    ),
                },
            ),
            Err(SimRunActorErrorV2::RequestTimeOverflow)
        );

        assert_eq!(
            fixture.execute(
                "invalid-cancel-count",
                &ModelIntentV1::CancelOrders {
                    platform_order_ids: Vec::new()
                }
            ),
            Err(SimRunActorErrorV2::InvalidBusinessRequest(
                "批量撤单不能为空"
            ))
        );

        let mut maximal_ids = vec!["sim-1".to_owned(); usize::from(u16::MAX) + 1];
        let converted =
            cancel_orders_request(request_context("ordinal-end"), &maximal_ids).unwrap();
        assert_eq!(converted.items.len(), 65_536);
        assert_eq!(converted.items.last().unwrap().batch_item_ordinal, u16::MAX);
        assert_eq!(
            converted.items.last().unwrap().item.item_id,
            "ordinal-end:65535"
        );
        maximal_ids.push("sim-2".to_owned());
        assert_eq!(
            fixture.execute(
                "ordinal-overflow",
                &ModelIntentV1::CancelOrders {
                    platform_order_ids: maximal_ids
                }
            ),
            Err(SimRunActorErrorV2::InvalidBusinessRequest(
                "批量撤单 ordinal 超出 u16 表示范围"
            ))
        );

        let thousand_ids = (1..=1_001)
            .map(|index| format!("sim-{index}"))
            .collect::<Vec<_>>();
        let accepted = fixture
            .execute(
                "valid-cancel-count",
                &ModelIntentV1::CancelOrders {
                    platform_order_ids: thousand_ids,
                },
            )
            .expect("1001 项批撤必须完整交给 SimExecution");
        assert_eq!(accepted.input_sequence(), 3);
        assert_eq!(accepted.scheduled_action_count(), 1_001);
    }
}
