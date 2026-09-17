#[tokio::test(flavor="current_thread")]
async fn memory_ledger_real_source_background_v2_gap_idempotence_and_funds() {
    use ledger::*;
    use ::execution_normalization::{NormalizationRecord,SourceNeutralEvidenceKind};
    use contracts::execution_normalization::*;
    let temp=tempfile::tempdir().unwrap();let root=temp.path().join("dpt");
    let core=recorded_memory_with_probe(&root,true);let context=core.pm_context(CONDITION_ID).unwrap();
    let scenario=sealed_scenario_for_markets(vec![crate::scenario::PlatformMarketV1::try_new(context.market,context.assets.into_iter().map(|a|a.asset).collect::<Vec<_>>().try_into().unwrap()).unwrap()]);
    let path=temp.path().join("sim.jsonl");
    let (mut execution,observation)=memory_execution_with_cash::<noe_projection::MeasuredOrderEvidence>(&path,scenario,SimCallerRoleV1::Paper,128,None,"100").await;
    let mut ledger=LedgerWriterV1::new(AccountAdmissionPolicyV1{per_market_all_in_commitment_limit_pusd:ExactDecimal("1000".into())});
    let ledger_root=temp.path().join("ledger");
    execution.bind_ledger_recording_v2(&mut ledger,&ledger_root).unwrap();
    let release=execution.actor.background_log.as_ref().unwrap().pause_for_tests(false);
    let opening=observation.opening_record().unwrap();assert!(opening.durable_reference().is_none());
    assert!(matches!(ledger.apply_noe_memory_v1(&opening).unwrap(),LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::NewlyApplied{ledger_entry_seq:1})));
    assert_eq!(ledger.projection_v2().unwrap().available_through_utc_ms,None);
    let reject=ReserveIntentV1{request_identity:LedgerRequestIdentityV1("sticky-120".into()),condition_id:ConditionId(CONDITION_ID.into()),requested_principal_pusd:ExactDecimal("120".into()),immediate_fee_reserve_pusd:ExactDecimal("0".into())};
    assert_eq!(ledger.try_reserve_intent(&reject.request_identity,&reject.condition_id,&reject.requested_principal_pusd,&reject.immediate_fee_reserve_pusd).unwrap(),LedgerReservationOutcomeV1::InsufficientGlobalAvailableFunds);
    let source=RecordedPublicGroupSourceV1::open(&root,1,1<<20,SharedRecordedTimes).unwrap();
    let mut driver=FinitePublicConsumptionV1::new(execution,source,"shared-ledger-real".into());
    driver.next_group().unwrap().unwrap();driver.next_group().unwrap().unwrap();
    driver.decision().unwrap().request(BusinessRequestV2::PostOrder(PostOrderRequestV2{context:RequestContextV2{global_request_id:"shared-buy".into(),caller_correlation_id:None},item:post_item("item",&ConditionId(CONDITION_ID.into()),&TokenId(UP_TOKEN_ID.into()),SimBuyOrderKindV1::GtcLimit{limit_price_pusd_per_share:Decimal::new(2,1),size_shares:Decimal::from(5),post_only:true})})).unwrap();
    for _ in 2..=4 {driver.next_group().unwrap().unwrap();}
    driver.next_group().unwrap().unwrap();
    let (_,old_cutoff)=driver.next_group().unwrap().unwrap();
    let records=observation.take_closed_records();
    assert!(!records.is_empty());assert_eq!(old_cutoff.private_feedback_sequence(),0);
    assert!(observation.take_closed_feedback().is_empty(),"同一真实队列已被新入口领取");
    let order=records.iter().find_map(|r|match r.record(){NormalizationRecord::Evidence{evidence:SourceNeutralEvidenceKind::Control{result:SourceNeutralControlResult::Submission{outcome:SourceNeutralSubmissionOutcomeV2::Accepted{order,..}}},..}=>Some(order.order_identity.clone()),_=>None}).unwrap();
    let economic=records.iter().find(|r|matches!(r.record(),NormalizationRecord::EconomicFact{..})).unwrap();
    let evidence=records.iter().find(|r|matches!(r.record(),NormalizationRecord::Evidence{..})&&r.record().source_reference()==economic.record().source_reference()).unwrap();
    assert_eq!(economic.visibility(),evidence.visibility());
    assert_ne!(economic.position().record_ordinal,evidence.position().record_ordinal);
    let mut late=Vec::new();
    for r in &records {if matches!(r.record(),NormalizationRecord::EconomicFact{..}) {late.push(r.clone());} else {ledger.apply_noe_memory_v1(r).unwrap();}}
    for r in &late {ledger.apply_noe_memory_v1(r).unwrap();}
    assert_eq!(ledger.funds_read_v1().order(&order).unwrap().principal_encumbrance_pusd,contracts::ExecutionReadValueV1::Known(ExactDecimal("1".into())));
    let before=ledger.projection_v2().unwrap();
    assert_eq!(before.available_through_utc_ms,Some(28_000));
    for r in &records {assert!(matches!(ledger.apply_noe_memory_v1(r).unwrap(),LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::AlreadyApplied{..})|LedgerMemoryApplyOutcomeV1::NotApplicable));}
    assert_eq!(ledger.projection_v2().unwrap(),before);
    let ::execution_normalization::MemoryIdentityKeyV1::Entity(::execution_normalization::SourceNeutralEntityKeyV1::Order {source_order_id})=&order.key else {panic!("原平台订单实体")};
    driver.decision().unwrap().request(BusinessRequestV2::CancelOrder(CancelOrderRequestV2{context:RequestContextV2{global_request_id:"shared-cancel".into(),caller_correlation_id:None},item:crate::execution::CancelOrderItemV1{item_id:"cancel".into(),platform_order_id:source_order_id.clone()}})).unwrap();
    while driver.next_group().unwrap().is_some() {}
    let remaining=observation.take_closed_records();
    for r in &remaining {ledger.apply_noe_memory_v1(r).unwrap();}
    assert_eq!(ledger.funds_read_v1().order(&order).unwrap().principal_encumbrance_pusd,contracts::ExecutionReadValueV1::Known(ExactDecimal("0".into())));
    let expected=ledger.projection_v2().unwrap();
    release.send(()).unwrap();
    driver.complete(&mut RecordedMaterials{root,hidden:None}).unwrap();
    assert!(opening.durable_reference().is_some());
    let durable=opening.durable_reference().unwrap();
    let view=::execution_normalization::read_durable_noe_record_v1(durable.result()).unwrap();
    assert_eq!(view.position(),opening.position());
    let mut recovered=LedgerWriterV1::open_durable(&ledger_root).unwrap();
    assert_eq!(recovered.projection_v2().unwrap(),expected);
    assert!(matches!(recovered.apply_noe_memory_v1(&opening).unwrap(),LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::AlreadyApplied{ledger_entry_seq:1})));
    assert_eq!(recovered.try_reserve_intent(&reject.request_identity,&reject.condition_id,&reject.requested_principal_pusd,&reject.immediate_fee_reserve_pusd).unwrap(),LedgerReservationOutcomeV1::InsufficientGlobalAvailableFunds);
    assert_eq!(recovered.funds_read_v1().order(&order).unwrap().principal_encumbrance_pusd,contracts::ExecutionReadValueV1::Known(ExactDecimal("0".into())));
    // 原经济直接前驱缺失不得推进任何账户或幂等位置。
    let mut gap=LedgerWriterV1::new(AccountAdmissionPolicyV1{per_market_all_in_commitment_limit_pusd:ExactDecimal("1000".into())});gap.apply_noe_memory_v1(&opening).unwrap();
    let next=remaining.iter().find(|r|matches!(r.record(),NormalizationRecord::EconomicFact{..})).unwrap();
    let unchanged=gap.projection_v2().unwrap();
    assert!(matches!(gap.apply_noe_memory_v1(next).unwrap(),LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::MissingPlatformPredecessor{..})));
    assert_eq!(gap.projection_v2().unwrap(),unchanged);
    for r in records.iter().chain(&remaining) {gap.apply_noe_memory_v1(r).unwrap();}
    assert_eq!(gap.projection_v2().unwrap().platform_source_prefix,expected.platform_source_prefix);
}
