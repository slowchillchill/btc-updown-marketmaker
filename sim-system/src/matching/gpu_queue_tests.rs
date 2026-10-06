//! 原生产FIFO状态对照GPU候选。只测试队列，不伪造NOE或账户经济事实。
use super::*;
use serde_json::{Value,json};
use contracts::ResyncQuality;

fn describe(asset:&AssetQueueV2,side:BookSide,price:Decimal)->Value {
    let levels = match side {BookSide::Bid=>&asset.bids,BookSide::Ask=>&asset.asks};
    let Some(queue) = levels.get(&price) else {return json!({"debt":"0","segments":[]});};
    let segments:Vec<_> = queue.segments.iter().map(|segment|match segment {
        QueueSegmentV2::External {total_base_units,total_quote_units,consumed_base_units}=>json!({
            "kind":"external","base":total_base_units.to_string(),"quote":total_quote_units.to_string(),
            "consumed":consumed_base_units.to_string()}),
        QueueSegmentV2::Account {platform_order_id,remaining_base_units}=>json!({
            "kind":"account","order":platform_order_id,"remaining":remaining_base_units.to_string()}),
    }).collect();
    json!({"debt":queue.account_taker_depletion_debt_units.to_string(),"segments":segments})
}

/// 原合法步骤直接经过原normalizer/FIFO的只读测试基线，不生成经济反馈。
pub(crate) struct HiddenQueueReference {
    queues: ExecutionQueueStateV2,
    condition: String,
}
impl HiddenQueueReference {
    pub(crate) fn new(directory: &SessionMarketDirectoryV1, condition: &str) -> Self {
        Self {queues: ExecutionQueueStateV2::new(directory), condition:condition.to_owned()}
    }
    pub(crate) fn insert_account(&mut self, token:&str, price:Decimal) {
        self.queues.asset_mut(&self.condition,token).unwrap().insert_account_buy_remainder(price,"account-order".into(),3_000_000);
    }
    pub(crate) fn apply(&mut self, event:&NormalizedMarketEventV2<'_>, at:i64, end:i64) -> Vec<Value> {
        if at >= end { self.queues.end_matching(&self.condition).unwrap(); }
        if self.queues.matching_closed(&self.condition).unwrap() { return Vec::new(); }
        let mut maker = Vec::new();
        match event {
            NormalizedMarketEventV2::InitialL2Snapshot {asset,bids,asks,..}=>{
                let bids:Vec<_>=bids.iter().map(|v|(v.price_pusd_per_share,micro_units_v1(v.quantity_shares).unwrap())).collect();
                let asks:Vec<_>=asks.iter().map(|v|(v.price_pusd_per_share,micro_units_v1(v.quantity_shares).unwrap())).collect();
                self.queues.apply_initial_levels(&self.condition,&asset.token_id.0,&bids,&asks).unwrap();
            },
            NormalizedMarketEventV2::LevelExecutionTransition {transition,price_pusd_per_share,quantity_before_shares,quantity_after_shares}=>{
                let effects = self.queues.asset_mut(&self.condition,&transition.asset.token_id.0).unwrap().apply_level_transition(transition.side,
                    *price_pusd_per_share,micro_units_v1(*quantity_before_shares).unwrap(),micro_units_v1(*quantity_after_shares).unwrap()).unwrap();
                maker = effects.maker_depletions.into_iter().map(|effect| json!({"order":effect.platform_order_id,
                    "base":effect.base_units.to_string()})).collect();
            },
            NormalizedMarketEventV2::QualityBoundary {boundary,resync} if *resync==ResyncQuality::Required=>{
                if let QualityScopeV1::Asset(asset)=&boundary.scope {
                    self.queues.asset_mut(&self.condition,&asset.token_id.0).unwrap().clear_external_segments();
                }
            },
            _=>{},
        }
        maker
    }
    pub(crate) fn describe(&self,slots:&[(String,BookSide,Decimal)]) -> Vec<Value> {
        slots.iter().map(|(token,side,price)|self.queues.asset(&self.condition,token)
            .map(|asset|describe(asset,*side,*price)).unwrap()).collect()
    }
}

#[test]
#[ignore="AutoDL同提交原FIFO→GPU精确候选/资源增长验证"]
fn gpu_fifo_preserves_segment_order_taker_debt_and_resync() {
    let bid:Decimal="0.4".parse().unwrap();let ask:Decimal="0.5".parse().unwrap();
    let mut fifo=AssetQueueV2::from_initial_levels(&[(bid,2_000_000)],&[]).unwrap();
    let mut debt=AssetQueueV2::from_initial_levels(&[],&[(ask,10_000_000)]).unwrap();
    let mut checkpoints=Vec::new();
    let mut checkpoint=|name:&str,fifo:&AssetQueueV2,debt:&AssetQueueV2| {
        checkpoints.push(json!({"stage":name,"queues":[describe(fifo,BookSide::Bid,bid),describe(debt,BookSide::Ask,ask)]}));
    };
    checkpoint("initial",&fifo,&debt);
    // 有效GTC余量的生产插入点；这里不承担新订单接纳/费用/账户验证。
    fifo.insert_account_buy_remainder(bid,"account-order".into(),3_000_000);
    checkpoint("account_tail",&fifo,&debt);
    fifo.apply_level_transition(BookSide::Bid,bid,2_000_000,7_000_000).unwrap();
    checkpoint("external_tail",&fifo,&debt);
    let plan=debt.calculate_taker_buy(ask,TakerTargetV1::LimitBase {total_base_units:4_000_000,total_quote_units:2_000_000}).unwrap();
    assert_eq!(plan.matched_base_units,4_000_000);assert_eq!(plan.used_quote_units,2_000_000);
    debt.apply_taker_consumption(plan);
    checkpoint("taker_applied",&fifo,&debt);
    let effects=fifo.apply_level_transition(BookSide::Bid,bid,7_000_000,4_000_000).unwrap();
    assert_eq!(effects.maker_depletions.len(),1);assert_eq!(effects.maker_depletions[0].base_units,1_000_000);
    debt.apply_level_transition(BookSide::Ask,ask,10_000_000,6_000_000).unwrap();
    checkpoint("fifo_depletion_and_debt_echo",&fifo,&debt);
    fifo.clear_external_segments();
    checkpoint("resync_preserves_account",&fifo,&debt);
    fifo.remove_account_order("account-order");
    checkpoint("cancel_removes_target_only",&fifo,&debt);
    debt.clear_external_segments();
    checkpoint("clear",&fifo,&debt);
    // 原planner在同价位两片中保留最初的段索引；第一片耗尽后不能压紧。
    let mut multi=AssetQueueV2::from_initial_levels(&[],&[(ask,2)]).unwrap();
    multi.apply_level_transition(BookSide::Ask,ask,2,5).unwrap();
    let multi_initial=describe(&multi,BookSide::Ask,ask);
    let plan=multi.calculate_taker_buy(ask,TakerTargetV1::Quote(2)).unwrap();
    assert_eq!(plan.fills.len(),2);
    assert_eq!(plan.matched_base_units,5);
    assert_eq!(plan.used_quote_units,2);
    let slices:Vec<_>=plan.fills.iter().map(|part|json!({
        "column":part.segment_index,"base":part.fill.base_units.to_string(),
        "quote":part.fill.quote_units.to_string(),
    })).collect();
    multi.apply_taker_consumption(plan);
    let multi_applied=describe(&multi,BookSide::Ask,ask);
    multi.apply_level_transition(BookSide::Ask,ask,5,0).unwrap();
    let multi_echo=describe(&multi,BookSide::Ask,ask);
    let mut quote_plans=Vec::new();
    for (base,after,budget) in [(2,5,2),(2,5,3),(10,10,1)] {
        let mut queue=AssetQueueV2::from_initial_levels(&[],&[(ask,base)]).unwrap();
        queue.apply_level_transition(BookSide::Ask,ask,base,after).unwrap();
        let initial=describe(&queue,BookSide::Ask,ask);
        let plan=queue.calculate_taker_buy(ask,TakerTargetV1::Quote(budget)).unwrap();
        let slices:Vec<_>=plan.fills.iter().map(|part|json!({"column":part.segment_index,
            "base":part.fill.base_units.to_string(),"quote":part.fill.quote_units.to_string()})).collect();
        let matched=plan.matched_base_units;let used=plan.used_quote_units;
        // FOK整预算不满足时根本不apply；此处只对照生产队列，不生成接受反馈。
        let accepted=used==budget;
        if accepted {queue.apply_taker_consumption(plan);}
        quote_plans.push(json!({"initial":initial,"base":base.to_string(),"after":after.to_string(),
            "budget":budget.to_string(),"matched":matched.to_string(),"used":used.to_string(),
            "accepted":accepted,"slices":slices,"applied":describe(&queue,BookSide::Ask,ask)}));
    }
    let cross_prices:Vec<Decimal>=["0.4","0.5","0.6"].iter().map(|value|value.parse().unwrap()).collect();
    let mut cross_plans=Vec::new();
    for budget in [7_u128,15,16] {
        let levels:Vec<_>=cross_prices.iter().map(|price|(*price,10)).collect();
        let mut source=AssetQueueV2::from_initial_levels(&[],&levels).unwrap();
        let initial:Vec<_>=cross_prices.iter().map(|price|describe(&source,BookSide::Ask,*price)).collect();
        let plan=source.calculate_taker_buy(cross_prices[2],TakerTargetV1::Quote(budget)).unwrap();
        let slices:Vec<_>=plan.fills.iter().map(|part|json!({"price":part.fill.price_pusd_per_share.to_string(),
            "column":part.segment_index,"base":part.fill.base_units.to_string(),"quote":part.fill.quote_units.to_string()})).collect();
        let matched=plan.matched_base_units;let used=plan.used_quote_units;
        if used==budget {source.apply_taker_consumption(plan);}
        cross_plans.push(json!({"budget":budget.to_string(),"matched":matched.to_string(),"used":used.to_string(),
            "initial":initial,"slices":slices,"applied":cross_prices.iter().map(|price|describe(&source,BookSide::Ask,*price)).collect::<Vec<_>>() }));
    }
    let mut floor_sums=Vec::new();
    for (n,m,a,b) in [
        (BigUint::zero(),BigUint::from(7_u8),BigUint::from(9_u8),BigUint::from(17_u8)),
        (BigUint::from(10_u8),BigUint::from(7_u8),BigUint::from(9_u8),BigUint::from(17_u8)),
        ((BigUint::one()<<160_usize)+BigUint::one(),BigUint::from(3_u8),
            (BigUint::one()<<130_usize)+BigUint::from(7_u8),BigUint::from(17_u8)),
        (BigUint::from(101_u8),(BigUint::one()<<160_usize)+BigUint::one(),BigUint::from(23_u8),BigUint::from(41_u8)),
    ] {
        let expected=floor_sum_biguint_v1(n.clone(),m.clone(),a.clone(),b.clone()).unwrap();
        floor_sums.push(json!({"n":n.to_string(),"m":m.to_string(),"a":a.to_string(),"b":b.to_string(),"expected":expected.to_string()}));
    }
    let empty=AssetQueueV2::from_initial_levels(&[],&[]).unwrap();
    let empty_quote=empty.calculate_taker_buy(cross_prices[2],TakerTargetV1::Quote(7)).unwrap();
    assert!(empty_quote.fills.is_empty());
    let mut limit_segments=Vec::new();
    for arguments in [
        [11_u128,7,3,6,13,8,4,2],
        [10,5,0,10,10,6,0,0],
        [10,5,0,5,10,5,0,0],
        [10,5,0,3,10,3,0,0],
        [10,5,4,6,10,5,4,2],
        [1_u128<<96,1_u128<<95,0,1_u128<<95,1_u128<<96,1_u128<<95,0,0],
    ] {
        let [eb,eq,c,x,ob,oq,m,u]=arguments;
        let expected=maximum_base_for_limit_order_segment_v1(eb,eq,c,x,ob,oq,m,u).unwrap();
        limit_segments.push(json!({"arguments":arguments.iter().map(|value|value.to_string()).collect::<Vec<_>>(),
            "result":expected.map(|(base,quote)|[base.to_string(),quote.to_string()])}));
    }
    let mut cross_limits=Vec::new();
    for (target,quote,maximum) in [(25_u128,15_u128,cross_prices[2]),(40,24,cross_prices[2]),(25,15,cross_prices[1])] {
        let levels:Vec<_>=cross_prices.iter().map(|price|(*price,10)).collect();
        let mut source=AssetQueueV2::from_initial_levels(&[],&levels).unwrap();
        let plan=source.calculate_taker_buy(maximum,TakerTargetV1::LimitBase {total_base_units:target,total_quote_units:quote}).unwrap();
        let slices:Vec<_>=plan.fills.iter().map(|part|json!({"price":part.fill.price_pusd_per_share.to_string(),
            "column":part.segment_index,"base":part.fill.base_units.to_string(),"quote":part.fill.quote_units.to_string()})).collect();
        let matched=plan.matched_base_units;let used=plan.used_quote_units;
        source.apply_taker_consumption(plan);
        cross_limits.push(json!({"base":target.to_string(),"quote":quote.to_string(),"maximum":maximum.to_string(),
            "matched":matched.to_string(),"used":used.to_string(),"slices":slices,
            "applied":cross_prices.iter().map(|price|describe(&source,BookSide::Ask,*price)).collect::<Vec<_>>() }));
    }
    let mut taker_fees=Vec::new();
    for (fills,rate,exponent) in [
        (vec![(1_u64,1_000_003_u128,cross_prices[0]),(2,1_000_003,cross_prices[1]),(3,1_000_003,cross_prices[2])],"0.25",2_u32),
        (vec![(1,1_000_040,cross_prices[1]),(2,1_000_040,cross_prices[1])],"0.25",1),
        (vec![(1,1,cross_prices[0])],"0.25",4_000_000_000),
    ] {
        let (total,allocations)=allocate_taker_fee_micro_units_by_key(&fills,rate.parse().unwrap(),exponent).unwrap();
        taker_fees.push(json!({"base":fills.iter().map(|(_,base,_)|base.to_string()).collect::<Vec<_>>(),
            "prices":fills.iter().map(|(_,_,price)|price.to_string()).collect::<Vec<_>>(),
            "rate":rate,"exponent":exponent,"total":total.to_string(),
            "allocations":allocations.iter().map(|value|value.to_string()).collect::<Vec<_>>() }));
    }
    let opening = contracts::AccountOpeningFactV2 {
        creation_request_id:"gpu-account-opening".into(),
        account_instance_key:contracts::AccountInstanceKeyV1 {run_id:"gpu-account-run".into(),
            caller_role:contracts::SimCallerRoleV1::ModelTrain,caller_instance_id:"gpu-account-caller".into(),
            account_id:"gpu-account".into(),platform_source:contracts::PlatformSourceV1::Simulated},
        session_id:"gpu-account-session".into(),initial_platform_scenario_file_sha256:contracts::Sha256Hex("a".repeat(64)),
        initial_account_state:contracts::InitialAccountStateV2 {available_pusd:contracts::ExactDecimal("2000".into())},
    };
    let mut account=PlatformAccountStateV2::try_from_opening_fact(&opening).unwrap();
    let account_condition=ConditionId("gpu-account-condition".into());
    let account_token=TokenId("1".into());
    let describe_account=|account:&PlatformAccountStateV2| {
        let (cash,available,mr,tr,ml,tl,outcomes)=account.economic_balances_for_tests();
        json!({"cash":cash.to_string(),"available":available.to_string(),
            "principal":account.principal_encumbrance_pusd().to_string(),
            "fee":account.immediate_taker_fee_encumbrance_pusd().to_string(),
            "rebates":[mr.to_string(),tr.to_string(),ml.to_string(),tl.to_string()],
            "outcomes":outcomes.iter().map(|(token,amount)|json!([token,amount.to_string()])).collect::<Vec<_>>(),
            "next_sequence":account.next_account_transition_seq().to_string()})
    };
    let account_opened=describe_account(&account);
    let accepted_delta=account.apply_order_accepted_with_required_available("sim-1",&account_condition,1_000,
        5_010_000,"4".parse().unwrap(),"0.01".parse().unwrap()).unwrap();
    let account_accepted=describe_account(&account);
    let fill_delta=account.apply_fill_finality_economic("trade-1",0,&account_condition,&account_token,1_001,
        crate::account::FillEconomicAmountsV1 {base_units:2_000_000,quote_units:1_000_000,immediate_taker_fee_units:10_000}).unwrap();
    let account_filled=describe_account(&account);
    let cancel_delta=account.apply_caller_cancel_release("sim-1",&account_condition,1_002,"3".parse().unwrap()).unwrap().unwrap();
    let account_cancelled=describe_account(&account);
    assert!(account.apply_caller_cancel_release("sim-1",&account_condition,1_003,Decimal::ZERO).unwrap().is_none());
    assert!(matches!(account.apply_order_accepted_with_required_available("sim-2",&account_condition,1_004,
        3_000_000_000,"3000".parse().unwrap(),Decimal::ZERO),Err(crate::account::PlatformAccountError::BalanceUnderflow("available_pusd"))));
    assert_eq!(describe_account(&account),account_cancelled);
    assert!(account.apply_caller_cancel_release("sim-1",&account_condition,1_005,"1".parse().unwrap()).is_err());
    assert_eq!(describe_account(&account),account_cancelled);
    let account_reference=json!({"opened":account_opened,"accepted":account_accepted,"filled":account_filled,
        "cancelled":account_cancelled,"deltas":[accepted_delta,fill_delta,cancel_delta]});
    let input=json!({"checkpoints":checkpoints,"maker_depletion":"1000000","taker_quote":"2000000",
        "multi_plan":{"initial":multi_initial,"slices":slices,"applied":multi_applied,"echo":multi_echo},
        "quote_plans":quote_plans,"cross_quote_plans":cross_plans,"floor_sums":floor_sums,"account":account_reference,"taker_fees":taker_fees,"cross_limits":cross_limits,"limit_segments":limit_segments,"empty_quote_plan":{"matched":empty_quote.matched_base_units.to_string(),"used":empty_quote.used_quote_units.to_string()}});
    let python=std::env::var("B_GPU_PYTHON").unwrap();
    let script=std::env::var("B_GPU_FIFO_VALIDATOR").unwrap();
    let mut child=std::process::Command::new(python).arg(script).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(),&input).unwrap();
    let output=child.wait_with_output().unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    eprintln!("gpu_fifo_reference={}",String::from_utf8(output.stdout).unwrap());
}
