//! 原生产FIFO状态对照GPU候选。只测试队列，不伪造NOE或账户经济事实。
use super::*;
use serde_json::{Value,json};

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
    let input=json!({"checkpoints":checkpoints,"maker_depletion":"1000000","taker_quote":"2000000",
        "multi_plan":{"initial":multi_initial,"slices":slices,"applied":multi_applied,"echo":multi_echo},
        "quote_plans":quote_plans});
    let python=std::env::var("B_GPU_PYTHON").unwrap();
    let script=std::env::var("B_GPU_FIFO_VALIDATOR").unwrap();
    let mut child=std::process::Command::new(python).arg(script).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(),&input).unwrap();
    let output=child.wait_with_output().unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    eprintln!("gpu_fifo_reference={}",String::from_utf8(output.stdout).unwrap());
}
