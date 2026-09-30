"""仅在独立诊断副本捕获首个真实支持错误，不改变支持或选择结果。"""
from pathlib import Path


def instrument(out: Path):
    src = out/'model-train/native-inputs/src'
    support = src/'support.rs'
    text = support.read_text()
    labels = ('actor_context', 'repair_action_before', 'repair_market_assets', 'repair_outcome_asset', 'tick_units')
    needle = '.ok_or(InputError::MissingAssociation)?'
    assert text.count(needle) == len(labels)
    for label in labels:
        text = text.replace(needle, '.ok_or_else(|| { eprintln!("support_failure_branch='+label+'"); InputError::MissingAssociation })?', 1)
    text = text.replace('let (market, order) = crate::values::leg(&c.origin)?;', 'let (market, order) = crate::values::leg(&c.origin).map_err(|e| { eprintln!("support_failure_branch=repair_origin_leg"); e })?;')
    text = text.replace('return Err(InputError::MissingAssociation);', 'eprintln!("support_failure_branch=repair_unknown_commitment"); return Err(InputError::MissingAssociation);')
    text += r'''
impl NativeActor {
    pub fn bench_support_failure(&self)->serde_json::Value {
        let row=&self.group.rows[self.row];
        let condition=&row.hidden_state_key.condition_id;
        let context=row.action_context.as_ref();
        let read=self.group.action_before.as_ref();
        let market=read.and_then(|r|r.economics().markets.iter().find(|m| &m.condition_id==condition));
        let commitments=read.map(|read|read.commitments().filter_map(|c| {
            let (m,leg)=crate::values::leg(&c.origin).ok()?;
            if m!=condition {return None;}
            let order=c.order_identity.and_then(|id| read.orders().find(|o|o.order_identity==id));
            Some(serde_json::json!({"request_id":c.origin.intent.global_request_id(),"ordinal":c.origin.batch_item_ordinal,
                "intent":c.origin.intent.intent(),"token":leg.token_id,"order_identity":c.order_identity,
                "remaining_target_shares":c.remaining_target_shares,
                "accepted_ref":order.as_ref().and_then(|o|o.accepted).map(|r|format!("{:?}",r.record().source_reference())),
                "formed_ref":order.as_ref().and_then(|o|o.formed).map(|r|format!("{:?}",r.record().source_reference())),
                "lifecycle_ref":order.as_ref().and_then(|o|o.latest_lifecycle).map(|r|format!("{:?}",r.record().source_reference())),
                "direct_ref":order.as_ref().and_then(|o|o.latest_direct_feedback).map(|r|format!("{:?}",r.record().source_reference()))}))
        }).collect::<Vec<_>>()).unwrap_or_default();
        serde_json::json!({"group":self.group.cutoff.receipt().group(),"input_position":self.group.cutoff.receipt().group().closed_at_input_position,
            "cutoff":self.group.cutoff,"row":self.row,"role":row.role,"condition":condition,"stage":format!("{:?}",self.state.stage),"path":self.path,
            "ticks":context.map(|c|c.ticks.iter().map(|t|serde_json::json!({"available":t.is_some(),"exact":t.as_ref().map(|v|format!("{v:?}"))})).collect::<Vec<_>>()),
            "tokens":context.map(|c|&c.tokens),"buy_enabled":context.map(|c|c.buy_enabled),
            "market_present":market.is_some(),"assets":market.and_then(|m|m.assets.as_ref()),"repair_gap_shares":market.map(|m|&m.repair_gap_shares),
            "commitments":commitments})
    }
}
'''
    support.write_text(text)
    runtime=out/'model-train/native-inputs/examples/common/frame_runtime.rs'
    text=runtime.read_text();needle='let support = actor.support().unwrap();';assert text.count(needle)==2
    text=text.replace(needle, r'''let support = match actor.support() {
                        Ok(value)=>value,
                        Err(error)=> {
                            let path=std::env::var("FRAME_SUPPORT_FAILURE").expect("独立首错产物位置");
                            let mut fields=actor.bench_support_failure();fields["error"]=serde_json::json!(format!("{error:?}"));
                            let file=std::fs::OpenOptions::new().write(true).create_new(true).open(path).unwrap();
                            serde_json::to_writer_pretty(file,&fields).unwrap();
                            panic!("captured first support failure: {error:?}");
                        }
                    };''',1);runtime.write_text(text)
    host=out/'model-train/native-inputs/examples/frame_latency.rs';text=host.read_text()
    needle='let mut count = 0u64;';assert text.count(needle)==1
    text=text.replace(needle,'let stop_after=std::env::var("FRAME_STOP_AFTER_GROUP").ok().map(|v|v.parse::<u64>().unwrap());\n    '+needle)
    text=text.replace('count += 1;', 'count += 1;\n        if stop_after==Some(position) {break;}')
    text=text.replace('"complete":true,"variant":variant', '"complete":true,"support_capture_only":true,"reproduced":false,"stop_after_group":stop_after,"variant":variant')
    host.write_text(text)
