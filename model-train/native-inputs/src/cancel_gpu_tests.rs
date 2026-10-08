//! 同一个原Native撤单账户的有限来源读面；不是GPU连续宿主或第二账本。
use super::*;

pub(super) fn intent(value:&ModelIntentV1)->serde_json::Value {
    fn leg(value:&BuyLegIntentV1)->serde_json::Value {
        let order=match &value.order {
            BuyOrderIntentV1::GtcLimit{limit_price_pusd_per_share,size_shares,post_only}=>serde_json::json!({"GtcLimit":{
                "limit_price_pusd_per_share":limit_price_pusd_per_share,"size_shares":size_shares,"post_only":post_only}}),
            BuyOrderIntentV1::GtdLimit{limit_price_pusd_per_share,size_shares,post_only,expires_at_utc_sec}=>serde_json::json!({"GtdLimit":{
                "limit_price_pusd_per_share":limit_price_pusd_per_share,"size_shares":size_shares,"post_only":post_only,"expires_at_utc_sec":expires_at_utc_sec}}),
            BuyOrderIntentV1::FokMarket{spend_pusd,worst_price_pusd_per_share}=>serde_json::json!({"FokMarket":{
                "spend_pusd":spend_pusd,"worst_price_pusd_per_share":worst_price_pusd_per_share}}),
            BuyOrderIntentV1::FakMarket{spend_pusd,worst_price_pusd_per_share}=>serde_json::json!({"FakMarket":{
                "spend_pusd":spend_pusd,"worst_price_pusd_per_share":worst_price_pusd_per_share}}),
        };
        serde_json::json!({"token_id":value.token_id,"order":order})
    }
    match value {
        ModelIntentV1::SubmitSingleBuy{condition_id,order}=>serde_json::json!({"SubmitSingleBuy":{"condition_id":condition_id,"order":leg(order)}}),
        ModelIntentV1::SubmitPairedBuy{condition_id,orders}=>serde_json::json!({"SubmitPairedBuy":{"condition_id":condition_id,"orders":[leg(&orders[0]),leg(&orders[1])]}}),
        _=>panic!("该原逐腿目录只保存实际BUY正文"),
    }
}

fn reference(record:&::execution_normalization::NoeConsumedRecordRefV2)->serde_json::Value {
    serde_json::json!({"record":record.record(),"position":record.position(),
        "context":record.context(),"visibility":record.visibility()})
}

pub(super) fn export(h:&Harness,g:&NativeInputGroup)->serde_json::Value {
    let view=g.action_before.as_ref().unwrap();
    let raw=h.shared.registry().orders().map(|o|serde_json::json!({
        "identity":o.order_identity,"condition":o.condition_id,"token":o.token_id,
        "origin":o.origin.as_ref().map(|origin|serde_json::json!({
            "request":origin.intent.global_request_id(),"item":origin.batch_item_ordinal,
            "intent":intent(origin.intent.intent())})),
        "accepted":o.accepted.map(reference),"formed":o.formed.map(reference),
        "latest_lifecycle":o.latest_lifecycle.map(reference),"latest_direct":o.latest_direct_feedback.map(reference),
        "latest_cancel":o.latest_cancel_feedback.map(reference),
        "trades":h.shared.registry().order_trades(o.order_identity).map(reference).collect::<Vec<_>>()
    })).collect::<Vec<_>>();
    let orders=view.orders().map(|o|serde_json::json!({"identity":o.order_identity,
        "condition":o.condition_id,"token":o.token_id,"matched":o.matched_shares,
        "active":o.active_unfilled_shares,"cancellable":o.cancellable})).collect::<Vec<_>>();
    let funds=view.order_funds().map(|f|serde_json::json!({"identity":f.order_identity,
        "principal":f.principal_encumbrance_pusd,"fee":f.immediate_fee_encumbrance_pusd,
        "last_sequence":f.last_ledger_entry_seq})).collect::<Vec<_>>();
    let ExecutionReadValueV1::Known(objects)=view.cancel_objects() else{panic!("原对象Known人口")};
    let objects=objects.iter().map(|object|{
        let request=view.cancellation_requests(Some(&object.request_scope()));
        let latest=match request.latest_response {
            ExecutionReadValueV1::Known(v)=>serde_json::json!({"Known":v.map(reference)}),
            ExecutionReadValueV1::Unavailable=>serde_json::json!("Unavailable"),
            ExecutionReadValueV1::NotApplicable=>serde_json::json!("NotApplicable"),
        };
        let (kind,members,condition,origin)=match object {
            CancelObjectReadV2::Single{order}=>(0,vec![order.order_identity],order.condition_id,None),
            CancelObjectReadV2::Paired{origin,orders}=>(1,orders.iter().map(|o|o.order_identity).collect(),
                orders[0].condition_id,Some(origin.global_request_id())),
            CancelObjectReadV2::Market{condition_id,orders}=>(2,orders.iter().map(|o|o.order_identity).collect(),
                Some(*condition_id),None),
        };
        serde_json::json!({"kind":kind,"members":members,"condition":condition,"origin":origin,
            "pending":request.pending_count,"counts":request.response_counts,"latest":latest})
    }).collect::<Vec<_>>();
    serde_json::json!({"stage":format!("native-cancel-{}",view.cutoff().decision_time().logical_time_utc_ms()),
        "logical_at":view.cutoff().decision_time().logical_time_utc_ms(),
        "current":g.current,"old":g.old,"bound_markets":h.native.account_binding_sources_for_tests(),
        "raw_orders":raw,"orders":orders,"order_funds":funds,"cancel_objects":objects,
        "buy_source_receipts":h.buy_receipts,"cancel_source_requests":h.cancel_receipts,"expected":g.cancel,
        "continuous_GPU_account":false,"funds_boundary":"actual_frozen_public_order_funds_cold_read"})
}
