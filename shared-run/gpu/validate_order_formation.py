"""原V1 Registry真实公开前缀与设备生产形成读面的独立对照。"""
import json,sys,time
from types import SimpleNamespace
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'sim-system/gpu'))
from validate_pending_post import T,run,integers,rational,values
from validate_registry import event_columns,position_key
from validate_projection import expected_semantic
from registry_projection import public_rows_from_closed
from registry import PublicRegistryState,FrozenIntentOrders
from commitments import SubmittedLegs,bind_submission_receipt
from core import EvidenceBank,OUT_EVIDENCE
import torch


def main():
    started=time.perf_counter();source=json.load(sys.stdin);orders=source['raw_orders'];requests={r['request']:i for i,r in enumerate(source['buy_source_receipts'])}
    refs={position_key(r):r for order in orders for r in [order['accepted'],order['formed'],*order['trades']] if r is not None}
    references=sorted(refs.values(),key=lambda r:(r['visibility']['Feedback']['delivery_sequence'],r['position']['record_ordinal']))
    def sequence(identity,kind):return int(json.loads(identity.split(':',1)[1])[-1][kind]['source_'+kind.lower()+'_id'].removeprefix('sim-'))
    identities={o['identity']:sequence(o['identity'],'Order') for o in orders}
    for r in references:
        body=r['record']['Evidence']['evidence'];trade=body.get('TradeLifecycleV3',body.get('TradeLifecycle'))
        if trade:
            value=trade['result'];value=value.get('result',value);identities[value['trade_identity']]=sequence(value['trade_identity'],'Trade')
    catalog=dict(contexts={r['position']['context_reference']:0 for r in references},runs={r['position']['source_watermark']['run_id']:0 for r in references},references={position_key(r):(i,r) for i,r in enumerate(references)})
    metadata=run(lambda:event_columns(references,identities,0,requests,catalog))
    semantics=[];schemas=[]
    for r in references:
        body=r['record']['Evidence']['evidence'];kind,value=next(iter(body.items()))
        if kind=='Control':
            value=value['result']['Submission']['outcome']['Accepted'];location=value['order'];schemas.append(27 if 'QuoteBudget' in value['measurement'] else 3)
        elif kind=='OrderLifecycle':location=value['result'];schemas.append(7)
        else:location=value['result'].get('result',value['result']);schemas.append(8)
        semantics.append(run(lambda r=r,location=location:expected_semantic(r,token_index=0 if location['token_id']=='1' else 1)))
    width=max(v.size(1) for v in semantics);semantic=torch.cat([torch.nn.functional.pad(v,(0,width-v.size(1))) for v in semantics])
    count=len(references);z=T([0]*count);rows=T([list(range(count))]);on=rows>=0
    bank=EvidenceBank(T([[0]]*count),T([[0]]*count),semantic,T([[0]]*count),z,T([[0,0,0,0]]*count),z,torch.zeros_like(z,dtype=torch.int32),T(schemas))
    batch=SimpleNamespace(rows=rows,emitted=on,normalized=SimpleNamespace(outcomes=T([[OUT_EVIDENCE]*count])),private_sequences=metadata.private_sequences,record_ordinals=metadata.record_ordinals,batch_sequences=metadata.batch_sequences,context_slots=metadata.context_slots,request_slots=metadata.request_slots,item_ordinals=metadata.item_ordinals,run_slots=metadata.run_slots,platform_at=metadata.platform_at,received_at=metadata.received_at)
    closed=SimpleNamespace(record_refs=rows,emitted=on,fault=T([0],torch.int32),delivered_sequences=metadata.delivery_sequences,available_at=metadata.available_at)
    events,binding=run(lambda:public_rows_from_closed(bank,[batch],closed,metadata.receive_rank))
    state=PublicRegistryState(events);present=T([[True]*len(orders)],torch.bool)
    directory=FrozenIntentOrders(present,run(lambda:integers([identities[o['identity']] for o in orders])),present,rational(['0']*len(orders)),torch.zeros_like(present),torch.zeros_like(present))
    leg_requests=T([[requests[o['origin']['request']] for o in orders]]);ordinals=T([[o['origin']['item'] for o in orders]])
    legs=SubmittedLegs(present,torch.zeros_like(present),leg_requests,ordinals,rational(['0']*len(orders)),torch.zeros_like(present))
    for receipt in source['buy_source_receipts']:
        original=SimpleNamespace(registered=T([True],torch.bool),duplicate=T([False],torch.bool),input_sequence=run(lambda receipt=receipt:integers([receipt['input_sequence']])))
        legs=run(lambda:bind_submission_receipt(legs,T([requests[receipt['request']]]),original))
    result=run(lambda:state.freeze_order_formation(directory,legs,T([source['logical_at']]),T([[True]],torch.bool)))
    assert result.fault.cpu().tolist()==[0] and result.processing_known.cpu().tolist()==[[True]*len(orders)] and result.source_known.cpu().tolist()==[[True]*len(orders)]
    for slot,order in enumerate(orders):
        accepted=order['accepted']['record']['Evidence']['evidence']['Control']['result']['Submission']['outcome']['Accepted']
        receipt=next(r for r in source['buy_source_receipts'] if r['request']==order['origin']['request'])
        assert result.processing_at.cpu().tolist()[0][slot]==accepted['effective_processing_at_utc_ms']
        assert values(result.input_sequences)[slot]==receipt['input_sequence'] and result.item_ordinals.cpu().tolist()[0][slot]==order['origin']['item']
        assert values(result.source_sequences)[slot]==order['formed']['record']['Evidence']['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence']
        assert result.market.cpu().tolist()[0][slot]==0
    unknown=run(lambda:state.freeze_order_formation(directory,legs,T([source['logical_at']]),T([[]],torch.bool)))
    assert unknown.market.cpu().tolist()==[[-1]*len(orders)]
    assert torch.equal(unknown.processing_at,result.processing_at) and values(unknown.source_sequences)==values(result.source_sequences)
    print(json.dumps(dict(original_V1_Registry_orders=len(orders),production_public_rows_and_formation=True,elapsed_seconds=time.perf_counter()-started)))

if __name__=='__main__':main()
