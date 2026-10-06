"""原SharedExecutionV2公开引用的有限连续订单读面对照。

冷适配器仅解码原引用和冻结意图，不用原matched/active/commitment初始化状态。
未导出的中间反馈、完整取消请求和全根时钟不在此组件的覆盖范围内。
"""
from dataclasses import replace
from pathlib import Path
from fractions import Fraction
import json
import sys
import time
import torch

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT/'sim-system/gpu'), str(ROOT/'ledger/gpu'), str(Path(__file__).resolve().parent)]
from registry import (PublicRegistryEvents, FrozenIntentOrders, PublicRegistryState,
    BASE_ACCEPTED, QUOTE_ACCEPTED, ORDER_LIFECYCLE, TRADE, CANCEL_TARGET)
from validate_base_execution import integers, rational, values
from queues import Numbers


def amounts(value):
    return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]


def position_key(reference):
    position = reference['position']
    watermark = position['source_watermark']
    return (position['context_reference'], watermark['run_id'], watermark['projection_batch_sequence'], position['record_ordinal'])


def event_columns(references, identities, rank_start):
    # 这是验证器冷边界的原强类型JSON解码，不是生产逐事件CPU业务循环。
    rows = []
    for reference in references:
        evidence = reference['record']['Evidence']
        source = evidence['source_order']
        row = dict(kind=ORDER_LIFECYCLE, order=0, trade=0, private=source['sequence']['PrivateFeedback']['private_feedback_sequence'],
            delivery=reference['visibility']['Feedback']['delivery_sequence'], ordinal=reference['position']['record_ordinal'],
            matched='0', opened='0', quantity='0', role=0, disposition=0, formed_matched=False, canceled=False, closed=False)
        payload = evidence['evidence']
        if 'Control' in payload:
            control = payload['Control']['result']
            if 'Submission' in control:
                accepted = control['Submission']['outcome']['Accepted']
                row['order'] = identities[accepted['order']['order_identity']]
                measurement = accepted['measurement']
                if 'Base' in measurement:
                    row.update(kind=BASE_ACCEPTED, matched=measurement['Base']['matched_base_quantity_shares'],
                        opened=measurement['Base']['open_base_quantity_shares'])
                else:
                    row.update(kind=QUOTE_ACCEPTED, matched=measurement['QuoteBudget']['actual_matched_base_quantity_shares'])
            elif 'Cancellation' in control:
                outcome = control['Cancellation']
                row.update(kind=CANCEL_TARGET, order=identities[outcome['order_identity']], canceled=outcome['outcome']=='Canceled')
            else:
                raise ValueError('该订单组件冷适配器未包含此Control')
        elif 'OrderLifecycle' in payload:
            result = payload['OrderLifecycle']['result']
            event = result['event']
            row.update(order=identities[result['order_identity']], formed_matched=isinstance(event,dict) and event.get('Accepted',{}).get('state')=='Matched',
                closed=event in ['CallerCanceled','GtdExpired','VenueDustTerminal'] if isinstance(event,str) else 'MarketResolvedCanceled' in event)
        elif 'TradeLifecycleV3' in payload or 'TradeLifecycle' in payload:
            result = payload.get('TradeLifecycleV3',payload.get('TradeLifecycle'))['result']
            result = result.get('result',result)
            row.update(kind=TRADE,order=identities[result['order_identity']],trade=identities[result['trade_identity']],
                quantity=result['base_quantity_shares'],role=0 if result['liquidity_role']=='Taker' else 1,
                disposition=['Pending','Retrying','FinalSuccess','FinalFailureNoEconomicEffect','FinalFailureReverted'].index(result['disposition']))
        else:
            raise ValueError('该订单组件冷适配器未包含此证据')
        rows.append(row)
    active = [True]*len(rows)
    if not rows:
        rows=[dict(kind=0,order=0,trade=0,private=0,delivery=0,ordinal=0,matched='0',opened='0',quantity='0',role=0,disposition=0,formed_matched=False,canceled=False,closed=False)]
        active=[False]
    tensor = lambda name,dtype=torch.int64:torch.tensor([[row[name] for row in rows]],dtype=dtype,device='cuda')
    integer = lambda name:integers([row[name] for row in rows])
    ratio = lambda name:rational([row[name] for row in rows])
    return PublicRegistryEvents(torch.tensor([active],dtype=torch.bool,device='cuda'), tensor('kind'),
        integer('order'),integer('trade'),integer('private'),integer('delivery'),integer('ordinal'),
        torch.arange(rank_start,rank_start+len(rows),device='cuda')[None,:],ratio('matched'),ratio('opened'),ratio('quantity'),
        tensor('role'),tensor('disposition'),tensor('formed_matched',torch.bool),tensor('canceled',torch.bool),
        tensor('closed',torch.bool),torch.zeros(1,dtype=torch.int32,device='cuda'))


def check_family(source):
    identities = {}
    targets = {}
    for frame in source:
        for order in frame['raw_orders']:
            identities.setdefault(order['identity'],len(identities)+1)
            origin = order['origin']
            assert origin is not None and 'GtcLimit' in origin['terms'], '此原夹具只覆盖GTC'
            targets[order['identity']] = origin['terms']['GtcLimit']['size_shares']
            for trade in order['trades']:
                payload=trade['record']['Evidence']['evidence']
                result=payload.get('TradeLifecycleV3',payload.get('TradeLifecycle'))['result']
                result=result.get('result',result)
                identities.setdefault(result['trade_identity'],len(identities)+1)
    catalog=list(targets)
    def directory(frame):
        public = {order['identity'] for order in frame['raw_orders']}
        present=torch.tensor([[key in public for key in catalog]],dtype=torch.bool,device='cuda')
        return FrozenIntentOrders(present,integers([identities[key] for key in catalog]),present.clone(),
            rational([targets[key] for key in catalog]),torch.zeros_like(present),present.clone())
    seen=set()
    state=None
    rank=0
    saved=[]
    for frame in source:
        incoming=[]
        for order in frame['raw_orders']:
            for name in ['accepted','formed','latest_lifecycle','latest_cancel']:
                reference=order[name]
                if reference is not None and position_key(reference) not in seen:
                    seen.add(position_key(reference));incoming.append(reference)
            for reference in order['trades']:
                if position_key(reference) not in seen:
                    seen.add(position_key(reference));incoming.append(reference)
        incoming.sort(key=lambda r:(r['visibility']['Feedback']['delivery_sequence'],r['position']['record_ordinal']))
        def compute():
            events=event_columns(incoming,identities,rank)
            candidate=PublicRegistryState(events) if state is None else state.receive(events)
            return candidate,candidate.freeze_orders(directory(frame))
        state,view=Numbers.run_exact_batch(compute)
        rank+=max(1,len(incoming))
        assert view.fault.cpu().tolist()==[0]
        indexes={key:index for index,key in enumerate(catalog)}
        unfinished=view.unfinished.cpu().tolist()[0]
        assert {key for key,index in indexes.items() if unfinished[index]}=={order['identity'] for order in frame['orders']},frame['stage']
        for order in frame['orders']:
            index=indexes[order['identity']]
            for name in ['matched','active']:
                oracle=order[name]
                known=getattr(view,name+'_known').cpu().tolist()[0][index]
                assert known==('Known' in oracle),(frame['stage'],name,oracle)
                if known:assert amounts(getattr(view,name))[index]==Fraction(oracle['Known']),(frame['stage'],name)
            assert view.cancellable_known.cpu().tolist()[0][index]==('Known' in order['cancellable'])
            if 'Known' in order['cancellable']:assert view.cancellable.cpu().tolist()[0][index]==order['cancellable']['Known']
        for commitment in frame['commitments']:
            if commitment['order'] is None:continue
            index=indexes[commitment['order']]
            known=view.commitment_known.cpu().tolist()[0][index]
            assert known==('Known' in commitment['remaining'])
            if known:assert amounts(view.commitment)[index]==Fraction(commitment['remaining']['Known'])
        saved.append((view,amounts(view.active),view.unfinished.clone()))
    for view,active,unfinished in saved:
        assert amounts(view.active)==active and torch.equal(view.unfinished,unfinished),'旧冻结读取不可倒灌'
    return len(source),len(seen)


def main():
    started=time.perf_counter()
    counts=[check_family(json.loads(Path(path).read_text())) for path in sys.argv[1:]]
    torch.cuda.synchronize()
    print(json.dumps({'frozen_prefixes':sum(n for n,_ in counts),'distinct_original_order_records':sum(n for _,n in counts),
        'continuous_received_state':True,'derived_oracle_initialization':False,'old_freeze_unchanged':True,
        'complete_first_delivery_stream':False,'cancel_request_completed_state':False,
        'full_registry_or_root':False,'seconds':time.perf_counter()-started,'device_peak_allocated_bytes':torch.cuda.max_memory_allocated()}))


if __name__=='__main__':main()
