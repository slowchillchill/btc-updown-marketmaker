"""原stage3合法NOE/本地事件→GPU Ledger消费者；不代表Query发送/公开调度。"""
from dataclasses import fields,replace
from pathlib import Path
from fractions import Fraction
import importlib.util
import json
import os
import sys
import time
# 本脚本目录含Ledger platform.py；torch先沿标准库搜索路径加载platform。
sys.path=[entry for entry in sys.path if Path(entry).resolve()!=Path(__file__).resolve().parent]
import torch

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('sim-system/gpu','execution-normalization/gpu','ledger/gpu','shared-run/gpu')]
from queues import Numbers,ExactInt,ExactRational,_take
from fields import encode_fields
from validate_base_execution import integers,rational,values
from lifecycle import OrderQueryResult,map_order_query,map_legacy_submission
from economic import EconomicEvidence
from projection import Projection
from core import EVIDENCE,ECONOMIC
from feedback import FeedbackEmitter
from memory import EconomicColumns
from admission import AdmissionLifecycle
from query_association import QueryAssociations
from accepted_principal import AcceptedPrincipal
from order_funds import OrderFunds
from position_cost import PositionCosts
from closed import ClosedAdmission
from ledger_drain import LedgerDrainStorage

spec=importlib.util.spec_from_file_location('query_platform',ROOT/'ledger/gpu/platform.py')
platform=importlib.util.module_from_spec(spec);sys.modules[spec.name]=platform;spec.loader.exec_module(platform)
BALANCES=('cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
    'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd')
STATES=('Open','Matched','VenueDustTerminal','MarketResolvedCanceled','Canceled','Expired','Delayed','Unmatched')


def exact(v):return [Fraction(n,d) for n,d in zip(values(v.numerator),values(v.denominator))]


def unchanged_funds(previous,current):
    # Query不是FundsState的来源；核全部原目录字段，不只核金额0。
    for field in fields(previous):
        a,b=getattr(previous,field.name),getattr(current,field.name)
        if isinstance(a,ExactRational):assert exact(a)==exact(b),field.name
        elif isinstance(a,ExactInt):assert values(a)==values(b),field.name
        else:assert torch.equal(a,b),field.name


def validate(source,artifact):
    started=time.perf_counter();run=Numbers.run_exact_batch
    progress_every=max(0,int(os.environ.get('B_GPU_QUERY_PROGRESS_EVERY','1')))
    t=lambda v,dtype=torch.int64:torch.tensor(v,dtype=dtype,device='cuda')
    zero=t([0]);yes=t([True],torch.bool)
    catalogs={kind:{} for kind in ('request','event','order','condition','token','context','run','raw')}
    def slot(kind,value):
        table=catalogs[kind]
        if value not in table:table[value]=len(table)
        return table[value]
    prepared=[];positions={};batches=[]
    projection=run(lambda:Projection.open(FeedbackEmitter.open(1),zero,zero,zero,zero,zero,integers([0]),zero,t([-1]),zero,zero))
    for step in source['steps']:
        kind,action=next(iter(step['action'].items()))
        if kind!='Noe':
            slot('request',action['request']);slot('event',action['event'])
            if kind=='Associate':slot('request',action['query'])
            else:slot('condition',action['condition'])
            prepared.append((kind,action,None));continue
        p=action['position'];key=(p['context_reference'],p['source_watermark']['run_id'],p['source_watermark']['projection_batch_sequence'],p['record_ordinal'])
        if key in positions:
            prepared.append((kind,action,positions[key]));continue
        body=next(iter(action['record'].values()));record=body.get('fact',body)
        original_order=body['source_order'];private=original_order['sequence']['PrivateFeedback']['private_feedback_sequence']
        original_kind,reference=next(iter(record['source_reference']['record'].items()))
        assert original_kind=='DirectResponse'
        binding_kind,binding=next(iter(reference['association'].items()))
        request=slot('request',binding['global_request_id'])
        item=binding.get('batch_item_ordinal',-1)
        projection=replace(projection,request_slots=t([request]),item_ordinals=t([item]),
            context_slots=t([slot('context',p['context_reference'])]),run_slots=t([slot('run',p['source_watermark']['run_id'])]))
        if 'fact' in body:
            value=record['disposition']['AccountTransition'];transition=value['transition']['OrderAcceptedEncumbranceApplied']
            order=slot('order',transition['order_identity']);condition=slot('condition',value['condition_id'])
            balance=value['balance_delta'];assert not balance['outcome_balance_deltas']
            seq=record['economic_identity']['AccountTransition']['account_transition_seq']
            def map_fact():
                amounts=tuple(rational([balance[name+'_delta']]) for name in BALANCES)
                empty=rational(['0']);sequence=integers([seq])
                semantic=encode_fields([zero,t([condition]),t([value['economic_effective_at_utc_ms']]),zero,integers([order]),zero,
                    *amounts,~yes,zero,empty,empty,empty])
                return EconomicEvidence(encode_fields([zero,t([11]),sequence]),encode_fields([zero,t([1]),sequence]),
                    semantic,t([[2,private&0xffffffff,private>>32,original_order['ordinal']]]),amounts,empty,~yes,yes,
                    t([0],torch.int32),sequence,asset_indices=zero)
            mapped=run(map_fact);record_kind=ECONOMIC
        else:
            control=body['evidence']['Control']['result'];name,result=next(iter(control.items()))
            if name=='OrderQuery':
                assert binding_kind=='Request'
                result=result['result'];outcome=0 if isinstance(result,dict) and 'Found' in result else 1 if result=='NotFound' else 2
                order=result['Found']['order'] if outcome==0 else None
                state=order['state'] if order else 'Open'
                raw=slot('raw',state['Unknown']['raw']) if isinstance(state,dict) else -1
                retention=result['OutOfRetention'] if outcome==2 else {}
                retention_name=next(iter(retention)) if retention else 'OrderHistory'
                retained=retention.get(retention_name,{})
                # 原V1事实没有quantity；不通过补0把它冒充Base/Quote已知。
                query=run(lambda:OrderQueryResult(t([outcome]),integers([slot('order',order['order_identity']) if order else 0]),
                    t([slot('condition',order['condition_id']) if order else 0]),t([slot('token',order['token_id']) if order else 0]),
                    t([8 if isinstance(state,dict) else STATES.index(state)]),t([raw]),t([2]),rational(['0']),rational(['0']),
                    t([('OrderHistory','TradeHistory','RootSnapshot').index(retention_name)]),t([retained.get('retained_from_utc_ms',0)]),
                    integers([retained.get('requested_root_sequence',0)]),integers([retained.get('earliest_retained_root_sequence',0)])))
                mapped=run(lambda:map_order_query(query,integers([private]),t([original_order['ordinal']]),zero,t([request]),yes))
            else:
                assert name=='Submission' and binding_kind=='SubmissionItem'
                order=result['outcome']['Accepted']['order'];assert order['state']=='Open'
                snapshot=run(lambda:OrderQueryResult(zero,integers([slot('order',order['order_identity'])]),
                    t([slot('condition',order['condition_id'])]),t([slot('token',order['token_id'])]),zero,t([-1]),
                    t([2]),rational(['0']),rational(['0']),t([-1]),zero,integers([0]),integers([0])))
                mapped=run(lambda:map_legacy_submission(snapshot,integers([private]),t([original_order['ordinal']]),zero,t([request]),t([item]),yes))
            record_kind=EVIDENCE
        projection,batch=run(lambda:projection._finish(projection.emitter,[(mapped,record_kind,integers([private]),
            integers([p['source_watermark']['projection_batch_sequence']]),t([reference['response_received_at_utc_ms']]),
            zero,t([reference['platform_responded_at_utc_ms']]),yes)],t([0],torch.int32),
            record_ordinal_base=integers([p['record_ordinal']])))
        assert batch.fault.cpu().tolist()==[0]
        assert values(batch.private_sequences)==[private]
        assert values(batch.batch_sequences)==[p['source_watermark']['projection_batch_sequence']]
        assert values(batch.record_ordinals)==[p['record_ordinal']]
        assert projection.normalizer.bank.source_order[batch.rows.flatten()].cpu().tolist()==[
            [2,private&0xffffffff,private>>32,original_order['ordinal']]]
        assert batch.request_slots.cpu().tolist()==[[request]] and batch.item_ordinals.cpu().tolist()==[[item]]
        positions[key]=len(batches);batches.append(batch);prepared.append((kind,action,len(batches)-1))
    columns=run(lambda:EconomicColumns.from_projection_batches(batches))
    initial=source['initial'];base=initial['platform_confirmed_projection']
    assert initial['platform_source_prefix']==0 and not base['outcome_balances']
    # 唯一实际token来自原V1来源。订单接纳不要求造二元对腿来安装成功池。
    assert len(catalogs['token'])==1 and len(catalogs['condition'])==1
    balances=run(lambda:platform.PlatformBalances.open(rational([base[n] for n in BALANCES]),rational(['0']),zero,integers([0])))
    width=columns.row_refs.size(1)
    ledger=run(lambda:AdmissionLifecycle.open(balances,projection.normalizer.bank,columns,
        integers([initial['ledger_entry_seq']]),integers([initial['ledger_local_prefix']]),None,
        accepted_principal=AcceptedPrincipal.open(1,zero,known=True),query_associations=QueryAssociations.empty(1,1,'cuda'),
        position_costs=PositionCosts.empty(1,width,'cuda',known=True),order_funds=OrderFunds.empty(1,1,width,width,'cuda',known=True)))
    ledger=replace(ledger,has_available=yes,available_through=t([initial['available_through_utc_ms']]))
    old=None;old_values=None;retries=0;full_funds_queries=0;reports=[]
    for ordinal,(step,(kind,action,index)) in enumerate(zip(source['steps'],prepared),1):
        if kind=='ReserveSubmit':
            request=t([[slot('request',action['request'])]])
            ledger,decision=run(lambda:ledger.reserve_action(request,zero[:,None],rational([action['principal']]),rational([action['fee']]),yes[:,None]))
            assert decision.fault.cpu().tolist()==[0]
            ledger=run(lambda:ledger.submission_started(request[:,0],t([slot('event',action['event'])]),request[:,0],zero,yes))
        elif kind=='Associate':
            event=t([slot('event',action['event'])]);query=t([slot('request',action['query'])]);request=t([slot('request',action['request'])])
            prior=ledger
            def register(original):
                compute=lambda:original.record_order_query_association(event,query,request,yes)
                Numbers.device_exact_batch(compute,word_capacity=16,device='cuda')
                graph=torch.cuda.CUDAGraph()
                with torch.cuda.graph(graph):captured=Numbers.device_exact_batch(compute,word_capacity=16,device='cuda')
                graph.replay();assert captured.fits.cpu().item()
                candidate,decision=run(compute)
                frozen,captured_decision=captured.value
                assert torch.equal(frozen.query_associations.events,candidate.query_associations.events)
                assert torch.equal(frozen.query_associations.queries,candidate.query_associations.queries)
                assert torch.equal(frozen.query_associations.requests,candidate.query_associations.requests)
                assert torch.equal(captured_decision.enough,decision.enough)
                assert values(frozen.current_entry_sequence)==values(candidate.current_entry_sequence)
                assert values(frozen.current_local_prefix)==values(candidate.current_local_prefix)
                assert values(captured_decision.ledger_sequences)==values(decision.ledger_sequences)
                return candidate,decision
            ledger,decision=register(ledger)
            if not decision.enough.cpu().item():
                assert values(ledger.current_entry_sequence)==values(prior.current_entry_sequence)
                assert torch.equal(ledger.query_associations.events,prior.query_associations.events)
                assert exact(ledger.reservations.principal)==exact(prior.reservations.principal)
                ledger=ledger.resized_query_associations(int(decision.required_capacity.max().cpu()));retries+=1
                ledger,decision=register(ledger)
            assert decision.fault.cpu().tolist()==[0] and decision.changed.cpu().item()
            assert yes.cpu().tolist()==[True]
            again,repeat=run(lambda:ledger.record_order_query_association(event,query,request,yes))
            assert not repeat.changed.cpu().item() and values(repeat.ledger_sequences)==values(decision.ledger_sequences)
            assert values(again.current_entry_sequence)==values(ledger.current_entry_sequence)
        else:
            at=t([index]);available=t([action['durable_available_at']])
            original=ledger
            compute=lambda:original.apply_record(at,yes,available,yes)
            Numbers.device_exact_batch(compute,word_capacity=16,device='cuda')
            graph=torch.cuda.CUDAGraph()
            with torch.cuda.graph(graph):captured=Numbers.device_exact_batch(compute,word_capacity=16,device='cuda')
            graph.replay();assert captured.fits.cpu().item()
            ledger,consumed=run(compute)
            candidate,captured_consumed=captured.value
            assert torch.equal(captured_consumed,consumed)
            assert values(candidate.current_entry_sequence)==values(ledger.current_entry_sequence)
            assert exact(candidate.platform.balances)==exact(ledger.platform.balances)
            assert exact(candidate.accepted_principal.amounts)==exact(ledger.accepted_principal.amounts)
            assert torch.equal(candidate.applied,ledger.applied)
            assert torch.equal(candidate.orders.accepted_principal_counted,ledger.orders.accepted_principal_counted)
            assert torch.equal(candidate.query_associations.events,ledger.query_associations.events)
            assert candidate.fault.cpu().tolist()==[0]
            if original.lifecycle.schemas[0,index].cpu().item()==25:
                unchanged_funds(original.order_funds,ledger.order_funds)
                unchanged_funds(original.order_funds,candidate.order_funds)
                _,fund_member,fund_fault=run(lambda:original.order_funds.lookup(_take(original.lifecycle.orders,at)))
                assert fund_fault.cpu().tolist()==[0]
                _,query_bound=run(lambda:original.query_associations.lookup(original.lifecycle.request_slots[:,index]))
                definitive=query_bound.cpu().item() and original.lifecycle.query_outcomes[0,index].cpu().item()==0 and (
                    original.lifecycle.query_states[0,index].cpu().item() in (0,1))
                if definitive and original.order_funds.occupied.all().cpu().item() and not fund_member.cpu().item():
                    full_funds_queries+=1
            # 同一原position已消费时apply_record不再签发新消费/条目。
            assert consumed.cpu().item()==(not original.applied[0,index].cpu().item())
        assert ledger.fault.cpu().tolist()==[0],(ordinal,kind)
        expected=step['expected'];p=expected['platform_confirmed_projection'];b=expected['local_pending_reservation_projection']
        assert values(ledger.current_entry_sequence)==[expected['ledger_entry_seq']],(ordinal,kind,'entry')
        assert values(ledger.current_local_prefix)==[expected['ledger_local_prefix']]
        assert values(ledger.platform.platform_source_prefix)==[expected['platform_source_prefix']]
        assert ledger.has_available.cpu().item() and ledger.available_through.cpu().tolist()==[expected['available_through_utc_ms']]
        # CPU project_balances合入待经济确认占款；platform.balances仅已应用经济。
        # 同时核原8余额及派生8余额，不把Query确认变成经济余额写入。
        expected_balances=[Fraction(p[n]) for n in BALANCES]
        pending_principal=Fraction(p['platform_confirmation_pending_economic_principal_pusd'])
        pending_fee=Fraction(p['platform_confirmation_pending_economic_immediate_fee_pusd'])
        expected_raw=expected_balances.copy()
        expected_raw[1]+=pending_principal+pending_fee
        expected_raw[2]-=pending_principal;expected_raw[3]-=pending_fee
        projected=exact(ledger.platform.balances)
        assert projected==expected_raw,(ordinal,kind,'raw_economic_balances',projected,expected_raw)
        assert exact(ledger.accepted_principal.amounts)==[Fraction(p['cumulative_accepted_principal_pusd'])]
        principal=ledger.reservations.principal;fee=ledger.reservations.fee
        for status,prefix in ((0,'before_send'),(1,'after_send_unknown'),(2,'platform_confirmation_pending_economic')):
            source_projection=b if status<2 else p
            mask=(ledger.reservations.statuses==status).flatten().cpu().tolist()
            principal_total=sum((q for q,keep in zip(exact(principal),mask) if keep),Fraction(0))
            fee_total=sum((q for q,keep in zip(exact(fee),mask) if keep),Fraction(0))
            assert principal_total==Fraction(source_projection[prefix+'_principal_pusd'])
            assert fee_total==Fraction(source_projection[prefix+'_immediate_fee_pusd'])
            assert sum(mask)==source_projection[prefix+'_count']
            if status==2:
                projected[1]-=principal_total+fee_total
                projected[2]+=principal_total;projected[3]+=fee_total
        assert projected==expected_balances,(ordinal,kind,'public_projected_balances',projected,expected_balances)
        funds=run(ledger.funds);local_pending=Fraction(b['principal_pusd'])+Fraction(b['immediate_fee_pusd'])
        assert exact(funds.local_pending)==[local_pending]
        assert exact(funds.platform_pending)==[pending_principal+pending_fee]
        assert exact(funds.actual_available)==[expected_balances[1]-local_pending]
        assert exact(funds.actual_reserved)==[expected_balances[2]+expected_balances[3]+local_pending]
        assert int((ledger.orders.occupied&ledger.orders.flags).sum().cpu())==p['open_order_count']
        if old is None and Fraction(p['cumulative_accepted_principal_pusd'])>0:
            old=LedgerDrainStorage(ledger,ClosedAdmission.empty(columns),word_capacity=16).snapshot().ledger
            old_values=({name:getattr(old.query_associations,name).clone() for name in ('events','queries','requests','capacity_required','fault')},
                values(old.query_associations.ledger_sequences),values(old.current_entry_sequence),exact(old.accepted_principal.amounts))
        if old is not None:
            for name,value in old_values[0].items():assert torch.equal(getattr(old.query_associations,name),value)
            assert values(old.query_associations.ledger_sequences)==old_values[1]
            assert values(old.current_entry_sequence)==old_values[2]
            assert exact(old.accepted_principal.amounts)==old_values[3]
        reports.append({'step':ordinal,'action':kind,'entry':expected['ledger_entry_seq']})
        if progress_every and (ordinal==1 or ordinal==len(prepared) or ordinal%progress_every==0):
            print(json.dumps({'step':ordinal,'total':len(prepared),'action':kind,'elapsed_seconds':time.perf_counter()-started}),flush=True)
    if source['label']=='query-results':assert full_funds_queries>0
    report={'status':'PASS','label':source['label'],'steps':reports,'association_capacity_retries':retries,
        'query_with_full_unrelated_funds_directory':full_funds_queries,
        'actual_V1_NOE_consumer':True,'ordinary_Query_SIM_Registry_host':False}
    (artifact/'report.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report),flush=True)


if __name__=='__main__':validate(json.loads(Path(sys.argv[1]).read_text()),Path(sys.argv[2]))
