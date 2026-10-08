"""原请求/接单前状态→生产pending→完整Post/NOE/共同Ledger的独立CPU对照。

CPU结果仅作断言。下一义务、处理时tick、SDK条款和订单/成交身份由设备
活动状态及原封存规则产生；不是MINED后态冷装，也不是完整Actor发送宿主。
"""
from dataclasses import fields, replace
from fractions import Fraction
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
import torch

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('sim-system/gpu','model-train/native-inputs/gpu',
    'data-pipeline/gpu','execution-normalization/gpu','ledger/gpu','shared-run/gpu')]
from queues import Numbers, _take
from validate_base_execution import integers,rational,values,units,assert_checkpoint
from hidden_market import HiddenAssetState,run_hidden_asset_batch
from hidden_reference import HiddenMarketControls
from account_state import PlatformAccountState
from base_orders import BaseOrders
from order_quantity import BaseOrderQuantity
from base_runtime import BaseSimRuntime
from mutation_requests import MutationRequests
from fill_finality import FillRecord
from fill_pool import FillPool
from maker_days import MakerDays
from taker_days import TakerDays
from rebate_coordinator import RebateCoordinator
from post_results import PostResults
from pending_post_intents import PendingPostIntents,REJECTED_REASONS
from pending_base_execution import accept_pending_base
from scheduling import ScheduledTime
from latency import EmpiricalDelaySampler,byte_column
from rules import PublicRuleGpu
from projection import Projection
from feedback import FeedbackEmitter
from core import EvidenceBank
from validate_projection import expected_semantic
from consumption import PendingRecords,ReceiveGates
from memory import EconomicColumns
from admission import AdmissionLifecycle
from closed import ClosedAdmission
from success_pool import SuccessPools
from position_cost import PositionCosts
from order_funds import OrderFunds
from accepted_principal import AcceptedPrincipal
from rebate_day import RebateDays
from projected_markets import ProjectedMarkets
from public_ledger import PublicLedgerState
from registry import PublicRegistryState,PublicRegistryEvents
from commitments import SubmissionBindings
from post_submission import submit_post_orders

T=lambda data,dtype=torch.int64:torch.tensor(data,dtype=dtype,device='cuda')
run=Numbers.run_exact_batch
BALANCES=('cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
    'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd')


def amounts(value):
    return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator),strict=True)]


def checkpoint(ledger,original):
    expected=original['projection']
    for actual,key in ((ledger.current_entry_sequence,'ledger_entry_seq'),
            (ledger.current_local_prefix,'ledger_local_prefix'),(ledger.platform.platform_source_prefix,'platform_source_prefix')):
        assert values(actual)==[expected[key]],(key,values(actual),expected[key])
    assert amounts(ledger.platform.balances)==[Fraction(expected['platform_confirmed_projection'][name]) for name in BALANCES]
    funds=run(ledger.funds)
    for name,key in (('actual_available','actual_available_pusd'),('maker_available','maker_available_pusd'),
                     ('actual_reserved','actual_reserved_pusd'),('maker_reserved','maker_reserved_pusd')):
        assert amounts(getattr(funds,name))==[Fraction(original['cash'][key])],(key,amounts(getattr(funds,name)),original['cash'][key])


def asset_layout(source):
    initial=source['initial'];conditions=list(dict.fromkeys(event['market']['condition_id'] for event in initial))
    markets=[conditions.index(event['market']['condition_id']) for event in initial]
    tokens={event['payload']['event']['asset']['token_id']:i for i,event in enumerate(initial)}
    return conditions,markets,tokens


def original_semantic(record,source):
    conditions,_,tokens=asset_layout(source)
    locations={}
    def scan(value):
        if isinstance(value,dict):
            if 'order_identity' in value and 'token_id' in value and 'condition_id' in value:
                locations[value['order_identity']]=(value['condition_id'],value['token_id'])
                if 'trade_identity' in value:locations[value['trade_identity']]=(value['condition_id'],value['token_id'])
            for member in value.values():scan(member)
        elif isinstance(value,list):
            for member in value:scan(member)
    all_records=[*source['records'],*source.get('cancellation',{}).get('records',[])]
    for value in all_records:scan(value['record'])
    found=[]
    def locate(value):
        if isinstance(value,dict):
            if 'condition_id' in value and 'token_id' in value:
                found.append((value['condition_id'],value['token_id']))
            elif value.get('order_identity') in locations:
                found.append(locations[value['order_identity']])
            elif value.get('trade_identity') in locations:
                found.append(locations[value['trade_identity']])
            for member in value.values():locate(member)
        elif isinstance(value,list):
            for member in value:locate(member)
    body=next(iter(record['record'].values()));locate(body.get('evidence') or body['fact']['disposition'])
    condition,token=found[0] if found else (conditions[0],next(iter(tokens)))
    return expected_semantic(record,token_index=tokens[token],condition_index=conditions.index(condition))


def public_open(source):
    opening=source['opening_ledger'];p=opening['projection'];balance=p['platform_confirmed_projection']
    conditions,markets,tokens=asset_layout(source);assets=len(markets);count_markets=len(conditions)
    assert p['platform_source_prefix']==0 and not opening['order_funds'] and not opening['trades']
    spec=importlib.util.spec_from_file_location('pending_post_ledger_platform',ROOT/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
    def build():
        platform=module.PlatformBalances.open(rational([balance[name] for name in BALANCES]),rational(['0']*assets),T([0]*assets),integers([0]))
        pools=SuccessPools.open(T([0]*assets),T(markets),T([i%2 for i in range(assets)]),T([i^1 for i in range(assets)]),1)
        columns=EconomicColumns.empty(1,'cuda')
        ledger=AdmissionLifecycle.open(platform,EvidenceBank.empty('cuda'),columns,integers([p['ledger_entry_seq']]),
            integers([p['ledger_local_prefix']]),pools,position_costs=PositionCosts.empty(1,count_markets,'cuda'),
            order_funds=OrderFunds.empty(1,1,1,0,'cuda'),rebate_days=RebateDays.empty(T([list(range(count_markets))]),1),
            accepted_principal=AcceptedPrincipal.open(1,T([0]),known=True),
            projected_markets=ProjectedMarkets.open(T(list(range(count_markets))),T([[False]*count_markets],torch.bool),T([True],torch.bool)))
        count=len(source['posts'])
        ledger=replace(ledger,reservations=ledger.reservations.resized(count),global_requests=T([[-1]*count]),item_ordinals=T([[-1]*count]))
        empty=T([[]]);integer=integers([])
        pending=PendingRecords.from_columns(empty.bool(),empty,integer,integer,empty,integers([0]))
        return PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')),ClosedAdmission.empty(columns),ledger,
            submission_bindings=SubmissionBindings.empty(1,'cuda',prefix_known=True))
    return run(build)


def full_item(item,original):
    if 'Accepted' not in original:
        assert item.outcomes.cpu().tolist()==[1]
        assert REJECTED_REASONS[int(item.reasons.cpu()[0])]==original['RejectedNoEffect']
        assert not item.fill_present.any().cpu().item()
        return
    body=original['Accepted'];m=body['measurement'];q=item.quantity
    assert item.outcomes.cpu().tolist()==[0]
    assert values(item.order_sequence)==[int(body['platform_order_id'].removeprefix('sim-'))]
    assert values(item.accepted_sequence)==body['account_transition_sequences']
    quote=m['measurement']=='QUOTE_BUDGET'
    assert q.quantity_kind.cpu().tolist()==[int(quote)]
    mapping={'accepted_quote_budget_units':'accepted_quote_budget_pusd','matched_quote_units':'used_quote_amount_pusd',
        'released_or_unspent_quote_units':'released_or_unspent_quote_amount_pusd','actual_matched_base_units':'actual_matched_base_quantity_shares',
        'sdk_taker_amount_audit_base_units':'sdk_taker_amount_audit_base_quantity_shares'} if quote else {
        'accepted_base_units':'accepted_base_quantity_shares','accepted_quote_units':'accepted_quote_amount_pusd',
        'matched_base_units':'matched_base_quantity_shares','matched_quote_units':'matched_quote_amount_pusd',
        'open_base_units':'open_base_quantity_shares','open_encumbered_quote_units':'open_encumbered_quote_amount_pusd',
        'released_price_improvement_quote_units':'released_price_improvement_quote_amount_pusd',
        'venue_dust_closed_base_units':'venue_dust_closed_base_quantity_shares','venue_dust_released_quote_units':'venue_dust_released_quote_amount_pusd'}
    for field,key in mapping.items():assert values(getattr(q,field))==[units(m[key])],(field,values(getattr(q,field)),m[key])
    assert values(item.fee)==[units(m['immediate_taker_fee_encumbered_pusd'])]
    selected=item.fill_present.cpu().tolist()[0]
    actual=[(seq,b,q,p) for seq,b,q,p,on in zip(values(item.fill_sequences),values(item.fill_bases),
        values(item.fill_quotes),amounts(item.fill_prices),selected,strict=True) if on]
    expected=[(int(f['trade_id'].removeprefix('sim-')),units(f['base_quantity_shares']),units(f['quote_amount_pusd']),
        Fraction(f['source_price_pusd_per_share'])) for f in body['fills']]
    assert actual==expected,(actual,expected)


def check(source,codec,progress,after_posts=None):
    started=time.perf_counter();yes=T([True],torch.bool);zero=T([0]);posts=source['posts'];count=len(posts)
    conditions,markets,tokens=asset_layout(source);asset_count=len(markets);market_count=len(conditions)
    sampler=EmpiricalDelaySampler.build((ROOT/'sim-system/config/latency-profiles'/source['profile_sha256']/'profile.json').read_bytes(),
        source['profile_sha256'],[source['session_seed'].encode('ascii')])
    rule=PublicRuleGpu(source['rule'])
    # 冷价位包含真实初始簿、原请求bid及后来原消息；未来档先为空，不安装未来数量。
    slots=set()
    for asset,event in enumerate(source['initial']):
        for side in ('bids','asks'):
            for level in event['payload']['event'][side]:slots.add((asset,side,Fraction(level['price_pusd_per_share'])))
    for post in posts:
        kind=post['item']['order']['kind']
        if 'GtcLimit' in kind:slots.add((tokens[post['item']['order']['token_id']],'bids',Fraction(kind['GtcLimit']['limit_price_pusd_per_share'])))
    for event in source['changed']:
        if event['payload']['event_type']=='level_execution_transition':
            value=event['payload']['event'];slots.add((0,'asks' if value['side']=='ASK' else 'bids',Fraction(value['price_pusd_per_share'])))
    for stage in source.get('cancellation',{}).get('stages',[]):
        if stage['late'] is not None:
            for post in stage['late']['request']['request']['items']:
                kind=post['item']['order']['kind']
                slots.add((tokens[post['item']['order']['token_id']],'bids',Fraction(kind['GtcLimit']['limit_price_pusd_per_share'])))
    slots=sorted(slots);prices=[str(price) for _,_,price in slots];owners=T([0]*len(slots));assets=T([s[0] for s in slots])
    def build():
        hidden=HiddenAssetState.empty(assets,rational(prices),T(markets),market_count,segment_capacity=1)
        for asset,event in enumerate(source['initial']):
            book=event['payload']['event'];quantities=['0']*len(slots)
            for side in ('bids','asks'):
                for level in book[side]:quantities[slots.index((asset,side,Fraction(level['price_pusd_per_share'])))]=level['quantity_shares']
            hidden,_=hidden.initial_snapshot(T([i==asset for i in range(asset_count)],torch.bool),rational(quantities),
                rational([book['initial_tick_size_pusd_per_share']]*asset_count),T([0]*asset_count))
        blank=integers([0]);quantity=BaseOrderQuantity.from_taker_plan(integers([1]),integers([1]),blank,blank)
        record=FillRecord.create(integers([1]),integers([1]),blank,zero,zero,~yes,source['finality_path'])
        return BaseSimRuntime(hidden,MutationRequests.empty(blank,capacity=1,action_capacity=count),BaseOrders.empty(quantity,capacity=1),
            PlatformAccountState.open(integers([units(source['cash'])]),T([0]*asset_count)),FillPool.empty(record,capacity=1),
            RebateCoordinator(MakerDays.empty(T([[0]]),T([[source['market_end']]]),yes[:,None],fills=1,days=1),TakerDays.empty(1,'cuda',fills=1,days=1)),
            market_controls=HiddenMarketControls.empty(1,market_count,'cuda'),post_results=PostResults.empty(quantity,1,1))
    runtime=run(build);ledger=public_open(source)
    principal=[intent['requested_principal_pusd'] for intent in source['reserve_intents']]
    fee=[intent['immediate_fee_reserve_pusd'] for intent in source['reserve_intents']]
    admission,reserved=run(lambda:ledger.ledger.reserve_action(T([list(range(count))]),T([[0]*count]),rational(principal),rational(fee),T([[True]*count],torch.bool)))
    assert reserved.fault.cpu().tolist()==[0]
    ledger=replace(ledger,ledger=admission)
    identity,offsets=byte_column([source['request_id'].encode()])
    ordinals=T([[p['batch_item_ordinal'] for p in posts]]);present=T([[True]*count],torch.bool)
    delayed=T([['FokMarket' in p['item']['order']['kind'] and source['rule']['taker_delay_enabled'] for p in posts]],torch.bool)
    sending=lambda r,l:submit_post_orders(r,l,sampler,identity,offsets,zero,T([1]),integers([source['input_sequence']]),
        T([source['sent_at']]),T([list(range(count))]),ordinals,present,delayed,T([source['rule']['taker_delay_ms']]),
        T([list(range(count))]),T([list(range(count))]),yes,T([0],torch.int32))
    runtime,account,receipt,enough=run(lambda:sending(runtime,ledger.ledger));ledger=replace(ledger,ledger=account)
    assert receipt.fault.cpu().tolist()==[0] and enough.cpu().tolist()==[True]
    assert receipt.arrived_at.cpu().tolist()==[source['receipt']['arrived_at']]
    checkpoint(ledger.ledger,source['sent_ledger'])
    emitter=replace(FeedbackEmitter.open(1),next_private_sequence=run(lambda:integers([source['first_private']])),next_batch_sequence=run(lambda:integers([source['first_batch']])))
    noe=run(lambda:Projection.open(emitter,zero,zero,zero,zero,T([0]*asset_count),integers([0]),T([-1]),T([-1]),zero,zero))
    bids=[]
    for post in posts:
        kind=post['item']['order']['kind'];price=Fraction(kind['GtcLimit']['limit_price_pusd_per_share']) if 'GtcLimit' in kind else next(p for a,s,p in slots if a==tokens[post['item']['order']['token_id']] and s=='bids')
        bids.append(slots.index((tokens[post['item']['order']['token_id']],'bids',price)))
    intents=PendingPostIntents((tuple(json.dumps(p['item']) for p in posts),),T([[tokens[p['item']['order']['token_id']] for p in posts]]),T([[conditions.index(p['item']['order']['condition_id']) for p in posts]]),T([bids]),T([[source['market_end']]*count]),T([0],torch.int32))
    ask_lists=[[i for i,(a,s,_) in enumerate(slots) if a==asset and s=='asks'] for asset in range(asset_count)]
    ask_width=max(map(len,ask_lists));ask_directory=T([row+[-1]*(ask_width-len(row)) for row in ask_lists])
    ask_rows=ask_directory[T([0])]
    taker,maker=run(lambda:(rational([source['taker_rebate_rate']]),rational([source['maker_rebate_rate']])))
    batches=[];timeline=[];effect_index=0;changed=False;capacity_checked=False
    horizon=runtime.requests.effective_at.flatten(1).max(1).values+max(step['offset_ms'] for step in source['finality_path']['steps'])
    end=T([e['market']['window']['end_at_utc_ms'] for e in source['initial']])
    # 测试驱动只提供实际有限hidden消息和EOF；CPU预期不参与下一事件选择。
    for _ in range(32):
        change_at=source['changed'][0]['time']['source_event_at_utc_ms'] if source['changed'] and not changed else None
        hidden_source=ScheduledTime(T([change_at]),yes,T([0],torch.int32)) if change_at is not None else ScheduledTime.absent(zero)
        choice=runtime.choose(hidden_source,yes if change_at is None else ~yes,horizon,yes)
        assert choice.fault.cpu().tolist()==[0]
        if not choice.present.cpu().item():break
        phase=int(choice.phase.cpu()[0]);at=int(choice.at.cpu()[0])
        if phase==4:
            hidden=runtime.hidden
            for event in source['changed']:
                value=event['payload']['event']
                if event['payload']['event_type']=='tick_size_change':
                    hidden=run(lambda:hidden.tick_change(T([True,False],torch.bool),rational([value['old_tick_size_pusd_per_share']]*2),
                        T([True,False],torch.bool),rational([value['new_tick_size_pusd_per_share']]*2)))
                else:
                    row=slots.index((0,'asks' if value['side']=='ASK' else 'bids',Fraction(value['price_pusd_per_share'])))
                    before=['0']*len(slots);after=before.copy();before[row]=value['quantity_before_shares'];after[row]=value['quantity_after_shares']
                    # 原层前驱/源量和队列归约共同入口；没有活动Maker订单的该有限时段。
                    external=runtime.hidden.external_shares
                    mask=T([i==row for i in range(len(slots))],torch.bool)
                    assert amounts(external)[row]==Fraction(before[row])
                    queue,_=run(lambda:hidden.queues.apply_level_transition(integers([units(v) for v in before]),integers([units(v) for v in after])))
                    from hidden_market import rational_select
                    hidden=replace(hidden,queues=queue,external_shares=run(lambda:rational_select(rational(after),mask,hidden.external_shares)))
            runtime=replace(runtime,hidden=hidden);changed=True;continue
        if phase==2:
            timeline.append({'phase':'finality','at':at})
            runtime,frame,_=run(lambda:runtime.finality(choice,maker,taker,end))
            noe,batch=run(lambda:noe.fill_from_origin(sampler,runtime.orders,runtime.fills,frame,T(list(range(asset_count))),runtime.calendars,taker,choice.at))
        elif phase==5:
            timeline.append({'phase':'post','at':at});pending=runtime.requests.next_pending()
            refs=pending.item_ref
            ask_rows=ask_directory[intents.assets.gather(1,refs[:,None])[:,0]]
            paths=T([[0]*(ask_rows.size(1)*runtime.hidden.queues.capacity)])
            terms=intents.encode(pending,runtime.hidden,runtime.market_controls,rule,codec,choice.enabled(5))
            if not capacity_checked and count==2 and effect_index==1:
                small=run(lambda:accept_pending_base(runtime.requests,pending,runtime.orders,runtime.hidden.queues,runtime.account,runtime.fills,
                    terms,ask_rows,rational([source['rule']['fee_rate']]),source['rule']['fee_exponent'],
                    paths,taker,owners,refs,calendars=runtime.calendars,enabled=choice.enabled(5),post_results=runtime.post_results))
                progress({'stage':'capacity_candidate','case':source['name'],'fault':small[-1].fault.cpu().tolist(),
                    'result_capacity':runtime.post_results.capacity,'result_ref':refs.cpu().tolist(),
                    'required_results':small[-1].required_results.cpu().tolist(),
                    'required_orders':small[-1].order.required_orders.cpu().tolist(),
                    'required_fills':small[-1].acceptance.required_records.cpu().tolist(),
                    'required_segments':small[-1].required_segments.cpu().tolist(),
                    'old_done':runtime.requests.done.cpu().tolist(),'candidate_done':small[0].done.cpu().tolist(),
                    'old_cash':values(runtime.account.cash),'candidate_cash':values(small[3].cash)})
                assert small[-1].fault.cpu().tolist()==[2]
                assert values(small[3].cash)==values(runtime.account.cash) and values(small[1].next_sequence)==values(runtime.orders.next_sequence)
                assert torch.equal(small[0].done,runtime.requests.done)
                capacity_checked=True
            runtime=runtime.advance_role_time(choice,end)
            choice=runtime.choose(hidden_source,yes,horizon,yes)
            runtime,noe,batch,effects=runtime.post_pending(choice,intents,rule,codec,ask_rows,paths,taker,owners,refs,noe,sampler,T(list(range(asset_count))))
            if effects.fault.cpu().tolist()!=[0]:
                progress({'stage':'post_failure','case':source['name'],'at':at,'fault':effects.fault.cpu().tolist(),
                    'prepared_fault':effects.prepared.fault.cpu().tolist(),'quantity_fault':effects.prepared.quantity.fault.cpu().tolist(),
                    'acceptance_fault':effects.acceptance.fault.cpu().tolist(),'order_fault':effects.order.fault.cpu().tolist(),
                    'rejection_reason':effects.prepared.rejection_reason.cpu().tolist(),'batch_fault':batch.fault.cpu().tolist(),
                    'accepted':effects.acceptance.submission_delta.emitted.cpu().tolist()})
            assert effects.fault.cpu().tolist()==[0],effects.fault.cpu().tolist()
            expected=source['effects'][effect_index]
            assert at==expected['key']['at'] and int(pending.ordinal.cpu()[0])==expected['key']['ordinal']
            full_item(run(lambda:runtime.post_results.read(refs)),expected['outcome'])
            cp=expected['checkpoint']
            for name in ('cash','available','principal','fee'):assert values(getattr(runtime.account,name))==[units(cp[name])],(name,values(getattr(runtime.account,name)),cp[name])
            assert values(runtime.account.next_sequence)==[cp['next_sequence']]
            effect_index+=1
            if effect_index<count:assert not effects.response.emitted.cpu().item()
            else:assert effects.response.emitted.cpu().item()
        else:raise AssertionError(('unexpected actual phase',phase,at))
        assert batch.fault.cpu().tolist()==[0]
        batches.append(batch)
        if batch.emitted.any().cpu().item():ledger=run(lambda:ledger.append_batch(noe.normalizer.bank,batch))
    assert timeline==source['drive'],(timeline,source['drive'])
    assert effect_index==count and runtime.requests.sealed.cpu().tolist()==[[True]]
    expected={(r['position']['source_watermark']['projection_batch_sequence'],r['position']['record_ordinal']):r for r in source['records']}
    seen={}
    for batch in batches:
        for column,on in enumerate(batch.emitted.cpu().tolist()[0]):
            if not on:continue
            key=values(batch.batch_sequences)[column],values(batch.record_ordinals)[column]
            original=expected[key];row=int(batch.rows[0,column].cpu());seen[row]=original
            wanted=run(lambda:original_semantic(original,source));actual=noe.normalizer.bank.semantic[row:row+1]
            width=max(wanted.size(1),actual.size(1))
            assert torch.equal(torch.nn.functional.pad(actual,(0,width-actual.size(1))),torch.nn.functional.pad(wanted,(0,width-wanted.size(1)))),key
            body=next(iter(original['record'].values()));ref=body.get('source_reference') or body['fact']['source_reference']
            category,meta=next(iter(ref['record'].items()))
            assert values(batch.private_sequences)[column]==meta['private_feedback_sequence']
            assert int(batch.received_at[0,column].cpu())==meta['response_received_at_utc_ms' if category=='DirectResponse' else 'feedback_received_at_utc_ms']
    assert len(seen)==len(source['records'])
    gates=ReceiveGates.unset(1).set_receive(T([source['closed_at']]),yes).set_public(T([source['closed_at']]),yes)
    ledger,delivery=run(lambda:ledger.receive(noe.normalizer.bank,gates,torch.ones_like(ledger.ledger.columns.enabled)))
    if delivery.fault.cpu().tolist()!=[0] or (ledger.waiting.received&~ledger.waiting.consumed).any().cpu().item():
        progress({'stage':'ledger_failure','case':source['name'],'delivery_fault':delivery.fault.cpu().tolist(),
            'ledger_fault':ledger.ledger.fault.cpu().tolist(),'waiting_fault':ledger.waiting.fault.cpu().tolist(),
            'received':ledger.waiting.received.cpu().tolist(),'consumed':ledger.waiting.consumed.cpu().tolist(),
            'schemas':ledger.ledger.lifecycle.schemas.cpu().tolist(),'rows':ledger.ledger.columns.row_refs.cpu().tolist(),
            'entry':values(ledger.ledger.current_entry_sequence),'local':values(ledger.ledger.current_local_prefix),
            'platform_prefix':values(ledger.ledger.platform.platform_source_prefix)})
    assert delivery.fault.cpu().tolist()==[0] and not (ledger.waiting.received&~ledger.waiting.consumed).any().cpu().item()
    checkpoint(ledger.ledger,source['received_ledger'])
    original_prefix=values(ledger.ledger.current_entry_sequence);original_cash=amounts(ledger.ledger.platform.balances)
    next_order=values(runtime.orders.next_sequence);next_fill=values(runtime.fills.next_sequence)
    repeated,account,repeat,_=run(lambda:sending(runtime,ledger.ledger))
    assert repeat.duplicate.cpu().tolist()==[True] and not repeat.registered.cpu().item()
    assert torch.equal(repeated.requests.done,runtime.requests.done) and torch.equal(repeated.requests.result_refs,runtime.requests.result_refs)
    assert values(account.current_entry_sequence)==original_prefix and amounts(account.platform.balances)==original_cash
    assert values(repeated.orders.next_sequence)==next_order and values(repeated.fills.next_sequence)==next_fill
    ledger,again=run(lambda:ledger.receive(noe.normalizer.bank,gates,torch.ones_like(ledger.ledger.columns.enabled)))
    assert not again.closed.emitted.any().cpu().item() and values(ledger.ledger.current_entry_sequence)==original_prefix
    # 原逐腿正文拥有接单时旧值，不能被以后最终性/下一腿计量覆盖。
    for original in source['effects']:
        ref=next(i for i,p in enumerate(posts) if p['batch_item_ordinal']==original['key']['ordinal'])
        full_item(run(lambda:runtime.post_results.read(T([ref]))),original['outcome'])
    report={'name':source['name'],'actual_pending_count':count,'actual_NOE_records':len(seen),
        'processing_times':[e['key']['at'] for e in source['effects']],'capacity_one_original_prefix_retry':capacity_checked,
        'original_full_post_and_NOE_Ledger_checked':True,'elapsed_seconds':time.perf_counter()-started}
    if after_posts is not None:
        report['cancellation']=after_posts(source,runtime,ledger,noe,sampler,rule,slots,owners,receipt,progress)
    progress(report);return report


def main():
    source=json.load(sys.stdin);started=time.perf_counter()
    spec=importlib.util.spec_from_file_location('model_native_inputs',os.environ['B_GPU_NATIVE_SDK_LIBRARY'])
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    artifact=Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR']);artifact.mkdir(parents=True,exist_ok=True)
    def progress(value):
        with (artifact/'pending-post-progress.jsonl').open('a') as target:target.write(json.dumps(value)+'\n')
    reports=[]
    for index,case in enumerate(source):
        progress({'stage':'case_started','case':case['name'],'completed':index,'total':len(source),'elapsed_seconds':time.perf_counter()-started})
        reports.append(check(case,module.post_terms_batch_v2,progress))
    print(json.dumps({'cases':reports,'elapsed_seconds':time.perf_counter()-started,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),'full_Actor_send_host_delivered':False}))


if __name__=='__main__':main()
