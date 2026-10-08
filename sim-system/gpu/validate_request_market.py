"""真实双市场pending请求的准入及共同接纳候选；CPU outcome仅作对照。"""
from validate_pending_post import *


def check_market(source,codec):
    started=time.perf_counter();yes=T([True],torch.bool);zero=T([0]);initial=source['initial']
    slots=[]
    for asset,event in enumerate(initial):
        for side in ('bids','asks'):
            for level in event['payload']['event'][side]:slots.append((asset,side,Fraction(level['price_pusd_per_share'])))
    slots=sorted(slots)
    def build():
        hidden=HiddenAssetState.empty(T([a for a,_,_ in slots]),rational([str(p) for _,_,p in slots]),T([0,0,1,1]),2,segment_capacity=1)
        for asset,event in enumerate(initial):
            book=event['payload']['event'];quantities=['0']*len(slots)
            for side in ('bids','asks'):
                for level in book[side]:quantities[slots.index((asset,side,Fraction(level['price_pusd_per_share'])))]=level['quantity_shares']
            hidden,_=hidden.initial_snapshot(T([i==asset for i in range(4)],torch.bool),rational(quantities),
                rational([book['initial_tick_size_pusd_per_share']]*4),T([0]*4))
        hidden=hidden.advance_matching_time(T([2000]*4),T([2000,2000,3000,3000]))
        blank=integers([0]);quantity=BaseOrderQuantity.from_taker_plan(integers([1]),integers([1]),blank,blank)
        record=FillRecord.create(integers([1]),integers([1]),blank,zero,zero,~yes,source['finality_path'])
        return hidden,MutationRequests.empty(blank,capacity=1,action_capacity=1),BaseOrders.empty(quantity,capacity=1),\
            PlatformAccountState.open(integers([units(source['before']['cash'])]),T([0]*4)),FillPool.empty(record,capacity=1)
    hidden,requests,orders,account,fills=run(build)
    rule=PublicRuleGpu(source['rule']);sampler=EmpiricalDelaySampler.build((ROOT/'sim-system/config/latency-profiles'/source['profile_sha256']/'profile.json').read_bytes(),
        source['profile_sha256'],[source['session_seed'].encode('ascii')]);identity,offset=byte_column([source['request_id'].encode()])
    requests,receipt=run(lambda:requests.register(sampler,identity,offset,zero,T([1]),integers([source['input_sequence']]),T([source['sent_at']]),
        T([[0]]),T([[0]]),yes[:,None],T([[source['rule']['taker_delay_enabled']]],torch.bool),T([source['rule']['taker_delay_ms']]),yes,T([0],torch.int32)))
    pending=requests.next_pending();assert pending.effective_at.cpu().tolist()==[source['processing']]
    assert receipt.arrived_at.cpu().tolist()==[source['arrived_at']]
    token=source['item']['order']['token_id'];tokens=[event['payload']['event']['asset']['token_id'] for event in initial]
    asset=tokens.index(token) if token in tokens else -1
    bid=next(i for i,(a,s,_) in enumerate(slots) if a==2 and s=='bids')
    intents=PendingPostIntents(((json.dumps(source['item']),),),T([[asset]]),T([[1]]),T([[bid]]),T([[3000]]),T([0],torch.int32))
    controls=HiddenMarketControls.empty(1,2,'cuda')
    calls=[]
    def encoding(body):calls.append(json.loads(body));return codec(body)
    terms=intents.encode(pending,hidden,controls,rule,encoding,yes)
    ask_rows=T([[i for i,(a,s,_) in enumerate(slots) if a==2 and s=='asks']])
    paths=T([[0]*(ask_rows.size(1)*hidden.queues.capacity)])
    candidate=run(lambda:accept_pending_base(requests,pending,orders,hidden.queues,account,fills,terms,ask_rows,
        rational([source['rule']['fee_rate']]),source['rule']['fee_exponent'],paths,rational(['0']),T([0]*len(slots)),zero,enabled=yes))
    new_requests,new_orders,queue,new_account,new_fills,effects=candidate
    assert effects.fault.cpu().tolist()==[0],effects.fault.cpu().tolist()
    if source['name']!='legal_market_token':
        assert REJECTED_REASONS[int(effects.prepared.rejection_reason.cpu()[0])]==source['outcome']['RejectedNoEffect']=='AssetNotInMarket'
        assert calls[0]['entries']==[]  # 不将非法资产进入官方SDK。
        assert not effects.acceptance.submission_delta.emitted.any().cpu().item()
        for name in ('cash','available','principal','fee','next_sequence'):assert values(getattr(new_account,name))==values(getattr(account,name))
        assert values(new_orders.next_sequence)==values(orders.next_sequence)
        assert torch.equal(new_orders.occupied,orders.occupied) and torch.equal(new_fills.occupied,fills.occupied)
        assert torch.equal(queue.kind,hidden.queues.kind) and torch.equal(queue.order_ids,hidden.queues.order_ids)
        for name in ('total_base','total_quote','consumed_base','account_remaining','debt'):
            assert values(getattr(queue,name))==values(getattr(hidden.queues,name))
    else:
        assert len(calls[0]['entries'])==1 and effects.acceptance.submission_delta.emitted.cpu().tolist()==[True]
        for name in ('cash','available','principal','fee'):assert values(getattr(new_account,name))==[units(source['after'][name])]
        assert values(new_account.next_sequence)==[source['after']['next_sequence']]
    return dict(name=source['name'],processing=source['processing'],reason=source['outcome'].get('RejectedNoEffect'),
        sdk_entries=len(calls[0]['entries']),original_cash_and_identity_checked=True,elapsed_seconds=time.perf_counter()-started)


def main():
    cases=json.load(sys.stdin);spec=importlib.util.spec_from_file_location('model_native_inputs',os.environ['B_GPU_NATIVE_SDK_LIBRARY'])
    native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
    reports=[]
    for case in cases:
        report=check_market(case,native.post_terms_batch_v2);reports.append(report);print(json.dumps(dict(event='request_market_checked',**report)),file=sys.stderr,flush=True)
    print(json.dumps(dict(cases=reports,actual_two_market_cpu_reference=True,full_Actor_send_host_delivered=False)))

if __name__=='__main__':main()
