"""由原SIM FIFO测试供给精确状态；验证实际GPU候选，不生成交易反馈。"""
import json
from pathlib import Path
import sys
import time
import torch

sys.path.insert(0,str(Path(__file__).resolve().parent))
from queues import ExactInt,ExactRational,PriceQueueBatch,run_queue_batch,Numbers


def integers(values):
    return ExactInt.from_values(values)


def values(value):
    words=value.words.cpu().tolist();signs=value.sign.cpu().tolist()
    assert not value.fault.any().cpu().item()
    return [sign*sum((word&0xffffffff)<<(32*index) for index,word in enumerate(row)) for row,sign in zip(words,signs)]


def describe(queue):
    assert not queue.fault.any().cpu().item()
    b,q,c,a=map(values,(queue.total_base,queue.total_quote,queue.consumed_base,queue.account_remaining))
    kinds=queue.kind.cpu().tolist();orders=queue.order_ids.cpu().tolist();debts=values(queue.debt)
    result=[]
    for root in range(queue.rows):
        segments=[]
        for col,kind in enumerate(kinds[root]):
            index=root*queue.capacity+col
            if kind==1:
                segments.append({'kind':'external','base':str(b[index]),'quote':str(q[index]),'consumed':str(c[index])})
            elif kind==2:
                assert orders[root][col]==7
                segments.append({'kind':'account','order':'account-order','remaining':str(a[index])})
        result.append({'debt':str(debts[root]),'segments':segments})
    return result


def main():
    reference=json.load(sys.stdin)
    started=time.perf_counter()
    price=Numbers.run_exact_batch(lambda:ExactRational.ratio(integers([4,5]),integers([10,10])))
    queue=PriceQueueBatch.empty(price,capacity=1)
    traces=[]
    def checkpoint(name):
        traces.append({'stage':name,'queues':describe(queue)})
    queue,_=run_queue_batch(queue,lambda state:state.append_external(integers([2_000_000,10_000_000])))
    checkpoint('initial')
    ids=torch.tensor([7,-1],dtype=torch.int64,device='cuda')
    queue,_=run_queue_batch(queue,lambda state:state.append_account(ids,integers([3_000_000,0])))
    checkpoint('account_tail')
    queue,_=run_queue_batch(queue,lambda state:state.apply_level_transition(integers([2_000_000,10_000_000]),integers([7_000_000,10_000_000])))
    checkpoint('external_tail')
    # 原有效段保持FIFO顺序，仅将一个empty物理槽放到中间；不合并B/Q。
    # 保孔洞的物理位置、完整字段和原CPU正量对照，尾部仍空。
    from dataclasses import replace, fields
    from queues import _take
    sparse=queue.resized(8)
    permutation=torch.tensor([0,7,1,2,3,4,5,6],device='cuda')
    flat=(torch.arange(sparse.rows,device='cuda')[:,None]*sparse.capacity+permutation[None,:]).flatten()
    sparse=replace(sparse,kind=sparse.kind[:,permutation],order_ids=sparse.order_ids[:,permutation],
        **{name:_take(getattr(sparse,name),flat) for name in
           ('total_base','total_quote','consumed_base','account_remaining')})
    assert sparse.kind[0,:4].cpu().tolist()==[1,0,2,1]
    before,after=integers([7_000_000,10_000_000]),integers([4_000_000,10_000_000])
    actual=Numbers.run_exact_batch(lambda:sparse.apply_level_transition(before,after))
    fixed=Numbers.device_exact_batch(lambda:sparse.apply_level_transition(before,after),word_capacity=4,device='cuda')
    assert fixed.fits.cpu().item()
    def same(left,right):
        if isinstance(left,torch.Tensor):torch.testing.assert_close(left,right,rtol=0,atol=0)
        elif isinstance(left,tuple):
            for a,b in zip(left,right,strict=True):same(a,b)
        elif hasattr(left,'__dataclass_fields__'):
            for field in fields(left):same(getattr(left,field.name),getattr(right,field.name))
        else:assert left==right
    same(actual,fixed.value)
    assert actual[1].order_ids.shape==(2,8) and actual[1].maker_base_units.sign.shape==(16,)
    assert values(actual[1].maker_base_units)[2]==int(reference['maker_depletion'])
    assert values(actual[1].maker_base_units)[8:]==[0]*8
    queue,quote=Numbers.run_exact_batch(lambda:queue.apply_taker_slice(torch.zeros(2,dtype=torch.int64,device='cuda'),integers([0,4_000_000])))
    assert values(quote)==[0,int(reference['taker_quote'])]
    queue=Numbers.run_exact_batch(lambda:queue.finish_taker_plan(torch.ones(2,dtype=torch.bool,device='cuda')))
    checkpoint('taker_applied')
    queue,effects=run_queue_batch(queue,lambda state:state.apply_level_transition(integers([7_000_000,10_000_000]),integers([4_000_000,6_000_000])))
    depletion=values(effects.maker_base_units)
    assert sum(depletion[:queue.capacity])==int(reference['maker_depletion'])
    assert sum(depletion[queue.capacity:])==0
    checkpoint('fifo_depletion_and_debt_echo')
    queue=Numbers.run_exact_batch(lambda:queue.clear_external(torch.tensor([True,False],device='cuda')))
    checkpoint('resync_preserves_account')
    queue,cancelled,found=Numbers.run_exact_batch(lambda:queue.cancel(ids))
    assert values(cancelled)==[2_000_000,0]
    assert found.cpu().tolist()==[True,False]
    checkpoint('cancel_removes_target_only')
    queue=Numbers.run_exact_batch(lambda:queue.clear_external(torch.tensor([False,True],device='cuda')))
    checkpoint('clear')
    assert traces==reference['checkpoints'],{'actual':traces,'expected':reference['checkpoints']}
    # 不合并外部段。段池从1按真实需求长到16，没有容量业务拒绝或CPU读取队列值推进。
    for _ in range(10):
        queue,_=run_queue_batch(queue,lambda state:state.append_external(integers([0,2])))
    grown=describe(queue)
    assert len(grown[1]['segments'])==10 and queue.capacity==16
    # 新增价位外部段原B/Q被保留，已消耗的段不改成新本金。
    assert all(segment['base']=='2' and segment['quote']=='1' for segment in grown[1]['segments'])
    # 同价位原planner的两片必须保留规划坐标；第一片耗尽也不能移动第二片。
    plan=reference['multi_plan']
    multi_price=Numbers.run_exact_batch(lambda:ExactRational.ratio(integers([5]),integers([10])))
    multi=PriceQueueBatch.empty(multi_price,capacity=2)
    multi,_=run_queue_batch(multi,lambda state:state.append_external(integers([2])))
    multi,_=run_queue_batch(multi,lambda state:state.apply_level_transition(integers([2]),integers([5])))
    assert describe(multi)==[plan['initial']]
    for ordinal,part in enumerate(plan['slices']):
        multi,used=Numbers.run_exact_batch(lambda:multi.apply_taker_slice(
            torch.tensor([part['column']],dtype=torch.int64,device='cuda'),integers([int(part['base'])])))
        assert values(used)==[int(part['quote'])]
        if ordinal==0:
            assert multi.kind.cpu().tolist()==[[1,1]]
            assert values(multi.consumed_base)==[2,0]
            # 未完成行即使经过完成屏障，也不改变原坐标。
            multi=Numbers.run_exact_batch(lambda:multi.finish_taker_plan(torch.tensor([False],device='cuda')))
            assert values(multi.total_base)==[2,3]
    multi=Numbers.run_exact_batch(lambda:multi.finish_taker_plan(torch.tensor([True],device='cuda')))
    assert describe(multi)==[plan['applied']]
    multi,_=run_queue_batch(multi,lambda state:state.apply_level_transition(integers([5]),integers([0])))
    assert describe(multi)==[plan['echo']]
    for original in reference['quote_plans']:
        source=PriceQueueBatch.empty(multi_price,capacity=2)
        source,_=run_queue_batch(source,lambda state:state.append_external(integers([int(original['base'])])))
        source,_=run_queue_batch(source,lambda state:state.apply_level_transition(
            integers([int(original['base'])]),integers([int(original['after'])])))
        assert describe(source)==[original['initial']]
        budget=integers([int(original['budget'])])
        candidate=Numbers.run_exact_batch(lambda:source.plan_quote_budget(budget))
        assert values(candidate.matched_base)==[int(original['matched'])]
        assert values(candidate.used_quote)==[int(original['used'])]
        actual_slices=[{'column':index,'base':str(base),'quote':str(quote)}
                      for index,(base,quote) in enumerate(zip(values(candidate.delta_base),values(candidate.delta_quote))) if base>0]
        assert actual_slices==original['slices']
        acceptance=candidate.used_quote.compare(budget)
        accepted=(acceptance.order==0)&(acceptance.fault==0)&(candidate.fault==0)
        applied=Numbers.run_exact_batch(lambda:source.apply_quote_plan(candidate,accepted))
        assert describe(applied)==[original['applied']]
        assert describe(source)==[original['initial']]
        try:
            applied.apply_quote_plan(candidate,accepted)
        except ValueError:
            pass
        else:
            raise AssertionError('旧计划不得重复应用到新队列')
    from asset_buy import plan_asset_quote, apply_asset_quote
    cross_price=Numbers.run_exact_batch(lambda:ExactRational.ratio(integers([4,5,6]),integers([10,10,10])))
    cross=PriceQueueBatch.empty(cross_price,capacity=1)
    cross,_=run_queue_batch(cross,lambda value:value.append_external(integers([10,10,10])))
    layout=torch.tensor([[0,1,2]],dtype=torch.int64,device='cuda')
    maximum=Numbers.run_exact_batch(lambda:ExactRational.ratio(integers([6]),integers([10])))
    for original in reference['cross_quote_plans']:
        budget=integers([int(original['budget'])])
        assert describe(cross)==original['initial']
        plan=Numbers.run_exact_batch(lambda:plan_asset_quote(cross,layout,budget,maximum))
        assert not plan.fault.any().cpu().item()
        assert values(plan.matched_base)==[int(original['matched'])]
        assert values(plan.used_quote)==[int(original['used'])]
        actual=[]
        for row,(base,quote) in enumerate(zip(values(plan.queue_plan.delta_base),values(plan.queue_plan.delta_quote))):
            if base>0:
                actual.append({'price':str([0.4,0.5,0.6][row]),'column':0,'base':str(base),'quote':str(quote)})
        assert actual==original['slices']
        result=Numbers.run_exact_batch(lambda:apply_asset_quote(cross,plan,torch.ones(1,dtype=torch.bool,device='cuda')))
        assert describe(result)==original['applied']
        assert describe(cross)==original['initial']
    empty_price=Numbers.run_exact_batch(lambda:ExactRational.ratio(integers([]),integers([])))
    empty_queue=PriceQueueBatch.empty(empty_price,capacity=1)
    empty_layout=torch.empty((1,0),dtype=torch.int64,device='cuda')
    empty_plan=Numbers.run_exact_batch(lambda:plan_asset_quote(empty_queue,empty_layout,integers([7]),maximum))
    assert not empty_plan.fault.any().cpu().item()
    assert values(empty_plan.matched_base)==[int(reference['empty_quote_plan']['matched'])]
    assert values(empty_plan.used_quote)==[int(reference['empty_quote_plan']['used'])]
    assert empty_plan.complete.cpu().tolist()==[False]
    empty_applied=Numbers.run_exact_batch(lambda:apply_asset_quote(empty_queue,empty_plan,torch.ones(1,dtype=torch.bool,device='cuda')))
    assert describe(empty_applied)==[] and empty_applied.rows==0
    originals=reference['floor_sums']
    n=integers([int(item['n']) for item in originals])
    m=integers([int(item['m']) for item in originals])
    a=integers([int(item['a']) for item in originals])
    b=integers([int(item['b']) for item in originals])
    sums=Numbers.run_exact_batch(lambda:n.floor_sum(m,a,b))
    assert values(sums)==[int(item['expected']) for item in originals]
    assert sums.words.shape[1]>=15, '真实450位结果须增长，不截到输入位宽'
    invalid=Numbers.run_exact_batch(lambda:integers([5,-1]).floor_sum(integers([0,7]),integers([9,9]),integers([17,17])))
    assert invalid.fault.cpu().tolist()==[1,1]
    from limit_segment import maximum_limit_segment
    limit_originals=reference['limit_segments']
    args=[integers([int(item['arguments'][index]) for item in limit_originals]) for index in range(8)]
    limit_started=time.perf_counter()
    limit=Numbers.run_exact_batch(lambda:maximum_limit_segment(*args))
    limit_base,limit_quote=values(limit.base),values(limit.quote)
    assert not limit.fault.any().cpu().item(),limit.fault.cpu().tolist()
    for index,item in enumerate(limit_originals):
        actual=[str(limit_base[index]),str(limit_quote[index])] if bool(limit.present[index].cpu()) else None
        assert actual==item['result'],(item['arguments'],actual,item['result'])
    limit_seconds=time.perf_counter()-limit_started
    from asset_buy import plan_asset_limit,apply_asset_limit
    limit_cross_started=time.perf_counter()
    for original in reference['cross_limits']:
        total_base,total_quote=integers([int(original['base'])]),integers([int(original['quote'])])
        from fractions import Fraction
        price=Fraction(original['maximum'])
        maximum_limit=Numbers.run_exact_batch(lambda:ExactRational.ratio(integers([price.numerator]),integers([price.denominator])))
        plan=Numbers.run_exact_batch(lambda:plan_asset_limit(cross,layout,total_base,total_quote,maximum_limit))
        assert not plan.fault.any().cpu().item(),plan.fault.cpu().tolist()
        assert values(plan.matched_base)==[int(original['matched'])]
        assert values(plan.used_quote)==[int(original['used'])]
        assert plan.complete.cpu().tolist()==[original['matched']==original['base']]
        actual=[]
        for row,(base,quote) in enumerate(zip(values(plan.queue_plan.delta_base),values(plan.queue_plan.delta_quote))):
            if base>0:
                actual.append({'price':str([0.4,0.5,0.6][row]),'column':0,'base':str(base),'quote':str(quote)})
        assert actual==original['slices']
        result=Numbers.run_exact_batch(lambda:apply_asset_limit(cross,plan,torch.ones(1,dtype=torch.bool,device='cuda')))
        assert describe(result)==original['applied']
    limit_cross_seconds=time.perf_counter()-limit_cross_started
    from taker_fee import allocate_taker_fee
    for original in reference['taker_fees']:
        count=len(original['base'])
        base=integers([int(value) for value in original['base']])
        prices=[Fraction(value) for value in original['prices']]
        rate_value=Fraction(original['rate'])
        price=Numbers.run_exact_batch(lambda:ExactRational.ratio(integers([value.numerator for value in prices]),integers([value.denominator for value in prices])))
        rate=Numbers.run_exact_batch(lambda:ExactRational.ratio(integers([rate_value.numerator]),integers([rate_value.denominator])))
        fee=Numbers.run_exact_batch(lambda:allocate_taker_fee(base,price,rate,original['exponent'],1,count))
        assert not fee.fault.any().cpu().item(),fee.fault.cpu().tolist()
        assert values(fee.total_micro)==[int(original['total'])]
        assert values(fee.per_fill_micro)==[int(value) for value in original['allocations']]
    failed_price=Numbers.run_exact_batch(lambda:ExactRational.ratio(integers([1]),integers([0])))
    rate=Numbers.run_exact_batch(lambda:ExactRational.ratio(integers([1]),integers([4])))
    failed_fee=Numbers.run_exact_batch(lambda:allocate_taker_fee(integers([1]),failed_price,rate,4_000_000_000,1,1))
    assert failed_fee.fault.cpu().tolist()==[1]
    assert failed_fee.total_micro.fault.cpu().tolist()==[1] and failed_fee.per_fill_micro.fault.cpu().tolist()==[1]
    from account_state import PlatformAccountState
    account_reference=reference['account']
    enabled=torch.ones(1,dtype=torch.bool,device='cuda')
    account=Numbers.run_exact_batch(lambda:PlatformAccountState.open(integers([2_000_000_000]),torch.zeros(1,dtype=torch.int64,device='cuda')))
    def describe_account(state):
        rebate_n,rebate_d=values(state.rebates.numerator),values(state.rebates.denominator)
        fields={name:str(Fraction(values(getattr(state,name))[0],1_000_000)) for name in ('cash','available','principal','fee')}
        # Compare exact quantities, not decimal formatting.
        return fields,[Fraction(n,d) for n,d in zip(rebate_n,rebate_d)],values(state.outcomes),values(state.next_sequence)
    def check_account(state,expected):
        fields,rebates,outcomes,sequence=describe_account(state)
        for name,value in fields.items():
            assert Fraction(value)==Fraction(expected[name]),(name,value,expected[name])
        assert rebates==[Fraction(value) for value in expected['rebates']]
        original_outcomes={token:Fraction(value)*1_000_000 for token,value in expected['outcomes']}
        assert outcomes==[int(original_outcomes.get('1',0))]
        assert sequence==[int(expected['next_sequence'])]
    def check_delta(delta,expected):
        assert delta.emitted.cpu().tolist()==[True]
        assert values(delta.sequence)==[expected['account_transition_seq']]
        source=expected['balance_delta']
        for name,field in [('cash','cash_pusd_delta'),('available','available_pusd_delta'),
                           ('principal','principal_encumbrance_pusd_delta'),('fee','immediate_taker_fee_encumbrance_pusd_delta')]:
            assert values(getattr(delta.movement,name))==[int(Fraction(source[field])*1_000_000)]
        original_outcomes={item['token_id']:Fraction(item['balance_shares_delta'])*1_000_000 for item in source['outcome_balance_deltas']}
        assert values(delta.movement.outcomes)==[int(original_outcomes.get('1',0))]
    check_account(account,account_reference['opened'])
    account,delta,rejected=Numbers.run_exact_batch(lambda:account.accept_order(integers([4_000_000]),integers([10_000]),integers([5_010_000]),enabled))
    assert rejected.cpu().tolist()==[False]
    check_account(account,account_reference['accepted']);check_delta(delta,account_reference['deltas'][0])
    account,delta=Numbers.run_exact_batch(lambda:account.fill_economic(integers([2_000_000]),integers([1_000_000]),integers([10_000]),torch.zeros(1,dtype=torch.int64,device='cuda'),enabled))
    check_account(account,account_reference['filled']);check_delta(delta,account_reference['deltas'][1])
    account,delta=Numbers.run_exact_batch(lambda:account.release_principal(integers([3_000_000]),enabled))
    check_account(account,account_reference['cancelled']);check_delta(delta,account_reference['deltas'][2])
    same,delta=Numbers.run_exact_batch(lambda:account.release_principal(integers([0]),enabled))
    check_account(same,account_reference['cancelled']);assert delta.emitted.cpu().tolist()==[False]
    rejected_state,delta,rejected=Numbers.run_exact_batch(lambda:account.accept_order(integers([3_000_000_000]),integers([0]),integers([3_000_000_000]),enabled))
    check_account(rejected_state,account_reference['cancelled']);assert rejected.cpu().tolist()==[True]
    assert delta.emitted.cpu().tolist()==[False] and not rejected_state.fault.any().cpu().item()
    failed,delta=Numbers.run_exact_batch(lambda:account.release_principal(integers([1_000_000]),enabled))
    check_account(failed,account_reference['cancelled']);assert delta.emitted.cpu().tolist()==[False]
    assert failed.fault.cpu().tolist()==[1]
    torch.cuda.synchronize()
    print(json.dumps({'checks':['original_fifo_all_fields','taker_original_quote_difference','debt_echo_no_second_depletion',
        'resync_retains_account_order','cancel_only_target','segment_growth_retains_original_B_Q',
        'original_multi_segment_plan_coordinates','plan_finish_preserves_unfinished_rows',
        'original_quote_budget_planning','incomplete_FOK_queue_unchanged','partial_segment_stops',
        'stale_plan_not_applied','original_cross_price_quote_plan','partial_price_stops_later_prices',
        'whole_budget_FOK_gate_before_any_queue_apply','original_Euclid_floor_sum',
        'floor_sum_grows_for_large_exact_result','floor_sum_invalid_not_normal_value','original_empty_asset_quote_no_liquidity','original_nonmonotone_limit_segment',
        'limit_segment_exact_96bit_quantity','original_GTC_cross_price_cumulative_credit',
        'GTC_partial_target_applies_without_FOK_gate','maximum_price_stops_before_later_ask','original_order_total_taker_fee_H5',
        'exact_remainder_allocation_original_fill_order','taker_fee_large_exponent_strict_zero','original_SIM_account_encumbrance_and_economic_delta',
        'cancellation_principal_only_zero_is_no_delta','insufficient_available_is_rejection_without_mutation',
        'account_underflow_no_partial_commit','large_exponent_preserves_failed_price_fault'],
        'limit_cross_seconds':limit_cross_seconds,'limit_segment_seconds':limit_seconds,'capacity':queue.capacity,'elapsed_seconds':time.perf_counter()-started,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'noe_ledger_execution_delivered':False}))


if __name__=='__main__':
    main()
