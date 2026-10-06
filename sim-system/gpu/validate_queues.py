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
    torch.cuda.synchronize()
    print(json.dumps({'checks':['original_fifo_all_fields','taker_original_quote_difference','debt_echo_no_second_depletion',
        'resync_retains_account_order','cancel_only_target','segment_growth_retains_original_B_Q',
        'original_multi_segment_plan_coordinates','plan_finish_preserves_unfinished_rows',
        'original_quote_budget_planning','incomplete_FOK_queue_unchanged','partial_segment_stops',
        'stale_plan_not_applied'],
        'capacity':queue.capacity,'elapsed_seconds':time.perf_counter()-started,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'noe_ledger_execution_delivered':False}))


if __name__=='__main__':
    main()
