"""同一原NOE人口的开放端点和原包含端点兼容；不归约经济结果。"""
from pathlib import Path
import json,sys,time
import torch
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('shared-run/gpu','sim-system/gpu','execution-normalization/gpu','ledger/gpu')]
from queues import ExactInt,Numbers
from consumption import PendingRecords,ReceiveGates
from input_frontier import InputCommitFrontier
from validate_base_execution import values


def main():
    started=time.perf_counter();source=json.loads(Path(sys.argv[1]).read_text());records=source['records']
    received=[];private=[];ordinals=[]
    for item in records:
        body=next(iter(item['record'].values()))
        reference=body.get('source_reference') or body['fact']['source_reference']
        source_record=next(iter(reference['record'].values()))
        received.append(source_record.get('feedback_received_at_utc_ms',source_record.get('response_received_at_utc_ms')))
        order=body['source_order'];private.append(order['sequence']['PrivateFeedback']['private_feedback_sequence']);ordinals.append(order['ordinal'])
    t=lambda values,dtype=torch.int64:torch.tensor(values,dtype=dtype,device='cuda')
    pending=Numbers.run_exact_batch(lambda:PendingRecords.from_columns(t([[True]*len(records)],torch.bool),
        t([received]),ExactInt.from_values(private),ExactInt.from_values(ordinals),t([list(range(len(records)))]),ExactInt.from_values([0])))
    at=t([1414]);yes=t([True],torch.bool)
    # 实际第一个Direct先领取：source private=3，原delivery=1。
    early,first=Numbers.run_exact_batch(lambda:pending.take_closed(ReceiveGates.unset(1).before(t([1394]),yes)))
    assert int(first.emitted.sum().cpu())==1
    frontier=Numbers.run_exact_batch(lambda:InputCommitFrontier.open(1,source['run_id'],source['session_id']))
    # 此代表只核cutoff的反馈前沿；公共组来源引用不参与领取分类。
    frontier,_=Numbers.run_exact_batch(lambda:frontier.public_group(object(),t([0]),yes))
    _,cutoff,fault=Numbers.run_exact_batch(lambda:frontier.observe(t([1394]),early,None,yes))
    assert fault.cpu().tolist()==[0]
    assert values(cutoff.private_feedback_sequence)==[1]
    assert [private[i] for i in first.record_refs[first.emitted].cpu().tolist()]==[3]

    gates=ReceiveGates.unset(1).before(at,yes)
    retained,closed=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert int(closed.emitted.sum().cpu())==6 and closed.available_at[closed.emitted].cpu().tolist()==[1414]*6
    assert retained.received_at[retained.active].cpu().tolist()==[1414,1415,1415]
    # 原四列构造不设新flags，仍包含received==1414；旧调用无需迁移。
    original=ReceiveGates(at,at,yes,yes)
    _,inclusive=Numbers.run_exact_batch(lambda:pending.take_closed(original))
    assert int(inclusive.emitted.sum().cpu())==7
    # 明确切回原包含门，只领取此前保留的同刻记录，不重复6条旧领取。
    _,later=Numbers.run_exact_batch(lambda:retained.take_closed(gates.set_receive(at,yes).set_public(at,yes)))
    assert int(later.emitted.sum().cpu())==1 and later.available_at[later.emitted].cpu().tolist()==[1414]
    print(json.dumps(dict(passed=True,scope='original nine NOE time metadata / exclusive first claim / legacy inclusive flags / no re-claim',
        early_source_private=3,early_cutoff_delivery=1,strict_count=6,inclusive_count=7,retained=[1414,1415,1415],elapsed_seconds=time.perf_counter()-started)),flush=True)

if __name__=='__main__':main()
