"""合法FIFO的阶段私有行候选、同行承接和共同安装反例。"""
from dataclasses import replace
import json
import torch
from queues import ExactInt, ExactRational, PriceQueueBatch, Numbers
from base_runtime_storage import BaseRuntimeStorage


def main():
    device='cuda'
    T=lambda values:torch.tensor(values,dtype=torch.int64,device=device)
    I=lambda values:ExactInt.from_i64_tensor(T(values))
    price=ExactRational.ratio(I([1,2,3]),I([4,5,5]))
    queue=PriceQueueBatch.empty(price,4)
    queue,_=Numbers.run_exact_batch(lambda:queue.append_external(I([10,20,30])))
    original=BaseRuntimeStorage.snapshot_output(queue)
    stage=BaseRuntimeStorage.stage_queue(queue)
    with stage.begin(queue):
        local=queue.take_rows(T([1]))
        update,_=Numbers.run_exact_batch(lambda:local.apply_level_transition(I([20]),I([25])))
        candidate=queue.put_rows(T([1]),update)
        assert torch.equal(queue.kind,original.kind),'pending不能修改进入态'
        queue=stage.accept(candidate)
        again=queue.take_rows(T([1]))
        assert again.total_base.report_values()==[20,5,0,0]
        update,_=Numbers.run_exact_batch(lambda:again.apply_level_transition(I([25]),I([18])))
        queue=stage.accept(queue.put_rows(T([1]),update))
        assert queue.take_rows(T([1])).consumed_base.report_values()==[7,0,0,0]
        assert queue.take_rows(T([0,2])).total_base.report_values()==[10,0,0,0,30,0,0,0]
        assert torch.equal(queue.kind,original.kind)
        assert torch.equal(queue.consumed_base.words,original.consumed_base.words)
    # 后部失败时不调用install；重算同一进入态必须得到相同候选。
    with stage.begin(original):
        assert original.take_rows(T([1])).total_base.report_values()==[20,0,0,0]
        local=original.take_rows(T([1]))
        update,_=Numbers.run_exact_batch(lambda:local.apply_level_transition(I([20]),I([25])))
        candidate=stage.accept(original.put_rows(T([1]),update))
        local=candidate.take_rows(T([1]))
        update,_=Numbers.run_exact_batch(lambda:local.apply_level_transition(I([25]),I([18])))
        candidate=stage.accept(candidate.put_rows(T([1]),update))
        held=BaseRuntimeStorage.snapshot_output(original)
    result=stage.install(candidate)
    assert result.consumed_base.report_values()[4:8]==[7,0,0,0]
    assert result.total_base.report_values()[4:8]==[20,5,0,0]
    assert held.consumed_base.report_values()==[0]*12,'旧冻结输出必须独立'
    assert result.total_base.report_values()[:4]==[10,0,0,0]
    assert result.total_base.report_values()[8:]==[30,0,0,0]
    # 同一进入矩阵绑定跨桶变化：计数是固定物理缓冲，不能携前桶值。
    with stage.begin(result):
        assert stage.segment_counts(result).cpu().tolist()==[1,2,1]
    counts=stage.counts;installed=stage.installed_counts
    with stage.begin(result):
        assert stage.counts is counts and stage.installed_counts is installed
        local=result.take_rows(T([2]))
        changed,effect=Numbers.run_exact_batch(lambda:local.apply_level_transition(I([30]),I([0])))
        candidate=stage.accept(result.put_rows(T([2]),changed))
        assert stage.segment_counts(candidate).cpu().tolist()==[1,2,0]
    result=stage.install(candidate)
    assert stage.installed_counts is installed and installed.cpu().tolist()==[1,2,0]
    with stage.begin(result):
        assert stage.counts is counts and counts.cpu().tolist()==[1,2,0]
        local=result.take_rows(T([2]))
        changed,_=Numbers.run_exact_batch(lambda:local.apply_level_transition(I([0]),I([3])))
        candidate=stage.accept(result.put_rows(T([2]),changed))
    result=stage.install(candidate)
    assert installed.cpu().tolist()==[1,2,1]
    # 后部失败的安装掩码：私有同行已修改，权威行和无关既有fault仍保持。
    entering=replace(result,fault=T([0,0,8]).to(torch.int32))
    frozen=BaseRuntimeStorage.snapshot_output(entering)
    with stage.begin(entering):
        local=entering.take_rows(T([1]))
        changed,_=Numbers.run_exact_batch(lambda:local.apply_level_transition(I([18]),I([19])))
        candidate=stage.accept(entering.put_rows(T([1]),changed))
    failed=stage.install(candidate,torch.zeros((),dtype=torch.bool,device=device))
    assert torch.equal(failed.kind,frozen.kind) and torch.equal(failed.total_base.words,frozen.total_base.words)
    assert failed.fault.cpu().tolist()==[0,0,8]
    # 真正字宽不足：固定批提出8字需求；原输入不变，原资源入口同输入重算。
    factor=ExactInt.from_values([1<<120])
    huge=(1<<240)+18
    with stage.begin(entering):
        local=entering.take_rows(T([1]))
        compute=lambda:local.apply_level_transition(I([18]),factor*factor+I([18]))
        short=Numbers.device_exact_batch(compute,word_capacity=4,device=device)
        assert not short.fits.cpu().item() and int(short.required_words.cpu())>4
        changed,_=Numbers.run_exact_batch(compute)
        candidate=stage.accept(entering.put_rows(T([1]),changed))
        assert stage.scratch.total_base.words.size(1)>entering.total_base.words.size(1)
    failed=stage.install(candidate,torch.zeros((),dtype=torch.bool,device=device))
    assert failed.total_base.report_values()==frozen.total_base.report_values()
    grown=stage.install(candidate)
    assert sum(grown.take_rows(T([1])).total_base.report_values())==huge+7
    assert frozen.total_base.report_values()==entering.total_base.report_values()
    # 段池增长保本桶已接受的前缀，并从同一进入输入重新计算未接受步。
    small=PriceQueueBatch.empty(price,1)
    small,_=Numbers.run_exact_batch(lambda:small.append_external(I([10,20,30])))
    pool_stage=BaseRuntimeStorage.stage_queue(small)
    with pool_stage.begin(small):
        local=small.take_rows(T([1]))
        changed,demand=Numbers.run_exact_batch(lambda:local.apply_level_transition(I([20]),I([21])))
        assert demand.required_segments.cpu().tolist()==[2] and demand.fault.cpu().tolist()==[0]
        assert small.total_base.report_values()==[10,20,30]
        wide=small.resized(2)
        changed,demand=Numbers.run_exact_batch(lambda:wide.take_rows(T([1])).apply_level_transition(I([20]),I([21])))
        assert demand.fault.cpu().tolist()==[0]
        candidate=pool_stage.accept(wide.put_rows(T([1]),changed))
    wide=pool_stage.install(candidate)
    assert wide.total_base.report_values()==[10,0,20,1,30,0]
    print(json.dumps(dict(stage_queue=True,repeated_row=True,unrelated_rows=True,
        deferred_install=True,same_input_recompute=True,held_output=True,fixed_counts_cross_bucket=True,
        failed_install_mask=True,unrelated_fault=True,word_growth_retry=True,segment_growth_retry=True)),flush=True)


if __name__=='__main__':main()
