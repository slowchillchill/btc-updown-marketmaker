#!/usr/bin/env python3
"""100ms模型接线/递推梯度/条件概率的短工程验证；不是正式学习或公共支持验收。"""
import argparse
import json
from pathlib import Path
import subprocess
import time

import torch
from model_train.gpu_policy import ModelShape100ms, Policy100ms, deterministic_condition, score_condition


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    started = time.perf_counter()
    torch.manual_seed(181101)  # 单次工程接线，不登记为正式候选/根/训练种子。
    device = 'cuda'
    model = Policy100ms(ModelShape100ms(layers=2, hidden=64)).to(device)
    stages = model.stages()
    root_count, times, depth = 4, 7, 5
    inputs = (torch.randn(times,root_count,626,device=device),
              torch.randn(times,root_count,433,device=device),
              torch.randn(times,root_count,6,depth,9,device=device),
              torch.ones(times,root_count,6,depth,device=device,dtype=torch.bool))
    hidden = torch.randn(2,root_count,64,device=device,requires_grad=True)
    for value in inputs[:3]: value.requires_grad_(True)
    burn = tuple(value[:2] for value in inputs)
    learning = tuple(value[2:] for value in inputs)
    mask = torch.ones(times,root_count,dtype=torch.bool,device=device)
    z, values, final = model.learning_unroll(burn,mask[:2],learning,mask[2:],hidden)
    assert z.shape == (5,4,64) and final.shape == (2,4,64)
    # 只在后段DecisionRequired行计算loss；前面的ContextOnly仍必须传递梯度。
    loss = values[-1].sum() + z[-1].square().sum()
    loss.backward()
    assert hidden.grad is None
    assert torch.count_nonzero(inputs[0].grad[:2]) == 0
    assert torch.count_nonzero(inputs[0].grad[2]) > 0
    assert torch.count_nonzero(inputs[0].grad[-1]) > 0
    with torch.no_grad():
        one_z, one_values, one_hidden = model.unroll(inputs,hidden.detach(),mask)
        _, _, carried = model.unroll(tuple(value[:3] for value in inputs),hidden.detach(),mask[:3])
        continued_z, _, continued = model.unroll(tuple(value[3:] for value in inputs),carried,mask[3:])
        assert torch.equal(one_hidden,continued)
        assert torch.equal(one_z[3:],continued_z)
        # bootstrap只用hidden副本计算，不推进采集器的hidden。
        snapshot = carried.clone()
        model.state_and_value(*(value[3] for value in inputs),carried)
        assert torch.equal(snapshot,carried)
        rule = torch.randn(root_count,30,device=device)
        prefix = torch.randn(root_count,36,device=device)
        reduced = torch.zeros(root_count,96,device=device)
        exists = torch.zeros(root_count,dtype=torch.bool,device=device)
        family, context = stages['action_family'](one_z[-1],rule,reduced,exists)
        quantity = stages['action_quantity'](one_z[-1],context,prefix)
        variant = stages['action_variant'](one_z[-1],context,prefix)
        assert family.shape == (4,6) and quantity.shape == (4,16) and variant.shape == (4,2)
        indices = torch.tensor([0,4,998],dtype=torch.int64,device=device)
        price = stages['action_price'](one_z[-1],context,prefix,indices,torch.zeros(4,3,device=device))
        assert price.shape == (4,3)
    logits = torch.tensor([[2.,2.,-4.],[1.,3.,4.],[2.,-3.,8.],[0.,1.,2.]],device=device,requires_grad=True)
    support = torch.tensor([[True,True,False],[False,True,False],[True,False,True],[False,False,False]],device=device)
    active = torch.tensor([True,True,True,True],device=device)
    support_fault = torch.zeros(4,dtype=torch.int32,device=device)
    selected = deterministic_condition(logits,support,active,support_fault)
    assert selected.choice.cpu().tolist() == [0,1,2,-1]
    assert selected.fault.cpu().tolist() == [0,0,0,5]
    assert selected.log_prob[1] == 0 and selected.entropy[1] == 0
    replay = score_condition(logits,support,selected.choice,active,support_fault)
    assert torch.equal(selected.log_prob,replay.log_prob)
    assert torch.equal(selected.entropy,replay.entropy)
    valid = selected.fault == 0
    reference = torch.distributions.Categorical(logits=logits[:3].masked_fill(~support[:3],-torch.inf),validate_args=False)
    assert torch.equal(selected.log_prob[:3],reference.log_prob(selected.choice[:3]))
    assert torch.allclose(selected.entropy[:3],reference.entropy(),rtol=0,atol=1e-7)
    selected.log_prob[valid].sum().backward()
    assert torch.count_nonzero(logits.grad[1]) == 0
    bad_replay = score_condition(logits,support,torch.tensor([2,1,2,0],device=device),active,support_fault)
    assert bad_replay.fault[0] == 4
    torch.cuda.synchronize()
    report = {'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
              'identity':'engineering_short_model_and_probability_only', 'device':torch.cuda.get_device_name(),
              'seconds':time.perf_counter()-started,'peak_allocated_bytes':torch.cuda.max_memory_allocated(),
              'passed':['all_layer_100ms_state','burn_in_no_grad_all_layer_detach','context_only_learning_gradient',
                        'split_time_axis_carry','bootstrap_does_not_commit_hidden','original_actor_parameter_heads',
                        'gpu_global_choice_and_replay','mature_categorical_probability','unique_zero_gradient','fault_not_choice'],
              'not_delivered':['public_gpu_support_generation','stochastic_path_rng','full_gpu_environment','ppo','active_root_restore']}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False),flush=True)


if __name__ == '__main__':
    main()
