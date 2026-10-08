#!/usr/bin/env python3
"""原Native价格估费→共享价格头；有限工程组件，不采样或预留资金。"""
import argparse
from pathlib import Path
import json
import time
import torch
from gpu_actor_support_validation import context,check_accepted_only
from actor_support import DeviceNativeActor
from actor_context import _owned
from queues import Numbers
from model_train.gpu_policy import ModelShape100ms,Policy100ms


def expected(case):
    values=torch.zeros((1,999),device='cuda')
    indices=torch.tensor(case['price_indices'],dtype=torch.int64,device='cuda')
    values[:,indices]=torch.tensor([case['price_fee_features']],device='cuda')
    return values,torch.tensor([case['price_support']],dtype=torch.bool,device='cuda')


def check(value,case):
    fees,fault,support=value
    original,mask=expected(case)
    assert fault.cpu().tolist()==[0]
    assert torch.equal(support,mask)
    torch.testing.assert_close(fees,original,rtol=1e-6,atol=1e-7)
    assert torch.equal(fees[~mask],torch.zeros_like(fees[~mask]))


def at_path(actor,path):
    state=actor.initial()
    for choice in path:
        state=Numbers.run_exact_batch(lambda:actor.choose(state,torch.tensor([choice],device='cuda')))
        assert state.fault.cpu().tolist()==[0]
    return state


def compute(actor,state):
    support=actor.support(state)
    fees,fault=actor.price_fee_features(state,support)
    return fees,fault,support.price


def publisher(actor,reference):
    count=0
    for trace in reference['traces']:
        state=actor.initial()
        for step in trace['steps']:
            if step['stage'].startswith('Price'):
                case={**step,'price_support':step['mask']}
                check(Numbers.run_exact_batch(lambda:compute(actor,state)),case)
                count+=1
            state=Numbers.run_exact_batch(lambda:actor.choose(state,torch.tensor([step['choice']],device='cuda')))
            assert state.fault.cpu().tolist()==[0]
    return {'actual_public_publisher_price_prefixes':count}


def main():
    p=argparse.ArgumentParser();p.add_argument('--sources',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);args=p.parse_args();started=time.perf_counter()
    source=json.loads((args.sources/'buy.json').read_text())
    actor=DeviceNativeActor(context(source));cases=source['price_fee_cases'];assert len(cases)==3
    for index,case in enumerate(cases):
        state=at_path(actor,case['prefix_path'])
        check(Numbers.run_exact_batch(lambda:compute(actor,state)),case)
        print(json.dumps({'stage':'original_fee_case','index':index+1,'total':3,
            'elapsed_seconds':time.perf_counter()-started}),flush=True)
    assert not expected(cases[0])[0].any().cpu().item()
    assert expected(cases[1])[0].any().cpu().item() and expected(cases[2])[0].any().cpu().item()
    # 捕获真实B/FOK前缀；覆写其同一个A价格源，原Native另一合法路径作独立预期。
    state=at_path(actor,cases[2]['prefix_path'])
    with torch.no_grad():
        warm=Numbers.device_exact_batch(lambda:compute(actor,state),word_capacity=16,device='cuda')
        assert warm.fits.cpu().item()
        graph=torch.cuda.CUDAGraph()
        with torch.cuda.graph(graph):captured=Numbers.device_exact_batch(lambda:compute(actor,state),word_capacity=16,device='cuda')
        graph.replay();torch.cuda.synchronize()
        assert captured.fits.cpu().item();check(captured.value,cases[2])
        owned=tuple(_owned(value) for value in captured.value)
        state.prices[:,0].fill_(1)
        graph.replay();torch.cuda.synchronize()
        assert captured.fits.cpu().item();check(captured.value,cases[1]);check(owned,cases[2])
        assert not torch.equal(captured.value[0],owned[0])
    # 原价格头实际消费费用特征；使用同一参数核独立原输入及全部输入/参数梯度。
    torch.manual_seed(181620);model=Policy100ms(ModelShape100ms(1,32)).cuda().eval()
    head=model.stages()['action_price'];indices=torch.arange(999,device='cuda')
    prefix,fault=Numbers.run_exact_batch(lambda:actor.prefix_features(state));assert fault.cpu().tolist()==[0]
    original_prefix=torch.tensor([cases[1]['prefix_features']],device='cuda')
    z=torch.zeros((1,32),device='cuda');rule=torch.zeros((1,16),device='cuda')
    gradients=[];scores=[]
    for features,fee in ((prefix,captured.value[0]),(original_prefix,expected(cases[1])[0])):
        features=features.detach().clone().requires_grad_(True);fee=fee.detach().clone().requires_grad_(True)
        score=head(z,rule,features,indices,fee);scores.append(score)
        gradients.append(torch.autograd.grad(score.square().sum(),(features,fee)+tuple(head.parameters())))
    torch.testing.assert_close(*scores,rtol=1e-6,atol=1e-7)
    for actual,original in zip(*gradients):torch.testing.assert_close(actual,original,rtol=1e-6,atol=1e-7)
    published=check_accepted_only(args.sources/'accepted-only/reference.json',prefix_check=publisher)
    report={'status':'PASS','original_price_cases':3,'GTC_zero_FOK_nonzero':True,
        'global_price_width':999,'same_capture_source_overwritten':True,'old_owned_fee_stable':True,
        'price_model_input_and_parameter_gradients':True,'actual_publisher':published,
        'word_capacity':16,'required_words':int(captured.required_words.cpu()),'fits':bool(captured.fits.cpu()),
        'elapsed_seconds':time.perf_counter()-started,'engineering_model':{'layers':1,'hidden':32,'seed':181620},
        'z_and_rule_context':'explicit_zero_component_inputs','complete_sampler':False}
    args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report),flush=True)


if __name__=='__main__':main()
