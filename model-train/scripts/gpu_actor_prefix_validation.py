#!/usr/bin/env python3
"""原NativeActor逐前缀36列→现Policy100ms参数头；不是完整采样/联合概率。"""
import argparse
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
import json
import time
import torch

from gpu_actor_support_validation import context,check_accepted_only
from actor_context import _owned
from actor_support import DeviceNativeActor
from queues import Numbers
from model_train.gpu_policy import ModelShape100ms,Policy100ms


def check(actor,reference,label,model):
    started=time.perf_counter();checked=0;stages=model.stages()
    z=torch.zeros((1,model.shape.hidden),device='cuda')
    rule=torch.zeros((1,model.shape.hidden//2),device='cuda')
    for trace in reference['traces']:
        state=actor.initial()
        for index,step in enumerate(trace['steps']):
            features,fault=Numbers.run_exact_batch(lambda:actor.prefix_features(state))
            expected=torch.tensor([step['prefix_features']],dtype=torch.float32,device='cuda')
            assert fault.cpu().tolist()==[0],(label,index,fault.cpu().tolist())
            # 原CPU精确末端投影是独立预期，不由GPU首次结果建立。
            assert torch.equal(features,expected),(label,step['stage'],features.cpu().tolist(),expected.cpu().tolist())
            name=step['stage']
            if name=='CancelObject':
                support=Numbers.run_exact_batch(lambda:actor.support(state))
                expected_mask=torch.tensor([step['mask']],dtype=torch.bool,device='cuda')
                count=expected_mask.shape[1]
                assert support.fault.cpu().tolist()==[0]
                assert support.cancel.shape[1]>=count
                assert torch.equal(support.cancel[:,:count],expected_mask)
                assert not support.cancel[:,count:].any().cpu().item()
            if name!='Family':
                graph=stages['action_quantity' if name=='QuantityBranch' else
                    'action_variant' if name.startswith('Variant') else
                    'action_price' if name.startswith('Price') else 'action_cancel_context']
                args=(torch.arange(999,device='cuda'),torch.zeros((1,999),device='cuda')) if name.startswith('Price') else ()
                with torch.no_grad():
                    actual=graph(z,rule,features,*args);original=graph(z,rule,expected,*args)
                assert torch.equal(actual,original),(label,name,'model scores')
            coordinate=torch.tensor([step['choice']],dtype=torch.int64,device='cuda')
            state=Numbers.run_exact_batch(lambda:actor.choose(state,coordinate))
            assert state.fault.cpu().tolist()==[0],(label,trace['family'],name,step['choice'],
                actor.context.cancels.present.cpu().tolist(),actor.cancel_slots.cpu().tolist(),state.fault.cpu().tolist())
            checked+=1
        assert state.stage.cpu().tolist()==[7]
    print(json.dumps({'stage':'prefix36','source':label,'prefixes':checked,
        'elapsed_seconds':time.perf_counter()-started}),flush=True)
    report={'source':label,'prefixes':checked,'physical_object_width':actor.context.cancels.present.shape[1],
        'global_object_count':actor.context.cancels.present.sum(1).cpu().tolist()}
    if label=='actual-Accepted-only-publisher':report['global_to_physical']=actor.cancel_slots.cpu().tolist()
    return report


def capture_and_gradients(source,model):
    actor=DeviceNativeActor(context(source));state=actor.initial()
    for choice in source['a_price_support']['prefix_path']+[18]:
        state=Numbers.run_exact_batch(lambda:actor.choose(state,torch.tensor([choice],device='cuda')))
        assert state.fault.cpu().tolist()==[0]
    assert state.stage.cpu().tolist()==[5] and state.prices.cpu().tolist()==[[19,-1]]
    graph_head=model.stages()['action_price'];indices=torch.arange(999,device='cuda')
    z=torch.zeros((1,model.shape.hidden),device='cuda')
    rule=torch.zeros((1,model.shape.hidden//2),device='cuda');fee=torch.zeros((1,999),device='cuda')
    def compute():
        prefix,fault=actor.prefix_features(state)
        return prefix,fault,graph_head(z,rule,prefix,indices,fee)
    with torch.no_grad():
        warm=Numbers.device_exact_batch(compute,word_capacity=16,device='cuda')
        assert warm.fits.cpu().item()
        graph=torch.cuda.CUDAGraph()
        with torch.cuda.graph(graph):captured=Numbers.device_exact_batch(compute,word_capacity=16,device='cuda')
        graph.replay();torch.cuda.synchronize()
    assert captured.fits.cpu().item() and captured.value[1].cpu().tolist()==[0]
    expected=torch.tensor([source['b_price_prefix_features']],dtype=torch.float32,device='cuda')
    assert torch.equal(captured.value[0],expected)
    saved=tuple(_owned(value) for value in captured.value)
    with torch.no_grad():
        # 实际覆盖同一个捕获源：合法500百份GTC/FOK前缀A价从.19改.01。
        state.prices[:,0].fill_(1)
        graph.replay();torch.cuda.synchronize()
    assert captured.fits.cpu().item() and captured.value[1].cpu().tolist()==[0]
    changed=expected.clone();changed[:,26]=float(Fraction(1,50)-1)
    assert torch.equal(captured.value[0],changed) and torch.equal(saved[0],expected)
    assert not torch.equal(captured.value[2],saved[2]) and torch.equal(saved[1],torch.zeros_like(saved[1]))
    # 共享同一实际参数头，对独立原投影核输入与参数梯度，不只核无梯度前向。
    actual_prefix=captured.value[0].detach().clone().requires_grad_(True)
    expected_prefix=changed.clone().requires_grad_(True)
    parameters=tuple(graph_head.parameters())
    gradients=[]
    for features in (actual_prefix,expected_prefix):
        output=graph_head(z,rule,features,indices,fee)
        gradients.append(torch.autograd.grad(output.square().sum(),(features,)+parameters))
    for actual,original in zip(*gradients):assert torch.equal(actual,original)
    return {'CUDA_capture_replay':True,'actual_source_overwritten':True,'owned_prefix_stable':True,
        'prefix_and_parameter_gradients':True,'word_capacity':16,'required_words':int(captured.required_words.cpu())}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--sources',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();started=time.perf_counter()
    # 仅此有限数学/模型接缝使用工程参数与seed，不形成正式训练profile。
    torch.manual_seed(18136);model=Policy100ms(ModelShape100ms(1,32)).cuda().eval()
    sources=[args.sources/'buy.json',*sorted(args.sources.glob('cancel-*.json')),*sorted(args.sources.glob('repair-*.json'))]
    assert len(sources)==5
    reports=[]
    for source in sources:
        value=json.loads(source.read_text());reports.append(check(DeviceNativeActor(context(value)),value['actor_paths'],source.name,model))
    original=json.loads(sources[0].read_text())
    encoding=json.loads((args.sources/'encoding.json').read_text())
    actor=DeviceNativeActor(context(encoding))
    state=Numbers.run_exact_batch(lambda:actor.choose(actor.initial(),torch.tensor([2],device='cuda')))
    encoded,fault=Numbers.run_exact_batch(lambda:actor.prefix_features(state))
    assert fault.cpu().tolist()==[0]
    assert torch.equal(encoded,torch.tensor([encoding['prefix_features']],dtype=torch.float32,device='cuda'))
    inactive=DeviceNativeActor(replace(actor.context,decision_required=actor.context.decision_required&False))
    neutral,neutral_fault=Numbers.run_exact_batch(lambda:inactive.prefix_features(inactive.initial()))
    assert neutral_fault.cpu().tolist()==[0] and torch.equal(neutral,torch.zeros_like(neutral))
    accepted=check_accepted_only(args.sources/'accepted-only/reference.json',
        prefix_check=lambda actor,reference:check(actor,reference,'actual-Accepted-only-publisher',model))
    report={'status':'PASS','sources':reports,'accepted_only':accepted,
        'capture':capture_and_gradients(original,model),'elapsed_seconds':time.perf_counter()-started,
        'original_root_exceeds_encoding_limit':int(encoding['root_upper'])>actor.encoding_limit,
        'inactive_prefix_neutral':True,
        'engineering_model':{'layers':1,'hidden':32,'seed':18136},
        'parameter_head_inputs':'explicit_zero_z_and_rule_context_component_reference',
        'cancel184_complete':False,'full_sampler_joint_probability_and_intent':False}
    args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report),flush=True)


if __name__=='__main__':main()
