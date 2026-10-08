"""有限原21叶/AdamW参考及实际联合冻结消费；不是PPO或完整采集器。"""
from dataclasses import replace,fields,is_dataclass
from pathlib import Path
import copy
import json
import os
import sys
import time
import unittest
import torch

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('model-train/scripts','sim-system/gpu','shared-run/gpu',
    'data-pipeline/gpu','ledger/gpu','execution-normalization/gpu','model-train/native-inputs/gpu')]
from queues import Numbers
from actor_support import DeviceNativeActor
from input_cancel import CancelInputs
from .action_inputs import ActorPolicyInputs
from .action_random import ActionRandom
from .device_actor import CapturedDeviceActor,ActorPathCandidate
from .action_replay import ReplaySchedule,replay_action,entropy_sample_objective
from .gpu_policy import Policy100ms,ModelShape100ms


def mask_at(support,stage):
    return getattr(support,{0:'family',1:'quantity',2:'variant',3:'variant',4:'price',5:'price',6:'cancel'}[stage])


def original_path(inputs,native,choices,capacity=32):
    """真实原Actor独立支持/投影→同路径设备状态，有限参考允许CPU检查。"""
    actor=DeviceNativeActor(inputs.actor);state=Numbers.run_exact_batch(actor.initial)
    original=native.actor(0);stages={0:[0],2:[0,1,2,3,4,5],5:[0,6]}[choices[0]]
    assert len(stages)==len(choices)
    scored=[]
    for stage,choice in zip(stages,choices):
        support=Numbers.run_exact_batch(lambda:actor.support(state));expected=original.support()
        assert state.stage.cpu().tolist()==[stage]
        assert mask_at(support,stage).cpu().tolist()==[expected]
        projected,error=Numbers.run_exact_batch(lambda:actor.prefix_features(state))
        assert error.cpu().tolist()==[0]
        torch.testing.assert_close(projected,projected.new_tensor([original.prefix_features()]),rtol=1e-6,atol=1e-7)
        scored.append(sum(expected)>1)
        state=Numbers.run_exact_batch(lambda:actor.choose(state,state.stage.new_tensor([choice])))
        original.choose(choice)
    assert original.complete() and state.stage.cpu().tolist()==[7] and state.fault.cpu().tolist()==[0]
    tensor=lambda v,dtype=torch.int64:torch.tensor(v,dtype=dtype,device='cuda')
    length=len(choices)
    return ActorPathCandidate(state,tensor([stages+[-1]*(capacity-length)]),
        tensor([list(choices)+[-1]*(capacity-length)]),tensor([scored+[False]*(capacity-length)],torch.bool),
        tensor([length]),tensor([0.],torch.float32),ActionRandom(tensor([181601]),tensor([[0,0,0,0]])),
        inputs.actor.identities.clone(),inputs.actor.cutoff.clone(),inputs.policy_version.clone(),
        tensor(0),tensor(length),tensor(True,torch.bool),tensor([True],torch.bool),tensor([0],torch.int32))


def candidate_summary(candidate):
    """仅在验证边界读回实际故障与资源出口，不参与设备业务推进。"""
    return dict(fault=candidate.fault.cpu().tolist(),complete=candidate.complete.cpu().tolist(),
        stage=candidate.prefix.stage.cpu().tolist(),length=candidate.length.cpu().tolist(),
        choices=candidate.choices.cpu().tolist(),fits=bool(candidate.fits.cpu()),
        required_steps=int(candidate.required_steps.cpu()),required_words=int(candidate.required_words.cpu()),
        random_counter=candidate.random.counter.cpu().tolist())


def assert_candidate(candidate):
    result=candidate_summary(candidate)
    assert result['fits'] and result['fault']==[0] and result['complete']==[True],result
    assert result['stage']==[7],result


def original_score(policy,inputs,native,path):
    from .native_inputs import ComponentGroup
    from .probability_validation import RepeatedReference
    from .actor import consume_actor
    reference=copy.deepcopy(policy).cpu()
    z=inputs.state.detach().cpu().clone().requires_grad_()
    # 这是同一原冻结组的真实规则/184与显式数学z，不重放第二账户。
    group=ComponentGroup([z],[inputs.rules.detach().cpu()],['DecisionRequired'],
        inputs.cancels.values.detach().cpu(),None,None)
    result=consume_actor(RepeatedReference(reference),group,0,native.actor(0),replay=list(path))
    return reference,z,result.log_prob


class OriginalTrace:
    """只读取原Native导出的有限步骤；不计算支持或模拟业务状态。"""
    def __init__(self,trace):self.steps=trace['steps'];self.position=0;self.choices=[]
    def complete(self):return self.position==len(self.steps)
    def support(self):return self.steps[self.position]['mask']
    def prefix_features(self):return self.steps[self.position]['prefix_features']
    def price_indices(self):return self.steps[self.position]['price_indices']
    def price_fee_features(self):return self.steps[self.position]['price_fee_features']
    def head_index(self):
        return {'Family':0,'QuantityBranch':1,'Variant(A)':2,'Variant(B)':2,
            'Price(A)':3,'Price(B)':3,'CancelObject':5}[self.steps[self.position]['stage']]
    def choose(self,choice):
        assert self.support()[choice]
        # 唯一可变化的选择是最后Cancel对象；后面没有待推导的支持。
        if self.position<len(self.steps)-1:assert choice==self.steps[self.position]['choice']
        self.choices.append(choice);self.position+=1


def check_branches(directory,output):
    """原Repair/FOK和259对象，实际新驱动经过六族与全局高ordinal。"""
    from gpu_actor_support_validation import context
    from .native_inputs import ComponentGroup
    from .probability_validation import RepeatedReference
    from .actor import consume_actor
    directory=Path(directory);started=time.perf_counter();reports=[]
    for name,family in (('repair-before-economic.json',1),('cancel-36000.json',3),('cancel-36000.json',4)):
        source=json.loads((directory/name).read_text());actor=context(source)
        count=len(source['cancel_features']);device='cuda'
        tensor=lambda v,dtype=torch.float32:torch.tensor(v,dtype=dtype,device=device)
        cancels=CancelInputs(tensor(source['cancel_features']).reshape(1,count,184),actor.cancels.present,
            torch.arange(count,device=device)[None],actor.fault)
        torch.manual_seed(181603);policy=Policy100ms(ModelShape100ms(1,32)).cuda().eval()
        inputs=ActorPolicyInputs(actor,torch.randn(1,32,device=device),tensor([source['rule_features']]),cancels,
            torch.full_like(actor.cutoff,11),torch.ones(1,dtype=torch.bool,device=device),actor.fault)
        with torch.no_grad():
            policy.action.actor.family[-1].weight.zero_();policy.action.actor.family[-1].bias.zero_()
            policy.action.actor.family[-1].bias[family]=20.
            if family==1:
                policy.action.actor.quantity.weight.zero_();policy.action.actor.quantity.bias.copy_(torch.arange(16,device=device)*10.)
                policy.action.actor.variant.weight.zero_();policy.action.actor.variant.bias.copy_(tensor([0.,20.]))
                policy.action.actor.price.weight.zero_();policy.action.actor.price.bias.copy_(-torch.arange(999,device=device).float())
                policy.action.actor.fee_weight.weight.zero_()
            elif family==3:
                for parameter in policy.action.actor.cancel.parameters():parameter.zero_()
        stream=ActionRandom(torch.full_like(actor.cutoff,181603),torch.zeros((1,4),dtype=torch.int64,device=device))
        driver=CapturedDeviceActor(policy,inputs,stream,path_capacity=32,word_capacity=16,deterministic=family!=3)
        trace=source['fok_trace'] if family==1 else next(t for t in source['actor_paths']['traces'] if t['family']==family)
        trials=1
        for trials in range(1,17):
            stream.seed.fill_(181602+trials)
            value=driver.replay();torch.cuda.synchronize();assert_candidate(value)
            path=value.choices[0,:value.length[0]].cpu().tolist()
            if family!=3 or path[-1]>=128:break
        assert path[0]==family
        if family==3:assert path[-1]>=128 and count==259
        elif family==1:assert path==[step['choice'] for step in trace['steps']]
        if family==1:assert value.prefix.variants[0,1].cpu().item()==1
        saved=value.snapshot();schedule=ReplaySchedule.for_update(saved)
        policy.zero_grad(set_to_none=True)
        score=replay_action(policy,inputs,saved,schedule,word_capacity=16)
        assert score.fault.cpu().tolist()==[0] and score.fits.cpu().item()
        reference=copy.deepcopy(policy).cpu()
        group=ComponentGroup([inputs.state.detach().cpu()],[inputs.rules.cpu()],['DecisionRequired'],
            inputs.cancels.values.cpu(),None,None)
        original=OriginalTrace(trace)
        # Native示例取首个合法对象；Pair模型仍保非零评分，用原CPU消费者独立选择。
        expected=consume_actor(RepeatedReference(reference),group,0,original,
            deterministic=family==4,replay=None if family==4 else path).log_prob
        assert path==original.choices,(candidate_summary(saved),original.choices)
        if family in (3,4):assert saved.prefix.cancel_index.cpu().tolist()==[path[-1]]
        torch.testing.assert_close(score.log_prob.cpu(),expected.unsqueeze(0),rtol=2e-5,atol=2e-6)
        score.log_prob.sum().backward();expected.sum().backward()
        gradient=parameter_gradients(policy,reference,classify_zero=False)
        # 对象身份和Pair原腿仍来自完整Native全局对象，不按family重新编号。
        selected=None if family==1 else source['actor_context']['cancel_intents'][path[-1]]
        if family==4:assert 'CancelOrders' in selected
        if family==3:assert 'CancelOrder' in selected
        reports.append(dict(source=name,family=family,path=path,cancel_intent=selected,
            original_CPU_path=original.choices,word_capacity=16,required_words=int(value.required_words.cpu()),seed_trials=trials,
            engineering_seed=int(stream.seed.cpu()[0]),gradients=gradient))
        print(json.dumps(dict(stage='branch_done',**reports[-1],elapsed_seconds=time.perf_counter()-started)),flush=True)
    report=dict(status='PASS',branches=reports,source_sha=os.environ['B_GPU_ACTOR_SOURCE_COMMIT'],
        native_complete_support_unchanged=True,elapsed_seconds=time.perf_counter()-started)
    Path(output).write_text(json.dumps(report,indent=2)+'\n')


def parameter_gradients(policy,reference,*,classify_zero=True):
    counts={'none':0,'zero':0,'nonzero':0}
    for name,value in policy.named_parameters():
        expected=dict(reference.named_parameters())[name]
        assert (value.grad is None)==(expected.grad is None),name
        if value.grad is None:counts['none']+=1;continue
        torch.testing.assert_close(value.grad.cpu(),expected.grad,rtol=2e-4,atol=2e-5,msg=name)
        kind='nonzero' if torch.count_nonzero(expected.grad) else 'zero'
        if classify_zero:assert bool(torch.count_nonzero(value.grad))==bool(torch.count_nonzero(expected.grad)),name
        counts[kind]+=1
    return counts


def check_joint(policy,inputs,artifact,index,reports,*,published_sources):
    """已验证1500/1600同账户实际model/Actor/184所有权入口。"""
    started=time.perf_counter()
    stream=ActionRandom(torch.full_like(inputs.actor.cutoff,181601+index),
        torch.zeros((1,4),dtype=torch.int64,device='cuda'))
    before=stream.snapshot()
    captured=CapturedDeviceActor(policy,inputs,stream,path_capacity=32,word_capacity=16)
    candidate=captured.replay();torch.cuda.synchronize();assert_candidate(candidate)
    owned=candidate.snapshot()
    assert torch.equal(stream.counter,before.counter)
    schedule=ReplaySchedule.for_update(owned)
    policy.zero_grad(set_to_none=True)
    scored=replay_action(policy,inputs,owned,schedule,word_capacity=16)
    assert scored.fits.cpu().item() and scored.fault.cpu().tolist()==[0]
    torch.testing.assert_close(scored.log_prob,owned.log_prob,rtol=2e-5,atol=2e-6)
    (-scored.log_prob.sum()).backward()
    assert any(p.grad is not None for p in policy.state.parameters())
    assert any(p.grad is not None for p in policy.action.parameters())
    assert all(p.grad is None for p in policy.critic.parameters())
    # 必须覆盖实际publish的A源，而非另一个同形状B对象；学习完成后这些暂存退役。
    market,evaluation,cancels=published_sources
    state_before=inputs.state.detach().clone();rules_before=inputs.rules.clone();cancel_before=inputs.cancels.snapshot()
    with torch.no_grad():
        evaluation.state.add_(3.);market.rule.zero_();cancels.values.add_(7.)
        cancels.present.zero_();cancels.physical_slots.fill_(-1)
    assert torch.equal(inputs.state,state_before) and torch.equal(inputs.rules,rules_before)
    for name in ('values','present','physical_slots','fault'):
        assert torch.equal(getattr(inputs.cancels,name),getattr(cancel_before,name))
    captured.replay();torch.cuda.synchronize();assert_candidate(candidate)
    assert torch.equal(candidate.choices,owned.choices) and torch.equal(candidate.log_prob,owned.log_prob)
    # 覆写同一个采样发布源；owned完整路径/终态/随机游标仍保原值。
    saved=owned.choices.clone();saved_counter=owned.random.counter.clone();saved_ell=owned.log_prob.clone()
    stream.counter[:,0].add_(17)
    captured.replay();torch.cuda.synchronize()
    assert torch.equal(owned.choices,saved) and torch.equal(owned.random.counter,saved_counter)
    assert torch.equal(owned.log_prob,saved_ell) and not torch.equal(candidate.random.counter,saved_counter)
    report=dict(stage='actual_joint_actor_path',cutoff=int(inputs.actor.cutoff.cpu()[0]),
        length=int(owned.length.cpu()[0]),choices=owned.choices[0,:owned.length[0]].cpu().tolist(),
        scored_conditions=int(owned.scored.sum().cpu()),model_state_gradients=True,
        actual_publish_source_overwrite_stable=True,
        policy_version=int(owned.policy_version.cpu()[0]),physical_cancel_width=inputs.cancels.present.shape[1],
        cancel_objects=int(inputs.cancels.present.sum().cpu()),elapsed_seconds=time.perf_counter()-started)
    reports.append(report);(artifact/f'actor-path-{index}.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report),flush=True)


def run(native,source,history):
    from gpu_actor_support_validation import context
    from .probability_validation import NativeProbabilityTests,small_domain
    started=time.perf_counter();torch.set_num_threads(8)
    legacy={}
    suite=unittest.TestSuite(NativeProbabilityTests(name,native,history,legacy) for name in (
        'test_complete_joint_distribution_entropy_and_weighted_surrogate',
        'test_actual_singletons_none_zero_and_adamw_state'))
    assert unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
    features=native.features(shared_books=True)
    torch.manual_seed(181601);policy=Policy100ms(ModelShape100ms(1,32)).cuda().eval()
    actor=context(source);device='cuda';count=len(features['cancel'])
    t=lambda v,dtype=torch.float32:torch.tensor(v,dtype=dtype,device=device)
    cancels=CancelInputs(t(features['cancel']).reshape(1,count,184),torch.ones((1,count),dtype=torch.bool,device=device),
        torch.arange(count,device=device)[None],torch.zeros(1,dtype=torch.int32,device=device))
    inputs=ActorPolicyInputs(actor,torch.randn(1,32,device=device),t([features['rows'][0]['rule']]),
        cancels,torch.full_like(actor.cutoff,11),torch.ones(1,dtype=torch.bool,device=device),actor.fault)
    actions,_=small_domain();probabilities=[]
    for index,path in enumerate(sorted(actions)):
        original=original_path(inputs,native,path);schedule=ReplaySchedule.for_update(original)
        scored=replay_action(policy,inputs,original,schedule,word_capacity=16)
        assert scored.fits.cpu().item() and scored.fault.cpu().tolist()==[0]
        reference,z,expected=original_score(policy,inputs,native,path)
        torch.testing.assert_close(scored.log_prob.cpu(),expected.unsqueeze(0),rtol=2e-5,atol=2e-6)
        probabilities.append(scored.log_prob.detach().exp())
        print(json.dumps(dict(stage='joint_leaf',current=index+1,total=21,
            elapsed_seconds=time.perf_counter()-started)),flush=True)
    torch.testing.assert_close(torch.stack(probabilities).sum(),torch.tensor(1.,device=device),rtol=2e-5,atol=2e-6)
    optimizer_checks=[]
    for path in ((2,5,0,0,0,0),(2,15,0,0,0,0),(5,0)):
        for established in (False,True):
            model=copy.deepcopy(policy);value=replace(inputs,state=inputs.state.detach().clone().requires_grad_())
            original=original_path(value,native,path);reference,z,expected=original_score(model,value,native,path)
            optimizer=torch.optim.AdamW(model.parameters(),lr=.003,weight_decay=.1)
            oracle=torch.optim.AdamW(reference.parameters(),lr=.003,weight_decay=.1)
            if established:
                for network in (model,reference):
                    for parameter in network.parameters():parameter.grad=torch.full_like(parameter,.007)
                optimizer.step();oracle.step()
                # 原预计必须按更新后的同一参数重算，不能拿首次评分值沿用。
                # 保留oracle optimizer已建状态/引用，同时取更新后的独立评分图。
                from .native_inputs import ComponentGroup
                from .probability_validation import RepeatedReference
                from .actor import consume_actor
                group=ComponentGroup([z],[value.rules.cpu()],['DecisionRequired'],value.cancels.values.cpu(),None,None)
                expected=consume_actor(RepeatedReference(reference),group,0,native.actor(0),replay=list(path)).log_prob
            model.zero_grad(set_to_none=True);reference.zero_grad(set_to_none=True)
            scored=replay_action(model,value,original,ReplaySchedule.for_update(original),word_capacity=16)
            scored.log_prob.sum().backward();expected.sum().backward()
            counts=parameter_gradients(model,reference)
            torch.testing.assert_close(value.state.grad.cpu(),z.grad,rtol=2e-4,atol=2e-5)
            optimizer.step();oracle.step()
            for actual,wanted in zip(model.parameters(),reference.parameters()):
                torch.testing.assert_close(actual.cpu(),wanted,rtol=2e-4,atol=2e-5)
                a,b=optimizer.state.get(actual,{}),oracle.state.get(wanted,{})
                assert a.keys()==b.keys()
                for key in a:torch.testing.assert_close(a[key].cpu(),b[key].cpu(),rtol=2e-4,atol=2e-5)
            optimizer_checks.append(dict(path=path,established_AdamW=established,**counts))
            print(json.dumps(dict(stage='optimizer_done',current=len(optimizer_checks),total=6,
                elapsed_seconds=time.perf_counter()-started)),flush=True)
    print(json.dumps(dict(stage="sampling_and_capacity",elapsed_seconds=time.perf_counter()-started)),flush=True)
    # 同一个有限冻结的装批代表：第二行是NoAct，非第二账户／正式根。
    # 未调用行的NaN若先经全批编码，再mask，仍可污染第一行的共享参数梯度。
    def two_rows(value):
        if isinstance(value,torch.Tensor):return value.clone() if value.dim()==0 else value.repeat((2,)+(1,)*(value.dim()-1))
        if is_dataclass(value):return replace(value,**{field.name:two_rows(getattr(value,field.name)) for field in fields(value)})
        raise TypeError('装批参考只重复实际张量列')
    model=copy.deepcopy(policy)
    mixed_actor=replace(actor,**{name:two_rows(value) for name,value in vars(actor).items() if name!='rules'})
    mixed_actor.decision_required[1]=False
    mixed=replace(inputs,actor=mixed_actor,state=two_rows(inputs.state).detach().requires_grad_(),
        rules=two_rows(inputs.rules),cancels=two_rows(inputs.cancels),policy_version=two_rows(inputs.policy_version),
        ready=two_rows(inputs.ready),fault=two_rows(inputs.fault))
    with torch.no_grad():mixed.state[1].fill_(float('nan'));mixed.rules[1].fill_(float('nan'));mixed.cancels.values[1].fill_(float('nan'))
    single=original_path(inputs,native,(2,5,0,0,0,0));batched=two_rows(single)
    batched.length[1]=0;batched.stages[1].fill_(-1);batched.choices[1].fill_(-1);batched.scored[1].zero_()
    reference,z,expected=original_score(model,inputs,native,(2,5,0,0,0,0))
    result=replay_action(model,mixed,batched,ReplaySchedule.for_update(batched),word_capacity=16)
    assert result.fault.cpu().tolist()==[0,0] and result.fits.cpu().item()
    torch.testing.assert_close(result.log_prob[:1].cpu(),expected.unsqueeze(0),rtol=2e-5,atol=2e-6)
    assert result.log_prob[1].cpu().item()==0.
    model.zero_grad(set_to_none=True);result.log_prob.sum().backward();expected.sum().backward()
    mixed_gradients=parameter_gradients(model,reference)
    torch.testing.assert_close(mixed.state.grad[:1].cpu(),z.grad,rtol=2e-4,atol=2e-5)
    assert torch.equal(mixed.state.grad[1],torch.zeros_like(mixed.state.grad[1]))
    stream=ActionRandom(torch.full_like(actor.cutoff,181601),torch.zeros((1,4),dtype=torch.int64,device=device))
    sampling=CapturedDeviceActor(policy,inputs,stream,path_capacity=32,word_capacity=16)
    sampled=sampling.replay();torch.cuda.synchronize();assert_candidate(sampled);owned=sampled.snapshot()
    path=tuple(owned.choices[0,:owned.length[0]].cpu().tolist());assert path in actions
    _,_,expected=original_score(policy,inputs,native,path)
    torch.testing.assert_close(owned.log_prob.cpu(),expected.unsqueeze(0),rtol=2e-5,atol=2e-6)
    assert int(owned.random.counter[0,0].cpu())==int(owned.scored.sum().cpu())
    assert not stream.counter.any().cpu().item()
    no_trade=copy.deepcopy(policy)
    with torch.no_grad():
        no_trade.action.actor.family[-1].weight.zero_();no_trade.action.actor.family[-1].bias.zero_()
        no_trade.action.actor.family[-1].bias[0]=100.
    no_trade_actor=CapturedDeviceActor(no_trade,inputs,stream,path_capacity=32,word_capacity=16)
    selected_no_trade=no_trade_actor.replay();torch.cuda.synchronize();assert_candidate(selected_no_trade)
    assert selected_no_trade.length.cpu().tolist()==[1] and selected_no_trade.choices[0,0].cpu().item()==0
    assert selected_no_trade.random.counter.cpu().tolist()==[[1,0,0,0]]
    # 独立熵流抽完整当前策略动作，行为流保持；重算无随机推进。
    entropy_stream=ActionRandom(stream.seed+1,stream.counter.clone())
    entropy_actor=CapturedDeviceActor(policy,inputs,entropy_stream,path_capacity=32,word_capacity=16)
    entropy=entropy_actor.replay();torch.cuda.synchronize();assert_candidate(entropy);entropy=entropy.snapshot()
    score=replay_action(policy,inputs,entropy,ReplaySchedule.for_update(entropy),word_capacity=16)
    h,surrogate=entropy_sample_objective(score,target_scale=.5)
    torch.testing.assert_close(h,-.5*score.log_prob.detach())
    torch.testing.assert_close(surrogate,.5*(score.log_prob.detach()+1.)*score.log_prob)
    assert not entropy_stream.counter.any().cpu().item() and not stream.counter.any().cpu().item()
    # 选择最长合法叶仅靠工程模型参数评分，不改支持；容量1须保共同候选出口。
    forced=copy.deepcopy(policy)
    with torch.no_grad():
        forced.action.actor.family[-1].weight.zero_();forced.action.actor.family[-1].bias.zero_()
        forced.action.actor.family[-1].bias[2]=10.
        forced.action.actor.quantity.weight.zero_();forced.action.actor.quantity.bias.zero_()
        forced.action.actor.quantity.bias[15]=10.
    small=CapturedDeviceActor(forced,inputs,stream,path_capacity=1,word_capacity=16,deterministic=True)
    incomplete=small.replay();torch.cuda.synchronize()
    assert not incomplete.fits.cpu().item() and incomplete.required_steps.cpu().item()==2
    assert not incomplete.complete.all().cpu().item() and not stream.counter.any().cpu().item()
    capacity_exit=candidate_summary(incomplete)
    print(json.dumps(dict(event='path_capacity_exit',**capacity_exit)),flush=True)
    resumed=CapturedDeviceActor(forced,inputs,stream,path_capacity=32,word_capacity=16,deterministic=True)
    complete=resumed.replay();torch.cuda.synchronize();assert_candidate(complete)
    assert complete.choices[0,:complete.length[0]].cpu().tolist()==[2,15,0,0,0,0]
    assert complete.scored.sum().cpu().item()==2 and not complete.random.counter.any().cpu().item()
    capacity_resumed=candidate_summary(complete)
    print(json.dumps(dict(event='path_capacity_resumed',**capacity_resumed)),flush=True)
    # singleton价格头包含NaN但设备IF不调用；NoAct全无头与随机调用。
    with torch.no_grad():forced.action.actor.price.weight.fill_(float('nan'))
    replayed=resumed.replay();torch.cuda.synchronize();assert_candidate(replayed)
    assert torch.isfinite(replayed.log_prob).all().cpu().item()
    singleton_path=replayed.snapshot();forced.zero_grad(set_to_none=True)
    singleton_score=replay_action(forced,inputs,singleton_path,ReplaySchedule.for_update(singleton_path),word_capacity=16)
    singleton_score.log_prob.sum().backward()
    assert all(parameter.grad is None for parameter in forced.action.actor.price.parameters())
    assert all(parameter.grad is None for parameter in forced.action.actor.fee_weight.parameters())
    assert all(parameter.grad is None or torch.isfinite(parameter.grad).all().cpu().item() for parameter in forced.parameters())
    inactive=replace(inputs,actor=replace(actor,decision_required=torch.zeros_like(actor.decision_required)))
    quiet=CapturedDeviceActor(forced,inactive,stream,path_capacity=1,word_capacity=16)
    noact=quiet.replay();torch.cuda.synchronize();assert_candidate(noact)
    assert noact.length.cpu().tolist()==[0] and noact.log_prob.cpu().tolist()==[0.]
    assert not noact.random.counter.any().cpu().item()
    unused=replace(inactive,state=torch.full_like(inputs.state,float('nan')).requires_grad_(),
        rules=torch.full_like(inputs.rules,float('nan')),
        cancels=replace(inputs.cancels,values=torch.full_like(inputs.cancels.values,float('nan'))))
    forced.zero_grad(set_to_none=True);quiet_path=noact.snapshot()
    uncalled=replay_action(forced,unused,quiet_path,ReplaySchedule.for_update(quiet_path),word_capacity=16)
    assert uncalled.log_prob.cpu().tolist()==[0.] and uncalled.fault.cpu().tolist()==[0]
    uncalled.log_prob.sum().backward()
    assert all(parameter.grad is None for parameter in forced.parameters())
    assert torch.equal(unused.state.grad,torch.zeros_like(unused.state))
    saved_choices=owned.choices.clone();saved_counter=owned.random.counter.clone()
    stream.counter[:,0].add_(17);sampling.replay();torch.cuda.synchronize()
    assert torch.equal(owned.choices,saved_choices) and torch.equal(owned.random.counter,saved_counter)
    report=dict(status='PASS',joint_leaves=21,original_probability_tests=legacy,optimizer_checks=optimizer_checks,
        actual_sample_path=path,independent_entropy_scale=.5,formal_profile=False,
        mixed_NoAct_NaN_gradients=mixed_gradients,
        resource_path_retry=True,capacity_exit=capacity_exit,capacity_resumed=capacity_resumed,singleton_NaN_unused=True,NoAct_draws=0,NoAct_NaN_uncalled_gradients=True,
        elapsed_seconds=time.perf_counter()-started,full_PPO_or_continuous_host=False)
    print(json.dumps(report),flush=True);return json.dumps(report,indent=2)
