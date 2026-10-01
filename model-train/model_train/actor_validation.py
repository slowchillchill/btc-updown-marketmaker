"""有限真实原生组的三端证据入口；不启动训练或长期运行。"""
import torch
from .graphs import TwoGraphs, CONFIGURATIONS, ACTION_INPUTS, STATE_INPUTS
from .native_inputs import NativeComponentConsumer
from .actor import consume_actor, conditional_choice
from .validate_graphs import call_ort, session, compare, ort_stages


def assert_close(reference, actual, *, rtol=1e-4, atol=1e-5):
    """沿原数值口径拒绝任一端非有限；精确重算另保留零容差。"""
    assert reference.shape == actual.shape
    if reference.numel():
        compare((reference.detach().reshape(-1),), (actual.detach().reshape(-1),), {}, {})
    if rtol == atol == 0:
        torch.testing.assert_close(reference, actual, rtol=0, atol=0)


def compare_actor_steps(reference_path, reference_steps, actual_path, actual_steps, context, stats):
    """逐步核对真实支持/前缀、当前可选分数，然后才核对严格选择。"""
    for step, (reference, actual) in enumerate(zip(reference_steps, actual_steps)):
        label = {**context, 'step': step}
        assert reference[:3] == actual[:3], {**label, 'failure': 'support/state/prefix mismatch',
                                            'reference': reference[:3], 'actual': actual[:3]}
        mask = torch.tensor(reference[0], dtype=torch.bool)
        left, right = torch.tensor(reference[3]), torch.tensor(actual[3])
        assert left.shape == right.shape == mask.shape, label
        try:
            compare((left[mask],), (right[mask],), label, stats)
        except AssertionError as error:
            detail = error.args[0]
            detail['original_head_index'] = int(mask.nonzero().flatten()[detail['index'][0]])
            detail['index_space'] = 'supported scores; original_head_index names the full head coordinate'
            raise
        a, b = reference_path[step], actual_path[step]
        assert a == b, {**label, 'failure': 'strict choice mismatch', 'reference_choice': a,
                       'actual_choice': b, 'reference_scores_at_choices': [float(left[a]), float(left[b])],
                       'actual_scores_at_choices': [float(right[a]), float(right[b])]}
    assert len(reference_steps) == len(actual_steps) == len(reference_path) == len(actual_path), context


def _scores(consumer, group, actor, row=0):
    support = actor.support()
    if sum(support) == 1:
        return group.states[row].new_zeros(len(support))
    outputs = consumer.parameters(group, row, actor.prefix_features(),
        price_fee_features=actor.price_fee_features(), price_indices=actor.price_indices())
    head = actor.head_index()
    return (torch.cat([block[head][0] for block in outputs])[:len(support)]
            if head == 5 else outputs[0][head][0])


def _records(consumer, group, native_group, path, row=0):
    actor = native_group.actor(row)
    records = []
    for choice in path:
        records.append((actor.support(), actor.validation_state(), actor.prefix_features(),
                        _scores(consumer, group, actor, row).tolist()))
        actor.choose(choice)
    assert actor.complete()
    return records


def _pair(history, native_group, directory):
    torch.set_num_threads(1)
    config = next(c for c in CONFIGURATIONS if c.name == directory.name)
    model = TwoGraphs(config).eval()
    model.load_state_dict(torch.load(directory/'initial_weights.pt', weights_only=True))
    consumers = [NativeComponentConsumer(model), NativeComponentConsumer(model)]
    stages = ort_stages(directory,1)
    consumers[1].stages = {name:(lambda *args, call=call: call(*args)[0])
                          for name,call in stages.items() if name != 'action_family'}
    consumers[1].stages['action_family'] = stages['action_family']
    # 定向重放可从已选族起步；规则仍由同一个分图获得。
    def ort_rule(group,row):
        if row not in group.rule_contexts:
            _,context = stages['action_family'](group.states[row],group.rules[row],group.cancel_reduced,group.cancel_exists)
            group.rule_contexts[row] = context
        return group.rule_contexts[row]
    consumers[1]._rule = ort_rule
    forwards = (model.state.forward,call_ort(session(directory/'state_encoder.onnx',1),STATE_INPUTS))
    traces,groups = [],[]
    with torch.no_grad():
        for consumer,forward in zip(consumers,forwards):
            rows=[]
            def state(*args):
                z,h=forward(*args);rows.append((z.flatten().tolist(),h.flatten().tolist()));return z,h
            model.state.forward=state
            for prior in history: consumer.consume(prior)
            groups.append(consumer.consume(native_group.features(shared_books=True)))
            traces.append(rows)
    model.state.forward=forwards[0]
    for left,right in zip(*traces):
        for a,b in zip(left,right):assert_close(torch.tensor(a),torch.tensor(b))
    assert len(traces[0]) == len(traces[1])
    def continuation(native):
        rows,results = [],[]
        with torch.no_grad():
            for consumer,forward in zip(consumers,forwards):
                trace=[]
                def state(*args):
                    z,h=forward(*args);trace.append((z.flatten().tolist(),h.flatten().tolist()));return z,h
                model.state.forward=state
                group=consumer.consume(native.features(shared_books=True))
                decisions=[]
                for row,role in enumerate(group.roles):
                    if role != 'DecisionRequired':continue
                    result=consume_actor(consumer,group,row,native.actor(row),deterministic=True)
                    decisions.append((row,result.actor.path(),_records(consumer,group,native,result.actor.path(),row)))
                rows.append(trace);results.append(decisions)
        model.state.forward=forwards[0]
        for left,right in zip(*rows):
            for a,b in zip(left,right):assert_close(torch.tensor(a),torch.tensor(b))
        for left,right in zip(*results):compare_actor_steps(left[1],left[2],right[1],right[2],{}, {})
        return rows[0],results[0]
    return model,config,consumers,groups,traces[0],continuation


def _training(model,history,native_group,family=None):
    consumer=NativeComponentConsumer(model)
    for prior in history:consumer.consume(prior)
    group=consumer.consume(native_group.features(shared_books=True))
    torch.manual_seed(742001)
    first,second=native_group.actor(0),native_group.actor(0)
    if family is not None:
        first.choose(family);second.choose(family)
    sampled=consume_actor(consumer,group,0,first)
    repeated=consume_actor(consumer,group,0,second,replay=sampled.actor.path()[int(family is not None):])
    assert_close(sampled.log_prob,repeated.log_prob,rtol=0,atol=0)
    params=tuple(model.parameters())
    left=torch.autograd.grad(sampled.log_prob,params,allow_unused=True,retain_graph=True)
    right=torch.autograd.grad(repeated.log_prob,params,allow_unused=True,retain_graph=True)
    assert any(value is not None and torch.count_nonzero(value) for value in left)
    if family==4:
        assert any(value is not None and torch.count_nonzero(value)
            for (name,_),value in zip(model.named_parameters(),left) if 'object_encoder' in name)
    for a,b in zip(left,right):
        assert (a is None)==(b is None)
        if a is not None:assert_close(a,b,rtol=0,atol=0)
    # 与实际轨迹独立的完整动作样本，不把节点熵和冒充联合熵。
    from .actor import joint_entropy_sample
    report,surrogate,result=joint_entropy_sample(consumer,group,0,native_group.actor(0))
    assert_close(report,-.5*result.log_prob.detach(),rtol=0,atol=0)
    actual=torch.autograd.grad(surrogate,params,allow_unused=True,retain_graph=True)
    grad=torch.autograd.grad(result.log_prob,params,allow_unused=True)
    for a,b in zip(actual,grad):
        assert (a is None)==(b is None)
        if a is not None:assert_close(a,.5*(result.log_prob.detach()+1)*b)


def validate_python(native_group,history,directory,replay,market_probe=True):
    model,config,consumers,groups,traces,continuation=_pair(history,native_group,directory)
    paths,records,probes,mixed=[],[],[],[]
    with torch.no_grad():
        for consumer,group in zip(consumers,groups):
            result=consume_actor(consumer,group,0,native_group.actor(0),deterministic=True)
            paths.append(result.actor.path());records.append(_records(consumer,group,native_group,result.actor.path()))
            probes.append(_records(consumer,group,native_group,replay))
            actors=[];probe=native_group.actor(0)
            for choice in replay:
                if not actors or probe.head_index()==3:
                    duplicate=native_group.actor(0)
                    for picked in probe.path():duplicate.choose(picked)
                    actors.append(duplicate)
                probe.choose(choice)
            batched=consumer.parameters_batch([(group,0,a) for a in actors])
            scores=[blocks[0][a.head_index()][0] for a,blocks in zip(actors,batched)]
            for a,value in zip(actors,scores):
                mask=torch.tensor(a.support());assert_close(value[mask],_scores(consumer,group,a)[mask])
            mixed.append([value.tolist() for value in scores])
        compare_actor_steps(paths[0],records[0],paths[1],records[1],{}, {})
        compare_actor_steps(replay,probes[0],replay,probes[1],{}, {})
    _training(model,history,native_group)
    return traces,paths[0],records[0],probes[0],mixed[0],config.layers,config.hidden,continuation


def validate_cancel(native_group,history,directory):
    model,config,consumers,groups,traces,continuation=_pair(history,native_group,directory)
    paths,records=[],[]
    with torch.no_grad():
        for consumer,group in zip(consumers,groups):
            result=consume_actor(consumer,group,0,native_group.actor(0),deterministic=True)
            current=[result.actor.path()]
            for family in (3,4,5):
                actor=native_group.actor(0);actor.choose(family)
                choice=_scores(consumer,group,actor).masked_fill(~torch.tensor(actor.support()),-torch.inf).argmax().item()
                current.append([family,choice])
            paths.append(current);records.append([_records(consumer,group,native_group,path) for path in current])
        for a,b,ra,rb in zip(paths[0],paths[1],records[0],records[1]):compare_actor_steps(a,ra,b,rb,{}, {})
        # 真实Market族仅一项；在评分入口前跳过，同时保留实际选择与RNG。
        for consumer,group in zip(consumers,groups):
            actor=native_group.actor(0);actor.choose(5)
            assert sum(actor.support())==1
            before=torch.random.get_rng_state().clone()
            original=consumer.parameters
            def forbidden(*args,**kwargs):raise AssertionError('singleton invoked neural scoring')
            consumer.parameters=forbidden
            result=consume_actor(consumer,group,0,actor)
            consumer.parameters=original
            assert result.log_prob.item()==result.conditional_entropy_sum.item()==0
            assert torch.equal(before,torch.random.get_rng_state()) and len(actor.path())==2
    _training(model,history,native_group,family=4)
    return traces,paths[0],records[0],config.layers,config.hidden,continuation


def main():
    import argparse, hashlib, json, os, subprocess, sys, sysconfig
    from pathlib import Path
    import onnxruntime
    parser=argparse.ArgumentParser()
    parser.add_argument('--graphs',type=Path,required=True)
    parser.add_argument('--old-graphs',type=Path,help='可选历史权重，仅记录SHA；新动作结构不要求跨版本逐值一致')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--configs',nargs='+',default=[c.name for c in CONFIGURATIONS])
    args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=True)
    root=Path(__file__).resolve().parents[1]
    library=Path(onnxruntime.__file__).parent/'capi/libonnxruntime.so.1.28.0'
    def sha(path):
        with path.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
    evidence={'scope':'finite actual actor; full C/P remains separate','ort_library_sha256':sha(library),'configurations':{}}
    for config in (c for c in CONFIGURATIONS if c.name in args.configs):
        current=args.graphs/config.name/'initial_weights.pt'
        new=torch.load(current,weights_only=True,map_location='cpu')
        initialized=TwoGraphs(config).state_dict()
        assert set(new)==set(initialized)
        assert all(torch.equal(value,new[name]) for name,value in initialized.items())
        evidence['configurations'][config.name]={'seed':config.seed,
            'input_contract_version':'native_inputs_v7','account_economics_version':'account_economics_v2','action_contract_version':'native_actions_v2',
            'feature_schema_sha256':sha(root/'native-inputs/feature_schema.json'),
            'same_seed_initialized_parameters':len(new),'new_weights_sha256':sha(current)}
        if args.old_graphs:
            previous=args.old_graphs/config.name/'initial_weights.pt'
            if not previous.exists():previous=args.old_graphs/'resumed'/config.name/'initial_weights.pt'
            evidence['configurations'][config.name]['historical_weights_sha256']=sha(previous)
            evidence['configurations'][config.name]['cross_version_equality']='不适用：native_actions_v2前缀和动作头改变；状态数学保持'
        del initialized,new
    for name in args.configs:
        env=os.environ.copy()
        env.update(PYTHONHOME=sys.base_prefix,PYO3_PYTHON=sys.executable,
            PYTHONPATH=os.pathsep.join([str(root),sysconfig.get_path('purelib')]),
            LD_LIBRARY_PATH=os.pathsep.join([str(Path(sys.base_prefix)/'lib'),env.get('LD_LIBRARY_PATH','')]),
            ORT_DYLIB_PATH=str(library),ACTOR_GRAPH_DIRECTORY=str(args.graphs/name))
        command=['cargo','test','--manifest-path',str(root/'native-inputs/Cargo.toml'),'--features','python,ort','--locked','--offline','--lib','same_process','--','--ignored','--nocapture','--test-threads=1']
        log=args.output/(name+'.log')
        with log.open('w') as f:result=subprocess.run(command,env=env,stdout=f,stderr=subprocess.STDOUT)
        evidence['configurations'][name]['exit_code']=result.returncode
        evidence['configurations'][name]['log_sha256']=sha(log)
        (args.output/'report.json').write_text(json.dumps(evidence,indent=2))
        if result.returncode:raise RuntimeError(f'{name} failed; see {log}')
    (args.output/'report.json').write_text(json.dumps(evidence,indent=2))




if __name__ == '__main__': main()
