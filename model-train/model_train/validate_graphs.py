"""实际导出和纯图验证；各线程复用同一份初始化权重。"""
import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import time
import onnx
import onnxruntime as ort
import torch
from .graphs import CONFIGURATIONS, TwoGraphs, STATE_INPUTS, STATE_OUTPUTS, ACTION_INPUTS, ACTION_OUTPUTS, encode_all, score_all
from .graph_fixtures import fixture, account_slice, cancel_slices, market_slice, paired_gtc_prefixes


def sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for part in iter(lambda: f.read(1024 * 1024), b''):
            h.update(part)
    return h.hexdigest()


def session(path, threads):
    opts = ort.SessionOptions()
    opts.intra_op_num_threads = threads
    opts.inter_op_num_threads = 1
    opts.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
    opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    opts.enable_mem_pattern = False
    for key in ('session.intra_op.allow_spinning', 'session.inter_op.allow_spinning'):
        opts.add_session_config_entry(key, '0')
    return ort.InferenceSession(str(path), sess_options=opts, providers=['CPUExecutionProvider'])


def call_ort(sess, names):
    def call(*args):
        if names == ACTION_INPUTS and len(args) == 7:
            args = (*args, torch.zeros((args[0].shape[0], 9999), dtype=torch.float32))
        return tuple(torch.from_numpy(x) for x in sess.run(None, {
            n: v.detach().numpy() for n, v in zip(names, args)}))
    return call


def compare(reference, actual, label, stats):
    """rtol基于PyTorch；首个差异保留完整位置，两端非有限值都失败。"""
    for output, (ref, value) in enumerate(zip(reference, actual)):
        finite = torch.isfinite(ref) & torch.isfinite(value)
        error = (value - ref).abs()
        good = finite & (error <= 1e-5 + 1e-4 * ref.abs())
        if not bool(good.all()):
            index = tuple((~good).nonzero()[0].tolist())
            context = {k:v for k,v in label.items() if k != 'object_identities'}
            if 'object_identities' in label:
                context['object_identity'] = label['object_identities'][index[0]]
            raise AssertionError({'context': context, 'output': output, 'index': index,
                                  'reference': float(ref[index]), 'actual': float(value[index]),
                                  'finite': bool(finite[index])})
        stats['max_absolute_error'] = max(stats.get('max_absolute_error', 0), float(error.max()))
    stats['comparisons'] = stats.get('comparisons', 0) + len(reference)


def compare_selection(reference, actual, indices, identities, label):
    # torch.argmax首个最大值等价于宿主严格较大才替换；不使用近似并列。
    p, o = int(reference.argmax()), int(actual.argmax())
    if p != o:
        candidates = [p, o]
        raise AssertionError({'context': label, 'pytorch_selected': int(indices[p]),
            'actual_selected': int(indices[o]), 'candidates': [
                {'object_index': int(indices[i]), 'identity': identities[int(indices[i])],
                 'pytorch': float(reference[i]), 'actual': float(actual[i])} for i in candidates]})


def identities_for(pairs, removed):
    singles = [f'Single(pair={i},leg={leg})' for i in range(pairs) for leg in ('A', 'B')
               if not (removed and i == 0 and leg == 'A')]
    groups = [f'Paired(pair={i},ordinals=0/1)' for i in range(pairs) if not (removed and i == 0)]
    return singles + groups + (['Market(current)'] if pairs else [])


def export(model, config, directory):
    fast, slow, rule, prefix, objects, mask = fixture(2, 1, True)
    h = torch.zeros(config.layers, 2, config.hidden)
    z = model.state(fast, slow, h)[0]
    B = torch.export.Dim('B', min=1)
    K = torch.export.Dim('K', min=1, max=256)
    examples = (
        (model.state, (fast, slow, h), STATE_INPUTS, STATE_OUTPUTS, ({0:B}, {0:B}, {1:B}), 'state_encoder'),
        (model.action, (z, rule, prefix, objects, mask, torch.zeros(2, 3*config.hidden//2), torch.zeros(2, dtype=torch.bool), torch.zeros(2, 9999)),
         ACTION_INPUTS, ACTION_OUTPUTS, ({0:B}, {0:B}, {0:B}, {0:B,1:K}, {0:B,1:K}, {0:B}, {0:B}, {0:B}), 'action_decoder'))
    metadata = {}
    for module, args, inputs, outputs, dynamic, name in examples:
        program = torch.onnx.export(module, args, dynamo=True, opset_version=18, optimize=True,
            verify=True, report=True, artifacts_dir=str(directory), input_names=list(inputs),
            output_names=list(outputs), dynamic_shapes=dynamic)
        path = directory/(name+'.onnx')
        program.save(path, include_initializers=True, keep_initializers_as_inputs=False, external_data=True)
        graph = onnx.load(path, load_external_data=False)
        def spec(x):
            return {'name':x.name, 'type':x.type.tensor_type.elem_type,
                    'shape':[d.dim_param or d.dim_value for d in x.type.tensor_type.shape.dim]}
        metadata[name] = {'ir':graph.ir_version, 'opsets':[(x.domain, x.version) for x in graph.opset_import],
            'nodes':len(graph.graph.node), 'operators':sorted(set(n.op_type for n in graph.graph.node)),
            'inputs':list(map(spec, graph.graph.input)), 'outputs':list(map(spec, graph.graph.output))}
    return metadata


def verify(model, config, graph_directory, threads):
    state = call_ort(session(graph_directory/'state_encoder.onnx', threads), STATE_INPUTS)
    action = call_ort(session(graph_directory/'action_decoder.onnx', threads), ACTION_INPUTS)
    stats = {'threads':threads, 'cases':[], 'pure_graph_cpu_ms':{}}
    start = time.perf_counter()
    with torch.no_grad():
        for batch in (1, 2, 3):
            fast, slow, rule, prefix, objects, mask = fixture(batch, 1, True)
            hp = torch.zeros(config.layers, batch, config.hidden)
            ho = hp.clone()
            # 固定张量独立递推，不代表70分钟连续公共业务帧。
            for step in range(12):
                zp, hp = model.state(fast, slow, hp)
                zo, ho = state(fast, slow, ho)
                label = {'config':config.name, 'B':batch, 'N':2, 'step':step}
                compare((zp, hp), (zo, ho), label, stats)
                args = (rule, prefix, objects, mask, torch.zeros(batch, 3*config.hidden//2), torch.zeros(batch, dtype=torch.bool))
                compare(model.action(zp, *args), action(zo, *args), label, stats)
            stats['cases'].append({'batch':batch, 'independent_recurrence_steps':12})
        for pairs, removed, mixed in ((0,False,False), (1,True,False), (85,False,False), (86,True,False), (86,False,False), (86,True,True)):
            batch = 2 if mixed else 1
            fast, slow, rule, prefix, objects, mask = fixture(batch, pairs, removed, mixed=mixed)
            zp, hp = model.state(fast, slow, torch.zeros(config.layers, batch, config.hidden))
            zo, ho = state(fast, slow, torch.zeros_like(hp))
            n = objects.shape[1]
            lengths = [256]*(n//256) + ([n%256] if n%256 else [])
            empty = pairs == 0
            label = {'config':config.name, 'B':batch, 'N':mask.sum(1).tolist(), 'blocks':lengths}
            rp, ep = encode_all(model.action, zp, rule, objects, mask, lengths, empty=empty)
            ro, eo = encode_all(action, zo, rule, objects, mask, lengths, empty=empty)
            compare((rp,), (ro,), {**label, 'path':'reduce'}, stats)
            assert torch.equal(ep, eo)
            args = (rule, prefix, objects[:,:1], mask[:,:1])
            compare(model.action(zp, *args, rp, ep), action(zo, *args, ro, eo), {**label, 'path':'family'}, stats)
            identities = identities_for(pairs, removed)
            for family, kind in ((3,0), (4,1), (5,2)):
                selected = mask & (objects[:,:,kind] == 1)
                cp = torch.zeros_like(prefix)
                cp[:,family] = 1; cp[:,6] = 1; cp[:,13] = 1
                ps = score_all(model.action, zp, rule, cp, objects, selected, lengths, rp, ep, empty=empty)
                oscores = score_all(action, zo, rule, cp, objects, selected, lengths, ro, eo, empty=empty)
                for row, (p, o) in enumerate(zip(ps, oscores)):
                    if p.numel():
                        context = {**label, 'family':family, 'row':row, 'object_identities':[(i, identities[i]) for i in selected[row].nonzero().flatten().tolist()]}
                        compare((p,), (o,), context, stats)
                        compare_selection(p, o, selected[row].nonzero().flatten(), identities, context)
                if n == 257:
                    for name, caller, z, reduced, exists, reference in (
                            ('PyTorch', model.action, zp, rp, ep, ps), ('ORT', action, zo, ro, eo, oscores)):
                        split, split_exists = encode_all(caller, z, rule, objects, mask, [128,129], empty=False)
                        compare((reduced,), (split,), {**label, 'runtime':name, 'split':[128,129]}, stats)
                        split_scores = score_all(caller, z, rule, cp, objects, selected, [128,129], split, split_exists, empty=False)
                        for row, (ref, value) in enumerate(zip(reference, split_scores)):
                            if ref.numel():
                                context = {**label, 'runtime':name, 'family':family, 'row':row, 'split':[128,129], 'object_identities':[(i, identities[i]) for i in selected[row].nonzero().flatten().tolist()]}
                                compare((ref,), (value,), context, stats)
                                compare_selection(ref, value, selected[row].nonzero().flatten(), identities, context)
            stats['cases'].append({'objects':mask.sum(1).tolist(), 'logical_blocks':lengths,
                                   'physical_blocks':[1] if empty else [256]*len(lengths)})
        # 图接口的动态K不随宿主补齐消失；直接调用原图各合法K。
        for k in (1, 2, 128, 129, 255, 256):
            fast, slow, rule, prefix, objects, mask = fixture(2, 85)
            z, _ = model.state(fast, slow, torch.zeros(config.layers, 2, config.hidden))
            args = (z, rule, prefix, objects[:,:k], mask[:,:k], torch.zeros(2,3*config.hidden//2), torch.zeros(2,dtype=torch.bool))
            compare(model.action(*args), action(*args), {'config':config.name,'dynamic_K':k}, stats)
        fast, slow, rule, _, objects, mask = fixture(2)
        z, _ = model.state(fast, slow, torch.zeros(config.layers, 2, config.hidden))
        for step, prefix in enumerate(paired_gtc_prefixes(2)):
            args = (z, rule, prefix, objects, mask, torch.zeros(2,3*config.hidden//2), torch.zeros(2,dtype=torch.bool))
            compare(model.action(*args), action(*args), {'config':config.name,'paired_gtc_prefix':step}, stats)
        stats['pure_graph_cpu_ms']['validation_elapsed'] = (time.perf_counter()-start)*1000
    return stats


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--config', choices=[c.name for c in CONFIGURATIONS], required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--source', type=Path, help='复用已有权重和图的配置父目录；不会改写来源')
    p.add_argument('--threads', type=int, choices=(1,4), required=True)
    p.add_argument('--export', action='store_true')
    a = p.parse_args()
    assert os.environ['OMP_NUM_THREADS'] == str(a.threads) and os.environ['MKL_NUM_THREADS'] == str(a.threads)
    torch.set_num_threads(a.threads); torch.set_num_interop_threads(1)
    c = next(c for c in CONFIGURATIONS if c.name == a.config)
    directory = a.output/c.name
    directory.mkdir(parents=True, exist_ok=True)
    source = (a.source/c.name) if a.source else directory
    model = TwoGraphs(c).eval()
    weights = source/'initial_weights.pt'
    if not weights.exists() and not a.source:
        torch.save(model.state_dict(), weights)
    model.load_state_dict(torch.load(weights, weights_only=True, map_location='cpu'))
    result = {'configuration':c.__dict__, 'python':platform.python_version(),
        'versions':{n:importlib.metadata.version(n) for n in ('torch','onnx','onnxruntime','onnxscript','numpy')},
        'torch_backend':torch.__config__.show(), 'ort_build':ort.get_build_info(),
        'providers':['CPUExecutionProvider'], 'initial_weights_sha256':sha(weights),
        'scope':'纯图固定张量；非公共经济/完整支持集/三端/长期/目标CPU/GPU验收'}
    if a.export:
        with torch.no_grad(): result['graphs'] = export(model, c, directory)
        (directory/'export_metadata.json').write_text(json.dumps(result, ensure_ascii=False, indent=2))
    graph_directory = directory if a.export else source
    try:
        result['validation'] = verify(model, c, graph_directory, a.threads)
    except Exception as error:
        result['failure'] = repr(error)
        (directory/f'failure_threads_{a.threads}.json').write_text(json.dumps(result, ensure_ascii=False, indent=2))
        raise
    result['files'] = {x.name:{'bytes':x.stat().st_size,'sha256':sha(x)}
                      for x in graph_directory.iterdir() if x.suffix in ('.onnx','.data','.pt')}
    lib = Path(ort.__file__).parent/'capi'/'libonnxruntime.so.1.28.0'
    result['ort_library'] = {'name':lib.name,'sha256':sha(lib)}
    (directory/f'validation_threads_{a.threads}.json').write_text(json.dumps(result, ensure_ascii=False, indent=2))
    fast, rule = market_slice()
    trace = {'fast':fast.trace,'rule':rule.trace,'slow_empty':account_slice().trace,
             'slow_one_b':account_slice(1,True).trace,'cancel_one_b':[r.trace for r in cancel_slices(1,True)]}
    (a.output/'fixture_trace.json').write_text(json.dumps(trace, ensure_ascii=False, indent=2))
    print(json.dumps(result['validation'], ensure_ascii=False), flush=True)

if __name__ == '__main__':
    main()
