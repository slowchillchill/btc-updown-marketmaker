"""#172 两代表图的独立算子诊断、同序标准图候选筛选与输出选择能力核查。"""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import resource
import shutil
import sys
import time

import numpy as np
import onnx
import onnxruntime as ort

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'model_train'))
from benchmark_optimization import feed, usage, sha


def options(threads, profile=None):
    o = ort.SessionOptions()
    o.intra_op_num_threads = threads
    o.inter_op_num_threads = 1
    o.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
    o.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    o.enable_mem_pattern = False
    for k in ('session.intra_op.allow_spinning', 'session.inter_op.allow_spinning'):
        o.add_session_config_entry(k, '0')
    if profile:
        o.enable_profiling = True
        o.profile_file_prefix = str(profile)
    return o


def category(node):
    text = ' '.join(p.value for p in node.metadata_props if p.key == 'namespace')
    if node.op_type == 'GRU' or 'grus.' in text:
        return 'gru'
    if 'AlignedLayerNorm' in text:
        return 'fixed_tree_normalization'
    if 'BookLinear' in text:
        return 'book_encoding'
    if 'BookEncoder' in text or any('book_sum_' in x for x in node.output):
        return 'book_reduction_and_layout'
    return 'other' if text else 'unattributed'


def loop_candidate(source, destination):
    """仅改归约 Loop 的整数形状控制，不改浮点 Slice/Add/Concat 次序。"""
    m = onnx.load(source, load_external_data=False)
    changed = 0
    h = onnx.helper
    for loop in m.graph.node:
        if loop.op_type != 'Loop' or not loop.output[0].startswith('book_sum_'):
            continue
        prefix = loop.output[0].removesuffix('result')
        body = next(a.g for a in loop.attribute if a.name == 'body')
        remove = {prefix+x for x in ('shape','n','next_shape','next_n','half','paired')}
        nodes = [n for n in body.node if not any(v in remove for v in n.output)]
        nodes[:0] = [h.make_node('Div',[prefix+'n',prefix+'two'],[prefix+'half']),
                    h.make_node('Mul',[prefix+'half',prefix+'two'],[prefix+'paired']),
                    h.make_node('Add',[prefix+'n',prefix+'one'],[prefix+'n_plus_one']),
                    h.make_node('Div',[prefix+'n_plus_one',prefix+'two'],[prefix+'next_n'])]
        del body.node[:]
        body.node.extend(nodes)
        body.input.append(h.make_tensor_value_info(prefix+'n',onnx.TensorProto.INT64,[1]))
        body.output.append(h.make_tensor_value_info(prefix+'next_n',onnx.TensorProto.INT64,[1]))
        loop.input.append(prefix+'initial_n')
        loop.output.append(prefix+'final_n')
        changed += 1
    assert changed == 1
    destination.parent.mkdir(parents=True,exist_ok=True)
    for f in source.parent.glob('state_encoder.onnx.*'):
        target = destination.parent/f.name
        if target.is_symlink(): target.unlink()
        if not target.exists(): shutil.copyfile(f,target)
    onnx.save(m,destination)
    onnx.checker.check_model(str(destination))
    return changed


def profile_state(args, directory, name, threads, frames, data, indices):
    cfg=json.loads((directory/'benchmark_configuration.json').read_text())['configuration']
    model=onnx.load(directory/'state_encoder.onnx',load_external_data=False)
    names={n.name:category(n) for n in model.graph.node}
    shapes={'source_nodes':dict(Counter(n.op_type for n in model.graph.node))}
    o=options(threads,args.output/(name+'-state-profile'))
    s=ort.InferenceSession(str(directory/'state_encoder.onnx'),sess_options=o,providers=['CPUExecutionProvider'])
    start=time.perf_counter()
    for i in indices: s.run(None,feed(data,frames[i],cfg))
    raw=Path(s.end_profiling())
    by=defaultdict(lambda:[0,0])
    # Loop 本身包含子图；单独记录嵌套子图，不与 Loop 墙钟相加。
    nodes=[]
    for e in json.loads(raw.read_text()):
        if e.get('cat')!='Node' or not e['name'].endswith('_kernel_time'): continue
        n=e['name'].removesuffix('_kernel_time')
        c=names.get(n,'book_loop_child' if n.startswith('book_sum_') else 'unattributed')
        by[c][0]+=1;by[c][1]+=e['dur']
        nodes.append([n,e.get('args',{}).get('op_name'),e['dur'],c])
    result={'configuration':name,'threads':threads,'sample_indices':indices,'diagnostic_only':True,
            'stage_node_count_duration_us':dict(by),'elapsed_seconds':time.perf_counter()-start,
            'raw_profile':raw.name,'raw_profile_sha256':sha(raw),'graph_sha256':sha(directory/'state_encoder.onnx'),
            'nodes':nodes,**shapes}
    (args.output/(name+'-profile.json')).write_text(json.dumps(result,ensure_ascii=False)+'\n')
    del s


def screen(args, directory, name, threads, frames, data, indices):
    cfg=json.loads((directory/'benchmark_configuration.json').read_text())['configuration']
    candidate=args.output/name/'state_encoder.onnx'
    loop_candidate(directory/'state_encoder.onnx',candidate)
    before=usage(); start=time.perf_counter()
    feeds=[feed(data,frames[i],cfg) for i in indices]
    preparation=time.perf_counter()-start
    results=[]
    for round in (0,1):
        for label,path in ([('baseline',directory/'state_encoder.onnx'),('loop_shape',candidate)] if round==0 else [('loop_shape',candidate),('baseline',directory/'state_encoder.onnx')]):
            load=time.perf_counter();s=ort.InferenceSession(str(path),sess_options=options(threads),providers=['CPUExecutionProvider']);load=time.perf_counter()-load
            for f in feeds[:16]:s.run(None,f)
            samples=[];cpu=time.process_time_ns();memory=usage()
            for i,f in zip(indices,feeds):
                begin=time.perf_counter_ns();v=s.run(None,f);elapsed=time.perf_counter_ns()-begin
                assert all(np.isfinite(a).all() for a in v)
                samples.append({'index':i,'ns':elapsed})
            results.append({'round':round,'version':label,'samples':samples,'cpu_ns':time.process_time_ns()-cpu,'memory_before':memory,'memory_after':usage(),'load_seconds':load})
            del s
    # 正式计时完成后才做配对数值对照。
    b=ort.InferenceSession(str(directory/'state_encoder.onnx'),sess_options=options(threads),providers=['CPUExecutionProvider'])
    c=ort.InferenceSession(str(candidate),sess_options=options(threads),providers=['CPUExecutionProvider'])
    exact=True
    for f in feeds:
        for x,y in zip(b.run(None,f),c.run(None,f)):
            np.testing.assert_allclose(x,y,atol=1e-5,rtol=1e-4);exact &= np.array_equal(x.view('u4'),y.view('u4'))
    result={'configuration':name,'threads':threads,'preparation_seconds':preparation,'before':before,'candidate':'整数形状循环变量代替重复 Shape/Gather；浮点树不变','f32_bits_exact':bool(exact),'rounds':results}
    (args.output/(name+'-screen.json')).write_text(json.dumps(result,ensure_ascii=False)+'\n')


def action(args,directory,name,threads):
    cfg=json.loads((directory/'benchmark_configuration.json').read_text())['configuration'];width=cfg['hidden']
    f={'z_t':np.zeros((1,width),'f4'),'rule_features':np.zeros((1,30),'f4'),'prefix_features':np.zeros((1,60),'f4'),
       'cancel_features':np.zeros((1,1,184),'f4'),'cancel_mask':np.ones((1,1),bool),
       'cancel_reduced':np.zeros((1,3*width//2),'f4'),'cancel_exists':np.ones((1,),bool),'price_fee_features':np.zeros((1,9999),'f4')}
    results=[]
    for fetch in (None,['family_logits'],['cancel_embeddings']):
        prefix=args.output/(name+'-action-'+('all' if fetch is None else fetch[0]))
        s=ort.InferenceSession(str(directory/'action_decoder.onnx'),sess_options=options(threads,prefix),providers=['CPUExecutionProvider'])
        run=ort.RunOptions();run.only_execute_path_to_fetches=True
        s.run(fetch,f,run_options=run)
        raw=Path(s.end_profiling());events=json.loads(raw.read_text())
        nodes=[{'name':e['name'],'op':e.get('args',{}).get('op_name'),'us':e['dur']} for e in events if e.get('cat')=='Node' and e['name'].endswith('_kernel_time')]
        results.append({'fetch':fetch,'nodes':nodes,'raw_profile':raw.name,'sha256':sha(raw)})
    (args.output/(name+'-action-fetch.json')).write_text(json.dumps({'configuration':name,'threads':threads,'ort_build':ort.get_build_info(),'calls':results},ensure_ascii=False)+'\n')


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--graphs',type=Path,required=True);p.add_argument('--packed',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--mode',choices=['profile','screen','action'],required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=True)
    frames=json.loads((a.packed/'frames.json').read_text());data=np.memmap(a.packed/'inputs.f32',dtype='<f4',mode='r')
    indices=sorted(set(np.linspace(0,len(frames)-1,128,dtype=int).tolist()+[74,380,944,int(np.argmax([max(f['depths']) for f in frames])),int(np.argmax([sum(f['depths']) for f in frames]))]))
    (a.output/'environment.json').write_text(json.dumps({'ort':ort.__version__,'build':ort.get_build_info(),'numpy':np.__version__,'torch_imported':'torch' in sys.modules,'inputs':{f:sha(a.packed/f) for f in ['inputs.f32','frames.json','metadata.json']},'source_sha256':sha(Path(__file__))},indent=2)+'\n')
    for name,threads in [('MAIN-1x256',4),('MAIN-30x1024',16)]:
        directory=a.graphs/name
        if a.mode=='action':action(a,directory,name,threads)
        elif a.mode=='profile':profile_state(a,directory,name,threads,frames,data,indices)
        else:screen(a,directory,name,threads,frames,data,indices)
        print(name,a.mode,'complete',flush=True)
if __name__=='__main__':main()
