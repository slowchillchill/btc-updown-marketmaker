"""同一归档权重/八图的固定技术切片三端对照，不推进经济系统。"""
import argparse
import json
from pathlib import Path
import subprocess
import time
import torch
import onnxruntime as ort
from .graphs import CONFIGURATIONS, TwoGraphs, STATE_INPUTS, ACTION_INPUTS, object_blocks, reduce_objects, score_all
from .graph_fixtures import fixture, paired_gtc_prefixes
from .validate_graphs import call_ort, session, sha, identities_for


def suite(config):
    cases = []
    def add(name, mode, batch, pairs=1, removed=True, mixed=False, steps=1, slots=None, active=()):
        values = list(fixture(batch, pairs, removed, mixed=mixed))
        if slots is not None:
            values[4], values[5] = values[4][:, :slots], values[5][:, :slots]
        n = values[4].shape[1]
        lengths = [256] * (n // 256) + ([n % 256] if n % 256 else [])
        c = dict(name=name, mode=mode, batch=batch, steps=steps, slots=n, empty=pairs == 0,
                 splits=[lengths] + ([[128,129]] if n == 257 else []), prefixes=[], active=list(active),
                 identities=identities_for(pairs, removed))
        for key, value in zip(('fast','slow','rule','prefix','objects','mask'), values):
            c[key] = value.flatten().tolist()
        cases.append(c)
        return c
    for b in (1,2,3):
        add(f'recurrent-B{b}', 'recurrent', b, steps=12)
    for pairs, removed, mixed in ((0,False,False),(1,True,False),(85,False,False),(86,True,False),(86,False,False),(86,True,True)):
        add(f'objects-P{pairs}-removed{int(removed)}-mixed{int(mixed)}','objects',2 if mixed else 1,pairs,removed,mixed)
    for k in (1,2,128,129,255,256):
        add(f'dynamic-K{k}','dynamic',2,85,False,slots=k)
    c = add('gtc-prefix','prefix',2,0,False)
    c['prefixes'] = [v.flatten().tolist() for v in paired_gtc_prefixes(2)]
    add('active-0-2','active',3,active=(0,2))
    add('active-empty','active',3,active=())
    return dict(layers=config.layers,hidden=config.hidden,cases=cases)


def trace(state, action, config, data):
    for c in data['cases']:
        b, n, d = c['batch'], c['slots'], config.hidden // 2
        f,s,r,p,x,m = [torch.tensor(c[k],dtype=torch.bool if k=='mask' else torch.float32).reshape(shape)
            for k,shape in zip(('fast','slow','rule','prefix','objects','mask'),((b,836),(b,412),(b,30),(b,60),(b,n,129),(b,n)))]
        h = torch.zeros(config.layers,b,config.hidden)
        zero, absent = torch.zeros(b,3*d), torch.zeros(b,dtype=torch.bool)
        for step in range(c['steps']):
            z,h = state(f,s,h)
            yield f"{c['name']}/state{step}",(z,h)
            if c['mode'] == 'objects':
                for split,lengths in enumerate(c['splits']):
                    base = f"{c['name']}/split{split}"
                    blocks = []
                    for part,(bx,bm,length) in enumerate(object_blocks(x,m,lengths,empty=c['empty'])):
                        embedding = action(z,r,p,bx,bm,zero,absent)[6][:,:length]
                        blocks.append((embedding,bm[:,:length]))
                        yield f'{base}/embedding{part}',(embedding,)
                    reduced,exists = reduce_objects(blocks)
                    yield f'{base}/reduced',(reduced,exists.float())
                    yield f'{base}/family',action(z,r,p,x[:,:1],m[:,:1],reduced,exists)
                    for kind in range(3):
                        selected = m & (x[:,:,kind] == 1)
                        prefix = torch.zeros_like(p); prefix[:,[kind+3,6,13]]=1
                        scores = score_all(action,z,r,prefix,x,selected,lengths,reduced,exists,empty=c['empty'])
                        for row,values in enumerate(scores):
                            label = f'{base}/family{kind}/row{row}'
                            yield label,(values,)
                            index = int(selected[row].nonzero().flatten()[values.argmax()]) if values.numel() else -1
                            yield label+'/selected',(torch.tensor([index],dtype=torch.float32),)
            else:
                prefixes = [torch.tensor(v).reshape(b,60) for v in c['prefixes']] or [p]
                for i,prefix in enumerate(prefixes):
                    indices = c['active'] if c['mode']=='active' else list(range(b))
                    base = f"{c['name']}/step{step}/prefix{i}"
                    yield base+'/rows',(torch.tensor(indices,dtype=torch.float32),)
                    if indices:
                        yield base+'/action',action(z[indices],r[indices],prefix[indices],x[indices],m[indices],zero[indices],absent[indices])


def compare(ref, actual, label, stats, exact=False):
    assert len(ref)==len(actual), label
    for output,(p,a) in enumerate(zip(ref,actual)):
        p,a = p.flatten(),torch.as_tensor(a,dtype=torch.float32).flatten()
        assert p.shape==a.shape,(label,output,p.shape,a.shape)
        error=(a-p).abs()
        good=torch.isfinite(p)&torch.isfinite(a)&(error <= (0 if exact else 1e-5+1e-4*p.abs()))
        if not bool(good.all()):
            i=int((~good).nonzero()[0]);raise AssertionError(dict(label=label,output=output,flat_index=i,reference=float(p[i]),actual=float(a[i])))
        if error.numel(): stats['max_absolute_error']=max(stats['max_absolute_error'],float(error.max()))
        stats['comparisons']+=1


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True);parser.add_argument('--threads',type=int,choices=(1,4),required=True)
    parser.add_argument('--graphs',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--binary',type=Path,default=Path('rust-ort-validation/target/debug/model-graph-ort-validation'))
    args=parser.parse_args();config=next(c for c in CONFIGURATIONS if c.name==args.config)
    args.output.mkdir(parents=True,exist_ok=True)
    data=suite(config);input_path=args.output/'inputs.json';input_path.write_text(json.dumps(data))
    native=next((Path(ort.__file__).parent/'capi').glob('libonnxruntime.so.*')).resolve()
    raw=args.output/'rust.jsonl';err=args.output/'rust.stderr'
    started=time.monotonic()
    with raw.open('w') as stdout,err.open('w') as stderr:
        subprocess.run([str(args.binary.resolve()),str(native),str(args.graphs.resolve()),str(args.threads),str(input_path.resolve())],stdout=stdout,stderr=stderr,check=True)
    torch.set_num_threads(args.threads)
    model=TwoGraphs(config).eval();model.load_state_dict(torch.load(args.graphs/'initial_weights.pt',weights_only=True,map_location='cpu'))
    state=call_ort(session(args.graphs/'state_encoder.onnx',args.threads),STATE_INPUTS)
    action=call_ort(session(args.graphs/'action_decoder.onnx',args.threads),ACTION_INPUTS)
    stats=dict(config=config.name,threads=args.threads,comparisons=0,max_absolute_error=0.,cases=len(data['cases']))
    split_records={}
    with raw.open() as stream,torch.no_grad():
        metadata=json.loads(next(stream))
        assert metadata['library']==str(native)
        for reference,python in zip(trace(model.state,model.action,config,data),trace(state,action,config,data),strict=True):
            label,values=reference;plabel,pvalues=python;rust=json.loads(next(stream))
            assert label==plabel==rust['label'],(label,plabel,rust['label'])
            case=next(c for c in data['cases'] if label.startswith(c['name']+'/'))
            context=dict(config=config.name,threads=args.threads,path=label,B=case['batch'],slots=case['slots'])
            if '/family' in label and '/row' in label:
                kind=int(label.split('/family')[-1].split('/')[0]);row=int(label.split('/row')[-1].split('/')[0])
                objects=torch.tensor(case['objects']).reshape(case['batch'],case['slots'],129)
                mask=torch.tensor(case['mask']).reshape(case['batch'],case['slots'])
                indices=(mask[row] & (objects[row,:,kind]==1)).nonzero().flatten().tolist()
                context.update(row=row,family=kind+3,objects=[(i,case['identities'][i]) for i in indices])
            exact=label.endswith('/selected') or label.endswith('/rows')
            compare(values,pvalues,{**context,'runtime':'PythonORT'},stats,exact)
            compare(values,rust['values'],{**context,'runtime':'RustORT'},stats,exact)
            if '/split' in label and ('/row' in label or label.endswith('/reduced')):
                canonical=label.replace('/split1/','/split0/')
                current=(values,pvalues,tuple(torch.tensor(v) for v in rust['values']))
                if '/split0/' in label: split_records[canonical]=current
                else:
                    for name,old,new in zip(('PyTorch','PythonORT','RustORT'),split_records[canonical],current):
                        compare(old,new,{**context,'runtime':name,'partition':'256+1 vs128+129'},stats,exact)
        assert stream.read()==''
    stats.update(elapsed_seconds=time.monotonic()-started,rust=metadata,native_sha256=sha(native),
        python_ort_version=ort.__version__,python_ort_extension_sha256=sha(next(native.parent.glob('onnxruntime_pybind11_state*.so'))),
        inputs_sha256=sha(input_path),rust_output_sha256=sha(raw),
        graphs={p.name:sha(p) for p in args.graphs.iterdir() if p.suffix in ('.onnx','.data','.pt')})
    (args.output/'result.json').write_text(json.dumps(stats,indent=2)+'\n')
    print(json.dumps({k:v for k,v in stats.items() if k not in ('rust','graphs')}))

if __name__=='__main__': main()
