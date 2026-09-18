"""本机CPU补齐开销描述；旧逻辑只作计时，不作为有效选择路径。"""
import argparse
import json
from pathlib import Path
import platform
import statistics
import time
import torch
from .graphs import CONFIGURATIONS, TwoGraphs, encode_all, score_all, reduce_objects
from .graph_fixtures import fixture
from .validate_graphs import session, call_ort, ACTION_INPUTS, sha


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',required=True)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--threads',type=int,choices=(1,4),required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    torch.set_num_threads(args.threads);torch.set_num_interop_threads(1)
    config=next(c for c in CONFIGURATIONS if c.name==args.config)
    directory=args.source/config.name
    model=TwoGraphs(config).eval()
    weights=directory/'initial_weights.pt'
    model.load_state_dict(torch.load(weights,weights_only=True))
    ort_call=call_ort(session(directory/'action_decoder.onnx',args.threads),ACTION_INPUTS)
    results=[]
    with torch.no_grad():
        for pairs,removed in ((0,False),(1,True),(85,False),(86,True),(86,False)):
            fast,slow,rule,prefix,objects,mask=fixture(1,pairs,removed)
            z,_=model.state(fast,slow,torch.zeros(config.layers,1,config.hidden))
            n=objects.shape[1]; lengths=[256]*(n//256)+([n%256] if n%256 else [])
            cp=torch.zeros_like(prefix);cp[:,3]=cp[:,6]=cp[:,13]=1
            selected=mask & (objects[:,:,0]==1)
            for runtime,call in (('PyTorch',model.action),('ORT',ort_call)):
                def before():
                    reduced=z.new_zeros((1,3*config.hidden//2));exists=torch.zeros(1,dtype=torch.bool)
                    blocks=[];offset=0
                    for k in lengths:
                        x,m=objects[:,offset:offset+k],mask[:,offset:offset+k]
                        blocks.append((call(z,rule,prefix,x,m,reduced,exists)[6],m));offset+=k
                    reduced,exists=reduce_objects(blocks);offset=0
                    scores=[]
                    for k in lengths:
                        x,m=objects[:,offset:offset+k],selected[:,offset:offset+k]
                        scores.append(call(z,rule,cp,x,m,reduced,exists)[7]);offset+=k
                    return scores
                def after():
                    reduced,exists=encode_all(call,z,rule,objects,mask,lengths,empty=pairs==0)
                    return score_all(call,z,rule,cp,objects,selected,lengths,reduced,exists,empty=pairs==0)
                record={'runtime':runtime,'N':int(mask.sum()),'logical_blocks':lengths,
                        'physical_blocks':[1] if pairs==0 else [256]*len(lengths)}
                for name,fn in (('before',before),('after',after)):
                    fn()  # 不计时预热；同模型/同会话，不是长期稳定性观察。
                    times=[]
                    for _ in range(10):
                        start=time.perf_counter();fn();times.append((time.perf_counter()-start)*1000)
                    record[name+'_ms']={'median':statistics.median(times),'max':max(times),'samples':times}
                results.append(record)
    output={'configuration':config.name,'threads':args.threads,'weights_sha256':sha(weights),
            'machine':platform.machine(),'processor':platform.processor(),
            'scope':'本机CPU B=1固定技术切片，两遍含宿主归约，10次描述性计时；非目标CPU或GPU验收',
            'results':results}
    args.output.write_text(json.dumps(output,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
