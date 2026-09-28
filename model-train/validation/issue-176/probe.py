"""预先固定的同配置纯计算探针；不推进环境，也不推导支持集。"""
import argparse
import hashlib
import json
from pathlib import Path
import resource
import time
import torch
from model_train.graphs import CONFIGURATIONS, TwoGraphs, action_stages, reduce_objects
from model_train.graph_fixtures import fixture, paired_gtc_prefixes


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--graphs',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();args.output.mkdir(parents=True)
    records=[]
    torch.set_num_interop_threads(1)
    for name,threads in [('MAIN-1x256',4),('MAIN-30x1024',16)]:
        torch.set_num_threads(threads)
        config=next(c for c in CONFIGURATIONS if c.name==name)
        model=TwoGraphs(config).eval();weights=args.graphs/name/'initial_weights.pt'
        model.load_state_dict(torch.load(weights,weights_only=True,map_location='cpu'))
        stages=action_stages(model.action)
        for label,batch,first,count in [('cent',1,0,99),('milli',1,0,999),('interval',1,19,11),('batch_interval',2,19,11)]:
            _,_,rule,_,features,mask=fixture(batch,1,True)
            prefix=paired_gtc_prefixes(batch)[2]
            # 冻结有限数值切片：不把这里的零状态或指定区间冒充闭环性能样本。
            z=torch.zeros(batch,config.hidden)
            indices=torch.arange(first,first+count)
            fees=torch.zeros(batch,count)
            full_fees=torch.zeros(batch,999)
            def reference():
                encoded=model.action.object_encoder(features)
                reduced,exists=reduce_objects([(encoded,mask)])
                return model.action(z,rule,prefix,features,mask,reduced,exists,full_fees)[3].index_select(1,indices)
            def prepare():
                encoded=stages['action_objects'](features,mask)
                reduced,exists=reduce_objects([(encoded,mask)])
                return stages['action_family'](z,rule,reduced,exists)[1]
            with torch.no_grad():
                began=time.perf_counter_ns();context=prepare();preparation=time.perf_counter_ns()-began
                def candidate():return stages['action_price'](z,context,prefix,indices,fees)
                for round,order in [(1,[('reference',reference),('candidate',candidate)]),(2,[('candidate',candidate),('reference',reference)])]:
                    first_outputs={}
                    for variant,call in order:
                        began=time.perf_counter_ns();first_outputs[variant]=call();cold=time.perf_counter_ns()-began
                        for _ in range(8):call()
                        elapsed=[]
                        for _ in range(32):
                            began=time.perf_counter_ns();call();elapsed.append(time.perf_counter_ns()-began)
                        records.append(dict(config=name,threads=threads,case=label,batch=batch,indices=[first,first+count-1],variant=variant,round=round,
                            first_call_ns=cold,shared_preparation_ns=preparation,samples_ns=elapsed,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
                    torch.testing.assert_close(first_outputs["reference"],first_outputs["candidate"],rtol=1e-4,atol=1e-5)
        del stages,model
    (args.output/'samples.json').write_text(json.dumps(records,indent=2)+'\n')

if __name__=='__main__':main()
