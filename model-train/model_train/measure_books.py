"""#166 新增盘口前端的有限CPU延迟与进程内存观测，不设性能完成阈值。"""
import argparse
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import time
import torch
from .book import BookEncoder
from .graph_fixtures import book_fixture


def rss():
    return int(Path('/proc/self/statm').read_text().split()[1]) * os.sysconf('SC_PAGE_SIZE')


def case(batch, depth):
    torch.set_num_threads(1);torch.set_num_interop_threads(1);torch.manual_seed(741003)
    model=BookEncoder().eval()
    x,mask=book_fixture(batch,depth)
    if batch>1:
        x[0,:,7:]=0;mask[0,:,7:]=False
    before=rss();peak_before=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
    durations=[]
    with torch.no_grad():
        for _ in range(3):model(x,mask)
        for _ in range(10):
            start=time.perf_counter_ns();out=model(x,mask);durations.append((time.perf_counter_ns()-start)/1e6)
    after=rss();peak_after=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
    return dict(batch=batch,depth=depth,valid_per_side=mask.sum(2).tolist(),median_ms=statistics.median(durations),
                min_ms=min(durations),max_ms=max(durations),rss_before_bytes=before,rss_after_bytes=after,
                peak_increment_bytes=peak_after-peak_before,levels_bytes=x.numel()*x.element_size(),
                mask_bytes=mask.numel()*mask.element_size(),encoded_bytes=batch*6*depth*32*4,summary_bytes=out.numel()*out.element_size())


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--case',nargs=2,type=int);p.add_argument('--output',type=Path)
    a=p.parse_args()
    if a.case:print(json.dumps(case(*a.case)));return
    result=dict(scope='每组新进程、CPU一线程、3次预热和10次计时；仅新增BookEncoder，非完整模型、目标机或实盘容量。RSS包含解释器/运行库/分配器；有限高水位增量不是严格张量峰值或长期无泄漏证明。',torch=torch.__version__,cases=[])
    for batch,depth in ((1,7),(1,257),(1,1001),(1,4096),(3,257),(3,4096)):
        result['cases'].append(json.loads(subprocess.check_output([sys.executable,'-m',__name__ if __name__!='__main__' else 'model_train.measure_books','--case',str(batch),str(depth)],text=True)))
    a.output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(result,ensure_ascii=False))

if __name__=='__main__':main()
