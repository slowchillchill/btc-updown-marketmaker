#!/usr/bin/env python3
"""固定盘口编码的同设备原坐标/梯度、图重放及实际padding成本；有限组件验证。"""
import argparse
import gc
import json
import subprocess
import time
from pathlib import Path
import torch
from model_train.book import BookEncoder, BOOK_EMBEDDING, reduce_levels
from model_train.components import AlignedLinear


class OriginalDense(BookEncoder):
    def __init__(self):
        super().__init__()
        for name in ('pm','binance'):
            setattr(self,name,torch.nn.Sequential(AlignedLinear(9,32),torch.nn.SiLU(),AlignedLinear(32,32)))

    def forward(self, levels, mask):
        values=torch.cat((self.pm(levels[:,:4]),self.binance(levels[:,4:])),dim=1)
        return reduce_levels(values,mask).flatten(1)


class PreviousSparse(BookEncoder):
    @staticmethod
    def encode_valid(encoder,levels,mask):
        flat=levels.flatten(0,2)
        indices=mask.flatten().nonzero().squeeze(1)
        encoded=encoder(flat.index_select(0,indices))
        return encoded.new_zeros((flat.size(0),BOOK_EMBEDDING)).index_copy(0,indices,encoded).reshape(*levels.shape[:3],BOOK_EMBEDDING)


def compare(model, actual, inputs, mask, upstream, reference):
    expected_inputs=inputs.detach().clone().requires_grad_()
    reference.zero_grad(set_to_none=True)
    expected=reference(expected_inputs,mask)
    (expected*upstream).sum().backward()
    torch.testing.assert_close(actual,expected,atol=0,rtol=0)
    torch.testing.assert_close(inputs.grad,expected_inputs.grad,atol=1e-5,rtol=1e-4)
    assert not inputs.grad[~mask].count_nonzero().cpu().item()
    for parameter,original in zip(model.parameters(),reference.parameters()):
        torch.testing.assert_close(parameter.grad,original.grad,atol=1e-5,rtol=1e-4)


def cost(model, levels, mask):
    with torch.no_grad():
        model(levels,mask)
        torch.cuda.synchronize()
        baseline=torch.cuda.memory_allocated()
        torch.cuda.reset_peak_memory_stats()
        started=time.perf_counter()
        for _ in range(5): model(levels,mask)
        torch.cuda.synchronize()
    return {'mean_forward_wall_seconds':(time.perf_counter()-started)/5,
        'resident_bytes':baseline,'component_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'iterations':5,'scope':'eager forward incl launch and previous nonzero synchronization; validation models/inputs resident'}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    torch.manual_seed(168)
    model=BookEncoder().cuda();reference=OriginalDense().cuda();previous=PreviousSparse().cuda()
    reference.load_state_dict(model.state_dict());previous.load_state_dict(model.state_dict())
    reports=[]
    for depth in (1,257,4096):
        levels=torch.randn(2,6,depth,9,device='cuda')
        mask=torch.rand(2,6,depth,device='cuda')>.3
        mask[0,:4]=False;mask[1,4:]=False
        inputs=levels.clone().requires_grad_();upstream=torch.randn(2,576,device='cuda')/depth
        model.zero_grad(set_to_none=True)
        actual=model(inputs,mask);(actual*upstream).sum().backward()
        compare(model,actual,inputs,mask,upstream,reference)
        reports.append({'depth':depth,'roots':2,'physical_rows':mask.numel(),
            'positive_rows':int(mask.sum().cpu()),'padding_fraction':float((~mask).float().mean().cpu()),
            'fixed':cost(model,levels,mask),'previous_sparse':cost(previous,levels,mask)})
        print(json.dumps({'stage':'GPU_book_values_input_parameter_gradients_and_cost','depth':depth}),flush=True)
        del inputs,actual,levels,mask,upstream
        gc.collect()
    # 同图捕获完整前向/反向；更换实际输入和mask后重放四种空侧边界。
    depth=257;levels=torch.randn(2,6,depth,9,device='cuda',requires_grad=True)
    mask=torch.ones(2,6,depth,dtype=torch.bool,device='cuda')
    upstream=torch.randn(2,576,device='cuda')/depth
    stream=torch.cuda.Stream();stream.wait_stream(torch.cuda.current_stream())
    with torch.cuda.stream(stream):
        for _ in range(3):
            model.zero_grad(set_to_none=True);levels.grad=None
            (model(levels,mask)*upstream).sum().backward()
    torch.cuda.current_stream().wait_stream(stream)
    graph=torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        model.zero_grad(set_to_none=False);levels.grad.zero_()
        actual=model(levels,mask);(actual*upstream).sum().backward()
    cases=[]
    for empty in ('none','pm','binance','all'):
        with torch.no_grad(): levels.copy_(torch.randn_like(levels))
        mask.copy_(torch.rand(mask.shape,device='cuda')>.3)
        if empty in ('pm','all'): mask[:,:4]=False
        if empty in ('binance','all'): mask[:,4:]=False
        graph.replay()
        compare(model,actual,levels,mask,upstream,reference)
        cases.append(empty)
    report={'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        'device':torch.cuda.get_device_name(),'passed':['same_device_original_dense_values_input_and_parameter_gradients',
            'invalid_input_zero_gradient','same_graph_forward_backward_distinct_masks'],
        'capture_empty_sides':cases,'actual_padding_cost':reports,
        'scope':'shared BookEncoder component; not full learner working set or training throughput'}
    args.output.write_text(json.dumps(report,indent=2));print(json.dumps(report),flush=True)


if __name__=='__main__': main()
