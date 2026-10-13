"""同页完整公开组的私有设备WHILE；只提交原来源位置及SIM输入收据。"""
from dataclasses import dataclass,replace
import torch
from queues import ExactInt,Numbers
from public_source import public_group_selection,group_committed


def _owned_exact(value,width):
    value=value.with_capacity(width)
    return ExactInt(value.words.clone(),value.sign.clone(),value.fault.clone())


def _owned_frontier(frontier,width):
    if frontier is None:return None
    receipt=frontier.public_receipt
    return replace(frontier,next_sequence=_owned_exact(frontier.next_sequence,width),
        public_receipt=replace(receipt,positions=receipt.positions.clone(),
            input_sequence=_owned_exact(receipt.input_sequence,width)))


def _copy_exact(target,value,enabled=None):
    for name in ('words','sign','fault'):
        old=getattr(target,name);new=getattr(value,name)
        old.copy_(new if enabled is None else torch.where(enabled,new,old))


def _copy_frontier(target,value,enabled=None):
    if target is None:return
    _copy_exact(target.next_sequence,value.next_sequence,enabled)
    old=target.public_receipt;new=value.public_receipt
    old.positions.copy_(new.positions if enabled is None else torch.where(enabled,new.positions,old.positions))
    _copy_exact(old.input_sequence,new.input_sequence,enabled)


@dataclass(frozen=True)
class PublicGroupResult:
    position: torch.Tensor
    frontier: object
    fault: torch.Tensor
    processed: int


class _CapturedPublicGroups:
    def __init__(self,source,frontier,choice,hidden,eof,bound,at,enabled,*,before,width):
        # 仅同页只读来源被图借用；下一页安装后布局键改变并释放旧图。
        self.source=source;self.before=before;self.width=width
        self.input_position=source.position.clone();self.position=source.position.clone()
        self.input_frontier=_owned_frontier(frontier,width)
        self.frontier=_owned_frontier(frontier,width)
        self.choice=replace(choice,at=choice.at.clone(),present=choice.present.clone(),fault=choice.fault.clone())
        self.hidden=replace(hidden,at=hidden.at.clone(),present=hidden.present.clone(),fault=hidden.fault.clone())
        self.eof=eof.clone();self.bound=None if bound is None else bound.clone()
        self.at=at.clone();self.enabled=enabled.clone()
        self.ready=torch.zeros((),dtype=torch.bool,device=at.device)
        self.processed=torch.zeros((),dtype=torch.int64,device=at.device)
        self.required_words=torch.zeros_like(self.processed)
        self.fits=torch.ones_like(self.ready)
        self.fault=torch.zeros_like(source.fault)

        def selection():
            group=source.next(self.position)
            due,take=public_group_selection(group,self.choice,self.hidden,self.eof,self.bound,
                self.at,self.enabled,before=before)
            ready=take.any()&~(due&~group.present).any()&~(self.choice.fault|group.fault).any()
            return group,take,ready

        def body():
            group,take,_=selection()
            if self.frontier is None:
                candidate=None;fault=torch.zeros_like(self.fault)
                fits=torch.ones_like(self.fits);required=torch.zeros_like(self.required_words)
            else:
                exact=Numbers.device_exact_batch(lambda:self.frontier.public_group(source.prepared,
                    group.position,take),word_capacity=width,device=at.device)
                candidate,fault=exact.value;fits=exact.fits;required=exact.required_words
            self.required_words.copy_(torch.maximum(self.required_words,required))
            self.fits.logical_and_(fits)
            # 原宿主在任一根fault时整组不安装；物理不足也不安装当前组。
            committed=fits&~fault.any()
            _copy_frontier(self.frontier,candidate,committed)
            self.position.add_(group_committed(group,take&committed).to(torch.int64))
            self.processed.add_(committed.to(torch.int64))
            self.fault.bitwise_or_(torch.where(fits,fault,0))
            _,_,next_ready=selection()
            self.ready.copy_(next_ready&self.fits&~self.fault.any())

        def reset():
            self.position.copy_(self.input_position)
            _copy_frontier(self.frontier,self.input_frontier)
            self.processed.zero_();self.required_words.zero_();self.fits.fill_(True);self.fault.zero_()
            _,_,ready=selection();self.ready.copy_(ready)

        stream=torch.cuda.Stream(device=at.device)
        stream.wait_stream(torch.cuda.current_stream(at.device))
        with torch.cuda.stream(stream):
            for _ in range(3):reset();body()
        torch.cuda.current_stream(at.device).wait_stream(stream)
        reset()
        self.body=torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.body):body()
        self.entry=torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.entry):reset()
        self.loop=Numbers.public_extension().DeviceConditionalLoop(
            self.entry.raw_cuda_graph(),self.body.raw_cuda_graph(),self.ready)

    def replay(self,position,frontier,choice,hidden,eof,bound,at,enabled):
        self.input_position.copy_(position)
        if frontier is not None:
            value=replace(frontier,next_sequence=frontier.next_sequence.with_capacity(self.width),
                public_receipt=replace(frontier.public_receipt,
                    input_sequence=frontier.public_receipt.input_sequence.with_capacity(self.width)))
            _copy_frontier(self.input_frontier,value)
        for target,value in ((self.choice,choice),(self.hidden,hidden)):
            for name in ('at','present','fault'):getattr(target,name).copy_(getattr(value,name))
        self.eof.copy_(eof)
        if bound is not None:self.bound.copy_(bound)
        self.at.copy_(at);self.enabled.copy_(enabled)
        self.loop.replay()
        # 输出必须在下次replay/冷布局释放前独立拥有；并非新权威游标。
        return (self.position.clone(),_owned_frontier(self.frontier,self.width),self.fault.clone(),
            self.processed.clone(),self.required_words.clone(),self.fits.clone())


class PublicGroupDeviceLoop:
    """一个时钟/流只保一个可重建布局；增长从未消费组续接，不重放成功前缀。"""
    def __init__(self):
        self.width=4;self.key=None;self.captured=None

    def run(self,source,frontier,choice,hidden,eof,bound,at,enabled,*,before,progress=None):
        position=source.position;processed=0
        while True:
            width=self.width if frontier is None else max(self.width,frontier.next_sequence.words.size(1),
                frontier.public_receipt.input_sequence.words.size(1))
            key=(id(source),source.page_begin,source.page_end,source.page_at.data_ptr(),source.fault.data_ptr(),
                at.numel(),at.device,torch.cuda.current_stream(at.device).cuda_stream,before,
                frontier is not None,bound is not None,width)
            if key!=self.key:
                self.captured=None;self.key=None
                if progress:progress('public_loop_capture_begin',begin=source.page_begin,end=source.page_end,words=width)
                self.captured=_CapturedPublicGroups(source,frontier,choice,hidden,eof,bound,at,enabled,before=before,width=width)
                self.key=key
                if progress:progress('public_loop_capture_end',begin=source.page_begin,end=source.page_end,words=width)
            position,frontier,fault,count,required,fits=self.captured.replay(
                position,frontier,choice,hidden,eof,bound,at,enabled)
            processed+=int(count.cpu())
            if bool(fits.cpu()):return PublicGroupResult(position,frontier,fault,processed)
            request=int(required.cpu());self.width=1<<(max(4,request)-1).bit_length()
            if progress:progress('public_loop_resource',words=self.width,processed=processed)
