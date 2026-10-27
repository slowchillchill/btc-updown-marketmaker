"""生产BufferObserver的stdlib接口检查；替身仅检schema/边界，不证明CUDA寿命。"""
import ast
import json
from pathlib import Path
import sys
import tempfile
import types

source=Path(sys.argv[1]);tree=ast.parse(source.read_text())
node=next(node for node in tree.body if isinstance(node,ast.ClassDef) and node.name=='BufferObserver')
namespace={'json':json,'Path':Path}
exec(compile(ast.fix_missing_locations(ast.Module(body=[node],type_ignores=[])),str(source),'exec'),namespace)
class Storage:
    def __init__(self,pointer,size):self.pointer,self.size=pointer,size
    def data_ptr(self):return self.pointer
    def nbytes(self):return self.size
class Tensor:
    def __init__(self,pointer,device='cuda:0',pinned=False,size=64):
        self.device=Device(device);self.shape=(size,);self.dtype='torch.uint8';self._base=None;self.requires_grad=False
        self.pointer,self.pinned,self.size=pointer,pinned,size
    def untyped_storage(self):return Storage(self.pointer,self.size)
    def numel(self):return self.size
    def element_size(self):return 1
    def storage_offset(self):return 0
    def is_inference(self):return False
    def is_pinned(self):return self.pinned
class Device:
    def __init__(self,name):self.name=name;self.type=name.split(':')[0]
    def __str__(self):return self.name
class Event:
    def query(self):return True
class Writer:
    def __init__(self,root):self.directory=root;self.rows=[]
    def write_metadata(self,name,row):
        json.dumps(row,allow_nan=False)
        self.rows.append((name,row))
with tempfile.TemporaryDirectory() as root:
    writer=Writer(Path(root));observer=namespace['BufferObserver'](types.SimpleNamespace(Tensor=Tensor),writer)
    hosts=[Tensor(1000+i,'cpu',True) for i in range(2)];raws=[Tensor(2000+i) for i in range(2)]
    copy=types.SimpleNamespace(device=Device('cuda:0'),cuda_stream=11)
    compute=types.SimpleNamespace(device=Device('cuda:0'),cuda_stream=22)
    observer({'event':'allocate','slots':[{'slot':i,'host':hosts[i],'raw':raws[i]} for i in range(2)],'copy_stream':copy})
    ready,owned=Event(),Event();columns=[Tensor(3000+i) for i in range(4)]
    for generation,prefetch in [(1,False),(2,True)]:
        if generation>1:
            observer({'event':'host_reuse_ready','slot':0,'generation':generation-1,'ready_event':ready,'completed':True})
            observer({'event':'reuse_wait_submitted','slot':0,'previous_generation':generation-1,'owned_event':owned,'copy_stream':copy})
        observer({'event':'h2d_submitted','slot':0,'generation':generation,'bytes':32,'host':hosts[0],'raw':raws[0],'ready_event':ready,'copy_stream':copy,'sample_keys':[('L',generation)],'prefetch':prefetch})
        observer({'event':'owned_copy_submitted','slot':0,'generation':generation,'bytes':32,'raw':raws[0],'owned_columns':columns,'owned_event':owned,'compute_stream':compute,'sample_keys':[('L',generation)],'prefetch':prefetch})
    live=0; groups={}
    for index,key in enumerate(['L','F-N','F-P','F-R','F-S']):
        values=[Tensor(4000+index*10+j) for j in range(4)];groups[key]=values;size=256;live+=size
        observer({'event':'graph_inputs_retained','account':key,'owned_columns':values,'bytes':size,'live_bytes':live})
    for key,values in groups.items():
        live-=256
        observer({'event':'graph_inputs_released','account':key,'owned_columns':[values],'bytes':256,'live_bytes':live})
    summary=observer.summary();assert summary['peak_live_accounts']==5 and summary['remaining_explicit_graph_input_bytes']==0
    assert summary['counts']['prefetch_h2d_submitted']==summary['counts']['prefetch_owned_copy_submitted']==1
    alias=namespace['BufferObserver'](types.SimpleNamespace(Tensor=Tensor),Writer(Path(root)))
    try:alias({'event':'allocate','slots':[{'slot':i,'host':hosts[i],'raw':raws[0]} for i in range(2)],'copy_stream':copy})
    except RuntimeError:pass
    else:raise AssertionError('alias raw base accepted')
assert 'torch' not in sys.modules
print('BufferObserver schema, prefetch generation, nested graph release and alias rejection PASS; CUDA behavior untested; torch imports 0')
