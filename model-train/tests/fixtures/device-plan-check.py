"""只执行生产字节布局/片段排程的纯Python方法；不import torch或执行模型。"""
import ast
import copy
import json
from pathlib import Path
import sys
import time
import types

root=Path(sys.argv[1]);baseline=Path(sys.argv[2])
def method(path,owner,name):
    tree=ast.parse(path.read_text());cls=next(node for node in tree.body if isinstance(node,ast.ClassDef) and node.name==owner)
    return next(node for node in cls.body if isinstance(node,ast.FunctionDef) and node.name==name)
def load(node):
    namespace={'time':time,'json':json,'sys':sys}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node],type_ignores=[])),'production-pure-plan','exec'),namespace)
    return namespace[node.name]
chunks=load(method(root/'input_cache.py','GroupInputCache','_host_chunks'))
for capacity in (4,12,64,4096):
    segments=[(column,'raw',0,19,7,item_bytes) for column,item_bytes in enumerate((4,4,4,1))]
    seen={column:[] for column in range(4)}
    for used,pieces in chunks(types.SimpleNamespace(streaming_capacity=capacity),segments):
        assert 0<used<=capacity
        for column,raw,source,count,destination,item_bytes,offset in pieces:
            assert offset%item_bytes==0 and offset+count*item_bytes<=used
            seen[column].extend((source+i*item_bytes,destination+i) for i in range(count))
    for column,item_bytes in enumerate((4,4,4,1)):
        assert seen[column]==[(i*item_bytes,7+i) for i in range(19)]
run_epoch=load(method(root/'learning_engine.py','LearningEngine','run_device_epoch'))
forward=method(root/'learning_engine.py','LearningEngine','_device_forward')
# Execute only the production frame-selection/prefetch prefix. The trunk is an
# explicit nonnumeric stub; no Tensor, model, loss or backward can be reached.
frame_loop=next(node for node in forward.body if isinstance(node,ast.For))
heads=next(index for index,node in enumerate(frame_loop.body) if isinstance(node,ast.Assign)
           and isinstance(node.value,ast.Call) and isinstance(node.value.func,ast.Attribute)
           and node.value.func.attr=='_teacher_heads')
prefix=copy.deepcopy(forward)
prefix.name='frame_plan'
prefix.body=copy.deepcopy(forward.body[:forward.body.index(frame_loop)])
loop=copy.deepcopy(frame_loop);loop.body=loop.body[:heads]
prefix.body.append(loop)
frame_plan=load(prefix)
def schedule_check(schedule,expected):
    state=types.SimpleNamespace(config={'tbptt_steps':32,'money_scale_pusd':1,'epsilon':0.2},service=types.SimpleNamespace(device=types.SimpleNamespace(type='cuda'),performance={}),counts={},epoch=0,sequence_observer=None,hidden={})
    state.cache=types.SimpleNamespace(schedule=schedule,frame=lambda account,index:{'account':account,'call':index+1})
    state.cache.frame_for_teacher=lambda frame:frame
    events=[]
    state.cache.prefetch_block=lambda frames:events.append(('prefetch',[(frame['account'],frame['call']) for frame in frames]))
    state.zeros=lambda:None
    expected_hidden={}
    def trunk(frame,before):
        account,call=frame['account'],frame['call']
        assert before==expected_hidden.get(account)
        following=(account,call)
        expected_hidden[account]=following
        events.append(('trunk',following))
        return None,None,None,following
    state._teacher_trunk=trunk
    state.check_cancel=lambda:None
    state._prepare_device_labels=lambda:None
    state.start_epoch=lambda:None
    state.end_epoch=lambda:{'planned':len(schedule)}
    observed=[]
    def segment(start,stop,next_frames):
        assert next_frames is None
        account=schedule[start][0];observed.append((account,stop-start))
        frame_plan(state,start,stop)
        state.counts[account]=state.counts.get(account,0)+stop-start
    state._device_segment=segment
    assert run_epoch(state)=={'planned':len(schedule)}
    assert observed==expected
    expected_events=[]
    for position,(account,index) in enumerate(schedule):
        expected_events.append(('trunk',(account,index+1)))
        if position+1<len(schedule):
            next_account,next_index=schedule[position+1]
            expected_events.append(('prefetch',[(next_account,next_index+1)]))
    assert events==expected_events
    assert state.hidden==expected_hidden
fixed=[('L',i) for i in range(65)]+[(case,0) for case in ('F-N','F-P','F-R','F-S')]
schedule_check(fixed,[('L',32),('L',32),('L',1),('F-N',1),('F-P',1),('F-R',1),('F-S',1)])
interleaved=[('A',i) for i in range(5)]+[('B',i) for i in range(2)]+[('A',i) for i in range(5,36)]+[('B',i) for i in range(2,39)]
schedule_check(interleaved,[('A',5),('B',2),('A',27),('A',4),('B',30),('B',7)])
def assigned(node,name):
    return [value for value in ast.walk(node) if isinstance(value,ast.Assign) and any(isinstance(target,ast.Name) and target.id==name for target in value.targets)]
old_critic=assigned(method(baseline/'learning_engine.py','LearningEngine','step'),'critic')[0]
new_critic=assigned(forward,'critic')[0]
assert ast.dump(old_critic,include_attributes=False)==ast.dump(new_critic,include_attributes=False)
assert ast.dump(method(baseline/'model_worker.py','Service','choose'),include_attributes=False)==ast.dump(method(root/'model_worker.py','Service','choose'),include_attributes=False)
trunk=method(root/'learning_engine.py','LearningEngine','_teacher_trunk')
gru_calls=[node for node in ast.walk(trunk) if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute) and node.func.attr=='gru']
assert len(gru_calls)==1
assert ast.dump(gru_calls[0].args[0],include_attributes=False)==ast.dump(ast.parse('features.reshape(1,1,-1)',mode='eval').body,include_attributes=False)
assert isinstance(gru_calls[0].args[1],ast.Name) and gru_calls[0].args[1].id=='hidden'
assert len([node for node in ast.walk(frame_loop) if isinstance(node,ast.Call)
            and isinstance(node.func,ast.Attribute) and node.func.attr=='_teacher_trunk'])==1
assert 'torch' not in sys.modules
print('production chunk capacities 4/12/64/4096, L65+four tails/interleaved TBPTT schedule, actual frame-selection/prefetch AST with nonnumeric trunk stub, per-account hidden order, exact Critic expression, unchanged CPU choose and one [1,1,W] GRU call site PASS; numeric/GPU runtime untested; torch imports 0')
