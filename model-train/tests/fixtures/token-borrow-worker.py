"""#368 stdlib替身：复用NoTrade完整响应形状；不导入Torch/模型。"""
import hashlib,json,pathlib,sys
control=json.loads(pathlib.Path(sys.argv[2]).read_text())
root=pathlib.Path(control['out'])
count=0
with (root/'lines.jsonl').open('x') as log:
 for raw in sys.stdin.buffer:
  value=json.loads(raw)
  log.write(json.dumps({'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})+'\n');log.flush()
  if 'hello' in value: response={'ready':True}
  else:
   if count==0:(root/'first-request.json').write_bytes(raw)
   results=[]
   for item in value['batch']:
    assert item['phase'] in ['begin','learn_step']
    if item['phase']=='begin':
     results.append({'option':0,'log_probability':0.0,'heads':0,'head_trace':[],'value':0.0,'behavior':{},'forward':{}})
    else:results.append({'ok':True})
    count+=1
   response={'batch':results}
  print(json.dumps(response),flush=True)
