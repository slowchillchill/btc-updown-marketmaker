"""#363 stdlib协议夹具：无Torch、无模型；记录原请求行摘要并返回固定关联响应。"""
import hashlib,json,pathlib,sys
control=json.loads(pathlib.Path(sys.argv[2]).read_text())
log=pathlib.Path(control['log'])
with log.open('w') as stream:
 for raw in sys.stdin.buffer:
  value=json.loads(raw)
  stream.write(json.dumps({'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'value':value if control.get('capture') else None})+'\n');stream.flush()
  if 'hello' in value:response={'ready':True}
  else:
   items=value['batch']
   if any(x.get('boundary')=='eof' for x in items):sys.exit(0)
   response={'batch':[] if any(x.get('boundary')=='count' for x in items) else [({'error':'fixture error'} if x.get('boundary')=='error' else {'account':x.get('account'),'call':x.get('call'),'phase':x.get('phase'),'post_only':True,'index':2,'rank':3}) for x in items]}
  print(json.dumps(response),flush=True)
