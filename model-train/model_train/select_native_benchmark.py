import json
from pathlib import Path
import argparse
parser=argparse.ArgumentParser(description='仅按真实输入属性冻结 #170 样本；不读取耗时')
parser.add_argument('output',type=Path)
parser.add_argument('--group-index',type=Path,required=True)
args=parser.parse_args();p=args.output
rows=[json.loads(l) for l in (p/'selection-pass/measurements.jsonl').open()]
selected={}
def add(row,tag):selected.setdefault(str(row['position']),[]).append(tag)
context=[r for r in rows if 1789517070000<=r['now']<1789517100000]
for r in context[:20]:add(r,'old20')
market=[r for r in rows if 1789517100000<=r['now']<1789517400000]
for minute in range(5):
 subset=[r for r in market if 1789517100000+minute*60000<=r['now']<1789517100000+(minute+1)*60000][:64]
 assert len(subset)==64
 for r in subset:add(r,f'minute{minute}')
events=[]
for key in ('changes','misses'):
 positions=sorted(range(len(market)-63),key=lambda i:(-market[i][key],market[i]['position']))
 i=next(i for i in positions if all(abs(i-other)>64 for other in events))
 events.append(i)
 for r in market[i:i+64]:add(r,'stress-'+key)
references={}
for line in args.group_index.open():
 row=json.loads(line)
 if str(row['group_position']) in selected:
  references[str(row['group_position'])]={key:row[key] for key in ('group_position','group_id','closed_at_input_position')}
assert set(references)==set(selected)
(p/'native-selection-sources.json').write_text(json.dumps(references,indent=2)+'\n')
(p/'native-selection.json').write_text(json.dumps(selected,indent=2)+'\n')
(p/'native-selection-attributes.json').write_text(json.dumps({'target_rows':len(market),'context_rows':len(context),'groups':len(selected),'events':[{**market[i],'key':key} for i,key in zip(events,('changes','misses'))],'selected_rows':[r|{'tags':selected[str(r['position'])]} for r in rows if str(r['position']) in selected],'scope':'按输入价量增删变化数及精确投影输入差集挑选，先固定后计时。'},ensure_ascii=False,indent=2)+'\n')
print('selection frozen',len(selected),flush=True)
