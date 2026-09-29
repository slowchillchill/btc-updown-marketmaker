"""在实际测量记录上补充字段取值；未记录的输入不记为恒零。"""
import argparse
import csv
import json
from pathlib import Path
import struct


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--measurements',type=Path,nargs='+',required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    samples={}
    def collect(part,row):
        for index,value in enumerate(row):
            bits=struct.unpack('<I',struct.pack('<f',value))[0]
            state=samples.setdefault((part,index),[0,float('inf'),float('-inf'),set()])
            state[0]+=1;state[1]=min(state[1],value);state[2]=max(state[2],value)
            state[3].add(bits)
    for path in a.measurements:
        for line in path.open():
            row=json.loads(line)
            if 'native_group' not in row:continue
            group=row['native_group']
            for item in group['rows']:
                for part in ('fast','slow','rule'):collect(part,item[part])
                for side in item['book_levels']:
                    for level in side:collect('book',level)
            for item in group['cancel']:collect('cancel',item)
            for detail in row['runtimes'][0]['timing'].get('action_details',[]):
                if 'prefix_features' in detail:collect('prefix',detail['prefix_features'])
    with a.output.open('w',newline='') as stream:
        writer=csv.writer(stream,lineterminator='\n')
        writer.writerow(['输入','索引','观测数','最小f32','最大f32','不同f32位模式数','覆盖解释'])
        for part,width in [('fast',596),('slow',433),('book',9),('rule',30),('cancel',184),('prefix',36)]:
            for index in range(width):
                state=samples.get((part,index))
                if state:
                    scope='仅此有限真实窗口；一个取值不证明合同恒定'
                elif part=='prefix':
                    scope='宿主未序列化动作前缀向量；无可统计记录，不表示模型未消费该输入'
                else:
                    scope='测量记录无该对象输入；不能据此称无用/恒零'
                writer.writerow([part,index,*(state[:3]+[len(state[3])] if state else [0,'','','']),scope])


if __name__=='__main__':main()
