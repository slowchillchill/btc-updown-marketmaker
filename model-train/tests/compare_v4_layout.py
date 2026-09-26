"""比较同一原公共夹具经旧/新原生入口的状态行；不从v4补成v5业务输入。"""
import argparse
import json
from pathlib import Path
import numpy as np

STARTS = (57,113,175,231,293,349)
RETAIN = [i for i in range(836) if not any(start <= i < start+40 for start in STARTS)]


def compare(old, new):
    rows = 0
    for old_group,new_group in zip(old,new,strict=True):
        assert old_group['cutoff'] == new_group['cutoff'], (rows,'cutoff')
        for old_row,new_row in zip(old_group['rows'],new_group['rows'],strict=True):
            for field in ('hidden_state_key','role','slow','rule'):
                assert old_row[field] == new_row[field], (rows,field)
            reference=np.asarray(old_row['fast'],dtype=np.float32)
            np.testing.assert_array_equal(reference[RETAIN].view('u4'),np.asarray(new_row['fast'],dtype=np.float32).view('u4'))
            restored=np.zeros(240,dtype=np.float32)
            for side,levels in enumerate(new_row['book_levels']):
                for level,value in enumerate(levels[:5]):
                    restored[side*40+level*8:side*40+(level+1)*8]=value[:8]
            expected=np.concatenate([reference[start:start+40] for start in STARTS])
            np.testing.assert_array_equal(expected.view('u4'),restored.view('u4'))
            rows += 1
    return dict(rows=rows,retained_596_bit_equal=True,restored_240_bit_equal=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('v4',type=Path);p.add_argument('v5',type=Path)
    a=p.parse_args();print(json.dumps(compare(json.loads(a.v4.read_text()),json.loads(a.v5.read_text()))))
