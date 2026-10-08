"""旧v3设备缓存补原来源旁列；原compact/成员不重写，页缺不冒充EOF。"""
from pathlib import Path
import hashlib,json,os,shutil,sys,time
import torch
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('data-pipeline/gpu','sim-system/gpu','shared-run/gpu',
    'ledger/gpu','execution-normalization/gpu','model-train/native-inputs/gpu')]
from public_device_page import PreparedPublicPage
from public_source import PublicGroupSource


def main():
    original=Path(sys.argv[1]);destination=Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR'])
    started=time.perf_counter();shutil.copytree(original/'public-page',destination/'old-page')
    path=destination/'old-page/manifest.json';manifest=json.loads(path.read_bytes())
    # 本批之前的v3缓存具有相同ABI/成员，只没有两个新增元数据。
    for name in ('source_progress_changes','group_applied_prefixes'):manifest['metadata'].pop(name)
    path.write_text(json.dumps(manifest,separators=(',',':')))
    compact=original/'public-compact';before=(compact/'manifest.json').read_bytes()
    old=PreparedPublicPage(destination/'old-page',compact)
    pm,*_=old.install()  # 原消费接口不要求新增整根前沿旁列。
    assert 'source_progress_changes' not in old.columns
    source=PublicGroupSource(old,1,columns_binary=os.environ['B_GPU_COLUMNS_BINARY'])
    current=PreparedPublicPage(original/'public-page',compact)
    for name in ('source_progress_changes','group_applied_prefixes'):assert old.columns[name]==current.columns[name]
    at=[];groups=len(old.columns['groups'])
    while not source.next().eof.cpu().item():
        pending=source.next()
        assert not pending.present.cpu().item() and not pending.eof.cpu().item()
        position=int(source.position.cpu()[0]);source.load_next(1)
        pending=source.next();assert pending.present.cpu().item()
        assert pending.at.cpu().tolist()==[int(old.arrays['causal_visible_at'][position])]
        assert source.prefix_at(pm,pending.at).cpu().tolist()==[position-1]
        at.append(int(pending.at.cpu()[0]));source.consume(torch.ones(1,dtype=torch.bool,device='cuda'))
    assert source.position.cpu().tolist()==[groups] and source.next().eof.cpu().tolist()==[True]
    assert (compact/'manifest.json').read_bytes()==before
    assert 'source_progress_changes' not in json.loads(path.read_bytes())['metadata']
    print(json.dumps(dict(passed=True,scope='old v3 cache / same verified compact / lineage derivation / real page headers and normal EOF',
        groups=groups,source_manifest_sha256=hashlib.sha256(before).hexdigest(),page_header_times=at,
        elapsed_seconds=time.perf_counter()-started)),flush=True)

if __name__=='__main__':main()
