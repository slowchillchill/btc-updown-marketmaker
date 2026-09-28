"""首次正式计时前核对录制/前史/状态权重并冻结两版本源码、图和实际二进制。"""
import argparse
import gc
import hashlib
import json
from pathlib import Path
import subprocess
import torch

BASELINE='a39f2cc2bb160c49fd53dba1a106364b0666f7c3'


def sha(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('source','graphs','packed','recording','config','checkpoints','ort-library','harnesses','output'):
        parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args()
    prior_path=args.source/'model-train/validation/issue-172/evidence/provenance.json'
    prior=json.loads(prior_path.read_text())
    assert sha(args.recording/'record-completion.json')==prior['record_completion_sha256']
    for name,digest in prior['packed'].items():assert sha(args.packed/name)==digest
    assert sha(args.ort_library)==prior['ort_library']['sha256']
    config=json.loads(args.config.read_text())
    projected=json.loads(json.dumps(config))
    source_name=lambda path:'/'.join(Path(path).parts[-3:])
    for item in projected['files']:item['path']=source_name(item['path'])
    projected['source_root']=Path(projected['source_root']).name
    projected['source_overrides']={source_name(path):{**override,'source_root':Path(override['source_root']).name}
                                  for path,override in projected['source_overrides'].items()}
    for key in ('rule_snapshot','finality_path'):
        projected[key]={'artifact_role':key,'sha256':sha(Path(config[key]))}
    assert projected==prior['configuration_without_local_paths']
    sources=json.loads((args.harnesses/'source-identities.json').read_text())
    assert sources['baseline']==BASELINE
    assert sources['candidate']==subprocess.check_output(['git','rev-parse','HEAD'],cwd=args.source,text=True).strip()
    for variant,files in sources['variants'].items():
        for relative,digest in files.items():assert sha(args.harnesses/(variant+'-harness')/relative)==digest
    binary_records=json.loads((args.harnesses/'binaries.json').read_text())
    for row in binary_records:assert sha(args.harnesses/'binaries'/row['binary'])==row['sha256']
    models={}
    for name in ('MAIN-1x256','MAIN-30x1024'):
        baseline=args.graphs/'baseline'/name;candidate=args.graphs/'candidate'/name
        for filename,expected in prior['models'][name]['files'].items():
            assert sha(baseline/filename)==expected['sha256']
        old=torch.load(baseline/'initial_weights.pt',weights_only=True,map_location='cpu')
        new=torch.load(candidate/'initial_weights.pt',weights_only=True,map_location='cpu')
        keys=[k for k in old if k.startswith('state.')]
        assert keys==[k for k in new if k.startswith('state.')]
        assert all(torch.equal(old[k],new[k]) for k in keys)
        # 已核对状态权重，旧无交易前史 hidden 可作为同一递推的入口；动作权重属于新策略。
        metadata=json.loads((candidate/'export_metadata.json').read_text())
        assert metadata['input_contract_version']=='native_inputs_v6'
        assert metadata['action_contract_version']=='native_actions_v2'
        (candidate/'benchmark_configuration.json').write_text(json.dumps({
            'configuration':metadata['configuration'],'input_contract_version':'native_inputs_v6',
            'action_contract_version':'native_actions_v2','source_commit':sources['candidate'],
            'weights_sha256':sha(candidate/'initial_weights.pt'),
            'state_weights_equal_baseline':True},indent=2)+'\n')
        models[name]={'state_parameter_tensors_equal':len(keys),
            'parameter_counts':{variant:{'state':sum(v.numel() for k,v in values.items() if k.startswith('state.')),
                'action':sum(v.numel() for k,v in values.items() if k.startswith('action.')),
                'total':sum(v.numel() for v in values.values())} for variant,values in [('baseline',old),('candidate',new)]},
            'variants':{}}
        for variant,directory in [('baseline',baseline),('candidate',candidate)]:
            models[name]['variants'][variant]={p.name:{'sha256':sha(p),'bytes':p.stat().st_size}
                for p in sorted(directory.iterdir()) if p.suffix in ('.onnx','.data','.pt') or p.name in ('benchmark_configuration.json','export_metadata.json')}
        del old,new;gc.collect()
    checkpoints={str(first):{'filename':f'entry-{first}.json','sha256':sha(args.checkpoints/f'entry-{first}.json'),
        'bytes':(args.checkpoints/f'entry-{first}.json').stat().st_size} for first in (201582,418584)}
    entries=json.loads((args.source/'model-train/validation/issue-172/evidence/preparation/prehistory.json').read_text())['entries']
    for entry in entries:
        assert checkpoints[str(entry['position'])]['sha256']==entry['sha256']
        assert checkpoints[str(entry['position'])]['bytes']==entry['bytes']
    identity={'baseline':BASELINE,'candidate':sources['candidate'],'input_contract_version':'native_inputs_v6',
        'action_contract_version':'native_actions_v2','feature_schema_sha256':sha(args.source/'model-train/native-inputs/feature_schema.json'),
        'prior_identity':{'relative_path':str(prior_path.relative_to(args.source)),'sha256':sha(prior_path)},
        'configuration_sha256':sha(args.config),'record_completion_sha256':sha(args.recording/'record-completion.json'),
        'packed':prior['packed'],'ort_library':prior['ort_library'],'models':models,'checkpoints':checkpoints,
        'source_identities_sha256':sha(args.harnesses/'source-identities.json'),'binaries':binary_records,
        'locks':{name:sha(args.source/name) for name in ('Cargo.lock','model-train/uv.lock','model-train/native-inputs/Cargo.lock')},
        'checkpoint_scope':'复用已验证的无交易前史；状态参数逐张量相等。两版本输入缓存首组为空且计时；新策略动作独立执行，不要求旧策略路径/账户相等。'}
    serialized=json.dumps(identity,ensure_ascii=False,indent=2)+'\n'
    assert '/home/' not in serialized and '/tmp/' not in serialized
    args.output.write_text(serialized)


if __name__=='__main__':main()
