"""核对原图与检查点证据链；只记录可公开的相对角色名及摘要。"""
import argparse
import hashlib
import json
from pathlib import Path


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream,'sha256').hexdigest()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('source','graphs','checkpoints','warm-baseline','warm-candidate','config','recording','packed','ort-library','output'):
        p.add_argument('--'+name,type=Path,required=True)
    for variant in ('baseline','candidate'):
        p.add_argument('--warm-'+variant+'-binary',type=Path,required=True)
        p.add_argument('--warm-'+variant+'-harness',type=Path,required=True)
    a=p.parse_args()
    prior_path=a.source/'model-train/validation/issue-176/evidence/source/provenance.json'
    prior=json.loads(prior_path.read_text())
    models={}
    for name in ('MAIN-1x256','MAIN-30x1024'):
        files=prior['models'][name]['variants']['candidate']
        for filename,record in files.items():
            assert sha(a.graphs/name/filename)==record['sha256'],(name,filename)
        models[name]={'files':files,'state_parameter_tensors_equal_to_original_history':prior['models'][name]['state_parameter_tensors_equal'],
                      'baseline_and_candidate_use_identical_graphs':True}
    assert sha(a.recording/'record-completion.json')==prior['record_completion_sha256']
    assert sha(a.ort_library)==prior['ort_library']['sha256']
    for filename,digest in prior['packed'].items():assert sha(a.packed/filename)==digest
    entries={}
    for first in (201582,418584):
        old=a.checkpoints/f'entry-{first}.json'
        assert sha(old)==prior['checkpoints'][str(first)]['sha256']
        entries[str(first)]={'model_history_sha256':sha(old),'actual_native_warm_caches':{}}
        for variant,directory in [('baseline',a.warm_baseline),('candidate',a.warm_candidate)]:
            warm=directory/f'warm-{first}.json'
            metadata=json.loads(warm.read_text())
            assert metadata['business_history_matches_previous_checkpoint']
            entries[str(first)]['actual_native_warm_caches'][variant]={'sha256':sha(warm),'bytes':warm.stat().st_size,
                'native_preparation_groups':metadata['native_preparation_groups'],'native_preparation_ns':metadata['native_preparation_ns'],
                'historical_model_inference_rerun':False}
    result={'baseline':'bdb54b4e46adf29a8b607171338a6efce701d301','prior_evidence':{'path':str(prior_path.relative_to(a.source)),'sha256':sha(prior_path)},
            'models':models,'checkpoints':entries,'config_sha256':sha(a.config),'record_completion_sha256':sha(a.recording/'record-completion.json'),
            'packed':prior['packed'],'ort_library':prior['ort_library'],
            'actual_native_warm_builds':{variant:{
                'binary_sha256':sha(getattr(a,'warm_'+variant+'_binary')),
                'source_identities':json.loads((getattr(a,'warm_'+variant+'_harness')/'source-identities.json').read_text())
            } for variant in ('baseline','candidate')},
            'scope':'当前v6八图及权重与已归档证据逐文件相同。沿用已验证真实历史hidden；本轮重放native历史与缓存，不宣称重新执行了历史模型推理。'}
    text=json.dumps(result,ensure_ascii=False,indent=2)+'\n'
    assert '/home/' not in text and '/tmp/' not in text
    a.output.write_text(text)


if __name__=='__main__':main()
