"""从准确基线和候选源生成离线驱动；复用既有前史恢复，不修改生产接口。"""
import argparse
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tomllib
from instrument import instrument

BASELINE='a39f2cc2bb160c49fd53dba1a106364b0666f7c3'

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();source=args.source.resolve();out=args.output.resolve();out.mkdir(parents=True)
    project=source/'model-train/validation/issue-176'
    spec=importlib.util.spec_from_file_location('previous',project.parent/'issue-174/prepare.py')
    previous=importlib.util.module_from_spec(spec);spec.loader.exec_module(previous)
    archive=subprocess.check_output(['git','archive',BASELINE],cwd=source)
    identities={'baseline':BASELINE,'candidate':subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip(),'variants':{}}
    for variant in ('baseline','candidate'):
        destination=out/(variant+'-harness');destination.mkdir()
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:tar.extractall(destination,filter='data')
        with tarfile.open(project.parent/'issue-172/evidence/source/formal-overlay.tar.gz') as tar:
            for member in tar.getmembers():
                if member.name.split('/')[0] in previous.DEPENDENCIES:tar.extract(member,destination,filter='data')
        native=destination/'model-train/native-inputs'
        if variant=='candidate':shutil.copytree(source/'model-train/native-inputs',native,dirs_exist_ok=True,ignore=shutil.ignore_patterns('target'))
        manifest=native/'Cargo.toml';lock=native/'Cargo.lock'
        version=next(p['version'] for p in tomllib.loads(lock.read_text())['package'] if p['name']=='libc')
        manifest.write_text(manifest.read_text().replace('[dev-dependencies]\n','[dev-dependencies]\nlibc = "='+version+'"\n'))
        text=lock.read_text();a=text.index('name = "model-native-inputs"');b=text.index('[[package]]',a)
        lock.write_text(text[:a]+text[a:b].replace(' "ledger",\n',' "ledger",\n "libc",\n')+text[b:])
        src=native/'src'
        scorer=src/'ort_actor.rs'
        scorer.write_text(instrument(scorer.read_text(),variant=='candidate'))
        (src/'market.rs').write_text(previous.market_helpers((src/'market.rs').read_text(),True))
        root=previous.root_helpers((src/'root.rs').read_text(),'market',True)
        a=root.index('    pub fn baseline(&self)');b=root.index('    pub fn clear_added_inputs',a)
        root=root[:a]+root[b:]
        (src/'root.rs').write_text(root)
        tail=(project.parent/'issue-174/frame.rs').read_text().replace('VARIANT_NAME',variant)
        (native/'examples/frame_latency.rs').write_text(previous.host((native/'examples/replay_projection.rs').read_text(),tail,True))
        (native/'examples/common').mkdir(exist_ok=True)
        runtime=project/('runtime.rs' if variant=='candidate' else '../issue-174/runtime.rs')
        text=runtime.read_text()
        if variant=='baseline':
            text=text.replace('hidden: width,','hidden: width,\n                profile: Default::default(),')
            text=text.replace('    action_steps: Vec<(usize, usize, u128)>,','    action_steps: Vec<(usize, usize, u128)>,\n    action_details: Vec<Value>,')
            text=text.replace('        let mut calls = Vec::new();','        let mut calls = Vec::new();\n        let mut details = Vec::new();')
            text=text.replace('                    let support = actor.support().unwrap();','                    self.scorer.profile.clear();\n                    let support = actor.support().unwrap();\n                    let support_ns = start.elapsed().as_nanos();\n                    let scoring = Instant::now();')
            text=text.replace('                    let mut best = None;','                    let scoring_ns = scoring.elapsed().as_nanos();\n                    let selecting = Instant::now();\n                    let mut best = None;')
            text=text.replace('                    calls.push((i, head, start.elapsed().as_nanos()));','                    details.push(json!({"row":i,"head":head,"support_ns":support_ns,"scoring_ns":scoring_ns,"choose_ns":selecting.elapsed().as_nanos(),"components":self.scorer.profile}));\n                    calls.push((i, head, start.elapsed().as_nanos()));')
            text=text.replace('                action_steps: calls,','                action_steps: calls,\n                action_details: details,')
        (native/'examples/common/frame_runtime.rs').write_text(text)
        identities['variants'][variant]={str(p.relative_to(destination)):hashlib.sha256(p.read_bytes()).hexdigest()
            for folder in (native,*(destination/d for d in previous.DEPENDENCIES)) for p in sorted(folder.rglob('*'))
            if p.is_file() and p.suffix in ('.rs','.toml','.lock')}
    (out/'source-identities.json').write_text(json.dumps(identities,indent=2)+'\n')
    print(out)

if __name__=='__main__':main()
