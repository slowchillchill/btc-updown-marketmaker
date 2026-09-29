"""原生输入改动的现有业务/跨运行时/所有权检查与固定仓库回归。"""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('source','output','build','graphs','ort-library'):p.add_argument('--'+name,type=Path,required=True)
    a=p.parse_args();source=a.source.resolve();out=a.output.resolve();out.mkdir(parents=True)
    train=source/'model-train';python=train/'.venv/bin/python'
    base,site,libdir=json.loads(subprocess.check_output([str(python),'-c',
        'import json,sys,site,sysconfig;print(json.dumps([sys.base_prefix,site.getsitepackages()[0],sysconfig.get_config_var("LIBDIR")]))'],text=True))
    env={**os.environ,'PYO3_PYTHON':str(python),'PYTHONHOME':base,'PYTHONPATH':str(train)+':'+site,
         'LD_LIBRARY_PATH':libdir,'CARGO_TARGET_DIR':str(a.build.resolve()),'CARGO_BUILD_JOBS':'8',
         'ORT_DYLIB_PATH':str(a.ort_library.resolve()),'OMP_NUM_THREADS':'1','MKL_NUM_THREADS':'1','RAYON_NUM_THREADS':'8',
         'NATIVE_CANCEL_GRAPH':str(a.graphs.resolve()/'MAIN-1x256'),
         'NATIVE_COMPONENT_WEIGHTS':str(a.graphs.resolve()/'MAIN-1x256/initial_weights.pt'),
         'NATIVE_INPUT_OWNERSHIP_OUTPUT':str(out/'ownership.json')}
    def public(v):
        text=str(v)
        for path,role in [(source,'.'),(out,'target/issue-178-validation'),(a.build.resolve(),'target/build'),
                          (a.graphs.resolve(),'target/current-graphs'),(a.ort_library.resolve(),'target/onnxruntime.so')]:
            text=text.replace(str(path),role)
        return text
    records=[]
    def run(label,command,cwd=source,extra=None):
        row={'label':label,'command':[public(v) for v in command],'cwd':public(cwd),'started_utc':datetime.now(timezone.utc).isoformat()}
        logfile=out/(label+'.log')
        with logfile.open('w') as log:
            code=subprocess.call([str(v) for v in command],cwd=cwd,env={**env,**(extra or {})},stdout=log,stderr=subprocess.STDOUT)
        with logfile.open('rb') as stream:digest=hashlib.file_digest(stream,'sha256').hexdigest()
        row.update(exit_code=code,raw_log_sha256=digest,completed_utc=datetime.now(timezone.utc).isoformat())
        if label=='workspace-tests':row['explicitly_skipped']=['four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second']
        records.append(row);(out/'commands.json').write_text(json.dumps(records,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps(row,ensure_ascii=False),flush=True)
        if code:
            print('\n'.join(logfile.read_text().splitlines()[-60:]),flush=True);raise SystemExit(code)
    native=['--manifest-path','model-train/native-inputs/Cargo.toml']
    run('wheel-build',[train/'.venv/bin/maturin','build','--release','--features','python','--locked','--offline','--interpreter',python,'--out',out/'wheels'],train)
    wheels=list((out/'wheels').glob('*.whl'));assert len(wheels)==1
    run('wheel-install',['uv','tool','run','--from','uv==0.12.9','uv','pip','install','--python',python,'--offline','--no-deps','--reinstall',wheels[0]],out)
    run('native-continuous-tests',['cargo','test',*native,'--features','continuous-validation','--lib','--tests','--locked','--offline'])
    run('ownership',['cargo','test',*native,'--features','continuous-validation','--lib','--locked','--offline',
        'native_shared_inputs_preserve_delayed_backward_and_detached_aliases','--','--ignored','--nocapture'])
    run('frozen-cancel-race',['cargo','test',*native,'--features','continuous-validation','--lib','--locked','--offline',
        'actual_graph_pair_intent_execution_and_frozen_cancel_race','--','--ignored','--nocapture'])
    for model in ('MAIN-1x256','MAIN-30x1024'):
        run('full-book-'+model,['cargo','test',*native,'--features','continuous-validation','--lib','--locked','--offline',
            'full_book_three_runtime_and_original_execution','--','--ignored','--nocapture'],extra={
                'ACTOR_GRAPH_DIRECTORY':str(a.graphs.resolve()/model),'NATIVE_DEEP_EXECUTION_OUTPUT':str(out/(model+'-full-book.json'))})
    run('workspace-tests',['cargo','test','--workspace','--lib','--bins','--tests','--examples','--locked','--offline','--',
        '--skip','four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second'])
    run('native-clippy',['cargo','clippy',*native,'--all-targets','--features','continuous-validation','--locked','--offline','--',
        '-D','warnings','-A','clippy::needless_borrow','-A','clippy::too_many_arguments'])
    run('native-fmt',['cargo','fmt',*native,'--','--check'])
    run('workspace-fmt',['cargo','fmt','--all','--','--check'])


if __name__=='__main__':main()
