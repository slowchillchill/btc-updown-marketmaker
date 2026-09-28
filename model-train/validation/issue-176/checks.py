"""当前图接口的共享输入、真实完整盘口/执行及必要静态检查；固定工作区回归单独记录。"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('source','output','build','graphs'):parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args();source=args.source.resolve();output=args.output.resolve();output.mkdir(parents=True)
    build=args.build.resolve();graphs=args.graphs.resolve();train=source/'model-train';python=train/'.venv/bin/python'
    info=json.loads(subprocess.check_output([str(python),'-c','import json,sys,site,sysconfig,onnxruntime; from pathlib import Path; print(json.dumps([sys.base_prefix,site.getsitepackages()[0],sysconfig.get_config_var("LIBDIR"),str(next((Path(onnxruntime.__file__).parent/"capi").glob("libonnxruntime.so.*")))]))'],text=True))
    base,site,libdir,ort=info
    env=dict(os.environ,PYO3_PYTHON=str(python),PYTHONHOME=base,PYTHONPATH=str(train)+':'+site,
        LD_LIBRARY_PATH=libdir,CARGO_TARGET_DIR=str(build),ORT_DYLIB_PATH=ort,OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',RAYON_NUM_THREADS='8',
        NATIVE_CANCEL_GRAPH=str(graphs/'MAIN-1x256'),NATIVE_COMPONENT_WEIGHTS=str(graphs/'MAIN-1x256/initial_weights.pt'),
        NATIVE_INPUT_OWNERSHIP_OUTPUT=str(output/'ownership.json'))
    public=lambda value:str(value).replace(str(source),'.').replace(str(output),'target/issue-176-performance/validation').replace(str(build),'target/issue-176-build').replace(str(graphs),'target/issue-176-performance/graphs')
    records=[]
    def run(label,command,cwd=source,extra=None):
        row={'label':label,'args':[public(v) for v in command],'cwd':public(cwd),'started_utc':datetime.now(timezone.utc).isoformat()}
        path=output/(label+'.log')
        with path.open('w') as log:result=subprocess.run([str(v) for v in command],cwd=cwd,env={**env,**(extra or {})},stdout=log,stderr=subprocess.STDOUT)
        with path.open('rb') as stream:digest=hashlib.file_digest(stream,'sha256').hexdigest()
        row.update(exit_code=result.returncode,completed_utc=datetime.now(timezone.utc).isoformat(),raw_log_sha256=digest)
        records.append(row);(output/'commands.json').write_text(json.dumps(records,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps(row,ensure_ascii=False),flush=True)
        if result.returncode:
            print('\n'.join(path.read_text().splitlines()[-60:]),flush=True);raise SystemExit(result.returncode)
    native=['--manifest-path','model-train/native-inputs/Cargo.toml']
    run('wheel-build',[train/'.venv/bin/maturin','build','--release','--features','python','--locked','--offline','--interpreter',python,'--out',output/'wheels'],train)
    wheels=list((output/'wheels').glob('*.whl'));assert len(wheels)==1
    run('wheel-install',['uv','tool','run','--from','uv==0.12.9','uv','pip','install','--python',python,'--offline','--no-deps','--reinstall',wheels[0]],output)
    run('ownership',['cargo','test',*native,'--features','continuous-validation','--lib','--locked','--offline','native_shared_inputs_preserve_delayed_backward_and_detached_aliases','--','--ignored','--nocapture'])
    for name in ('MAIN-1x256','MAIN-30x1024'):
        run('full-book-'+name,['cargo','test',*native,'--features','continuous-validation','--lib','--locked','--offline','full_book_three_runtime_and_original_execution','--','--ignored','--nocapture'],
            extra={'ACTOR_GRAPH_DIRECTORY':str(graphs/name),'NATIVE_DEEP_EXECUTION_OUTPUT':str(output/(name+'-full-book.json'))})
    run('cancel-race',['cargo','test',*native,'--features','continuous-validation','--lib','--locked','--offline','actual_graph_pair_intent_execution_and_frozen_cancel_race','--','--ignored','--nocapture'])
    run('native-tests',['cargo','test',*native,'--features','continuous-validation','--lib','--tests','--locked','--offline'])
    run('native-clippy',['cargo','clippy',*native,'--all-targets','--features','continuous-validation','--locked','--offline','--','-D','warnings','-A','clippy::needless_borrow','-A','clippy::too_many_arguments'])
    run('rust-ort-tests',['cargo','test','--manifest-path','model-train/rust-ort-validation/Cargo.toml','--locked','--offline'])
    run('rust-ort-clippy',['cargo','clippy','--manifest-path','model-train/rust-ort-validation/Cargo.toml','--all-targets','--locked','--offline','--','-D','warnings'])
    run('workspace-fmt',['cargo','fmt','--all','--','--check'])
    run('native-fmt',['cargo','fmt',*native,'--','--check'])
    run('rust-ort-fmt',['cargo','fmt','--manifest-path','model-train/rust-ort-validation/Cargo.toml','--','--check'])


if __name__=='__main__':main()
