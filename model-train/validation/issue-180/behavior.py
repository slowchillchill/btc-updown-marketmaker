"""步骤8：准确全仓副本上的短连续行为对照，不运行容量前史。"""
import argparse
import hashlib
import difflib
import io
import json
from pathlib import Path
import subprocess
import tarfile


def replace_once(text, old, new):
    assert text.count(old) == 1, old
    return text.replace(old, new)


def prepare(source, revision, output, candidate):
    revision = subprocess.check_output(['git','rev-parse',revision], cwd=source,text=True).strip()
    output.mkdir(parents=True)
    archive = subprocess.check_output(['git', 'archive', revision], cwd=source)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(output, filter='data')
    # 开发校验可施加当前受控差异，最终正式对照必须只使用已提交精确头。
    src = output / 'model-train/native-inputs/src'
    originals = {name:(src/name).read_text() for name in ['continuous_tests.rs','continuous_runtime.rs','root.rs']}
    fixture = (src / 'continuous_tests.rs').read_text()
    fixture = replace_once(fixture, 'for at in (START..=END).step_by(1000) {',
        'for at in (START..=END).step_by(1000).filter(|at| *at == START || (FIRST-30000..=FIRST+20000).contains(at) || [FIRST+270000,FIRST+271000,FIRST+280000,FIRST+300000,FIRST+301000,END].contains(at)) {')
    fixture += '\n' + Path(__file__).with_suffix('.rs').read_text()
    if candidate:
        fixture += '''
fn issue180_consume(root: &mut ContinuousRoot) -> Option<NativeInputGroup> {
    root.shared.apply_closed().unwrap();
    let decision = root.driver.decision().unwrap();
    let frames = decision.frames();
    root.shared.observe_group(&frames);
    root.native.observe_sources(&frames);
    let before = root.shared.freeze_action_before(decision.cutoff().clone()).unwrap();
    let mut group = root.native.encode(&frames, before).unwrap();
    if frames.is_empty() { group.action_before = None; }
    Some(group)
}
'''
    else:
        fixture += '''
fn issue180_consume(root: &mut ContinuousRoot) -> Option<NativeInputGroup> {
    root.native.consume(&mut root.shared, &mut root.driver).unwrap()
}
'''
    # Actor兼容入口在两侧相同；候选实际调用纯选择和共享预留执行边界。
    fixture += '''
async fn issue180_execute(actor: &NativeActor, root: &mut ContinuousRoot, ordinal: usize) {
    let context = RequestContextV2 { global_request_id:format!("behavior-{ordinal}"), caller_correlation_id:None };
    let receipt = actor.execute(&mut root.shared, &mut root.driver.decision().unwrap(), context).await.unwrap();
    crate::continuous_runtime::issue180_trace(serde_json::json!({"kind":"execute","ordinal":ordinal,"receipt":format!("{receipt:?}"),"ledger":root.shared.ledger().projection_v2().unwrap()}));
}
'''
    (src / 'continuous_tests.rs').write_text(fixture)
    root = (src / 'root.rs').read_text() + '''
impl NativeInputRoot {
    pub(crate) fn issue180_history_lengths(&self) -> Vec<(String, Vec<usize>)> {
        self.histories.iter().map(|(id,h)| (id.clone(), h.series.iter().map(|q|q.len()).collect())).collect()
    }
}
'''
    (src / 'root.rs').write_text(root)
    runtime = (src / 'continuous_runtime.rs').read_text()
    runtime = replace_once(runtime, '            let state_started = std::time::Instant::now();',
        '            let issue180_h_prev = h.clone();\n            let state_started = std::time::Instant::now();')
    runtime = replace_once(runtime, '            close(want_z, z);', '''
            issue180_trace(serde_json::json!({"kind":"state","frame":self.frames,"h_prev":issue180_h_prev,"h_next":h,"z_t":z,"python_and_python_ort_z":want_z,"python_and_python_ort_h":want_h}));
            close(want_z, z);''')
    runtime = replace_once(runtime, '                let choice = best.unwrap().0;', '''
                let maximum = actual.iter().zip(support).filter(|(_,legal)| **legal).map(|(x,_)|f64::from(*x)).fold(f64::NEG_INFINITY,f64::max);
                let denominator: f64 = actual.iter().zip(support).filter(|(_,legal)| **legal).map(|(x,_)|(f64::from(*x)-maximum).exp()).sum();
                let probabilities = actual.iter().zip(support).map(|(x,legal)| if *legal {(f64::from(*x)-maximum).exp()/denominator} else {0.}).collect::<Vec<_>>();
                let choice = best.unwrap().0;
                issue180_trace(serde_json::json!({"kind":"actor","root":decisions[i].0,"row":decisions[i].1,"step":step,"support":support,"prefix":prefix,"features":features,"scores":actual.iter().zip(support).map(|(x,legal)|legal.then_some(x)).collect::<Vec<_>>(),"reference_scores":want.iter().zip(support).map(|(x,legal)|legal.then_some(x)).collect::<Vec<_>>(),"conditional_probabilities":probabilities,"choice":choice}));''')
    runtime += '''
pub(super) fn issue180_trace(value: serde_json::Value) {
    use std::io::Write;
    let path = std::env::var("BEHAVIOR_TRACE").unwrap();
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap();
    serde_json::to_writer(&mut file, &value).unwrap();
    writeln!(file).unwrap();
}
'''
    (src / 'continuous_runtime.rs').write_text(runtime)
    patch = ''.join(''.join(difflib.unified_diff(originals[path.name].splitlines(True), path.read_text().splitlines(True), fromfile='a/'+str(path.relative_to(output)), tofile='b/'+str(path.relative_to(output)))) for path in [src/'continuous_tests.rs',src/'continuous_runtime.rs',src/'root.rs'])
    (output.parent/(output.name+'-diagnostic.patch')).write_text(patch)
    manifest = {'diagnostic_patch_sha256':hashlib.sha256(patch.encode()).hexdigest(),'revision':revision,'candidate_phased_entry':candidate,'archive_sha256':hashlib.sha256(archive).hexdigest(),
        'diagnostic_files':{str(path.relative_to(output)):hashlib.sha256(path.read_bytes()).hexdigest() for path in [src/'continuous_tests.rs',src/'continuous_runtime.rs',src/'root.rs']}}
    (output.parent / (output.name+'-freeze.json')).write_text(json.dumps(manifest,indent=2)+'\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--candidate', action='store_true')
    args = parser.parse_args()
    prepare(args.source.resolve(),args.revision,args.output.resolve(),args.candidate)


if __name__ == '__main__':
    main()
