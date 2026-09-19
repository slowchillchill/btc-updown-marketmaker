//! 仅用于阶段 #6 固定技术切片；输入没有 Python 隐藏状态或归约结果。
use ort::{
    ep,
    session::{Session, builder::GraphOptimizationLevel},
    value::Tensor,
};
use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    io::{BufWriter, Write},
    path::Path,
};
type Result<T> = std::result::Result<T, Box<dyn Error>>;
#[derive(Deserialize)]
struct Suite {
    layers: usize,
    hidden: usize,
    cases: Vec<Case>,
}
#[derive(Deserialize)]
struct Case {
    name: String,
    mode: String,
    batch: usize,
    steps: usize,
    slots: usize,
    empty: bool,
    fast: Vec<f32>,
    slow: Vec<f32>,
    rule: Vec<f32>,
    prefix: Vec<f32>,
    objects: Vec<f32>,
    mask: Vec<bool>,
    splits: Vec<Vec<usize>>,
    prefixes: Vec<Vec<f32>>,
    active: Vec<usize>,
}
#[derive(Serialize)]
struct Record<'a> {
    label: &'a str,
    values: &'a [Vec<f32>],
}
fn emit(out: &mut impl Write, label: &str, values: &[Vec<f32>]) -> Result<()> {
    serde_json::to_writer(&mut *out, &Record { label, values })?;
    writeln!(out)?;
    Ok(())
}
fn session(path: &Path, threads: usize) -> Result<Session> {
    Ok(Session::builder()?
        .with_execution_providers([ep::CPU::default().with_arena_allocator(true).build()])?
        .with_intra_threads(threads)?
        .with_inter_threads(1)?
        .with_parallel_execution(false)?
        .with_optimization_level(GraphOptimizationLevel::All)?
        .with_memory_pattern(false)?
        .with_intra_op_spinning(false)?
        .with_inter_op_spinning(false)?
        .commit_from_file(path)?)
}
fn tensor(shape: &[usize], values: &[f32]) -> Result<Tensor<f32>> {
    Ok(Tensor::from_array((shape.to_vec(), values.to_vec()))?)
}
fn state(
    session: &mut Session,
    case: &Case,
    layers: usize,
    hidden: usize,
    h: &[f32],
) -> Result<Vec<Vec<f32>>> {
    let outputs = session.run(ort::inputs![
        "fast_features" => tensor(&[case.batch,836], &case.fast)?,
        "slow_features" => tensor(&[case.batch,412], &case.slow)?,
        "h_prev" => tensor(&[layers,case.batch,hidden], h)?
    ])?;
    Ok(["z_t", "h_next"]
        .iter()
        .map(|name| {
            outputs[*name]
                .try_extract_tensor::<f32>()
                .map(|(_, v)| v.to_vec())
        })
        .collect::<ort::Result<_>>()?)
}
// 参数调用只收到本进程计算的 z/reduced，永远不接收或更新隐藏状态。
#[allow(clippy::too_many_arguments)]
fn action(
    session: &mut Session,
    batch: usize,
    hidden: usize,
    slots: usize,
    z: &[f32],
    rule: &[f32],
    prefix: &[f32],
    objects: &[f32],
    mask: &[bool],
    reduced: &[f32],
    exists: &[bool],
) -> Result<Vec<Vec<f32>>> {
    let outputs = session.run(ort::inputs![
        "z_t" => tensor(&[batch,hidden], z)?, "rule_features" => tensor(&[batch,30],rule)?,
        "prefix_features" => tensor(&[batch,60],prefix)?, "cancel_features" => tensor(&[batch,slots,129],objects)?,
        "cancel_mask" => Tensor::from_array(([batch,slots],mask.to_vec()))?,
        "cancel_reduced" => tensor(&[batch,3*hidden/2],reduced)?,
        "cancel_exists" => Tensor::from_array(([batch],exists.to_vec()))?,
        "price_fee_features" => tensor(&[batch,9999], &vec![0.;batch*9999])?
    ])?;
    Ok([
        "family_logits",
        "quantity_branch_logits",
        "buy_variant_logits",
        "price_logits",
        "post_only_logits",
        "expiry_logits",
        "cancel_embeddings",
        "cancel_logits",
    ]
    .iter()
    .map(|name| {
        outputs[*name]
            .try_extract_tensor::<f32>()
            .map(|(_, v)| v.to_vec())
    })
    .collect::<ort::Result<_>>()?)
}
fn rows<T: Clone>(data: &[T], width: usize, selected: &[usize]) -> Vec<T> {
    selected
        .iter()
        .flat_map(|&row| data[row * width..(row + 1) * width].iter().cloned())
        .collect()
}
fn block(
    case: &Case,
    offset: usize,
    length: usize,
    padded: usize,
    family: Option<usize>,
) -> (Vec<f32>, Vec<bool>) {
    let mut x = vec![0.; case.batch * padded * 129];
    let mut mask = vec![false; case.batch * padded];
    for row in 0..case.batch {
        for j in 0..length {
            let source = (row * case.slots + offset + j) * 129;
            let dest = (row * padded + j) * 129;
            x[dest..dest + 129].copy_from_slice(&case.objects[source..source + 129]);
            mask[row * padded + j] = case.mask[row * case.slots + offset + j]
                && family.is_none_or(|f| case.objects[source + f] == 1.);
        }
    }
    (x, mask)
}
fn selected(scores: &[f32], indices: &[usize]) -> Vec<f32> {
    if scores.is_empty() {
        return vec![-1.];
    }
    let mut best = 0;
    for i in 1..scores.len() {
        if scores[i] > scores[best] {
            best = i;
        }
    }
    vec![indices[best] as f32]
}
fn encode_reduce(
    session: &mut Session,
    c: &Case,
    hidden: usize,
    z: &[f32],
    lengths: &[usize],
    label: &str,
    out: &mut impl Write,
) -> Result<(Vec<f32>, Vec<bool>)> {
    let d = hidden / 2;
    let mut sum = vec![0f64; c.batch * d];
    let mut max = vec![f64::NEG_INFINITY; c.batch * d];
    let mut counts = vec![0usize; c.batch];
    let mut offset = 0;
    for (part, &length) in lengths.iter().enumerate() {
        let k = if c.empty { 1 } else { 256 };
        let (x, mask) = block(c, offset, length, k, None);
        let result = action(
            session,
            c.batch,
            hidden,
            k,
            z,
            &c.rule,
            &c.prefix,
            &x,
            &mask,
            &vec![0.; c.batch * 3 * d],
            &vec![false; c.batch],
        )?;
        let mut embeddings = Vec::new();
        for row in 0..c.batch {
            for j in 0..length {
                let embedding = &result[6][(row * k + j) * d..(row * k + j + 1) * d];
                embeddings.extend_from_slice(embedding);
                if mask[row * k + j] {
                    counts[row] += 1;
                    for f in 0..d {
                        let v = f64::from(embedding[f]);
                        sum[row * d + f] += v;
                        if v > max[row * d + f] {
                            max[row * d + f] = v;
                        }
                    }
                }
            }
        }
        emit(out, &format!("{label}/embedding{part}"), &[embeddings])?;
        offset += length;
    }
    let mut reduced = vec![0f32; c.batch * 3 * d];
    let exists = counts.iter().map(|&n| n > 0).collect::<Vec<_>>();
    for row in 0..c.batch {
        if counts[row] > 0 {
            for f in 0..d {
                reduced[row * 3 * d + f] = sum[row * d + f] as f32;
                reduced[row * 3 * d + d + f] = (sum[row * d + f] / counts[row] as f64) as f32;
                reduced[row * 3 * d + 2 * d + f] = max[row * d + f] as f32;
            }
        }
    }
    emit(
        out,
        &format!("{label}/reduced"),
        &[
            reduced.clone(),
            counts.iter().map(|&n| f32::from(n > 0)).collect(),
        ],
    )?;
    Ok((reduced, exists))
}
fn object_case(
    session: &mut Session,
    c: &Case,
    hidden: usize,
    z: &[f32],
    out: &mut impl Write,
) -> Result<()> {
    for (split, lengths) in c.splits.iter().enumerate() {
        let (reduced, exists) = encode_reduce(
            session,
            c,
            hidden,
            z,
            lengths,
            &format!("{}/split{split}", c.name),
            out,
        )?;
        let (x, mask) = block(c, 0, 1, 1, None);
        emit(
            out,
            &format!("{}/split{split}/family", c.name),
            &action(
                session, c.batch, hidden, 1, z, &c.rule, &c.prefix, &x, &mask, &reduced, &exists,
            )?,
        )?;
        for family in 0..3 {
            let mut prefix = vec![0.; c.batch * 60];
            for row in 0..c.batch {
                for j in [family + 3, 6, 13] {
                    prefix[row * 60 + j] = 1.;
                }
            }
            let mut scores = vec![Vec::new(); c.batch];
            let mut indices = vec![Vec::new(); c.batch];
            let mut offset = 0;
            for &length in lengths {
                let k = if c.empty { 1 } else { 256 };
                let (x, mask) = block(c, offset, length, k, Some(family));
                let result = action(
                    session, c.batch, hidden, k, z, &c.rule, &prefix, &x, &mask, &reduced, &exists,
                )?;
                for row in 0..c.batch {
                    for j in 0..length {
                        if mask[row * k + j] {
                            scores[row].push(result[7][row * k + j]);
                            indices[row].push(offset + j);
                        }
                    }
                }
                offset += length;
            }
            for row in 0..c.batch {
                let label = format!("{}/split{split}/family{family}/row{row}", c.name);
                emit(out, &label, &[scores[row].clone()])?;
                emit(
                    out,
                    &format!("{label}/selected"),
                    &[selected(&scores[row], &indices[row])],
                )?;
            }
        }
    }
    Ok(())
}
// 真实原生组的状态图窄验收；操作顺序由Rust输入给定，不重推资格。
#[derive(Deserialize)]
struct NativeSuite {
    layers: usize,
    hidden: usize,
    groups: Vec<NativeGroup>,
}
#[derive(Deserialize)]
struct NativeGroup {
    pre_frame_hidden_ops: Vec<serde_json::Value>,
    rows: Vec<NativeRow>,
    post_frame_hidden_ops: Vec<serde_json::Value>,
    cancel: Vec<Vec<f32>>,
}
#[derive(Deserialize)]
struct NativeRow {
    hidden_state_key: serde_json::Value,
    fast: Vec<f32>,
    slow: Vec<f32>,
    role: String,
    rule: Vec<f32>,
    #[serde(default)]
    fixture_prefixes: Vec<Vec<f32>>,
}
fn native_operations(
    ops: &[serde_json::Value],
    hidden: &mut std::collections::BTreeMap<String, Vec<f32>>,
    layers: usize,
    width: usize,
) -> Result<()> {
    for op in ops {
        if let Some(root) = op.get("ResetRootHidden") {
            hidden.retain(|key, _| {
                let key: serde_json::Value = serde_json::from_str(key).unwrap();
                key["trajectory_id"] != root["trajectory_id"]
                    || key["account_id"] != root["account_id"]
            });
        } else if let Some(value) = op.get("InitializeZero") {
            hidden.insert(
                serde_json::to_string(&value["hidden_state_key"])?,
                vec![0.; layers * width],
            );
        } else if let Some(value) = op.get("Retire") {
            hidden.remove(&serde_json::to_string(&value["hidden_state_key"])?);
        } else {
            return Err("unknown native hidden operation".into());
        }
    }
    Ok(())
}
fn native_state(
    session: &mut Session,
    actions: &mut Session,
    path: &Path,
    out: &mut impl Write,
) -> Result<()> {
    let suite: NativeSuite = serde_json::from_reader(std::fs::File::open(path)?)?;
    let mut hidden = std::collections::BTreeMap::new();
    for (g, group) in suite.groups.iter().enumerate() {
        native_operations(
            &group.pre_frame_hidden_ops,
            &mut hidden,
            suite.layers,
            suite.hidden,
        )?;
        let mut states = Vec::new();
        for (r, row) in group.rows.iter().enumerate() {
            let key = serde_json::to_string(&row.hidden_state_key)?;
            let previous = hidden
                .get(&key)
                .ok_or("native hidden key not initialized")?;
            let outputs=session.run(ort::inputs!["fast_features"=>tensor(&[1,836],&row.fast)?,"slow_features"=>tensor(&[1,412],&row.slow)?,"h_prev"=>tensor(&[suite.layers,1,suite.hidden],previous)?])?;
            let values = ["z_t", "h_next"]
                .iter()
                .map(|name| {
                    outputs[*name]
                        .try_extract_tensor::<f32>()
                        .map(|(_, v)| v.to_vec())
                })
                .collect::<ort::Result<Vec<_>>>()?;
            hidden.insert(key, values[1].clone());
            emit(out, &format!("native/{g}/{r}/state"), &values)?;
            states.push(values[0].clone());
        }
        // 整组状态帧完成后才消费参数；函数没有隐藏状态输入或更新。
        for (r, row) in group
            .rows
            .iter()
            .enumerate()
            .filter(|(_, r)| r.role == "DecisionRequired")
        {
            let count = group.cancel.len();
            let empty = count == 0;
            let lengths = if empty {
                vec![0]
            } else {
                (0..count)
                    .step_by(256)
                    .map(|i| (count - i).min(256))
                    .collect::<Vec<_>>()
            };
            let c = Case {
                name: format!("native/{g}/{r}"),
                mode: "native".into(),
                batch: 1,
                steps: 1,
                slots: count,
                empty,
                fast: row.fast.clone(),
                slow: row.slow.clone(),
                rule: row.rule.clone(),
                prefix: row.fixture_prefixes[0].clone(),
                objects: group.cancel.iter().flatten().copied().collect(),
                mask: vec![true; count],
                splits: vec![],
                prefixes: vec![],
                active: vec![],
            };
            let z = &states[r];
            let (reduced, exists) =
                encode_reduce(actions, &c, suite.hidden, z, &lengths, &c.name, out)?;
            for (p, prefix) in row.fixture_prefixes.iter().enumerate() {
                let mut offset = 0;
                for (part, &length) in lengths.iter().enumerate() {
                    let k = if empty { 1 } else { 256 };
                    let (objects, mask) = block(&c, offset, length, k, None);
                    let values = action(
                        actions,
                        1,
                        suite.hidden,
                        k,
                        z,
                        &row.rule,
                        prefix,
                        &objects,
                        &mask,
                        &reduced,
                        &exists,
                    )?;
                    let stage = prefix[7..14]
                        .iter()
                        .position(|v| *v == 1.)
                        .expect("fixture stage");
                    let head = [0, 1, 2, 4, 5, 3, 7][stage];
                    emit(
                        out,
                        &format!("{}/prefix{p}/part{part}", c.name),
                        &[values[head].clone()],
                    )?;
                    offset += length;
                }
            }
            emit(
                out,
                &format!("{}/hidden_after_parameters", c.name),
                &[hidden[&serde_json::to_string(&row.hidden_state_key)?].clone()],
            )?;
        }
        native_operations(
            &group.post_frame_hidden_ops,
            &mut hidden,
            suite.layers,
            suite.hidden,
        )?;
    }
    Ok(())
}
fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let library = Path::new(&args[1]).canonicalize()?;
    ort::init_from(&library)?.commit();
    let threads: usize = args[3].parse()?;
    let mut state_session = session(&Path::new(&args[2]).join("state_encoder.onnx"), threads)?;
    let mut action_session = session(&Path::new(&args[2]).join("action_decoder.onnx"), threads)?;

    let mut out = BufWriter::new(std::io::stdout().lock());
    writeln!(
        out,
        "{}",
        serde_json::json!({"build":ort::info(),"api":ort::MINOR_VERSION,"library":library,
        "mapped":std::fs::read_to_string("/proc/self/maps")?.lines().filter(|l|l.contains("libonnxruntime.so")).collect::<Vec<_>>(),"threads":threads})
    )?;
    if args[4] == "--native" {
        native_state(
            &mut state_session,
            &mut action_session,
            Path::new(&args[5]),
            &mut out,
        )?;
        out.flush()?;
        return Ok(());
    }
    let suite: Suite = serde_json::from_reader(std::fs::File::open(&args[4])?)?;
    for c in &suite.cases {
        let mut h = vec![0.; suite.layers * c.batch * suite.hidden];
        for step in 0..c.steps {
            let outputs = state(&mut state_session, c, suite.layers, suite.hidden, &h)?;
            let z = &outputs[0];
            h = outputs[1].clone();
            emit(&mut out, &format!("{}/state{step}", c.name), &outputs)?;
            let zero = vec![0.; c.batch * 3 * suite.hidden / 2];
            let absent = vec![false; c.batch];
            if c.mode == "objects" {
                object_case(&mut action_session, c, suite.hidden, z, &mut out)?;
            } else {
                let prefixes = if c.prefixes.is_empty() {
                    vec![c.prefix.clone()]
                } else {
                    c.prefixes.clone()
                };
                for (p, prefix) in prefixes.iter().enumerate() {
                    let selected_rows = if c.mode == "active" {
                        c.active.clone()
                    } else {
                        (0..c.batch).collect()
                    };
                    emit(
                        &mut out,
                        &format!("{}/step{step}/prefix{p}/rows", c.name),
                        &[selected_rows.iter().map(|&r| r as f32).collect()],
                    )?;
                    if selected_rows.is_empty() {
                        continue;
                    }
                    let result = action(
                        &mut action_session,
                        selected_rows.len(),
                        suite.hidden,
                        c.slots,
                        &rows(z, suite.hidden, &selected_rows),
                        &rows(&c.rule, 30, &selected_rows),
                        &rows(prefix, 60, &selected_rows),
                        &rows(&c.objects, c.slots * 129, &selected_rows),
                        &rows(&c.mask, c.slots, &selected_rows),
                        &rows(&zero, 3 * suite.hidden / 2, &selected_rows),
                        &rows(&absent, 1, &selected_rows),
                    )?;
                    emit(
                        &mut out,
                        &format!("{}/step{step}/prefix{p}/action", c.name),
                        &result,
                    )?;
                }
            }
        }
    }
    out.flush()?;
    Ok(())
}
