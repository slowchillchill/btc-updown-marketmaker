//! 测量宿主：长寿命ORT会话、独立隐藏历史、真实活动行压缩回填。
use crate::ort_actor::OrtActorScorer;
use crate::*;
use ort::{session::Session, value::TensorRef};
use pyo3::prelude::*;
use std::{collections::BTreeMap, path::Path, sync::Arc};

type Step = (Vec<bool>, String, Vec<f32>, Vec<f32>);
type Evidence = (
    Vec<(Vec<f32>, Vec<f32>)>,
    Vec<(usize, usize, Vec<usize>, Vec<Step>)>,
);
fn close(reference: &[f32], actual: &[f32]) {
    assert_eq!(reference.len(), actual.len());
    for (i, (&r, &a)) in reference.iter().zip(actual).enumerate() {
        assert!(
            r.is_finite() && a.is_finite() && (r - a).abs() <= 1e-5 + 1e-4 * r.abs(),
            "numeric mismatch index={i} reference={r} actual={a}"
        );
    }
}
fn close_supported(reference: &[f32], actual: &[f32], support: &[bool]) {
    assert_eq!(reference.len(), actual.len());
    assert_eq!(reference.len(), support.len());
    for (index, ((&reference, &actual), &legal)) in
        reference.iter().zip(actual).zip(support).enumerate()
    {
        if legal {
            assert!(
                reference.is_finite()
                    && actual.is_finite()
                    && (reference - actual).abs() <= 1e-5 + 1e-4 * reference.abs(),
                "supported score mismatch original_index={index} reference={reference} actual={actual}"
            );
        }
    }
}
fn key(key: &HiddenKey) -> String {
    serde_json::to_string(key).unwrap()
}

pub(super) struct ThreeRuntime {
    python: Py<PyAny>,
    state: Session,
    packed_books: Vec<crate::PackedBooks>,
    scorer: OrtActorScorer,
    hidden: BTreeMap<String, (HiddenKey, Vec<f32>)>,
    layers: usize,
    width: usize,
    pub frames: usize,
    pub decisions: usize,
    hidden_operations: usize,
    last_states: Vec<Vec<Vec<f32>>>,
    timings: BTreeMap<&'static str, Vec<f64>>,
}
impl ThreeRuntime {
    pub fn new(directory: &str, threads: usize) -> Self {
        ort::init_from(std::env::var("ORT_DYLIB_PATH").unwrap())
            .unwrap()
            .commit();
        let (python, layers, width) = Python::attach(|py| {
            let runtime = py
                .import("model_train.continuous_validation")
                .unwrap()
                .getattr("ContinuousPython")
                .unwrap()
                .call1((directory, threads))
                .unwrap();
            let cfg = runtime.getattr("config").unwrap();
            let layers = cfg.getattr("layers").unwrap().extract().unwrap();
            let width = cfg.getattr("hidden").unwrap().extract().unwrap();
            eprintln!(
                "python loading_seconds={}",
                runtime.getattr("loading_seconds").unwrap()
            );
            (runtime.unbind(), layers, width)
        });
        let session = |file| {
            Session::builder()
                .unwrap()
                .with_optimization_level(ort::session::builder::GraphOptimizationLevel::All)
                .unwrap()
                .with_execution_providers([ort::ep::CPU::default()
                    .with_arena_allocator(true)
                    .build()])
                .unwrap()
                .with_intra_threads(threads)
                .unwrap()
                .with_inter_threads(1)
                .unwrap()
                .with_parallel_execution(false)
                .unwrap()
                .with_memory_pattern(false)
                .unwrap()
                .with_intra_op_spinning(false)
                .unwrap()
                .with_inter_op_spinning(false)
                .unwrap()
                .commit_from_file(Path::new(directory).join(file))
                .unwrap()
        };
        Self {
            python,
            state: session("state_encoder.onnx"),
            packed_books: Vec::new(),
            scorer: OrtActorScorer {
                measurement: (0, None),
                session: session("action_decoder.onnx"),
                hidden: width,
            },
            hidden: BTreeMap::new(),
            layers,
            width,
            frames: 0,
            decisions: 0,
            hidden_operations: 0,
            timings: BTreeMap::new(),
            last_states: Vec::new(),
        }
    }
    pub fn enable_population(&mut self) {
        self.scorer.measurement.1 = Some(Vec::new());
        Python::attach(|py| {
            self.python
                .bind(py)
                .setattr("keep_population", true)
                .unwrap()
        });
    }
    pub fn audit_population(&mut self, groups: &[Arc<NativeInputGroup>], seed: usize) {
        let state_calls_before = self.timings.get("state_native").map_or(0, Vec::len);
        type Probe = (usize, usize, Vec<f32>, Vec<bool>, Vec<usize>);
        let evidence: Vec<Probe> = Python::attach(|py| {
            let native = groups
                .iter()
                .map(|group| {
                    Py::new(
                        py,
                        crate::python::PythonGroup {
                            group: group.clone(),
                        },
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();
            self.python
                .call_method1(py, "audit_population", (native, seed))
                .unwrap()
                .extract(py)
                .unwrap()
        });
        if groups.len() == 2 {
            let rows = groups
                .iter()
                .map(|g| {
                    g.rows
                        .iter()
                        .position(|r| r.role == InputRole::DecisionRequired)
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let mut actors = groups
                .iter()
                .zip(&rows)
                .map(|(g, &row)| NativeActor::new(g.clone(), row).unwrap())
                .collect::<Vec<_>>();
            let reduced = actors
                .iter()
                .enumerate()
                .map(|(i, a)| {
                    self.scorer
                        .reduced(a, &self.last_states[i][rows[i]])
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let entries = actors
                .iter()
                .enumerate()
                .map(|(i, a)| {
                    (
                        a,
                        self.last_states[i][rows[i]].as_slice(),
                        reduced[i].as_slice(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(self.scorer.scores_batch(&entries).unwrap().len(), 2);
            actors[0].choose(0).unwrap();
            actors[1].choose(4).unwrap();
            let active = actors
                .iter()
                .enumerate()
                .filter(|(_, a)| !a.is_complete())
                .map(|(i, _)| i)
                .collect::<Vec<_>>();
            assert_eq!(active, vec![1]);
            let entries = active
                .iter()
                .map(|&i| {
                    (
                        &actors[i],
                        self.last_states[i][rows[i]].as_slice(),
                        reduced[i].as_slice(),
                    )
                })
                .collect::<Vec<_>>();
            let scores = self.scorer.scores_batch(&entries).unwrap();
            let expected = evidence
                .iter()
                .find(|(root, family, ..)| *root == 1 && *family == 4)
                .unwrap();
            close_supported(&expected.2, &scores[0], &expected.3);
            actors[1].choose(*expected.4.last().unwrap()).unwrap();
            assert_eq!(actors[0].path, vec![0]);
            assert!(actors.iter().all(NativeActor::is_complete));
            let before_empty = self.scorer.measurement.0;
            assert!(self.scorer.scores_batch(&[]).unwrap().is_empty());
            assert_eq!(self.scorer.measurement.0, before_empty);
        }
        // P6/P7 在同一真实人口上直接调用原公开块入口；不改变 C 的生产分块。
        if groups.iter().any(|g| g.cancel.len() == 257) {
            for family in [3, 4, 5] {
                let actors = groups
                    .iter()
                    .map(|g| {
                        let row = g
                            .rows
                            .iter()
                            .position(|r| r.role == InputRole::DecisionRequired)
                            .unwrap();
                        let mut actor = NativeActor::new(g.clone(), row).unwrap();
                        if actor.support().unwrap()[family] {
                            actor.choose(family).unwrap();
                        }
                        actor
                    })
                    .collect::<Vec<_>>();
                let width = self.width / 2;
                let zero = vec![0.; 3 * width];
                let fees = vec![0.; 9999];
                let mut layouts = Vec::new();
                for lengths in [[256usize, 1], [128, 129]] {
                    let before = self.scorer.measurement.0;
                    let mut sums = vec![vec![0f64; width]; groups.len()];
                    let mut maxima = vec![vec![f32::NEG_INFINITY; width]; groups.len()];
                    let mut offset = 0;
                    for length in lengths {
                        let rows = actors
                            .iter()
                            .enumerate()
                            .map(|(i, a)| {
                                let n = a.group.cancel.len();
                                (
                                    a,
                                    self.last_states[i][a.row].as_slice(),
                                    &a.group.cancel[offset.min(n)..(offset + length).min(n)],
                                    zero.as_slice(),
                                    false,
                                    fees.as_slice(),
                                )
                            })
                            .collect::<Vec<_>>();
                        let output = self.scorer.blocks(&rows).unwrap();
                        for (i, values) in output.iter().enumerate() {
                            for embedding in
                                values[6][..rows[i].2.len() * width].chunks_exact(width)
                            {
                                for k in 0..width {
                                    sums[i][k] += f64::from(embedding[k]);
                                    maxima[i][k] = maxima[i][k].max(embedding[k]);
                                }
                            }
                        }
                        offset += length;
                    }
                    let reduced = groups
                        .iter()
                        .enumerate()
                        .map(|(i, g)| {
                            if g.cancel.is_empty() {
                                zero.clone()
                            } else {
                                sums[i]
                                    .iter()
                                    .map(|x| *x as f32)
                                    .chain(
                                        sums[i].iter().map(|x| (*x / g.cancel.len() as f64) as f32),
                                    )
                                    .chain(maxima[i].iter().copied())
                                    .collect::<Vec<_>>()
                            }
                        })
                        .collect::<Vec<_>>();
                    let mut scores = vec![Vec::new(); groups.len()];
                    let mut offset = 0;
                    for length in lengths {
                        let active = actors
                            .iter()
                            .enumerate()
                            .filter_map(|(i, _)| {
                                evidence
                                    .iter()
                                    .any(|(root, f, ..)| *root == i && *f == family)
                                    .then_some(i)
                            })
                            .collect::<Vec<_>>();
                        let rows = active
                            .iter()
                            .map(|&i| {
                                let a = &actors[i];
                                let n = a.group.cancel.len();
                                (
                                    a,
                                    self.last_states[i][a.row].as_slice(),
                                    &a.group.cancel[offset.min(n)..(offset + length).min(n)],
                                    reduced[i].as_slice(),
                                    n > 0,
                                    fees.as_slice(),
                                )
                            })
                            .collect::<Vec<_>>();
                        let output = self.scorer.blocks(&rows).unwrap();
                        for ((&i, row), values) in active.iter().zip(&rows).zip(&output) {
                            scores[i].extend_from_slice(&values[7][..row.2.len()]);
                        }
                        offset += length;
                    }
                    assert_eq!(self.scorer.measurement.0 - before, 4);
                    let mut choices = Vec::new();
                    for (i, g) in groups.iter().enumerate() {
                        if g.cancel.is_empty() {
                            assert!(scores[i].is_empty());
                            choices.push(None);
                            continue;
                        }
                        let Some(expected) = evidence
                            .iter()
                            .find(|(root, f, ..)| *root == i && *f == family)
                        else {
                            assert!(!actors[i].support().unwrap()[family]);
                            assert!(scores[i].is_empty(), "无支持族不进入第二遍评分");
                            choices.push(None);
                            continue;
                        };
                        close_supported(&expected.2, &scores[i], &expected.3);
                        let choice = expected
                            .3
                            .iter()
                            .zip(&scores[i])
                            .enumerate()
                            .filter(|(_, (legal, _))| **legal)
                            .max_by(|(ia, (_, a)), (ib, (_, b))| {
                                a.total_cmp(b).then_with(|| ib.cmp(ia))
                            })
                            .unwrap()
                            .0;
                        assert_eq!(Some(&choice), expected.4.last());
                        choices.push(Some(choice));
                    }
                    eprintln!(
                        "population_native_layout={}",
                        serde_json::json!({"seed":seed,"family":family,"lengths":lengths,"root_mapping":(0..groups.len()).collect::<Vec<_>>(),"graph_calls":self.scorer.measurement.0-before,"sum_mean_max":reduced,"global_scores":scores,"global_choices":choices})
                    );
                    layouts.push((reduced, scores, choices));
                }
                for i in 0..groups.len() {
                    close(&layouts[0].0[i], &layouts[1].0[i]);
                    if let Some((_, _, _, support, _)) = evidence
                        .iter()
                        .find(|(root, f, ..)| *root == i && *f == family)
                    {
                        close_supported(&layouts[0].1[i], &layouts[1].1[i], support);
                    } else {
                        assert!(layouts[0].1[i].is_empty() && layouts[1].1[i].is_empty());
                    }
                }
                assert_eq!(layouts[0].2, layouts[1].2);
            }
        }
        for (root, family, want, support, path) in evidence {
            let row = groups[root]
                .rows
                .iter()
                .position(|r| r.role == InputRole::DecisionRequired)
                .unwrap();
            let mut actor = NativeActor::new(groups[root].clone(), row).unwrap();
            actor.choose(family).unwrap();
            assert_eq!(actor.support().unwrap(), support);
            let reduced = self
                .scorer
                .reduced(&actor, &self.last_states[root][row])
                .unwrap();
            let scores = self
                .scorer
                .scores_batch(&[(&actor, &self.last_states[root][row], &reduced)])
                .unwrap()
                .remove(0);
            close_supported(&want, &scores, &support);
            let choice = support
                .iter()
                .zip(&scores)
                .enumerate()
                .filter(|(_, (legal, _))| **legal)
                .max_by(|(ia, (_, a)), (ib, (_, b))| a.total_cmp(b).then_with(|| ib.cmp(ia)))
                .unwrap()
                .0;
            actor.choose(choice).unwrap();
            assert_eq!(actor.path, path);
            assert!(actor.intent().unwrap().is_some());
        }
        assert_eq!(
            self.timings.get("state_native").map_or(0, Vec::len),
            state_calls_before,
            "P参数链不推进GRU"
        );
        let python_report = Python::attach(|py| {
            py.import("json")
                .unwrap()
                .call_method1(
                    "dumps",
                    (self.python.bind(py).getattr("population_report").unwrap(),),
                )
                .unwrap()
                .extract::<String>()
                .unwrap()
        });
        eprintln!(
            "population_graph_calls={}",
            serde_json::json!({"seed":seed,"native_valid_rows_per_call":self.scorer.measurement.1.as_ref().unwrap(),"python":serde_json::from_str::<serde_json::Value>(&python_report).unwrap(),"parameter_gru_calls":0,"runtime":self.sample()})
        );
        self.scorer.measurement.1.as_mut().unwrap().clear();
    }
    pub fn provenance(&self) -> serde_json::Value {
        Python::attach(|py| {
            let value = self.python.bind(py).getattr("provenance").unwrap();
            let encoded = py
                .import("json")
                .unwrap()
                .call_method1("dumps", (value,))
                .unwrap()
                .extract::<String>()
                .unwrap();
            serde_json::from_str(&encoded).unwrap()
        })
    }
    pub fn sample(&self) -> serde_json::Value {
        let mut value: serde_json::Value = Python::attach(|py| {
            let result = self.python.call_method0(py, "sample").unwrap();
            let json = py
                .import("json")
                .unwrap()
                .call_method1("dumps", (result,))
                .unwrap()
                .extract::<String>()
                .unwrap();
            serde_json::from_str(&json).unwrap()
        });
        value["native_held_z_elements"] = self
            .last_states
            .iter()
            .flatten()
            .map(Vec::len)
            .sum::<usize>()
            .into();
        value["actual_frames_total"] = self.frames.into();
        value["native_hidden_operations_since_statistics"] = self.hidden_operations.into();
        value["native_state_calls_since_statistics"] =
            self.timings.get("state_native").map_or(0, Vec::len).into();
        value["native_action_graph_calls_including_object_encoding"] =
            self.scorer.measurement.0.into();
        value["native_action_steps_since_statistics"] =
            self.timings.get("action_native").map_or(0, Vec::len).into();
        value["native_hidden_rows"] = self.hidden.len().into();
        value["native_hidden_elements"] = self
            .hidden
            .values()
            .map(|(_, h)| h.len())
            .sum::<usize>()
            .into();
        value
    }
    pub fn release_observation(&mut self) {
        Python::attach(|py| {
            self.python.call_method0(py, "release_observation").unwrap();
        });
        self.hidden.clear();
        self.packed_books.clear();
        self.last_states.clear();
        assert_eq!(self.sample()["hidden_rows"], serde_json::json!([0, 0]));
    }
    pub fn statistics(&mut self) -> serde_json::Value {
        let mut value: serde_json::Value = Python::attach(|py| {
            let result = self.python.call_method1(py, "statistics", (true,)).unwrap();
            let json = py
                .import("json")
                .unwrap()
                .call_method1("dumps", (result,))
                .unwrap()
                .extract::<String>()
                .unwrap();
            serde_json::from_str(&json).unwrap()
        });
        value["native_hidden_operations"] = self.hidden_operations.into();
        self.hidden_operations = 0;
        for (name, mut samples) in std::mem::take(&mut self.timings) {
            samples.sort_by(f64::total_cmp);
            if !samples.is_empty() {
                value[name] = serde_json::json!({"count":samples.len(),"median_seconds":samples[samples.len()/2],"p95_seconds":samples[(samples.len()-1)*95/100],"max_seconds":samples.last()});
            }
        }
        value
    }
    fn operations(&mut self, operations: &[HiddenOp]) {
        self.hidden_operations += operations.len();
        for op in operations {
            match op {
                HiddenOp::ResetRootHidden {
                    trajectory_id,
                    account_id,
                } => {
                    self.packed_books.clear();
                    self.hidden.retain(|_, (k, _)| {
                        &k.trajectory_id != trajectory_id || &k.account_id != account_id
                    });
                }
                HiddenOp::InitializeZero { hidden_state_key } => {
                    self.hidden.insert(
                        key(hidden_state_key),
                        (hidden_state_key.clone(), vec![0.; self.layers * self.width]),
                    );
                }
                HiddenOp::Retire { hidden_state_key } => {
                    self.packed_books.clear();
                    assert!(self.hidden.remove(&key(hidden_state_key)).is_some());
                }
            }
        }
    }
    pub fn advance(
        &mut self,
        groups: &[Arc<NativeInputGroup>],
        autonomous: bool,
    ) -> Vec<(usize, NativeActor)> {
        self.advance_selected(groups, &vec![autonomous; groups.len()])
    }
    pub fn advance_selected(
        &mut self,
        groups: &[Arc<NativeInputGroup>],
        selected: &[bool],
    ) -> Vec<(usize, NativeActor)> {
        assert_eq!(groups.len(), selected.len());
        let (traces, decisions): Evidence = Python::attach(|py| {
            let native = groups
                .iter()
                .map(|group| {
                    Py::new(
                        py,
                        crate::python::PythonGroup {
                            group: group.clone(),
                        },
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();
            self.python
                .call_method1(py, "advance", (native, selected.to_vec()))
                .unwrap()
                .extract(py)
                .unwrap()
        });
        for g in groups {
            self.operations(&g.pre_frame_hidden_ops);
        }
        let mut states = groups
            .iter()
            .map(|g| vec![Vec::new(); g.rows.len()])
            .collect::<Vec<_>>();
        self.packed_books
            .resize_with(traces.len(), Default::default);
        for (frame, (want_z, want_h)) in traces.iter().enumerate() {
            let active = groups
                .iter()
                .enumerate()
                .filter_map(|(i, g)| g.rows.get(frame).map(|row| (i, row)))
                .collect::<Vec<_>>();
            let batch = active.len();
            let fast = active
                .iter()
                .flat_map(|(_, r)| r.fast.iter().copied())
                .collect::<Vec<_>>();
            let slow = active
                .iter()
                .flat_map(|(_, r)| r.slow.iter().copied())
                .collect::<Vec<_>>();
            let mut h = Vec::new();
            for layer in 0..self.layers {
                for (_, row) in &active {
                    h.extend_from_slice(
                        &self.hidden[&key(&row.hidden_state_key)].1
                            [layer * self.width..(layer + 1) * self.width],
                    );
                }
            }
            let books = &mut self.packed_books[frame];
            books.update(
                active
                    .iter()
                    .map(|(_, r)| (&r.hidden_state_key, &r.book_levels)),
            );
            let depth = books.depth();
            let state_started = std::time::Instant::now();
            let output = self
                .state
                .run(ort::inputs![
                    "fast_features" => TensorRef::from_array_view(([batch,596],fast.as_slice())).unwrap(),
                    "slow_features" => TensorRef::from_array_view(([batch,433],slow.as_slice())).unwrap(),
                    "book_levels" => TensorRef::from_array_view(([batch,6,depth,9],books.values())).unwrap(),
                    "book_mask" => TensorRef::from_array_view(([batch,6,depth],books.mask())).unwrap(),
                    "h_prev" => TensorRef::from_array_view(([self.layers,batch,self.width],h.as_slice())).unwrap()
                ])
                .unwrap();
            self.timings
                .entry("state_native")
                .or_default()
                .push(state_started.elapsed().as_secs_f64());
            let z = output["z_t"].try_extract_tensor::<f32>().unwrap().1;
            let h = output["h_next"].try_extract_tensor::<f32>().unwrap().1;
            close(want_z, z);
            close(want_h, h);
            for (b, (index, row)) in active.iter().enumerate() {
                states[*index][frame] = z[b * self.width..(b + 1) * self.width].to_vec();
                let state = &mut self.hidden.get_mut(&key(&row.hidden_state_key)).unwrap().1;
                for layer in 0..self.layers {
                    state[layer * self.width..(layer + 1) * self.width].copy_from_slice(
                        &h[(layer * batch + b) * self.width..(layer * batch + b + 1) * self.width],
                    );
                }
            }
            self.frames += batch;
        }
        assert_eq!(
            traces.len(),
            groups.iter().map(|g| g.rows.len()).max().unwrap_or(0)
        );
        for g in groups {
            self.operations(&g.post_frame_hidden_ops);
        }
        let expected = groups
            .iter()
            .enumerate()
            .flat_map(|(root, g)| {
                g.rows.iter().enumerate().filter_map(move |(row, r)| {
                    (selected[root] && r.role == InputRole::DecisionRequired).then_some((root, row))
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            expected,
            decisions
                .iter()
                .map(|(root, row, _, _)| (*root, *row))
                .collect::<Vec<_>>()
        );
        let reduction_started = std::time::Instant::now();
        let mut actors = decisions
            .iter()
            .map(|(index, row, _, _)| NativeActor::new(groups[*index].clone(), *row).unwrap())
            .collect::<Vec<_>>();
        let reduced = actors
            .iter()
            .zip(&decisions)
            .map(|(actor, (index, row, _, _))| {
                self.scorer.reduced(actor, &states[*index][*row]).unwrap()
            })
            .collect::<Vec<_>>();
        self.timings
            .entry("reduction_native")
            .or_default()
            .push(reduction_started.elapsed().as_secs_f64());
        let state_calls_before_parameters = self.timings.get("state_native").map_or(0, Vec::len);
        let mut step = 0;
        while actors.iter().any(|a| !a.is_complete()) {
            let active = actors
                .iter()
                .enumerate()
                .filter(|(_, a)| !a.is_complete())
                .map(|(i, _)| i)
                .collect::<Vec<_>>();
            let entries = active
                .iter()
                .map(|&i| {
                    let (root, row, _, _) = &decisions[i];
                    (
                        &actors[i],
                        states[*root][*row].as_slice(),
                        reduced[i].as_slice(),
                    )
                })
                .collect::<Vec<_>>();
            let action_started = std::time::Instant::now();
            let scores = self.scorer.scores_batch(&entries).unwrap();
            self.timings
                .entry("action_native")
                .or_default()
                .push(action_started.elapsed().as_secs_f64());
            for (&i, actual) in active.iter().zip(scores) {
                let (_, _, path, steps) = &decisions[i];
                let (support, prefix, features, want) = &steps[step];
                let actor = &mut actors[i];
                let support_started = std::time::Instant::now();
                let actual_support = actor.support().unwrap();
                self.timings
                    .entry("support_native")
                    .or_default()
                    .push(support_started.elapsed().as_secs_f64());
                assert_eq!(&actual_support, support);
                assert_eq!(&format!("{:?}", actor.state), prefix);
                assert_eq!(actor.prefix_features().as_slice(), features);
                close_supported(want, &actual, support);
                let mut best = None;
                for (index, (&legal, &score)) in support.iter().zip(&actual).enumerate() {
                    if legal && best.is_none_or(|(_, old)| score > old) {
                        best = Some((index, score));
                    }
                }
                let choice = best.unwrap().0;
                assert_eq!(
                    choice, path[step],
                    "strict RustORT choice mismatch root={} row={} step={step} cutoff={:?}",
                    decisions[i].0, decisions[i].1, groups[decisions[i].0].cutoff
                );
                actor.choose(choice).unwrap();
            }
            step += 1;
        }
        assert_eq!(
            self.timings.get("state_native").map_or(0, Vec::len),
            state_calls_before_parameters
        );
        self.decisions += actors.len();
        self.last_states = states;
        actors
            .into_iter()
            .zip(decisions)
            .map(|(actor, (root, row, path, _))| {
                assert_eq!(actor.path, path);
                let mut replay = NativeActor::new(groups[root].clone(), row).unwrap();
                for choice in path {
                    replay.choose(choice).unwrap();
                }
                assert_eq!(actor.intent().unwrap(), replay.intent().unwrap());
                (root, actor)
            })
            .collect()
    }
}
