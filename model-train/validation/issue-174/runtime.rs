//! #174 离线 Rust ORT 宿主；打包沿生产实现，原生组由当前实例 consume 构造。
use model_native_inputs::ort_actor::OrtActorScorer;
use model_native_inputs::{HiddenOp, InputRole, NativeActor, NativeInputGroup, PackedBooks};
use ort::{session::Session, value::{Tensor, TensorRef}};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Instant};

#[derive(serde::Serialize)]
pub struct Timing {
    packing_ns: u128,
    state_ns: u128,
    state_calls: usize,
    object_reduction_ns: Vec<u128>,
    action_steps: Vec<(usize, usize, u128)>,
    frames: usize,
}
pub struct Runtime {
    state: Session,
    scorer: OrtActorScorer,
    hidden: BTreeMap<String, Vec<f32>>,
    books: Vec<PackedBooks>,
    layers: usize,
    width: usize,
    pub frames: usize,
    pub decisions: usize,
    pub load_seconds: f64,
    pub warmup_seconds: f64,
}
impl Runtime {
    pub fn new(directory: &Path, threads: usize) -> Self {
        let start = Instant::now();
        ort::init_from(std::env::var("ORT_DYLIB_PATH").unwrap())
            .unwrap()
            .commit();
        let config: Value = serde_json::from_slice(
            &std::fs::read(directory.join("benchmark_configuration.json")).unwrap(),
        )
        .unwrap();
        let layers = config["configuration"]["layers"].as_u64().unwrap() as usize;
        let width = config["configuration"]["hidden"].as_u64().unwrap() as usize;
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
                .commit_from_file(directory.join(file))
                .unwrap()
        };
        let mut runtime = Self {
            state: session("state_encoder.onnx"),
            scorer: OrtActorScorer {
                session: session("action_decoder.onnx"),
                hidden: width,
            },
            hidden: BTreeMap::new(),
            books: Vec::new(),
            layers,
            width,
            frames: 0,
            decisions: 0,
            load_seconds: start.elapsed().as_secs_f64(),
            warmup_seconds: 0.,
        };
        let warm = Instant::now();
        runtime.warm();
        runtime.warmup_seconds = warm.elapsed().as_secs_f64();
        runtime
    }
    fn warm(&mut self) {
        let packed = std::path::PathBuf::from(std::env::var("FRAME_PACKED").unwrap());
        let frames: Vec<Value> =
            serde_json::from_slice(&std::fs::read(packed.join("frames.json")).unwrap()).unwrap();
        let bytes = std::fs::read(packed.join("inputs.f32")).unwrap();
        // 沿原16个覆盖最大形状的真实帧预热，不写回hidden，不计入正式分布。
        for i in [
            0, 78, 157, 236, 314, 393, 472, 550, 629, 708, 784, 786, 865, 944, 1010, 1023,
        ] {
            let f = &frames[i];
            let offset = f["offset_f32"].as_u64().unwrap() as usize;
            let count = f["count_f32"].as_u64().unwrap() as usize;
            let values = bytes[offset * 4..(offset + count) * 4]
                .chunks_exact(4)
                .map(|v| f32::from_le_bytes(v.try_into().unwrap()))
                .collect::<Vec<_>>();
            let depths = f["depths"]
                .as_array()
                .unwrap()
                .iter()
                .map(|d| d.as_u64().unwrap() as usize)
                .collect::<Vec<_>>();
            let depth = *depths.iter().max().unwrap();
            let mut book = vec![0f32; 6 * depth.max(1) * 9];
            let mut mask = vec![false; 6 * depth.max(1)];
            let mut cursor = 1029;
            for (side, n) in depths.into_iter().enumerate() {
                book[side * depth * 9..side * depth * 9 + n * 9]
                    .copy_from_slice(&values[cursor..cursor + n * 9]);
                mask[side * depth..side * depth + n].fill(true);
                cursor += n * 9;
            }
            let _=self.state.run(ort::inputs![
                "fast_features"=>Tensor::from_array(([1,596],values[..596].to_vec())).unwrap(),"slow_features"=>Tensor::from_array(([1,433],values[596..1029].to_vec())).unwrap(),
                "book_levels"=>Tensor::from_array(([1,6,depth.max(1),9],book)).unwrap(),"book_mask"=>Tensor::from_array(([1,6,depth.max(1)],mask)).unwrap(),
                "h_prev"=>Tensor::from_array(([self.layers,1,self.width],vec![0f32;self.layers*self.width])).unwrap()]).unwrap();
        }
        // 动作会话也预热；它仅是空人口图输入，不伪装成市场Actor。
        for _ in 0..16 {
            self.empty_action_tensor();
        }
    }
    fn empty_action_tensor(&mut self) {
        let _=self.scorer.session.run(ort::inputs![
                "z_t"=>Tensor::from_array(([1,self.width],vec![0f32;self.width])).unwrap(),"rule_features"=>Tensor::from_array(([1,30],vec![0f32;30])).unwrap(),
                "prefix_features"=>Tensor::from_array(([1,60],vec![0f32;60])).unwrap(),"cancel_features"=>Tensor::from_array(([1,1,184],vec![0f32;184])).unwrap(),
                "cancel_mask"=>Tensor::from_array(([1,1],vec![false])).unwrap(),"cancel_reduced"=>Tensor::from_array(([1,3*self.width/2],vec![0f32;3*self.width/2])).unwrap(),
                "cancel_exists"=>Tensor::from_array(([1],vec![false])).unwrap(),"price_fee_features"=>Tensor::from_array(([1,9999],vec![0f32;9999])).unwrap()]).unwrap();
    }
    #[cfg(test)]
    pub fn empty_tensor_probe(&mut self) -> Vec<u128> {
        (0..16)
            .map(|_| {
                let start = Instant::now();
                self.empty_action_tensor();
                start.elapsed().as_nanos()
            })
            .collect()
    }
    fn operations(&mut self, ops: &[HiddenOp]) {
        for op in ops {
            match op {
                HiddenOp::ResetRootHidden { .. } => {
                    self.hidden.clear();
                    self.books.clear();
                },
                HiddenOp::InitializeZero { hidden_state_key } => {
                    self.hidden.insert(
                        serde_json::to_string(hidden_state_key).unwrap(),
                        vec![0f32; self.layers * self.width],
                    );
                }
                HiddenOp::Retire { hidden_state_key } => {
                    self.books.clear();
                    assert!(
                        self.hidden
                            .remove(&serde_json::to_string(hidden_state_key).unwrap())
                            .is_some()
                    );
                }
            }
        }
    }
    pub fn hidden_digest(&self) -> String {
        let mut digest = Sha256::new();
        for (k, h) in &self.hidden {
            digest.update(k.as_bytes());
            for v in h {
                digest.update(v.to_bits().to_le_bytes());
            }
        }
        format!("{:x}", digest.finalize())
    }
    pub fn checkpoint(&self) -> Value {
        json!({"layers":self.layers,"width":self.width,"frames":self.frames,"decisions":self.decisions,
            "hidden_bits":self.hidden.iter().map(|(k,v)|(k,v.iter().map(|v|v.to_bits()).collect::<Vec<_>>())).collect::<BTreeMap<_,_>>()})
    }
    pub fn restore(&mut self, value: &Value) {
        assert_eq!(value["layers"], self.layers);
        assert_eq!(value["width"], self.width);
        self.frames = value["frames"].as_u64().unwrap() as usize;
        self.decisions = value["decisions"].as_u64().unwrap() as usize;
        let bits: BTreeMap<String, Vec<u32>> =
            serde_json::from_value(value["hidden_bits"].clone()).unwrap();
        self.hidden = bits
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().map(f32::from_bits).collect()))
            .collect();
    }
    pub fn advance(
        &mut self,
        group: Arc<NativeInputGroup>,
        autonomous: bool,
    ) -> (Vec<NativeActor>, Timing, Vec<Vec<f32>>) {
        let mut packing = 0u128;
        let mut state_ns = 0u128;
        let mut states = Vec::new();
        self.operations(&group.pre_frame_hidden_ops);
        self.books.resize_with(group.rows.len(), Default::default);
        for (index, row) in group.rows.iter().enumerate() {
            let start = Instant::now();
            let key = serde_json::to_string(&row.hidden_state_key).unwrap();
            let books = &mut self.books[index];
            books.update([(&row.hidden_state_key, &row.book_levels)]);
            let depth = books.depth();
            let inputs = ort::inputs![
                "fast_features"=>TensorRef::from_array_view(([1,596],row.fast.as_slice())).unwrap(),
                "slow_features"=>TensorRef::from_array_view(([1,433],row.slow.as_slice())).unwrap(),
                "book_levels"=>TensorRef::from_array_view(([1,6,depth,9],books.values())).unwrap(),
                "book_mask"=>TensorRef::from_array_view(([1,6,depth],books.mask())).unwrap(),
                "h_prev"=>TensorRef::from_array_view(([self.layers,1,self.width],self.hidden[&key].as_slice())).unwrap()
            ];
            packing += start.elapsed().as_nanos();
            let start = Instant::now();
            let output = self.state.run(inputs).unwrap();
            state_ns += start.elapsed().as_nanos();
            let z = output["z_t"]
                .try_extract_tensor::<f32>()
                .unwrap()
                .1
                .to_vec();
            let h = output["h_next"]
                .try_extract_tensor::<f32>()
                .unwrap()
                .1
                .to_vec();
            assert!(z.iter().chain(&h).all(|x| x.is_finite()));
            self.hidden.insert(key, h);
            states.push(z);
            self.frames += 1;
        }
        self.operations(&group.post_frame_hidden_ops);
        let mut actors = Vec::new();
        let mut calls = Vec::new();
        let mut reductions = Vec::new();
        if autonomous {
            for (i, row) in group.rows.iter().enumerate() {
                if row.role != InputRole::DecisionRequired {
                    continue;
                }
                let mut actor = NativeActor::new(group.clone(), i).unwrap();
                let start = Instant::now();
                let reduced = self.scorer.reduced(&actor, &states[i]).unwrap();
                reductions.push(start.elapsed().as_nanos());
                while !actor.is_complete() {
                    let head = actor.head_index();
                    let start = Instant::now();
                    let support = actor.support().unwrap();
                    let scores = self.scorer.scores(&actor, &states[i], &reduced).unwrap();
                    let mut best = None;
                    for (j, (&legal, &score)) in support.iter().zip(&scores).enumerate() {
                        if legal {
                            assert!(score.is_finite());
                            if best.is_none_or(|(_, old)| score > old) {
                                best = Some((j, score));
                            }
                        }
                    }
                    actor.choose(best.unwrap().0).unwrap();
                    calls.push((i, head, start.elapsed().as_nanos()));
                }
                self.decisions += 1;
                actors.push(actor);
            }
        }
        (
            actors,
            Timing {
                packing_ns: packing,
                state_ns,
                state_calls: group.rows.len(),
                object_reduction_ns: reductions,
                action_steps: calls,
                frames: self.frames,
            },
            states,
        )
    }

    /// 只供独立真实业务夹具；强制合法家族，参数仍经同一支持集和精确前缀推进。
    #[cfg(test)]
    pub fn directed(
        &mut self,
        group: Arc<NativeInputGroup>,
        row: usize,
        z: &[f32],
        family: usize,
    ) -> (NativeActor, Value) {
        let start = Instant::now();
        let mut actor = NativeActor::new(group, row).unwrap();
        let began = Instant::now();
        let reduced = self.scorer.reduced(&actor, z).unwrap();
        let reduction_ns = began.elapsed().as_nanos();
        let mut steps = Vec::new();
        while !actor.is_complete() {
            let began = Instant::now();
            let head = actor.head_index();
            let support = actor.support().unwrap();
            let scores = self.scorer.scores(&actor, z, &reduced).unwrap();
            assert_eq!(support.len(), scores.len());
            assert!(
                scores
                    .iter()
                    .zip(&support)
                    .all(|(v, valid)| !valid || v.is_finite())
            );
            let selected = if actor.path.is_empty() {
                family
            } else if head == 2 && support[1] {
                1
            } else {
                support.iter().position(|valid| *valid).unwrap()
            };
            assert!(support[selected]);
            actor.choose(selected).unwrap();
            steps.push(json!({"head":head,"choice":selected,"ns":began.elapsed().as_nanos()}));
        }
        let result = json!({"action_service_ns":start.elapsed().as_nanos(),"object_reduction_ns":reduction_ns,"steps":steps,"path":actor.path,"cancel_objects":actor.group.cancel.len(),"intent":format!("{:?}",actor.intent().unwrap())});
        (actor, result)
    }
}
