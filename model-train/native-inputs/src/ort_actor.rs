//! native_actions_v2 分图消费者；支持、金额与全局选择仍由 NativeActor 唯一决定。
use crate::NativeActor;
use ort::{session::Session, value::Tensor};
use std::{cell::RefCell, collections::BTreeMap, path::Path};

type Error = Box<dyn std::error::Error>;
/// 只属于当前冻结决定；持有自己的编码，不覆盖旧帧或其他 Actor 的缓存。
#[derive(serde::Serialize)]
pub struct PreparedDecision {
    pub reduced: Vec<f32>,
    pub embeddings: Vec<f32>,
    #[serde(skip)]
    family: RefCell<Option<(Vec<f32>, Vec<f32>)>>,
}
pub type ActorBlockRow<'a> = (
    &'a NativeActor,
    &'a [f32],
    &'a [Vec<f32>],
    &'a [f32],
    bool,
    &'a [f32],
);

pub struct OrtActorScorer {
    pub objects: Session,
    pub family: Session,
    pub quantity: Session,
    pub variant: Session,
    pub price: Session,
    pub cancel_context: Session,
    pub cancel: Session,
    pub hidden: usize,
    pub calls: BTreeMap<&'static str, usize>,
    #[cfg(all(test, feature = "continuous-validation"))]
    pub measurement: (usize, Option<Vec<Vec<usize>>>),
}
impl OrtActorScorer {
    pub fn new(directory: &Path, hidden: usize, threads: usize) -> ort::Result<Self> {
        let session = |name: &str| -> ort::Result<Session> {
            Session::builder()?
                .with_optimization_level(ort::session::builder::GraphOptimizationLevel::All)?
                .with_execution_providers([ort::ep::CPU::default()
                    .with_arena_allocator(true)
                    .build()])?
                .with_intra_threads(threads)?
                .with_inter_threads(1)?
                .with_parallel_execution(false)?
                .with_memory_pattern(false)?
                .with_intra_op_spinning(false)?
                .with_inter_op_spinning(false)?
                .commit_from_file(directory.join(format!("action_{name}.onnx")))
        };
        Ok(Self {
            objects: session("objects")?,
            family: session("family")?,
            quantity: session("quantity")?,
            variant: session("variant")?,
            price: session("price")?,
            cancel_context: session("cancel_context")?,
            cancel: session("cancel")?,
            hidden,
            calls: BTreeMap::new(),
            #[cfg(all(test, feature = "continuous-validation"))]
            measurement: (0, None),
        })
    }
    fn called(&mut self, name: &'static str, lengths: &[usize]) {
        *self.calls.entry(name).or_default() += 1;
        #[cfg(all(test, feature = "continuous-validation"))]
        {
            self.measurement.0 += 1;
            if let Some(rows) = &mut self.measurement.1 {
                rows.push(lengths.to_vec());
            }
        }
        let _ = lengths;
    }
    fn encode(&mut self, blocks: &[&[Vec<f32>]]) -> ort::Result<Vec<Vec<f32>>> {
        let batch = blocks.len();
        if batch == 0 {
            return Ok(Vec::new());
        }
        let slots = blocks.iter().map(|b| b.len()).max().unwrap().max(1);
        let mut values = vec![0.; batch * slots * 184];
        let mut mask = vec![false; batch * slots];
        for (b, block) in blocks.iter().enumerate() {
            for (i, feature) in block.iter().enumerate() {
                values[(b * slots + i) * 184..(b * slots + i + 1) * 184].copy_from_slice(feature);
                mask[b * slots + i] = true;
            }
        }
        self.called(
            "objects",
            &blocks.iter().map(|b| b.len()).collect::<Vec<_>>(),
        );
        let out = self.objects.run(ort::inputs![
            "cancel_features" => Tensor::from_array(([batch, slots, 184], values))?,
            "cancel_mask" => Tensor::from_array(([batch, slots], mask))?
        ])?;
        let data = out["cancel_embeddings"].try_extract_tensor::<f32>()?.1;
        Ok(data
            .chunks_exact(slots * self.hidden / 2)
            .zip(blocks)
            .map(|(row, b)| row[..b.len() * self.hidden / 2].to_vec())
            .collect())
    }
    pub fn reduced(&mut self, actor: &NativeActor, _z: &[f32]) -> ort::Result<PreparedDecision> {
        let width = self.hidden / 2;
        let mut sum = vec![0f64; width];
        let mut max = vec![f32::NEG_INFINITY; width];
        let mut embeddings = Vec::with_capacity(actor.group.cancel.len() * width);
        for block in actor.group.cancel.chunks(256) {
            let values = self.encode(&[block])?.remove(0);
            for row in values.chunks_exact(width) {
                for i in 0..width {
                    sum[i] += f64::from(row[i]);
                    if row[i] > max[i] {
                        max[i] = row[i];
                    }
                }
            }
            embeddings.extend(values);
        }
        let count = actor.group.cancel.len();
        let reduced = if count == 0 {
            vec![0.; 3 * width]
        } else {
            sum.iter()
                .map(|v| *v as f32)
                .chain(sum.iter().map(|v| (*v / count as f64) as f32))
                .chain(max)
                .collect()
        };
        Ok(PreparedDecision {
            reduced,
            embeddings,
            family: RefCell::new(None),
        })
    }
    fn family_batch(
        &mut self,
        rows: &[(&NativeActor, &[f32], &PreparedDecision)],
    ) -> Result<(), Error> {
        let missing: Vec<_> = rows
            .iter()
            .filter(|row| row.2.family.borrow().is_none())
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        let b = missing.len();
        self.called("family", &vec![0; b]);
        let output = self.family.run(ort::inputs![
            "z_t" => Tensor::from_array(([b,self.hidden], missing.iter().flat_map(|r|r.1.iter().copied()).collect::<Vec<_>>()))?,
            "rule_features" => Tensor::from_array(([b,30], missing.iter().flat_map(|r|r.0.group.rows[r.0.row].rule).collect::<Vec<_>>()))?,
            "cancel_reduced" => Tensor::from_array(([b,3*self.hidden/2], missing.iter().flat_map(|r|r.2.reduced.iter().copied()).collect::<Vec<_>>()))?,
            "cancel_exists" => Tensor::from_array(([b], missing.iter().map(|r|!r.0.group.cancel.is_empty()).collect::<Vec<_>>()))?
        ])?;
        let family = output["family_logits"].try_extract_tensor::<f32>()?.1;
        let rule = output["rule_context"].try_extract_tensor::<f32>()?.1;
        for (i, row) in missing.iter().enumerate() {
            *row.2.family.borrow_mut() = Some((
                family[i * 6..(i + 1) * 6].to_vec(),
                rule[i * self.hidden / 2..(i + 1) * self.hidden / 2].to_vec(),
            ));
        }
        Ok(())
    }
    pub fn scores(
        &mut self,
        actor: &NativeActor,
        z: &[f32],
        prepared: &PreparedDecision,
    ) -> Result<Vec<f32>, Error> {
        Ok(self.scores_batch(&[(actor, z, prepared)])?.remove(0))
    }
    pub fn scores_batch(
        &mut self,
        rows: &[(&NativeActor, &[f32], &PreparedDecision)],
    ) -> Result<Vec<Vec<f32>>, Error> {
        let mut groups: BTreeMap<(usize, Vec<i64>), Vec<usize>> = BTreeMap::new();
        let mut result = vec![Vec::new(); rows.len()];
        for (i, row) in rows.iter().enumerate() {
            let support = row.0.support().map_err(|e| format!("{e:?}"))?;
            // 评分前处理单项；不调用规则、前缀或任何动作头。
            if support.iter().filter(|v| **v).count() == 1 {
                result[i] = vec![0.; support.len()];
                continue;
            }
            let indices = row.0.price_indices().map_err(|e| format!("{e:?}"))?;
            groups
                .entry((row.0.head_index(), indices))
                .or_default()
                .push(i);
        }
        for ((head, indices), positions) in groups {
            let active: Vec<_> = positions.iter().map(|&i| rows[i]).collect();
            self.family_batch(&active)?;
            if head == 0 {
                for &i in &positions {
                    result[i] = rows[i].2.family.borrow().as_ref().unwrap().0.clone();
                }
                continue;
            }
            let b = active.len();
            let z = active
                .iter()
                .flat_map(|r| r.1.iter().copied())
                .collect::<Vec<_>>();
            let rule = active
                .iter()
                .flat_map(|r| r.2.family.borrow().as_ref().unwrap().1.clone())
                .collect::<Vec<_>>();
            let prefix = active
                .iter()
                .flat_map(|r| r.0.prefix_features())
                .collect::<Vec<_>>();
            let name = match head {
                1 => "quantity",
                2 => "variant",
                3 => "price",
                5 => "cancel_context",
                _ => unreachable!(),
            };
            self.called(name, &vec![0; b]);
            let values = if head == 3 {
                let fees = active
                    .iter()
                    .map(|r| r.0.price_fee_features().map_err(|e| format!("{e:?}")))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();
                let p = indices.len();
                let output = self.price.run(ort::inputs![
                    "z_t"=>Tensor::from_array(([b,self.hidden],z))?, "rule_context"=>Tensor::from_array(([b,self.hidden/2],rule))?,
                    "prefix_features"=>Tensor::from_array(([b,36],prefix))?, "price_indices"=>Tensor::from_array(([p],indices.clone()))?,
                    "price_fee_features"=>Tensor::from_array(([b,p],fees))?
                ])?;
                output["price_logits"]
                    .try_extract_tensor::<f32>()?
                    .1
                    .to_vec()
            } else {
                let (session, output_name) = match head {
                    1 => (&mut self.quantity, "quantity_branch_logits"),
                    2 => (&mut self.variant, "buy_variant_logits"),
                    5 => (&mut self.cancel_context, "decoder_context"),
                    _ => unreachable!(),
                };
                let output = session.run(ort::inputs![
                    "z_t"=>Tensor::from_array(([b,self.hidden],z))?, "rule_context"=>Tensor::from_array(([b,self.hidden/2],rule))?,
                    "prefix_features"=>Tensor::from_array(([b,36],prefix))?
                ])?;
                output[output_name].try_extract_tensor::<f32>()?.1.to_vec()
            };
            let width = values.len() / b;
            for (&i, score) in positions.iter().zip(values.chunks_exact(width)) {
                if head == 3 {
                    result[i] = vec![0.; 999];
                    for (&k, &value) in indices.iter().zip(score) {
                        result[i][k as usize] = value;
                    }
                } else if head == 5 {
                    let prepared = rows[i].2;
                    for block in prepared.embeddings.chunks(256 * self.hidden / 2) {
                        let k = block.len() / (self.hidden / 2);
                        self.called("cancel", &[k]);
                        let output = self.cancel.run(ort::inputs![
                            "decoder_context"=>Tensor::from_array(([1,self.hidden/2],score.to_vec()))?,
                            "cancel_embeddings"=>Tensor::from_array(([1,k,self.hidden/2],block.to_vec()))?,
                            "cancel_mask"=>Tensor::from_array(([1,k],vec![true;k]))?
                        ])?;
                        result[i].extend_from_slice(
                            output["cancel_logits"].try_extract_tensor::<f32>()?.1,
                        );
                    }
                } else {
                    result[i] = score.to_vec();
                }
            }
        }
        Ok(result)
    }
    /// 验证入口：任意合法物理分块，对象数学与完整顺序归约由调用方核对。
    pub fn blocks(&mut self, rows: &[ActorBlockRow<'_>]) -> Result<Vec<Vec<Vec<f32>>>, Error> {
        let embeddings = self.encode(&rows.iter().map(|r| r.2).collect::<Vec<_>>())?;
        let mut result = Vec::new();
        for (row, values) in rows.iter().zip(embeddings) {
            let prepared = PreparedDecision {
                reduced: row.3.to_vec(),
                embeddings: values.clone(),
                family: RefCell::new(None),
            };
            let score = self.scores(row.0, row.1, &prepared)?;
            let mut output = vec![Vec::new(); 6];
            output[row.0.head_index()] = score;
            output[4] = values;
            result.push(output);
        }
        Ok(result)
    }
    pub fn greedy(&mut self, actor: &mut NativeActor, z: &[f32]) -> Result<(), Error> {
        let prepared = self.reduced(actor, z)?;
        while !actor.is_complete() {
            let support = actor.support().map_err(|e| format!("{e:?}"))?;
            let scores = self.scores(actor, z, &prepared)?;
            let mut best = None;
            for (i, (&legal, &score)) in support.iter().zip(&scores).enumerate() {
                if legal && best.is_none_or(|(_, old)| score > old) {
                    best = Some((i, score));
                }
            }
            actor
                .choose(best.ok_or("empty support")?.0)
                .map_err(|e| format!("{e:?}"))?;
        }
        Ok(())
    }
}
