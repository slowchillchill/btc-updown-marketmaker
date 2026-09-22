//! ORT 同样消费 NativeActor；这里不定义第二份完成性或金额规则。
use crate::NativeActor;
use ort::{session::Session, value::Tensor};

pub type ActorBlockRow<'a> = (
    &'a NativeActor,
    &'a [f32],
    &'a [Vec<f32>],
    &'a [f32],
    bool,
    &'a [f32],
);

pub struct OrtActorScorer {
    pub session: Session,
    pub hidden: usize,
    #[cfg(all(test, feature = "continuous-validation"))]
    pub measurement: (usize, Option<Vec<Vec<usize>>>),
}
impl OrtActorScorer {
    /// 独立根的活动参数行合批；每行保留自己的精确前缀、完整归约及掩码。
    pub fn blocks(&mut self, rows: &[ActorBlockRow<'_>]) -> ort::Result<Vec<Vec<Vec<f32>>>> {
        let prefixes: Vec<_> = rows.iter().map(|r| r.0.prefix_features()).collect();
        self.blocks_with_prefixes(rows, &prefixes)
    }
    fn blocks_with_prefixes(
        &mut self,
        rows: &[ActorBlockRow<'_>],
        prefixes: &[[f32; 60]],
    ) -> ort::Result<Vec<Vec<Vec<f32>>>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        #[cfg(all(test, feature = "continuous-validation"))]
        {
            self.measurement.0 += 1;
            if let Some(blocks) = &mut self.measurement.1 {
                blocks.push(rows.iter().map(|row| row.2.len()).collect());
            }
        }
        let batch = rows.len();
        let slots = rows.iter().map(|row| row.2.len()).max().unwrap().max(1);
        let mut objects = vec![0.; batch * slots * 184];
        let mut mask = vec![false; batch * slots];
        for (b, row) in rows.iter().enumerate() {
            for (i, feature) in row.2.iter().enumerate() {
                objects[(b * slots + i) * 184..(b * slots + i + 1) * 184].copy_from_slice(feature);
                mask[b * slots + i] = true;
            }
        }
        let out = self.session.run(ort::inputs![
            "z_t"=>Tensor::from_array(([batch,self.hidden],rows.iter().flat_map(|r|r.1.iter().copied()).collect::<Vec<_>>()))?,
            "rule_features"=>Tensor::from_array(([batch,30],rows.iter().flat_map(|r|r.0.group.rows[r.0.row].rule).collect::<Vec<_>>()))?,
            "prefix_features"=>Tensor::from_array(([batch,60],prefixes.iter().flatten().copied().collect::<Vec<_>>()))?,
            "cancel_features"=>Tensor::from_array(([batch,slots,184],objects))?,
            "cancel_mask"=>Tensor::from_array(([batch,slots],mask))?,
            "cancel_reduced"=>Tensor::from_array(([batch,3*self.hidden/2],rows.iter().flat_map(|r|r.3.iter().copied()).collect::<Vec<_>>()))?,
            "cancel_exists"=>Tensor::from_array(([batch],rows.iter().map(|r|r.4).collect::<Vec<_>>()))?,
            "price_fee_features"=>Tensor::from_array(([batch,9999],rows.iter().flat_map(|r|r.5.iter().copied()).collect::<Vec<_>>()))?
        ])?;
        let mut result = vec![Vec::new(); batch];
        for key in [
            "family_logits",
            "quantity_branch_logits",
            "buy_variant_logits",
            "price_logits",
            "post_only_logits",
            "expiry_logits",
            "cancel_embeddings",
            "cancel_logits",
        ] {
            let (_, data) = out[key].try_extract_tensor::<f32>()?;
            for (row, values) in result.iter_mut().zip(data.chunks_exact(data.len() / batch)) {
                row.push(values.to_vec());
            }
        }
        Ok(result)
    }
    /// 全人口的sum/mean/max沿原顺序使用f64归约；块不改变动作集合。
    pub fn reduced(&mut self, actor: &NativeActor, z: &[f32]) -> ort::Result<Vec<f32>> {
        let width = self.hidden / 2;
        let mut sum = vec![0f64; width];
        let mut max = vec![f32::NEG_INFINITY; width];
        let zero = vec![0.; 3 * width];
        let fees = vec![0.; 9999];
        let prefixes = [actor.prefix_features()];
        for block in actor.group.cancel.chunks(256) {
            let output = self
                .blocks_with_prefixes(&[(actor, z, block, &zero, false, &fees)], &prefixes)?
                .remove(0);
            for row in output[6][..block.len() * width].chunks_exact(width) {
                for i in 0..width {
                    sum[i] += f64::from(row[i]);
                    max[i] = max[i].max(row[i]);
                }
            }
        }
        if actor.group.cancel.is_empty() {
            return Ok(zero);
        }
        let count = actor.group.cancel.len() as f64;
        Ok(sum
            .iter()
            .map(|x| *x as f32)
            .chain(sum.iter().map(|x| (*x / count) as f32))
            .chain(max)
            .collect())
    }
    pub fn scores(
        &mut self,
        actor: &NativeActor,
        z: &[f32],
        reduced: &[f32],
    ) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
        let fees = actor.price_fee_features().map_err(|e| format!("{e:?}"))?;
        let mut scores = Vec::new();
        let head = actor.head_index();
        let prefixes = [actor.prefix_features()];
        if actor.group.cancel.is_empty() {
            return Ok(self
                .blocks_with_prefixes(&[(actor, z, &[], reduced, false, &fees)], &prefixes)?
                .remove(0)
                .swap_remove(head));
        }
        for block in actor.group.cancel.chunks(256) {
            let output = self
                .blocks_with_prefixes(&[(actor, z, block, reduced, true, &fees)], &prefixes)?
                .remove(0);
            if head != 7 {
                return Ok(output[head].clone());
            }
            scores.extend_from_slice(&output[head][..block.len()]);
        }
        Ok(scores)
    }

    pub fn scores_batch(
        &mut self,
        rows: &[(&NativeActor, &[f32], &[f32])],
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error>> {
        let fees = rows
            .iter()
            .map(|r| r.0.price_fee_features().map_err(|e| format!("{e:?}")))
            .collect::<Result<Vec<_>, _>>()?;
        let prefixes: Vec<_> = rows.iter().map(|r| r.0.prefix_features()).collect();
        let heads: Vec<_> = rows.iter().map(|r| r.0.head_index()).collect();
        let mut scores = vec![Vec::new(); rows.len()];
        let count = rows
            .iter()
            .map(|r| r.0.group.cancel.len())
            .max()
            .unwrap_or(0)
            .max(1);
        for offset in (0..count).step_by(256) {
            let active: Vec<_> = (0..rows.len())
                .filter(|&i| offset == 0 || heads[i] == 7 && rows[i].0.group.cancel.len() > offset)
                .collect();
            if active.is_empty() {
                break;
            }
            let blocks: Vec<_> = active
                .iter()
                .map(|&i| {
                    let r = rows[i];
                    let objects = &r.0.group.cancel;
                    let block =
                        &objects[offset.min(objects.len())..(offset + 256).min(objects.len())];
                    (
                        r.0,
                        r.1,
                        block,
                        r.2,
                        !objects.is_empty(),
                        fees[i].as_slice(),
                    )
                })
                .collect();
            let block_prefixes: Vec<_> = active.iter().map(|&i| prefixes[i]).collect();
            let output = self.blocks_with_prefixes(&blocks, &block_prefixes)?;
            for (i, values) in active.into_iter().zip(output) {
                let head = heads[i];
                if head == 7 {
                    let valid = rows[i].0.group.cancel.len().saturating_sub(offset).min(256);
                    scores[i].extend_from_slice(&values[head][..valid]);
                } else {
                    scores[i] = values[head].clone();
                }
            }
        }
        Ok(scores)
    }
    pub fn greedy(
        &mut self,
        actor: &mut NativeActor,
        z: &[f32],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reduced = self.reduced(actor, z)?;
        while !actor.is_complete() {
            let support = actor.support().map_err(|e| format!("{e:?}"))?;
            let scores = self.scores(actor, z, &reduced)?;
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
