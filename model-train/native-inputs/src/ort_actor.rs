//! ORT 同样消费 NativeActor；这里不定义第二份完成性或金额规则。
use crate::NativeActor;
use ort::{session::Session, value::Tensor};

pub struct OrtActorScorer {
    pub session: Session,
    pub hidden: usize,
}
impl OrtActorScorer {
    fn block(
        &mut self,
        actor: &NativeActor,
        z: &[f32],
        features: &[Vec<f32>],
        reduced: &[f32],
        exists: bool,
        fees: &[f32],
    ) -> ort::Result<Vec<Vec<f32>>> {
        let slots = if features.is_empty() { 1 } else { 256 };
        let mut objects = vec![0.; slots * 129];
        for (i, row) in features.iter().enumerate() {
            objects[i * 129..(i + 1) * 129].copy_from_slice(row);
        }
        let out=self.session.run(ort::inputs![
            "z_t"=>Tensor::from_array(([1,self.hidden],z.to_vec()))?,
            "rule_features"=>Tensor::from_array(([1,30],actor.group.rows[actor.row].rule.to_vec()))?,
            "prefix_features"=>Tensor::from_array(([1,60],actor.prefix_features().to_vec()))?,
            "cancel_features"=>Tensor::from_array(([1,slots,129],objects))?,
            "cancel_mask"=>Tensor::from_array(([1,slots],(0..slots).map(|i|i<features.len()).collect::<Vec<_>>()))?,
            "cancel_reduced"=>Tensor::from_array(([1,3*self.hidden/2],reduced.to_vec()))?,
            "cancel_exists"=>Tensor::from_array(([1],vec![exists]))?,
            "price_fee_features"=>Tensor::from_array(([1,9999],fees.to_vec()))?
        ])?;
        [
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
        .map(|key| {
            out[*key]
                .try_extract_tensor::<f32>()
                .map(|(_, data)| data.to_vec())
        })
        .collect()
    }
    /// 全人口的sum/mean/max沿原顺序使用f64归约；块不改变动作集合。
    pub fn reduced(&mut self, actor: &NativeActor, z: &[f32]) -> ort::Result<Vec<f32>> {
        let width = self.hidden / 2;
        let mut sum = vec![0f64; width];
        let mut max = vec![f32::NEG_INFINITY; width];
        let zero = vec![0.; 3 * width];
        let fees = vec![0.; 9999];
        for block in actor.group.cancel.chunks(256) {
            let output = self.block(actor, z, block, &zero, false, &fees)?;
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
        if actor.group.cancel.is_empty() {
            return Ok(self.block(actor, z, &[], reduced, false, &fees)?[head].clone());
        }
        for block in actor.group.cancel.chunks(256) {
            let output = self.block(actor, z, block, reduced, true, &fees)?;
            if head != 7 {
                return Ok(output[head].clone());
            }
            scores.extend_from_slice(&output[head][..block.len()]);
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
