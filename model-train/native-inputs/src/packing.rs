//! 同步消费者私有补齐存储；只复用已发布的不可变特征侧，不推断公共事实身份。
use crate::{BookLevels, HiddenKey};
use std::sync::Arc;

#[derive(Default)]
pub struct PackedBooks {
    keys: Vec<HiddenKey>,
    books: Vec<BookLevels>,
    depth: usize,
    values: Vec<f32>,
    mask: Vec<bool>,
}
impl PackedBooks {
    pub fn update<'a>(&mut self, rows: impl IntoIterator<Item = (&'a HiddenKey, &'a BookLevels)>) {
        let rows: Vec<_> = rows.into_iter().collect();
        let depth = rows
            .iter()
            .flat_map(|(_, b)| b.iter().map(|s| s.len()))
            .max()
            .unwrap_or(0)
            .max(1);
        let remap = self.keys.len() != rows.len()
            || self
                .keys
                .iter()
                .zip(&rows)
                .any(|(old, (key, _))| old != *key);
        let rebuild = remap || depth != self.depth;
        if rebuild {
            self.values = vec![0.; rows.len() * 6 * depth * 9];
            self.mask = vec![false; rows.len() * 6 * depth];
            self.keys = rows.iter().map(|(key, _)| (*key).clone()).collect();
        }
        for (batch, (_, book)) in rows.iter().enumerate() {
            for (side, levels) in book.iter().enumerate() {
                // 强引用保证同一不可变投影的存储仍存活；这不是来源对象地址或事实修订号。
                if !rebuild && Arc::ptr_eq(&self.books[batch][side], levels) {
                    continue;
                }
                let start = (batch * 6 + side) * depth;
                self.values[start * 9..(start + depth) * 9].fill(0.);
                self.mask[start..start + depth].fill(false);
                for (index, row) in levels.iter().enumerate() {
                    let slot = start + index;
                    self.values[slot * 9..(slot + 1) * 9].copy_from_slice(row);
                    self.mask[slot] = true;
                }
            }
        }
        self.books = rows.into_iter().map(|(_, b)| b.clone()).collect();
        self.depth = depth;
    }
    pub fn clear(&mut self) {
        *self = Self::default();
    }
    pub fn depth(&self) -> usize {
        self.depth
    }
    pub fn values(&self) -> &[f32] {
        &self.values
    }
    pub fn mask(&self) -> &[bool] {
        &self.mask
    }
}
