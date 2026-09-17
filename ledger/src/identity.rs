//! 同一生命周期索引的旧耐久键和新内存键；编码只发生于后台。
use contracts::execution_normalization::StableLogicalIdentityV1;
use execution_normalization::MemoryLogicalIdentityV1;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum LedgerIdentityKeyV1 {
    Stable(String),
    Memory { memory: MemoryLogicalIdentityV1 },
}
pub(crate) trait LedgerIdentity: Clone {
    fn ledger_key(&self) -> LedgerIdentityKeyV1;
}
impl LedgerIdentity for StableLogicalIdentityV1 {
    fn ledger_key(&self) -> LedgerIdentityKeyV1 {
        LedgerIdentityKeyV1::Stable(self.0.clone())
    }
}
impl LedgerIdentity for MemoryLogicalIdentityV1 {
    fn ledger_key(&self) -> LedgerIdentityKeyV1 {
        LedgerIdentityKeyV1::Memory {
            memory: self.clone(),
        }
    }
}

// 旧V1稳定键保持原JSON对象；新内存键仅在后台编码为有类型的键值行。
pub(crate) mod map {
    use super::*;
    use std::collections::BTreeMap;
    pub fn serialize<S: serde::Serializer, V: Serialize>(
        v: &BTreeMap<LedgerIdentityKeyV1, V>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        if v.keys()
            .all(|k| matches!(k, LedgerIdentityKeyV1::Stable(_)))
        {
            use serde::ser::SerializeMap;
            let mut m = s.serialize_map(Some(v.len()))?;
            for (k, v) in v {
                let LedgerIdentityKeyV1::Stable(k) = k else {
                    unreachable!()
                };
                m.serialize_entry(k, v)?;
            }
            m.end()
        } else {
            v.iter().collect::<Vec<_>>().serialize(s)
        }
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>, V: Deserialize<'de>>(
        d: D,
    ) -> Result<BTreeMap<LedgerIdentityKeyV1, V>, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire<V> {
            Legacy(BTreeMap<String, V>),
            Memory(Vec<(LedgerIdentityKeyV1, V)>),
        }
        Ok(match Wire::deserialize(d)? {
            Wire::Legacy(v) => v
                .into_iter()
                .map(|(k, v)| (LedgerIdentityKeyV1::Stable(k), v))
                .collect(),
            Wire::Memory(v) => v.into_iter().collect(),
        })
    }
}
