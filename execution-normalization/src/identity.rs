//! 同一归一化算法的两种身份表示；内存身份仅在后台编码时物化旧字符串。
use std::{fmt::Debug, sync::Arc};

use contracts::execution_normalization::{
    NoeEconomicFactReferenceV1, SourceNeutralAccountIdentityV1, SourceNeutralEconomicIdentityV1,
    StableLogicalIdentityV1,
};
use serde::{Serialize, Serializer};

use crate::core::{
    CoreErrorV1, NORMALIZATION_CORE_V1, SourceNeutralEntityKeyV1, SourceNeutralLogicalKeyV1,
};

pub trait NormalizationIdentity<F>: Clone + Debug + Eq + Ord {
    fn logical(
        account: &Arc<SourceNeutralAccountIdentityV1>,
        key: &SourceNeutralLogicalKeyV1,
    ) -> Result<Self, CoreErrorV1>;
    fn entity(
        account: &Arc<SourceNeutralAccountIdentityV1>,
        key: &SourceNeutralEntityKeyV1,
    ) -> Result<Self, CoreErrorV1>;
    fn fact_reference(identity: &SourceNeutralEconomicIdentityV1) -> Result<F, CoreErrorV1>;
    fn legacy_sort_key(&self) -> Result<String, CoreErrorV1>;
}

#[cfg(test)]
thread_local! { pub(crate) static ENCODINGS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
fn encode(value: &impl Serialize, prefix: &str) -> Result<String, CoreErrorV1> {
    #[cfg(test)]
    ENCODINGS.with(|count| count.set(count.get() + 1));
    serde_json::to_string(value)
        .map(|value| format!("{prefix}{value}"))
        .map_err(|error| CoreErrorV1::IdentityEncoding {
            message: error.to_string(),
        })
}
impl NormalizationIdentity<NoeEconomicFactReferenceV1> for StableLogicalIdentityV1 {
    fn logical(
        account: &Arc<SourceNeutralAccountIdentityV1>,
        key: &SourceNeutralLogicalKeyV1,
    ) -> Result<Self, CoreErrorV1> {
        encode(
            &(NORMALIZATION_CORE_V1, account.as_ref(), key),
            "noe-logical-v1:",
        )
        .map(Self)
    }
    fn entity(
        account: &Arc<SourceNeutralAccountIdentityV1>,
        key: &SourceNeutralEntityKeyV1,
    ) -> Result<Self, CoreErrorV1> {
        encode(
            &(NORMALIZATION_CORE_V1, account.as_ref(), "entity", key),
            "noe-entity-v1:",
        )
        .map(Self)
    }
    fn fact_reference(
        identity: &SourceNeutralEconomicIdentityV1,
    ) -> Result<NoeEconomicFactReferenceV1, CoreErrorV1> {
        encode(&(NORMALIZATION_CORE_V1, identity), "noe-economic-fact-v1:")
            .map(NoeEconomicFactReferenceV1)
    }
    fn legacy_sort_key(&self) -> Result<String, CoreErrorV1> {
        Ok(self.0.clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MemoryIdentityKeyV1 {
    Logical(SourceNeutralLogicalKeyV1),
    Entity(SourceNeutralEntityKeyV1),
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MemoryLogicalIdentityV1 {
    pub account: Arc<SourceNeutralAccountIdentityV1>,
    pub key: MemoryIdentityKeyV1,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryEconomicFactReferenceV1(pub SourceNeutralEconomicIdentityV1);

impl NormalizationIdentity<MemoryEconomicFactReferenceV1> for MemoryLogicalIdentityV1 {
    fn logical(
        account: &Arc<SourceNeutralAccountIdentityV1>,
        key: &SourceNeutralLogicalKeyV1,
    ) -> Result<Self, CoreErrorV1> {
        Ok(Self {
            account: account.clone(),
            key: MemoryIdentityKeyV1::Logical(key.clone()),
        })
    }
    fn entity(
        account: &Arc<SourceNeutralAccountIdentityV1>,
        key: &SourceNeutralEntityKeyV1,
    ) -> Result<Self, CoreErrorV1> {
        Ok(Self {
            account: account.clone(),
            key: MemoryIdentityKeyV1::Entity(key.clone()),
        })
    }
    fn fact_reference(
        identity: &SourceNeutralEconomicIdentityV1,
    ) -> Result<MemoryEconomicFactReferenceV1, CoreErrorV1> {
        Ok(MemoryEconomicFactReferenceV1(identity.clone()))
    }
    fn legacy_sort_key(&self) -> Result<String, CoreErrorV1> {
        match &self.key {
            MemoryIdentityKeyV1::Logical(key) => {
                StableLogicalIdentityV1::logical(&self.account, key)
            }
            MemoryIdentityKeyV1::Entity(key) => StableLogicalIdentityV1::entity(&self.account, key),
        }
        .map(|identity| identity.0)
    }
}
impl Serialize for MemoryLogicalIdentityV1 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.legacy_sort_key().map_err(serde::ser::Error::custom)?)
    }
}
impl Serialize for MemoryEconomicFactReferenceV1 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let value =
            StableLogicalIdentityV1::fact_reference(&self.0).map_err(serde::ser::Error::custom)?;
        value.serialize(serializer)
    }
}

// 仅在耐久读取边界还原原编码。内存消费者直接借用强类型身份。
impl<'de> serde::Deserialize<'de> for MemoryLogicalIdentityV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = <String as serde::Deserialize>::deserialize(d)?;
        Self::from_stable(&StableLogicalIdentityV1(value)).map_err(serde::de::Error::custom)
    }
}
impl MemoryLogicalIdentityV1 {
    pub fn from_stable(value: &StableLogicalIdentityV1) -> Result<Self, CoreErrorV1> {
        let decode = || -> Result<Self, Box<dyn std::error::Error>> {
            if let Some(v) = value.0.strip_prefix("noe-entity-v1:") {
                let (_version, account, _entity, key): (
                    String,
                    SourceNeutralAccountIdentityV1,
                    String,
                    SourceNeutralEntityKeyV1,
                ) = serde_json::from_str(v)?;
                Ok(Self {
                    account: Arc::new(account),
                    key: MemoryIdentityKeyV1::Entity(key),
                })
            } else if let Some(v) = value.0.strip_prefix("noe-logical-v1:") {
                let (_version, account, key): (
                    String,
                    SourceNeutralAccountIdentityV1,
                    SourceNeutralLogicalKeyV1,
                ) = serde_json::from_str(v)?;
                Ok(Self {
                    account: Arc::new(account),
                    key: MemoryIdentityKeyV1::Logical(key),
                })
            } else {
                Err("未知NOE身份编码".into())
            }
        };
        decode().map_err(|e| CoreErrorV1::IdentityEncoding {
            message: e.to_string(),
        })
    }
}
impl<'de> serde::Deserialize<'de> for MemoryEconomicFactReferenceV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = <String as serde::Deserialize>::deserialize(d)?;
        let raw = value
            .strip_prefix("noe-economic-fact-v1:")
            .ok_or_else(|| serde::de::Error::custom("未知经济身份编码"))?;
        let (_, identity): (String, SourceNeutralEconomicIdentityV1) =
            serde_json::from_str(raw).map_err(serde::de::Error::custom)?;
        Ok(Self(identity))
    }
}
