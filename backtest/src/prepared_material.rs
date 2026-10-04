//! 已成功准备材料的固定描述；不存策略、账户或可恢复任务状态。
use crate::{
    HistoricalInitializationProvenanceV1, HistoricalPreparationConfigV1, MaterialError,
    PreparedHistoricalRunV1,
};
use contracts::*;
use data_pipeline_training::hidden_preparation::HiddenPreparationV1;
use serde::{Deserialize, Serialize};
use sim_system::scenario::PlatformMarketV1;
use std::{
    collections::BTreeMap,
    io::Write,
    path::{Component, Path, PathBuf},
};

pub const PREPARED_HISTORICAL_MATERIAL_SCHEMA_V1: &str = "prepared_historical_material_v1";
pub const PREPARED_HISTORICAL_MATERIAL_FILE_V1: &str = "prepared-run-v1.json";
/// 一次成功发布后不改写。根定位由描述文件所在目录提供；来源路径仅作溯源。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreparedHistoricalMaterialV1 {
    pub schema_version: String,
    pub preparation: HistoricalPreparationConfigV1,
    pub source_manifests: Vec<(String, String)>,
    pub input_set_identity_sha256: Sha256Hex,
    pub source_identity_sha256: Sha256Hex,
    pub initialization: HistoricalInitializationProvenanceV1,
    pub markets: BTreeMap<String, PlatformMarketV1>,
    pub public_prefix: CommittedIngressPrefixV1,
    pub public_relative_root: PathBuf,
    pub hidden_consumer_stream_id: String,
    pub hidden_derived_relative_path: PathBuf,
    pub hidden_index_relative_path: PathBuf,
}
impl PreparedHistoricalMaterialV1 {
    pub(crate) fn from_prepared(
        config: &HistoricalPreparationConfigV1,
        prepared: &PreparedHistoricalRunV1,
    ) -> Result<Self, MaterialError> {
        let mut preparation = config.clone();
        preparation.header = config.effective_header();
        preparation.work_root = PathBuf::from(".");
        let material = Self {
            schema_version: PREPARED_HISTORICAL_MATERIAL_SCHEMA_V1.into(),
            preparation,
            source_manifests: prepared.source_manifests.clone(),
            input_set_identity_sha256: prepared.input_set_identity_sha256.clone(),
            source_identity_sha256: prepared.source_identity_sha256.clone(),
            initialization: prepared.initialization.clone(),
            markets: prepared.markets.clone(),
            public_prefix: prepared.hidden.prefix.clone(),
            public_relative_root: prepared.public_root.strip_prefix(&config.work_root)?.into(),
            hidden_consumer_stream_id: prepared.hidden.consumer_stream_id.clone(),
            hidden_derived_relative_path: prepared
                .hidden
                .derived_path
                .strip_prefix(&config.work_root)?
                .into(),
            hidden_index_relative_path: prepared
                .hidden
                .index_path
                .strip_prefix(&config.work_root)?
                .into(),
        };
        material.validate()?;
        Ok(material)
    }
    /// 校验描述结构；不预扫、重hash或发现其他材料目录。
    pub fn validate(&self) -> Result<(), MaterialError> {
        if self.schema_version != PREPARED_HISTORICAL_MATERIAL_SCHEMA_V1 {
            return Err("不支持的已准备历史材料版本".into());
        }
        for path in [
            &self.public_relative_root,
            &self.hidden_derived_relative_path,
            &self.hidden_index_relative_path,
        ] {
            if path.as_os_str().is_empty()
                || path
                    .components()
                    .any(|c| !matches!(c, Component::Normal(_)))
            {
                return Err("材料定位必须是根内固定相对路径".into());
            }
        }
        if self.preparation.work_root != Path::new(".")
            || !self.public_prefix.normal_end
            || self.public_prefix.input_stream_id != self.preparation.header.input_stream_id
            || self.hidden_consumer_stream_id != self.preparation.hidden_stream_id
        {
            return Err("描述的公共前缀或hidden绑定不符".into());
        }
        for (key, market) in &self.markets {
            if key != &market.market().condition_id.0
                || PlatformMarketV1::try_new(market.market().clone(), market.assets().clone())?
                    != *market
            {
                return Err("描述的固定市场身份不符".into());
            }
        }
        Ok(())
    }
    pub fn read(path: &Path) -> Result<Self, MaterialError> {
        let material: Self = serde_json::from_reader(std::fs::File::open(path)?)?;
        material.validate()?;
        Ok(material)
    }
    pub(crate) fn resolve(&self, root: &Path) -> Result<PreparedHistoricalRunV1, MaterialError> {
        self.validate()?;
        let public_root = root.join(&self.public_relative_root);
        Ok(PreparedHistoricalRunV1 {
            public_root,
            hidden: HiddenPreparationV1 {
                prefix: self.public_prefix.clone(),
                consumer_stream_id: self.hidden_consumer_stream_id.clone(),
                derived_path: root.join(&self.hidden_derived_relative_path),
                index_path: root.join(&self.hidden_index_relative_path),
            },
            markets: self.markets.clone(),
            source_manifests: self.source_manifests.clone(),
            input_set_identity_sha256: self.input_set_identity_sha256.clone(),
            source_identity_sha256: self.source_identity_sha256.clone(),
            initialization: self.initialization.clone(),
        })
    }
    pub(crate) fn publish(&self, root: &Path) -> Result<PathBuf, MaterialError> {
        let bytes = serde_json::to_vec(self)?;
        let pending = root.join("prepared-run-v1.json.pending");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&pending)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        let path = root.join(PREPARED_HISTORICAL_MATERIAL_FILE_V1);
        std::fs::rename(pending, &path)?;
        std::fs::File::open(root)?.sync_all()?;
        Ok(path)
    }
}
