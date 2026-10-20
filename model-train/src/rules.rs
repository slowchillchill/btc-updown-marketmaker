//! 模型组合根只打开一次规则；运行打开方式不改变原组的冻结身份。
use backtest::{MaterialError, PublicExportRulesV1};
use contracts::Sha256Hex;
use serde::{Deserialize, Serialize};
use sim_system::platform_rules::*;
use std::{path::PathBuf, sync::Arc};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConsumerRulesV1 {
    Explicit(Box<RuleOpenV1>),
    /// 已有配置保持原显式历史含义；新运行示例使用带mode的配置。
    Historical(PublicExportRulesV1),
}
impl From<PublicExportRulesV1> for ConsumerRulesV1 {
    fn from(v: PublicExportRulesV1) -> Self {
        Self::Historical(v)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum RuleOpenV1 {
    CurrentQuery {
        reference_market: ReferenceMarketSpecV1,
        source_snapshot_path: PathBuf,
        effective_config_path: PathBuf,
        snapshot_path: PathBuf,
        implementation_identity: ImplementationIdentityV1,
        public_maker_roles: Vec<contracts::SimCallerRoleV1>,
    },
    HistoricalAsOf {
        snapshot_path: PathBuf,
        expected_file_sha256: Sha256Hex,
    },
    Recovery {
        binding: FrozenRuleBindingV1,
    },
}
/// 原已封存文件为权威；必要来源/角色字段随group和checkpoint记录，不再计算摘要。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenRuleBindingV1 {
    pub source_mode: SnapshotAccessMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queried_source: Option<QueriedSourceV1>,
    pub snapshot_path: PathBuf,
    pub expected_file_sha256: Sha256Hex,
    pub implementation_identity: ImplementationIdentityV1,
    pub rules_observed_at_utc_ms: i64,
    pub reference_market: ReferenceMarketSpecV1,
    pub static_config_schema_version: String,
    pub maker_policy: L2QueueModelRuleV1,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueriedSourceV1 {
    pub snapshot_path: PathBuf,
    pub file_sha256: Sha256Hex,
}
impl FrozenRuleBindingV1 {
    pub fn from_sealed(
        rules: &SealedProcessRuleSnapshotV3,
        source_mode: SnapshotAccessMode,
    ) -> Self {
        Self {
            source_mode,
            queried_source: None,
            snapshot_path: rules.path().into(),
            expected_file_sha256: rules.file_sha256().clone(),
            implementation_identity: rules.implementation_identity().clone(),
            rules_observed_at_utc_ms: rules.rules_observed_at_utc_ms(),
            reference_market: rules.reference_market().clone(),
            static_config_schema_version: rules.static_config().schema_version.clone(),
            maker_policy: rules.maker_policy().clone(),
        }
    }
    pub fn public_rules(&self) -> PublicExportRulesV1 {
        PublicExportRulesV1 {
            snapshot_path: self.snapshot_path.clone(),
            expected_file_sha256: self.expected_file_sha256.clone(),
        }
    }
}
impl ConsumerRulesV1 {
    pub fn explicit(open: RuleOpenV1) -> Self {
        Self::Explicit(Box::new(open))
    }
    pub fn frozen(&self) -> Option<&FrozenRuleBindingV1> {
        match self {
            Self::Explicit(open) => match open.as_ref() {
                RuleOpenV1::Recovery { binding } => Some(binding),
                _ => None,
            },
            _ => None,
        }
    }
}
pub struct OpenedRulesV1 {
    pub rules: Arc<SealedProcessRuleSnapshotV3>,
    pub binding: FrozenRuleBindingV1,
    pub opened_via: SnapshotAccessMode,
}
/// 生产查询始终复用公共固定端点加载器；可注入函数仅用于合法夹具验证组合顺序。
pub(crate) fn open_rules_with(
    config: &crate::ConsumerRunConfig,
    query: impl FnOnce(
        CurrentProcessSnapshotRequestV1,
    ) -> Result<Arc<SealedProcessRuleSnapshotV3>, RuleSnapshotError>,
) -> Result<OpenedRulesV1, MaterialError> {
    let historical =
        |r: PublicExportRulesV1| -> Result<Arc<SealedProcessRuleSnapshotV3>, MaterialError> {
            Ok(load_historical_process_snapshot_v3(
                HistoricalProcessSnapshotRequestV1 {
                    snapshot_path: r.snapshot_path,
                    expected_file_sha256: r.expected_file_sha256,
                    replay_causal_start_utc_ms: config
                        .days
                        .iter()
                        .map(|d| d.preparation.scope.received_start_utc_ms)
                        .min()
                        .ok_or("未指定日输入")?,
                },
            )?)
        };
    let mode = match &config.rules {
        ConsumerRulesV1::Explicit(open) => open.as_ref().clone(),
        ConsumerRulesV1::Historical(r) => RuleOpenV1::HistoricalAsOf {
            snapshot_path: r.snapshot_path.clone(),
            expected_file_sha256: r.expected_file_sha256.clone(),
        },
    };
    let mut queried_source = None;
    let (rules, opened_via) = match &mode {
        RuleOpenV1::HistoricalAsOf {
            snapshot_path,
            expected_file_sha256,
        } => (
            historical(PublicExportRulesV1 {
                snapshot_path: snapshot_path.clone(),
                expected_file_sha256: expected_file_sha256.clone(),
            })?,
            SnapshotAccessMode::HistoricalAsOf,
        ),
        RuleOpenV1::CurrentQuery {
            reference_market,
            source_snapshot_path,
            effective_config_path: _,
            snapshot_path,
            implementation_identity,
            public_maker_roles,
        } => {
            let source = query(CurrentProcessSnapshotRequestV1 {
                reference_market: reference_market.clone(),
                target_path: source_snapshot_path.clone(),
                implementation_identity: implementation_identity.clone(),
            })?;
            queried_source = Some(QueriedSourceV1 {
                snapshot_path: source.path().into(),
                file_sha256: source.file_sha256().clone(),
            });
            (
                derive_public_maker_from_sealed_v3(
                    source,
                    snapshot_path.clone(),
                    implementation_identity.clone(),
                    public_maker_roles,
                )?,
                SnapshotAccessMode::CurrentQuery,
            )
        }
        RuleOpenV1::Recovery { binding } => {
            let rules = recover_process_snapshot_v3(RecoveryProcessSnapshotRequestV1 {
                snapshot_path: binding.snapshot_path.clone(),
                expected_file_sha256: binding.expected_file_sha256.clone(),
                expected_implementation_identity: binding.implementation_identity.clone(),
            })?;
            let mut actual = FrozenRuleBindingV1::from_sealed(&rules, binding.source_mode);
            actual.queried_source = binding.queried_source.clone();
            if actual != *binding {
                return Err("恢复规则元数据与原封存身份不同".into());
            }
            return Ok(OpenedRulesV1 {
                rules,
                binding: binding.clone(),
                opened_via: SnapshotAccessMode::Recovery,
            });
        }
    };
    let mut binding = FrozenRuleBindingV1::from_sealed(&rules, opened_via);
    binding.queried_source = queried_source;
    if let RuleOpenV1::CurrentQuery {
        effective_config_path,
        ..
    } = &mode
    {
        use std::io::Write;
        let effective = bind_consumer(config, &binding);
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(effective_config_path)?;
        serde_json::to_writer_pretty(&mut file, &effective)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    Ok(OpenedRulesV1 {
        rules,
        binding,
        opened_via,
    })
}
pub fn open_consumer_rules(
    config: &crate::ConsumerRunConfig,
) -> Result<OpenedRulesV1, MaterialError> {
    open_rules_with(config, |request| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| RuleSnapshotError::ClientBuild(e.to_string()))?
            .block_on(query_and_seal_current_process_snapshot_v3(request))
    })
}
pub(crate) fn bind_consumer(
    config: &crate::ConsumerRunConfig,
    binding: &FrozenRuleBindingV1,
) -> crate::ConsumerRunConfig {
    let mut c = config.clone();
    c.rules = ConsumerRulesV1::explicit(RuleOpenV1::Recovery {
        binding: binding.clone(),
    });
    c
}
