use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricUnavailable {
    pub metric: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CgroupMemoryStat {
    pub anon_bytes: u64,
    pub file_bytes: u64,
    pub slab_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct PsiSample {
    pub some_avg10: f64,
    pub full_avg10: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResourceSample {
    pub rss_bytes: Option<u64>,
    pub cpu_percent: Option<f64>,
    pub disk_free_bytes: Option<u64>,
    pub artifact_dir_bytes: Option<u64>,
    pub journal_log_bytes: Option<u64>,
    pub journal_limit_status: Option<String>,
    pub open_files: Option<u64>,
    pub queue_bytes: Option<u64>,
    pub queue_age_ms: Option<u64>,
    pub parquet_writer_memory_estimate_bytes: Option<u64>,
    pub cgroup_memory_current_bytes: Option<u64>,
    pub cgroup_memory_stat: Option<CgroupMemoryStat>,
    pub cgroup_memory_events: Option<String>,
    pub psi_cpu: Option<PsiSample>,
    pub psi_memory: Option<PsiSample>,
    pub psi_io: Option<PsiSample>,
    pub stage_latency_ms: Option<u64>,
    pub ttl_ms: Option<u64>,
    pub unavailable: Vec<MetricUnavailable>,
}

impl ResourceSample {
    pub fn into_watermark(self) -> ResourceWatermark {
        ResourceWatermark {
            rss_bytes: self.rss_bytes,
            cpu_percent: self.cpu_percent,
            disk_free_bytes: self.disk_free_bytes,
            artifact_dir_bytes: self.artifact_dir_bytes,
            journal_log_bytes: self.journal_log_bytes,
            journal_limit_status: self.journal_limit_status,
            open_files: self.open_files,
            queue_bytes: self.queue_bytes,
            queue_age_ms: self.queue_age_ms,
            parquet_writer_memory_estimate_bytes: self.parquet_writer_memory_estimate_bytes,
            cgroup_memory_current_bytes: self.cgroup_memory_current_bytes,
            cgroup_memory_stat: self.cgroup_memory_stat,
            cgroup_memory_events: self.cgroup_memory_events,
            psi_cpu: self.psi_cpu,
            psi_memory: self.psi_memory,
            psi_io: self.psi_io,
            stage_latency_ms: self.stage_latency_ms,
            ttl_ms: self.ttl_ms,
            unavailable: self.unavailable,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResourceWatermark {
    pub rss_bytes: Option<u64>,
    pub cpu_percent: Option<f64>,
    pub disk_free_bytes: Option<u64>,
    pub artifact_dir_bytes: Option<u64>,
    pub journal_log_bytes: Option<u64>,
    pub journal_limit_status: Option<String>,
    pub open_files: Option<u64>,
    pub queue_bytes: Option<u64>,
    pub queue_age_ms: Option<u64>,
    pub parquet_writer_memory_estimate_bytes: Option<u64>,
    pub cgroup_memory_current_bytes: Option<u64>,
    pub cgroup_memory_stat: Option<CgroupMemoryStat>,
    pub cgroup_memory_events: Option<String>,
    pub psi_cpu: Option<PsiSample>,
    pub psi_memory: Option<PsiSample>,
    pub psi_io: Option<PsiSample>,
    pub stage_latency_ms: Option<u64>,
    pub ttl_ms: Option<u64>,
    pub unavailable: Vec<MetricUnavailable>,
}

impl ResourceWatermark {
    pub fn has_required_contract_metrics(&self) -> bool {
        self.has_full_contract_metrics()
    }

    fn has_full_contract_metrics(&self) -> bool {
        self.metric_recorded("rss_bytes", self.rss_bytes.is_some())
            && self.metric_recorded("cpu_percent", self.cpu_percent.is_some())
            && self.metric_recorded("disk_free_bytes", self.disk_free_bytes.is_some())
            && self.metric_recorded("artifact_dir_bytes", self.artifact_dir_bytes.is_some())
            && self.metric_recorded("journal_log_bytes", self.journal_log_bytes.is_some())
            && self.metric_recorded(
                "journal_limit_status",
                self.journal_limit_status
                    .as_deref()
                    .is_some_and(|value| !value.is_empty()),
            )
            && self.metric_recorded("open_files", self.open_files.is_some())
            && self.metric_recorded("queue_bytes", self.queue_bytes.is_some())
            && self.metric_recorded("queue_age_ms", self.queue_age_ms.is_some())
            && self.metric_recorded(
                "parquet_writer_memory_estimate_bytes",
                self.parquet_writer_memory_estimate_bytes.is_some(),
            )
            && self.metric_recorded(
                "cgroup_memory_current_bytes",
                self.cgroup_memory_current_bytes.is_some(),
            )
            && self.metric_recorded("cgroup_memory_stat", self.cgroup_memory_stat.is_some())
            && self.metric_recorded(
                "cgroup_memory_events",
                self.cgroup_memory_events
                    .as_deref()
                    .is_some_and(|value| !value.is_empty()),
            )
            && self.metric_recorded("psi_cpu", self.psi_cpu.is_some())
            && self.metric_recorded("psi_memory", self.psi_memory.is_some())
            && self.metric_recorded("psi_io", self.psi_io.is_some())
            && self.metric_recorded("stage_latency_ms", self.stage_latency_ms.is_some())
            && self.metric_recorded("ttl_ms", self.ttl_ms.is_some())
    }

    pub fn has_compact_runtime_metrics(&self) -> bool {
        self.artifact_dir_bytes.is_some()
            && self.queue_bytes.is_some()
            && self.queue_age_ms.is_some()
            && self.parquet_writer_memory_estimate_bytes.is_some()
            && self.stage_latency_ms.is_some()
            && self.ttl_ms.is_some()
    }

    fn metric_recorded(&self, metric: &str, present: bool) -> bool {
        present
            || self
                .unavailable
                .iter()
                .any(|item| item.metric == metric && !item.reason.is_empty())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResourceThresholds {
    pub min_disk_free_bytes: u64,
    pub max_rss_bytes: u64,
    pub max_cpu_percent: f64,
    pub max_artifact_dir_bytes: u64,
    pub max_journal_log_bytes: u64,
    pub max_open_files: u64,
    pub max_queue_bytes: u64,
    pub max_queue_age_ms: u64,
    pub max_parquet_writer_memory_estimate_bytes: u64,
    pub max_cgroup_memory_current_bytes: u64,
    pub max_cgroup_memory_anon_bytes: u64,
    pub max_cgroup_memory_file_bytes: u64,
    pub max_cgroup_memory_slab_bytes: u64,
    pub max_psi_some_avg10: f64,
    pub max_psi_full_avg10: f64,
    pub max_stage_latency_ms: u64,
    pub max_ttl_ms: u64,
}

#[derive(Clone, Debug)]
pub struct ResourcePolicy {
    hard: ResourceThresholds,
    soft: ResourceThresholds,
}

impl ResourcePolicy {
    pub fn new(hard: ResourceThresholds, soft: ResourceThresholds) -> Self {
        Self { hard, soft }
    }

    pub fn evaluate(&self, watermark: ResourceWatermark) -> ResourceVerdict {
        if self.severity_for(&watermark) == ResourceSeverity::HardDegraded {
            return ResourceVerdict {
                severity: ResourceSeverity::HardDegraded,
                actions: vec![
                    ResourceAction::RecordBackpressure,
                    ResourceAction::ObserveRecovery,
                ],
                process_fatal: false,
            };
        }

        if self.severity_for(&watermark) == ResourceSeverity::SoftDegraded {
            return ResourceVerdict {
                severity: ResourceSeverity::SoftDegraded,
                actions: vec![
                    ResourceAction::RecordBackpressure,
                    ResourceAction::ObserveRecovery,
                ],
                process_fatal: false,
            };
        }

        ResourceVerdict {
            severity: ResourceSeverity::Healthy,
            actions: Vec::new(),
            process_fatal: false,
        }
    }

    fn severity_for(&self, watermark: &ResourceWatermark) -> ResourceSeverity {
        if self.crosses_hard(watermark) {
            ResourceSeverity::HardDegraded
        } else if self.crosses_soft(watermark) {
            ResourceSeverity::SoftDegraded
        } else {
            ResourceSeverity::Healthy
        }
    }

    fn crosses_hard(&self, watermark: &ResourceWatermark) -> bool {
        crosses_threshold(watermark, self.hard) || cgroup_events_indicate_hard_pressure(watermark)
    }

    fn crosses_soft(&self, watermark: &ResourceWatermark) -> bool {
        journal_status_indicates_pressure(watermark)
            || cgroup_events_indicate_hard_pressure(watermark)
            || crosses_threshold(watermark, self.soft)
    }
}

fn crosses_threshold(watermark: &ResourceWatermark, threshold: ResourceThresholds) -> bool {
    watermark
        .disk_free_bytes
        .is_some_and(|value| value < threshold.min_disk_free_bytes)
        || watermark
            .rss_bytes
            .is_some_and(|value| value > threshold.max_rss_bytes)
        || watermark
            .cpu_percent
            .is_some_and(|value| value > threshold.max_cpu_percent)
        || watermark
            .artifact_dir_bytes
            .is_some_and(|value| value > threshold.max_artifact_dir_bytes)
        || watermark
            .journal_log_bytes
            .is_some_and(|value| value > threshold.max_journal_log_bytes)
        || watermark
            .open_files
            .is_some_and(|value| value > threshold.max_open_files)
        || watermark
            .queue_bytes
            .is_some_and(|value| value > threshold.max_queue_bytes)
        || watermark
            .queue_age_ms
            .is_some_and(|value| value > threshold.max_queue_age_ms)
        || watermark
            .parquet_writer_memory_estimate_bytes
            .is_some_and(|value| value > threshold.max_parquet_writer_memory_estimate_bytes)
        || watermark
            .cgroup_memory_current_bytes
            .is_some_and(|value| value > threshold.max_cgroup_memory_current_bytes)
        || watermark.cgroup_memory_stat.as_ref().is_some_and(|stat| {
            stat.anon_bytes > threshold.max_cgroup_memory_anon_bytes
                || stat.file_bytes > threshold.max_cgroup_memory_file_bytes
                || stat.slab_bytes > threshold.max_cgroup_memory_slab_bytes
        })
        || psi_crosses(watermark.psi_cpu, threshold)
        || psi_crosses(watermark.psi_memory, threshold)
        || psi_crosses(watermark.psi_io, threshold)
        || watermark
            .stage_latency_ms
            .is_some_and(|value| value > threshold.max_stage_latency_ms)
        || watermark
            .ttl_ms
            .is_some_and(|value| value > threshold.max_ttl_ms)
}

fn psi_crosses(sample: Option<PsiSample>, threshold: ResourceThresholds) -> bool {
    sample.is_some_and(|value| {
        value.some_avg10 > threshold.max_psi_some_avg10
            || value.full_avg10 > threshold.max_psi_full_avg10
    })
}

fn journal_status_indicates_pressure(watermark: &ResourceWatermark) -> bool {
    watermark
        .journal_limit_status
        .as_deref()
        .is_some_and(|status| status != "within_limit")
}

fn cgroup_events_indicate_hard_pressure(watermark: &ResourceWatermark) -> bool {
    watermark
        .cgroup_memory_events
        .as_deref()
        .is_some_and(|events| {
            events.lines().any(|line| {
                let mut parts = line.split_whitespace();
                let Some(name) = parts.next() else {
                    return false;
                };
                let Some(value) = parts.next().and_then(|value| value.parse::<u64>().ok()) else {
                    return false;
                };
                matches!(name, "oom" | "oom_kill" | "max") && value > 0
            })
        })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceSeverity {
    Healthy,
    SoftDegraded,
    HardDegraded,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceAction {
    RecordBackpressure,
    ObserveRecovery,
    StopNewIntake,
    FinalizeOpenSegments,
    WriteIncompleteManifest,
    ResumeIntake,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceVerdict {
    pub severity: ResourceSeverity,
    pub actions: Vec<ResourceAction>,
    pub process_fatal: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResourceThresholdPolicyReceipt {
    pub soft_thresholds: ResourceThresholds,
    pub hard_thresholds: ResourceThresholds,
    pub threshold_unit: String,
    pub hard_hysteresis_samples: u32,
    pub recovery_hysteresis_samples: u32,
    pub soft_actions: Vec<ResourceAction>,
    pub hard_actions: Vec<ResourceAction>,
    pub recovery_actions: Vec<ResourceAction>,
    pub recovery_condition: String,
    pub last_result: ResourceSeverity,
}

impl ResourceThresholdPolicyReceipt {
    pub fn is_valid_for_complete_manifest(&self) -> bool {
        !self.threshold_unit.trim().is_empty()
            && self.hard_hysteresis_samples > 0
            && self.recovery_hysteresis_samples > 0
            && !self.recovery_condition.trim().is_empty()
            && self
                .soft_actions
                .contains(&ResourceAction::RecordBackpressure)
            && self.soft_actions.contains(&ResourceAction::ObserveRecovery)
            && self.hard_actions.contains(&ResourceAction::StopNewIntake)
            && self
                .hard_actions
                .contains(&ResourceAction::FinalizeOpenSegments)
            && self
                .hard_actions
                .contains(&ResourceAction::WriteIncompleteManifest)
            && self
                .recovery_actions
                .contains(&ResourceAction::ResumeIntake)
    }
}

pub struct ResourcePolicyMonitor {
    policy: ResourcePolicy,
    hard_hysteresis_samples: u32,
    recovery_hysteresis_samples: u32,
    consecutive_hard_samples: u32,
    consecutive_recovery_samples: u32,
    hard_action_active: bool,
    last_result: ResourceSeverity,
}

impl ResourcePolicyMonitor {
    pub fn new(
        policy: ResourcePolicy,
        hard_hysteresis_samples: u32,
        recovery_hysteresis_samples: u32,
    ) -> Self {
        Self {
            policy,
            hard_hysteresis_samples: hard_hysteresis_samples.max(1),
            recovery_hysteresis_samples: recovery_hysteresis_samples.max(1),
            consecutive_hard_samples: 0,
            consecutive_recovery_samples: 0,
            hard_action_active: false,
            last_result: ResourceSeverity::Healthy,
        }
    }

    pub fn observe(&mut self, watermark: ResourceWatermark) -> ResourceVerdict {
        let severity = self.policy.severity_for(&watermark);
        self.last_result = severity.clone();

        match severity {
            ResourceSeverity::HardDegraded => {
                self.consecutive_hard_samples = self.consecutive_hard_samples.saturating_add(1);
                self.consecutive_recovery_samples = 0;

                if self.consecutive_hard_samples >= self.hard_hysteresis_samples {
                    if self.hard_action_active {
                        return ResourceVerdict {
                            severity,
                            actions: vec![
                                ResourceAction::StopNewIntake,
                                ResourceAction::RecordBackpressure,
                                ResourceAction::ObserveRecovery,
                            ],
                            process_fatal: false,
                        };
                    }
                    self.hard_action_active = true;
                    ResourceVerdict {
                        severity,
                        actions: vec![
                            ResourceAction::StopNewIntake,
                            ResourceAction::FinalizeOpenSegments,
                            ResourceAction::WriteIncompleteManifest,
                        ],
                        process_fatal: false,
                    }
                } else {
                    ResourceVerdict {
                        severity,
                        actions: vec![
                            ResourceAction::RecordBackpressure,
                            ResourceAction::ObserveRecovery,
                        ],
                        process_fatal: false,
                    }
                }
            }
            ResourceSeverity::SoftDegraded => {
                self.consecutive_hard_samples = 0;
                self.consecutive_recovery_samples = 0;
                ResourceVerdict {
                    severity,
                    actions: vec![
                        ResourceAction::RecordBackpressure,
                        ResourceAction::ObserveRecovery,
                    ],
                    process_fatal: false,
                }
            }
            ResourceSeverity::Healthy => {
                self.consecutive_hard_samples = 0;
                let mut actions = Vec::new();
                if self.hard_action_active {
                    self.consecutive_recovery_samples =
                        self.consecutive_recovery_samples.saturating_add(1);
                    actions.push(ResourceAction::ObserveRecovery);
                    if self.consecutive_recovery_samples >= self.recovery_hysteresis_samples {
                        self.hard_action_active = false;
                        self.consecutive_recovery_samples = 0;
                        actions.push(ResourceAction::ResumeIntake);
                    }
                }
                ResourceVerdict {
                    severity,
                    actions,
                    process_fatal: false,
                }
            }
        }
    }

    pub fn policy_receipt(&self) -> ResourceThresholdPolicyReceipt {
        ResourceThresholdPolicyReceipt {
            soft_thresholds: self.policy.soft,
            hard_thresholds: self.policy.hard,
            threshold_unit: "bytes/percent/count/milliseconds/psi_avg10".to_owned(),
            hard_hysteresis_samples: self.hard_hysteresis_samples,
            recovery_hysteresis_samples: self.recovery_hysteresis_samples,
            soft_actions: vec![
                ResourceAction::RecordBackpressure,
                ResourceAction::ObserveRecovery,
            ],
            hard_actions: vec![
                ResourceAction::StopNewIntake,
                ResourceAction::FinalizeOpenSegments,
                ResourceAction::WriteIncompleteManifest,
            ],
            recovery_actions: vec![
                ResourceAction::ObserveRecovery,
                ResourceAction::ResumeIntake,
            ],
            recovery_condition: "below soft thresholds for recovery_hysteresis_samples".to_owned(),
            last_result: self.last_result.clone(),
        }
    }
}
