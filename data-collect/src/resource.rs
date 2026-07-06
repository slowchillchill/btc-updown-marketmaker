#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricUnavailable {
    pub metric: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CgroupMemoryStat {
    pub anon_bytes: u64,
    pub file_bytes: u64,
    pub slab_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PsiSample {
    pub some_avg10: f64,
    pub full_avg10: f64,
}

#[derive(Clone, Debug, PartialEq)]
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

#[derive(Clone, Debug, PartialEq)]
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

    fn metric_recorded(&self, metric: &str, present: bool) -> bool {
        present
            || self
                .unavailable
                .iter()
                .any(|item| item.metric == metric && !item.reason.is_empty())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
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
        if self.crosses_hard(&watermark) {
            return ResourceVerdict {
                severity: ResourceSeverity::HardDegraded,
                actions: vec![
                    ResourceAction::StopNewIntake,
                    ResourceAction::FinalizeOpenSegments,
                    ResourceAction::WriteIncompleteManifest,
                ],
                process_fatal: false,
            };
        }

        if self.crosses_soft(&watermark) {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourceSeverity {
    Healthy,
    SoftDegraded,
    HardDegraded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourceAction {
    RecordBackpressure,
    ObserveRecovery,
    StopNewIntake,
    FinalizeOpenSegments,
    WriteIncompleteManifest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceVerdict {
    pub severity: ResourceSeverity,
    pub actions: Vec<ResourceAction>,
    pub process_fatal: bool,
}
