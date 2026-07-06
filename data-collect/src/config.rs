use crate::backpressure::QueueConfigError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CrossTaskQueueConfig {
    pub name: String,
    pub capacity_rows: usize,
    pub max_bytes: u64,
    pub max_memory_watermark_bytes: u64,
    pub max_oldest_event_age_ms: u64,
    pub max_writer_lag_ms: u64,
}

impl CrossTaskQueueConfig {
    pub fn new(
        name: impl Into<String>,
        capacity_rows: usize,
        max_bytes: u64,
        max_memory_watermark_bytes: u64,
        max_oldest_event_age_ms: u64,
        max_writer_lag_ms: u64,
    ) -> Self {
        Self {
            name: name.into(),
            capacity_rows,
            max_bytes,
            max_memory_watermark_bytes,
            max_oldest_event_age_ms,
            max_writer_lag_ms,
        }
    }
}

pub fn validate_cross_task_queue_configs(
    configs: &[CrossTaskQueueConfig],
) -> Result<(), QueueConfigError> {
    if configs.is_empty() {
        return Err(QueueConfigError::new(
            "at least one cross-task queue config is required",
        ));
    }

    for config in configs {
        if config.name.trim().is_empty() {
            return Err(QueueConfigError::new("queue name must be non-empty"));
        }
        if config.capacity_rows == 0 {
            return Err(QueueConfigError::new(format!(
                "{} capacity_rows must be positive",
                config.name
            )));
        }
        if config.max_bytes == 0 {
            return Err(QueueConfigError::new(format!(
                "{} max_bytes must be positive",
                config.name
            )));
        }
        if config.max_memory_watermark_bytes == 0 {
            return Err(QueueConfigError::new(format!(
                "{} max_memory_watermark_bytes must be positive",
                config.name
            )));
        }
        if config.max_bytes > config.max_memory_watermark_bytes {
            return Err(QueueConfigError::new(format!(
                "{} max_bytes must not exceed max_memory_watermark_bytes",
                config.name
            )));
        }
        if config.max_oldest_event_age_ms == 0 {
            return Err(QueueConfigError::new(format!(
                "{} max_oldest_event_age_ms must be positive",
                config.name
            )));
        }
        if config.max_writer_lag_ms == 0 {
            return Err(QueueConfigError::new(format!(
                "{} max_writer_lag_ms must be positive",
                config.name
            )));
        }
    }

    Ok(())
}
