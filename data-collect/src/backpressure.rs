use tokio::sync::mpsc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueConfigError {
    message: String,
}

impl QueueConfigError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for QueueConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for QueueConfigError {}

pub fn bounded_event_queue<T>(
    capacity: usize,
) -> Result<(mpsc::Sender<T>, mpsc::Receiver<T>), QueueConfigError> {
    if capacity == 0 {
        return Err(QueueConfigError::new(
            "bounded event queue capacity must be positive",
        ));
    }
    Ok(mpsc::channel(capacity))
}
