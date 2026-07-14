use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;

pub const GLOBAL_INFLIGHT_MAX_ROWS: usize = 100_000;
pub const GLOBAL_INFLIGHT_MAX_BYTES: usize = 134_217_728;
pub const MAX_TYPED_ROW_BYTES: usize = 1_048_576;
pub const BUFFER_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub(crate) struct InflightBudget {
    rows: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
}

impl InflightBudget {
    pub(crate) fn new() -> Self {
        Self::with_limits(GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES)
    }

    pub(crate) fn with_limits(max_rows: usize, max_bytes: usize) -> Self {
        Self {
            rows: Arc::new(Semaphore::new(max_rows)),
            bytes: Arc::new(Semaphore::new(max_bytes)),
        }
    }

    pub(crate) async fn acquire(
        &self,
        rows: usize,
        bytes: usize,
        wait: Duration,
    ) -> io::Result<InflightReservation> {
        let row_permits = u32::try_from(rows).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "global inflight row request exceeds u32",
            )
        })?;
        let byte_permits = u32::try_from(bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "global inflight byte request exceeds u32",
            )
        })?;
        let row = timeout(wait, Arc::clone(&self.rows).acquire_many_owned(row_permits))
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "operator_intervention_required:global_inflight_row_acquire_timeout:rows={rows}"
                    ),
                )
            })?
            .map_err(|_| io::Error::other("global inflight row budget closed"))?;
        let bytes = match timeout(
            wait,
            Arc::clone(&self.bytes).acquire_many_owned(byte_permits),
        )
        .await
        {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(_)) => return Err(io::Error::other("global inflight byte budget closed")),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "operator_intervention_required:global_inflight_byte_acquire_timeout:bytes={bytes}"
                    ),
                ));
            }
        };
        Ok(InflightReservation {
            _rows: row,
            _bytes: bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn try_acquire(&self, rows: usize, bytes: usize) -> InflightReservation {
        let rows = Arc::clone(&self.rows)
            .try_acquire_many_owned(u32::try_from(rows).unwrap())
            .unwrap();
        let bytes = Arc::clone(&self.bytes)
            .try_acquire_many_owned(u32::try_from(bytes).unwrap())
            .unwrap();
        InflightReservation {
            _rows: rows,
            _bytes: bytes,
        }
    }

    #[cfg(test)]
    pub(crate) fn available(&self) -> (usize, usize) {
        (
            self.rows.available_permits(),
            self.bytes.available_permits(),
        )
    }
}

#[derive(Debug)]
pub(crate) struct InflightReservation {
    _rows: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
