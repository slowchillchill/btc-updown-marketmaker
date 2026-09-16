use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, timeout};

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
        let deadline = Instant::now() + wait;
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
                    format!("global_inflight_row_acquire_timeout:rows={rows}"),
                )
            })?
            .map_err(|_| io::Error::other("global inflight row budget closed"))?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let bytes = match timeout(
            remaining,
            Arc::clone(&self.bytes).acquire_many_owned(byte_permits),
        )
        .await
        {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(_)) => return Err(io::Error::other("global inflight byte budget closed")),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("global_inflight_byte_acquire_timeout:bytes={bytes}"),
                ));
            }
        };
        Ok(InflightReservation {
            _rows: row,
            _bytes: bytes,
        })
    }

    pub(crate) fn try_acquire_available(
        &self,
        rows: usize,
        bytes: usize,
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
        let row = Arc::clone(&self.rows)
            .try_acquire_many_owned(row_permits)
            .map_err(try_acquire_error)?;
        let bytes = match Arc::clone(&self.bytes).try_acquire_many_owned(byte_permits) {
            Ok(bytes) => bytes,
            Err(error) => {
                drop(row);
                return Err(try_acquire_error(error));
            }
        };
        Ok(InflightReservation {
            _rows: row,
            _bytes: bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn try_acquire(&self, rows: usize, bytes: usize) -> InflightReservation {
        self.try_acquire_available(rows, bytes).unwrap()
    }

    #[cfg(test)]
    pub(crate) fn available(&self) -> (usize, usize) {
        (
            self.rows.available_permits(),
            self.bytes.available_permits(),
        )
    }
}

fn try_acquire_error(error: tokio::sync::TryAcquireError) -> io::Error {
    match error {
        tokio::sync::TryAcquireError::NoPermits => io::Error::new(
            io::ErrorKind::WouldBlock,
            "global inflight budget exhausted",
        ),
        tokio::sync::TryAcquireError::Closed => io::Error::other("global inflight budget closed"),
    }
}

#[derive(Debug)]
pub(crate) struct InflightReservation {
    _rows: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

impl InflightReservation {
    pub(crate) fn shrink_to(&mut self, rows: usize, bytes: usize) -> io::Result<()> {
        shrink_permit(&mut self._rows, rows, "row")?;
        shrink_permit(&mut self._bytes, bytes, "byte")?;
        Ok(())
    }

    pub(crate) fn split(&mut self, rows: usize, bytes: usize) -> io::Result<Self> {
        let available_rows = self._rows.num_permits();
        let available_bytes = self._bytes.num_permits();
        if rows > available_rows || bytes > available_bytes {
            return Err(io::Error::other(format!(
                "inflight reservation split exceeds held permits:rows={rows}/{available_rows}:bytes={bytes}/{available_bytes}"
            )));
        }
        let split_rows = self
            ._rows
            .split(rows)
            .expect("validated inflight row split");
        let split_bytes = self
            ._bytes
            .split(bytes)
            .expect("validated inflight byte split");
        Ok(Self {
            _rows: split_rows,
            _bytes: split_bytes,
        })
    }
}

fn shrink_permit(permit: &mut OwnedSemaphorePermit, target: usize, kind: &str) -> io::Result<()> {
    let held = permit.num_permits();
    if target > held {
        return Err(io::Error::other(format!(
            "inflight reservation {kind} target exceeds held permits:target={target}:held={held}"
        )));
    }
    let excess = held - target;
    if excess > 0 {
        drop(permit.split(excess).expect("validated inflight shrink"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquire_uses_one_total_deadline_for_rows_and_bytes() {
        let budget = InflightBudget::with_limits(1, 1);
        let held_row = budget.try_acquire_available(1, 0).unwrap();
        let held_byte = budget.try_acquire_available(0, 1).unwrap();
        let waiting_budget = budget.clone();
        let wait = Duration::from_millis(400);
        let started = Instant::now();
        let waiting = tokio::spawn(async move { waiting_budget.acquire(1, 1, wait).await });

        tokio::time::sleep(Duration::from_millis(250)).await;
        drop(held_row);
        let error = waiting.await.unwrap().unwrap_err();
        let elapsed = started.elapsed();

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(error.to_string().contains("byte_acquire_timeout"));
        assert!(
            elapsed < Duration::from_millis(550),
            "row and byte waits must share the 400ms deadline, elapsed={elapsed:?}"
        );
        drop(held_byte);
        assert_eq!(budget.available(), (1, 1));
    }

    #[tokio::test]
    async fn global_inflight_budget_enforces_exact_production_boundaries_and_releases() {
        let budget = InflightBudget::new();
        let reservation = budget
            .acquire(
                GLOBAL_INFLIGHT_MAX_ROWS,
                GLOBAL_INFLIGHT_MAX_BYTES,
                Duration::from_millis(10),
            )
            .await
            .unwrap();
        assert_eq!(budget.available(), (0, 0));
        let error = budget
            .acquire(1, 1, Duration::from_millis(10))
            .await
            .expect_err("the 100,001st row must not pass the global budget");
        assert!(error.to_string().contains("row_acquire_timeout"));

        drop(reservation);
        assert_eq!(
            budget.available(),
            (GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES)
        );
    }
}
