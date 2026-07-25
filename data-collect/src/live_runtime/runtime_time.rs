use super::*;

pub(super) const UTC_HOUR_MS: i64 = 3_600_000;
pub(super) const UTC_DAY_MS: i64 = 86_400_000;

pub(super) fn deadline_reached(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|deadline| Instant::now() >= deadline)
}

pub(super) fn deadline_remaining(deadline: Option<Instant>) -> Option<Duration> {
    deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()))
}

pub(super) fn utc_day_string_from_ms(utc_ms: i64) -> Option<String> {
    let dt = OffsetDateTime::from_unix_timestamp(utc_ms.div_euclid(1000)).ok()?;
    Some(format!(
        "{:04}-{:02}-{:02}",
        dt.year(),
        u8::from(dt.month()),
        dt.day()
    ))
}

pub(super) fn utc_hour_string_from_ms(utc_ms: i64) -> Option<String> {
    let dt = OffsetDateTime::from_unix_timestamp(utc_ms.div_euclid(1000)).ok()?;
    Some(format!(
        "{:04}-{:02}-{:02}T{:02}",
        dt.year(),
        u8::from(dt.month()),
        dt.day(),
        dt.hour()
    ))
}

pub(super) fn next_utc_hour_boundary_ms(utc_ms: i64) -> i64 {
    utc_ms
        .div_euclid(UTC_HOUR_MS)
        .saturating_mul(UTC_HOUR_MS)
        .saturating_add(UTC_HOUR_MS)
}

pub(super) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub(super) fn monotonic_ms(started: Instant) -> u64 {
    started.elapsed().as_millis() as u64
}
