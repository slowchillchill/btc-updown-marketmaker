use crate::MaterialError;
use serde::Serialize;
use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::Path,
};
pub(crate) struct DecisionEvidenceV1(Option<BufWriter<std::fs::File>>);
impl DecisionEvidenceV1 {
    pub fn open(path: Option<&Path>) -> Result<Self, MaterialError> {
        Ok(Self(
            path.map(|p| {
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(p)
                    .map(BufWriter::new)
            })
            .transpose()?,
        ))
    }
    pub fn enabled(&self) -> bool {
        self.0.is_some()
    }
    pub fn record(&mut self, value: &impl Serialize) -> Result<(), MaterialError> {
        #[cfg(test)]
        flush_failure_fixture::record_type(
            serde_json::to_value(value)?
                .get("record_type")
                .and_then(|v| v.as_str()),
        );
        if let Some(writer) = &mut self.0 {
            serde_json::to_writer(&mut *writer, value)?;
            writer.write_all(b"\n")?;
        }
        Ok(())
    }
    pub fn flush(&mut self) -> Result<(), MaterialError> {
        #[cfg(test)]
        flush_failure_fixture::flush()?;
        if let Some(writer) = &mut self.0 {
            writer.flush()?;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn buffered_output_failure_is_propagated() {
        let mut out = DecisionEvidenceV1(Some(BufWriter::new(
            OpenOptions::new().write(true).open("/dev/full").unwrap(),
        )));
        assert!(out.record(&serde_json::json!({"value":1})).is_err() || out.flush().is_err());
    }
}

#[cfg(test)]
pub(crate) mod flush_failure_fixture {
    use std::cell::RefCell;
    thread_local! { static FAILURE: RefCell<Option<(&'static str,bool)>>=const {RefCell::new(None)}; }
    pub struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            FAILURE.with(|f| *f.borrow_mut() = None);
        }
    }
    pub fn after_record(record_type: &'static str) -> Guard {
        FAILURE.with(|f| *f.borrow_mut() = Some((record_type, false)));
        Guard
    }
    pub fn record_type(record: Option<&str>) {
        FAILURE.with(|f| {
            if let Some((target, armed)) = &mut *f.borrow_mut() {
                *armed |= Some(*target) == record;
            }
        });
    }
    pub fn flush() -> std::io::Result<()> {
        FAILURE.with(|f| {
            if f.borrow().is_some_and(|(_, armed)| armed) {
                *f.borrow_mut() = None;
                Err(std::io::Error::other(
                    "fixture required post-dispatch flush failure",
                ))
            } else {
                Ok(())
            }
        })
    }
}
