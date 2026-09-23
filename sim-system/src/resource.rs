use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceQuantityV1 {
    pub records: u64,
    pub bytes: u64,
}

impl ResourceQuantityV1 {
    pub const fn new(records: u64, bytes: u64) -> Self {
        Self { records, bytes }
    }
}

#[cfg(feature = "measurement")]
impl ResourceQuantityV1 {
    pub(crate) fn add_record(&mut self, bytes: u64) {
        self.records += 1;
        self.bytes += bytes;
    }
    pub(crate) fn add_records(&mut self, records: u64) {
        self.records += records;
    }
    pub(crate) fn include(&mut self, other: Self) {
        self.records += other.records;
        self.bytes += other.bytes;
    }
}
