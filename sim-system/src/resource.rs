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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct BusinessResourceDeltaV2 {
    pub(crate) added_records: u64,
    pub(crate) removed_records: u64,
    pub(crate) added_bytes: u64,
    pub(crate) removed_bytes: u64,
}

impl BusinessResourceDeltaV2 {
    pub(crate) fn add_record(&mut self, owned_bytes: u64) {
        self.added_records += 1;
        self.added_bytes += owned_bytes;
    }

    pub(crate) fn remove_record(&mut self, owned_bytes: u64) {
        self.removed_records += 1;
        self.removed_bytes += owned_bytes;
    }

    pub(crate) fn add_records(&mut self, records: u64) {
        self.added_records += records;
    }

    pub(crate) fn remove_records(&mut self, records: u64) {
        self.removed_records += records;
    }

    pub(crate) fn replace_bytes(&mut self, previous: u64, next: u64) {
        if next >= previous {
            self.added_bytes += next - previous;
        } else {
            self.removed_bytes += previous - next;
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.added_records += other.added_records;
        self.removed_records += other.removed_records;
        self.added_bytes += other.added_bytes;
        self.removed_bytes += other.removed_bytes;
    }

    pub(crate) fn apply_to(self, current: ResourceQuantityV1) -> Option<ResourceQuantityV1> {
        Some(ResourceQuantityV1::new(
            current
                .records
                .checked_add(self.added_records)?
                .checked_sub(self.removed_records)?,
            current
                .bytes
                .checked_add(self.added_bytes)?
                .checked_sub(self.removed_bytes)?,
        ))
    }
}
