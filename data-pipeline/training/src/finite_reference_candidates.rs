//! 单次有限重放的惰性校验前沿；只索引参考规范记录的物理位置。
use super::*;
use crate::public_recording::RecordingRowLocation;

#[derive(Clone, Copy)]
struct IndexedReference {
    input_position: u64,
    location: RecordingRowLocation,
    twap: bool,
}

pub(super) struct FiniteReferenceCandidates {
    frontier: PublicRecordingReaderV1,
    paths: Vec<PathBuf>,
    index: Vec<IndexedReference>,
    eof: bool,
    rows: usize,
    bytes: usize,
}

// 原页的顺序游标和单查询定位reader，不跨Input保留正文或解码批次。
#[derive(Default)]
pub(super) struct CandidateQuery {
    ordinal: usize,
    located: Option<(usize, usize, RunCursor)>,
}
impl CandidateQuery {
    fn load(
        &mut self,
        paths: &[PathBuf],
        location: RecordingRowLocation,
        rows: usize,
        bytes: usize,
    ) -> RecordingResult<PublicStoredRowV1> {
        #[cfg(test)]
        let _decode = cost::DecodeScope::new("candidate_location");
        if self
            .located
            .as_ref()
            .is_none_or(|(segment, _, _)| *segment != location.segment_index)
        {
            let mut cursor = RunCursor::open(&paths[location.segment_index], rows, bytes)?;
            cursor.start_row_group(location.row_group);
            self.located = Some((location.segment_index, location.row_group, cursor));
        }
        let (_, group, cursor) = self.located.as_mut().unwrap();
        if *group != location.row_group {
            cursor.start_row_group(location.row_group);
            *group = location.row_group;
        }
        while let Some((row, position)) = cursor.next_row_with_position()? {
            if position.row_group == location.row_group
                && position.row_in_group == location.row_in_group
            {
                return Ok(serde_json::from_slice(row.payload())?);
            }
            if position.row_group != location.row_group
                || position.row_in_group > location.row_in_group
            {
                break;
            }
        }
        Err("已验证参考记录的物理位置缺失".into())
    }
}
impl FiniteReferenceCandidates {
    pub(super) fn open(
        root: &Path,
        prefix: Option<CommittedIngressPrefixV1>,
        rows: usize,
        bytes: usize,
    ) -> RecordingResult<Self> {
        #[cfg(test)]
        let mut scan = cost::ScanGuard::new("candidate_open", None, None, true);
        let frontier = PublicRecordingReaderV1::open_committed(root, prefix, rows, bytes)?;
        let paths = frontier.segment_paths();
        #[cfg(test)]
        {
            scan.opened();
            scan.complete(0, None, None);
        }
        Ok(Self {
            frontier,
            paths,
            index: vec![],
            eof: false,
            rows,
            bytes,
        })
    }

    pub(super) fn page(
        &mut self,
        query: &mut CandidateQuery,
        cursor: TemporaryReferenceCursorV1,
        cutoff: u64,
    ) -> RecordingResult<(
        Vec<TemporaryReferenceCandidateV1>,
        TemporaryReferenceCursorV1,
        bool,
    )> {
        #[cfg(test)]
        let mut page = cost::ScanGuard::new(
            "candidate_page",
            Some(cursor.next_input_position),
            Some(cutoff),
            false,
        );
        let mut values = Vec::new();
        let mut next = cursor;
        let exhausted = loop {
            if values.len() == self.rows {
                break false;
            }
            // 必须先验证原页会读取的全部记录，包括非参考行和EOF计数。
            if query.ordinal == self.index.len() && !self.eof {
                #[cfg(test)]
                let mut validation = cost::ScanGuard::new(
                    "candidate_index_validation",
                    Some(cursor.next_input_position),
                    Some(cutoff),
                    false,
                );
                while query.ordinal == self.index.len() && !self.eof {
                    #[cfg(test)]
                    let _decode = cost::DecodeScope::new("candidate_index_validation");
                    match self.frontier.next_row_with_location()? {
                        None => self.eof = true,
                        Some((row, location)) => {
                            #[cfg(test)]
                            validation.row(&row);
                            if let PublicStoredRowV1::Canonical {
                                binding,
                                record: CanonicalEventRecordV2::ReferenceEvent(event),
                                ..
                            } = row
                            {
                                self.index.push(IndexedReference {
                                    input_position: binding.position.closed_at_input_position,
                                    location,
                                    twap: matches!(
                                        event.payload,
                                        CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(_)
                                    ),
                                });
                                #[cfg(test)]
                                cost::index_size(
                                    self.index.len(),
                                    self.index.capacity(),
                                    std::mem::size_of::<IndexedReference>(),
                                );
                            }
                        }
                    }
                }
                #[cfg(test)]
                validation.complete(0, None, Some(self.eof));
            }
            let Some(entry) = self.index.get(query.ordinal).copied() else {
                break true;
            };
            query.ordinal += 1;
            // 非TWAP Reference也必须在cursor过滤之前暴露原越界结束。
            if entry.input_position > cutoff {
                break true;
            }
            if entry.input_position < cursor.next_input_position || !entry.twap {
                continue;
            }
            let row = query.load(&self.paths, entry.location, self.rows, self.bytes)?;
            #[cfg(test)]
            page.row(&row);
            let PublicStoredRowV1::Canonical {
                binding,
                record: CanonicalEventRecordV2::ReferenceEvent(event),
                ..
            } = row
            else {
                return Err("已验证参考位置的规范记录类型不符".into());
            };
            next.next_input_position = entry.input_position + 1;
            values.push(reference_candidate(binding.position, event));
            #[cfg(test)]
            page.matched();
        };
        #[cfg(test)]
        {
            page.complete(
                values.len(),
                Some(next.next_input_position),
                Some(exhausted),
            );
        }
        Ok((values, next, exhausted))
    }

    #[cfg(test)]
    pub(super) fn statistics(&self) -> serde_json::Value {
        serde_json::json!({"item_size_bytes":std::mem::size_of::<IndexedReference>(),
            "len":self.index.len(),"capacity":self.index.capacity(),
            "capacity_bytes":self.index.capacity()*std::mem::size_of::<IndexedReference>(),
            "stable_paths_len":self.paths.len(),"stable_paths_capacity":self.paths.capacity(),
            "stable_path_buffer_capacity_bytes":self.paths.iter().map(|p|p.capacity()).sum::<usize>(),
            "validation_eof":self.eof,"note":"只保留已验证Reference物理位置/类型；路径与原reader、Parquet元数据/读批次另列，不包含完整TWAP历史"})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate_a::SequentialCaptureWriter;
    use crate::public_recording::tests::{apply, config, core, end, rule_record, twap_record};
    use std::sync::{Arc, Mutex};

    fn fixture(root: &Path) {
        let mut c = core();
        let mut writer =
            crate::public_recording::PublicRecordingWriterV1::create(root, c.header(), config())
                .unwrap();
        for input in [
            twap_record(0, 60, 21_002, "65000000000000000000000"),
            rule_record(1, 60, Some("65000")),
            twap_record(2, 30, 22_003, "65010000000000000000000"),
            end(3),
        ] {
            let result = apply(&mut c, &input);
            writer.record(input, result).unwrap();
        }
        writer.stabilize().unwrap();
    }
    fn cursor(position: u64) -> TemporaryReferenceCursorV1 {
        TemporaryReferenceCursorV1 {
            next_input_position: position,
        }
    }
    fn indexed(root: &Path, rows: usize) -> FiniteReferenceCandidates {
        let prefix = PublicRecordingReaderV1::recover(root, rows, 1 << 20)
            .unwrap()
            .prefix;
        FiniteReferenceCandidates::open(root, prefix, rows, 1 << 20).unwrap()
    }
    fn projected(
        page: (
            Vec<TemporaryReferenceCandidateV1>,
            TemporaryReferenceCursorV1,
            bool,
        ),
    ) -> serde_json::Value {
        serde_json::json!({"values":page.0.iter().map(|v|serde_json::json!({"canonical_fact":v.canonical_fact,"record_ref":v.record_ref,"anchor":v.anchor})).collect::<Vec<_>>(),"next":page.1.next_input_position,"finished":page.2})
    }
    fn corrupt_input_two(root: &Path, temporary: &Path) {
        let reader = PublicRecordingReaderV1::recover(root, 1, 1 << 20).unwrap();
        let path = root
            .join(reader.prefix.unwrap().stable_storage_ref)
            .join("captured.parquet");
        let mut cursor = RunCursor::open(&path, 1, 1 << 20).unwrap();
        let mut writer = SequentialCaptureWriter::create(
            "public_recording_v1",
            temporary,
            Arc::new(Mutex::new(0)),
            &config(),
        )
        .unwrap();
        let mut n = 0;
        let mut changed = false;
        while let Some(row) = cursor.next_row().unwrap() {
            let parsed: PublicStoredRowV1 = serde_json::from_slice(row.payload()).unwrap();
            let bytes = if matches!(parsed,PublicStoredRowV1::Input(ref i) if i.input_position==2) {
                changed = true;
                b"not-json".as_slice()
            } else {
                row.payload()
            };
            writer
                .push(n, row.sort_key_bytes().to_vec(), bytes)
                .unwrap();
            n += 1;
        }
        assert!(changed);
        drop(cursor);
        let new = writer.close_segment().unwrap();
        fs::rename(new, path).unwrap();
    }
    #[test]
    fn complete_pages_cursor_full_page_and_current_input_match_original() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("public");
        fixture(&root);
        let mut indexed = indexed(&root, 1);
        for cutoff in [0, 1, 2, 3, 1] {
            let mut old = ReferenceCandidateReaderV1::open(&root, 1, 1 << 20, cutoff).unwrap();
            let mut query = CandidateQuery::default();
            let mut c = cursor(0);
            loop {
                let a = old.page(c).unwrap();
                let b = indexed.page(&mut query, c, cutoff).unwrap();
                assert_eq!(projected((a.0.clone(), a.1, a.2)), projected(b));
                if a.2 {
                    break;
                }
                assert_eq!(a.0.len(), 1);
                c = a.1;
            }
        }
        assert!(indexed.eof);
    }
    #[test]
    fn full_page_defers_bad_json_and_non_twap_cutoff_preserves_original_frontier() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("public");
        fixture(&root);
        corrupt_input_two(&root, &temp.path().join("rewrite"));
        let mut state = indexed(&root, 1);
        let mut old = ReferenceCandidateReaderV1::open(&root, 1, 1 << 20, 3).unwrap();
        let mut query = CandidateQuery::default();
        let a = old.page(cursor(0)).unwrap();
        let b = state.page(&mut query, cursor(0), 3).unwrap();
        assert!(!a.2);
        assert_eq!(projected((a.0.clone(), a.1, a.2)), projected(b));
        let original = old.page(a.1).err().unwrap().to_string();
        assert_eq!(
            state.page(&mut query, a.1, 3).err().unwrap().to_string(),
            original
        );
        // 新状态只扩大到原Reference越界，不能为了EOF提前读后面的坏JSON。
        let mut state = indexed(&root, 2);
        let mut query = CandidateQuery::default();
        let mut old = ReferenceCandidateReaderV1::open(&root, 2, 1 << 20, 0).unwrap();
        let a = old.page(cursor(0)).unwrap();
        assert!(a.2);
        assert_eq!(a.0.len(), 1);
        assert_eq!(
            projected(a),
            projected(state.page(&mut query, cursor(0), 0).unwrap())
        );
        assert!(!state.eof);
        let mut query = CandidateQuery::default();
        assert_eq!(
            state
                .page(&mut query, cursor(0), 3)
                .err()
                .unwrap()
                .to_string(),
            original
        );
    }
    #[test]
    fn page_filled_before_eof_defers_original_count_error_until_next_page() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("public");
        fixture(&root);
        let reader = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
        let mut prefix = reader.prefix.unwrap();
        prefix.next_canonical_position += 1;
        fs::write(
            root.join(&prefix.stable_storage_ref).join("prefix.json"),
            serde_json::to_vec(&prefix).unwrap(),
        )
        .unwrap();
        let mut state = indexed(&root, 2);
        let mut query = CandidateQuery::default();
        let mut old = ReferenceCandidateReaderV1::open(&root, 2, 1 << 20, 3).unwrap();
        let a = old.page(cursor(0)).unwrap();
        assert_eq!(a.0.len(), 2);
        assert!(!a.2);
        assert_eq!(
            projected((a.0.clone(), a.1, a.2)),
            projected(state.page(&mut query, cursor(0), 3).unwrap())
        );
        let error = old.page(a.1).err().unwrap().to_string();
        assert_eq!(error, "稳定分片内容与前缀计数不符");
        assert_eq!(
            state.page(&mut query, a.1, 3).err().unwrap().to_string(),
            error
        );
    }
}
