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
        progress: &mut Option<ReplayProgress>,
        stage: Stage,
    ) -> RecordingResult<PublicStoredRowV1> {
        if let Some(p) = progress.as_mut() {
            p.checkpoint(stage, false);
        }
        if self
            .located
            .as_ref()
            .is_none_or(|(segment, _, _)| *segment != location.file_index)
        {
            let mut cursor = RunCursor::open(&paths[location.file_index], rows, bytes)?;
            cursor.start_row_group(location.row_group);
            self.located = Some((location.file_index, location.row_group, cursor));
        }
        let (_, group, cursor) = self.located.as_mut().unwrap();
        if *group != location.row_group {
            cursor.start_row_group(location.row_group);
            *group = location.row_group;
        }
        while let Some((row, position)) = cursor.next_row_with_position()? {
            if let Some(p) = progress.as_mut() {
                match stage {
                    Stage::DependencyLocation => p.work.dependency_location_rows_visited += 1,
                    _ => p.work.candidate_location_rows_visited += 1,
                }
                p.checkpoint(stage, false);
            }
            if position.row_group == location.row_group
                && position.row_in_group == location.row_in_group
            {
                let result = serde_json::from_slice(row.payload())?;
                if let Some(p) = progress.as_mut() {
                    match stage {
                        Stage::DependencyLocation => p.work.dependency_target_records_parsed += 1,
                        _ => p.work.candidate_target_records_parsed += 1,
                    }
                }
                return Ok(result);
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
        let result = Self::open_inner(root, prefix, rows, bytes)?;
        #[cfg(test)]
        {
            scan.opened();
            scan.complete(0, None, None);
        }
        Ok(result)
    }

    pub(super) fn open_for_dependencies(
        root: &Path,
        prefix: Option<CommittedIngressPrefixV1>,
        rows: usize,
        bytes: usize,
    ) -> RecordingResult<Self> {
        #[cfg(test)]
        let mut scan = cost::ScanGuard::new("dependency_index_open", None, None, true);
        let result = Self::open_inner(root, prefix, rows, bytes)?;
        #[cfg(test)]
        {
            scan.opened();
            scan.complete(0, None, None);
        }
        Ok(result)
    }

    fn open_inner(
        root: &Path,
        prefix: Option<CommittedIngressPrefixV1>,
        rows: usize,
        bytes: usize,
    ) -> RecordingResult<Self> {
        let frontier = PublicRecordingReaderV1::open_committed(root, prefix, rows, bytes)?;
        let paths = frontier.file_paths();
        Ok(Self {
            frontier,
            paths,
            index: vec![],
            eof: false,
            rows,
            bytes,
        })
    }

    // 所有用途都由原reader解析/observe并验证原稳定EOF；只将已验证位置纳入索引。
    fn advance(&mut self) -> RecordingResult<Option<PublicStoredRowV1>> {
        let Some((row, location)) = self.frontier.next_physical_row_with_location()? else {
            self.eof = true;
            return Ok(None);
        };
        if let PublicStoredRowV1::Canonical {
            binding,
            record: CanonicalEventRecordV2::ReferenceEvent(event),
            ..
        } = &row
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
        Ok(Some(row))
    }

    pub(super) fn dependencies(
        &mut self,
        refs: &[CanonicalInputRecordRefV1],
        progress: &mut Option<ReplayProgress>,
    ) -> RecordingResult<Vec<CanonicalReferenceEventV2>> {
        if refs.is_empty() {
            return Ok(vec![]);
        }
        let needs_validation = !self.eof;
        if needs_validation && let Some(p) = progress.as_mut() {
            p.checkpoint(Stage::DependencyValidation, false);
        }
        // 早处命中/异流/缺失均不能豁免首次非空请求的完整EOF责任。
        {
            #[cfg(test)]
            let mut validation =
                cost::ScanGuard::new("dependency_index_validation", None, None, false);
            while !self.eof {
                #[cfg(test)]
                let _decode = cost::DecodeScope::new("dependency_index_validation");
                if let Some(row) = self.advance()? {
                    if let Some(p) = progress.as_mut() {
                        p.work.dependency_records_validated += 1;
                        p.checkpoint(Stage::DependencyValidation, false);
                    }
                    #[cfg(test)]
                    validation.row(&row);
                    #[cfg(not(test))]
                    let _ = row;
                }
            }
            #[cfg(test)]
            validation.complete(0, None, Some(true));
        }
        if needs_validation && let Some(p) = progress.as_mut() {
            p.checkpoint(Stage::DependencyValidation, true);
        }
        let mut query = CandidateQuery::default();
        let mut output = vec![];
        #[cfg(test)]
        let mut locate = cost::ScanGuard::new("dependency_location", None, None, false);
        // observe已逐条保证binding流身份等于header流身份；不重复存储字符串。
        let stream = &self.frontier.header.input_stream_id;
        for entry in &self.index {
            if !refs
                .iter()
                .any(|r| r.input_stream_id == *stream && r.input_position == entry.input_position)
            {
                continue;
            }
            #[cfg(test)]
            let _decode = cost::DecodeScope::new("dependency_location");
            let row = query.load(
                &self.paths,
                entry.location,
                self.rows,
                self.bytes,
                progress,
                Stage::DependencyLocation,
            )?;
            #[cfg(test)]
            locate.row(&row);
            let PublicStoredRowV1::Canonical {
                record: CanonicalEventRecordV2::ReferenceEvent(event),
                ..
            } = row
            else {
                return Err("已验证参考位置的规范记录类型不符".into());
            };
            output.push(event);
            #[cfg(test)]
            locate.matched();
        }
        #[cfg(test)]
        locate.complete(output.len(), None, Some(true));
        Ok(output)
    }

    pub(super) fn page(
        &mut self,
        query: &mut CandidateQuery,
        cursor: TemporaryReferenceCursorV1,
        cutoff: u64,
        progress: &mut Option<ReplayProgress>,
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
                if let Some(p) = progress.as_mut() {
                    p.checkpoint(Stage::CandidateValidation, false);
                }
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
                    if let Some(row) = self.advance()? {
                        if let Some(p) = progress.as_mut() {
                            p.work.candidate_records_validated += 1;
                            p.checkpoint(Stage::CandidateValidation, false);
                        }
                        #[cfg(test)]
                        validation.row(&row);
                        #[cfg(not(test))]
                        let _ = row;
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
            #[cfg(test)]
            let _decode = cost::DecodeScope::new("candidate_location");
            let row = query.load(
                &self.paths,
                entry.location,
                self.rows,
                self.bytes,
                progress,
                Stage::CandidateLocation,
            )?;
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
        fixture_parts(root, false);
    }
    fn fixture_parts(root: &Path, split: bool) {
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
            let position = input.input_position;
            writer.record(input, result).unwrap();
            if split && position == 1 {
                writer.split_part_for_test().unwrap();
            }
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
        fixture_parts(&root, true);
        let mut indexed = indexed(&root, 1);
        for cutoff in [0, 1, 2, 3, 1] {
            let mut old = ReferenceCandidateReaderV1::open(&root, 1, 1 << 20, cutoff).unwrap();
            let mut query = CandidateQuery::default();
            let mut c = cursor(0);
            loop {
                let a = old.page(c).unwrap();
                let b = indexed.page(&mut query, c, cutoff, &mut None).unwrap();
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
        let b = state.page(&mut query, cursor(0), 3, &mut None).unwrap();
        assert!(!a.2);
        assert_eq!(projected((a.0.clone(), a.1, a.2)), projected(b));
        let original = old.page(a.1).err().unwrap().to_string();
        assert_eq!(
            state
                .page(&mut query, a.1, 3, &mut None)
                .err()
                .unwrap()
                .to_string(),
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
            projected(state.page(&mut query, cursor(0), 0, &mut None).unwrap())
        );
        assert!(!state.eof);
        let mut query = CandidateQuery::default();
        assert_eq!(
            state
                .page(&mut query, cursor(0), 3, &mut None)
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
            projected(state.page(&mut query, cursor(0), 3, &mut None).unwrap())
        );
        let error = old.page(a.1).err().unwrap().to_string();
        assert_eq!(error, "稳定分片内容与前缀计数不符");
        assert_eq!(
            state
                .page(&mut query, a.1, 3, &mut None)
                .err()
                .unwrap()
                .to_string(),
            error
        );
    }
    fn dependency_ref(position: u64) -> CanonicalInputRecordRefV1 {
        CanonicalInputRecordRefV1 {
            input_stream_id: "core-test".into(),
            input_position: position,
        }
    }
    #[test]
    fn saved_dependencies_keep_all_types_order_duplicates_missing_and_future_cutoff() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("public");
        fixture_parts(&root, true);
        let mut state = indexed(&root, 1);
        assert!(state.dependencies(&[], &mut None).unwrap().is_empty());
        assert!(state.index.is_empty());
        assert!(!state.eof);
        let mut foreign = dependency_ref(0);
        foreign.input_stream_id = "another-stream".into();
        let refs = vec![
            dependency_ref(2),
            dependency_ref(0),
            dependency_ref(1),
            dependency_ref(0),
            foreign,
            dependency_ref(99),
        ];
        let old = read_dependencies(&root, &refs, 1, 1 << 20).unwrap();
        let new = state.dependencies(&refs, &mut None).unwrap();
        assert_eq!(
            serde_json::to_value(&old).unwrap(),
            serde_json::to_value(&new).unwrap()
        );
        assert_eq!(new.len(), 4);
        assert!(!matches!(
            new[1].payload,
            CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(_)
        ));
        assert!(state.eof);
        // 已验证未来事实仍不能穿越候选cutoff，包括非TWAP越界和恰满页。
        for cutoff in [0, 1, 2, 3] {
            let mut query = CandidateQuery::default();
            let mut old = ReferenceCandidateReaderV1::open(&root, 1, 1 << 20, cutoff).unwrap();
            let mut c = cursor(0);
            loop {
                let a = old.page(c).unwrap();
                assert_eq!(
                    projected((a.0.clone(), a.1, a.2)),
                    projected(state.page(&mut query, c, cutoff, &mut None).unwrap())
                );
                if a.2 {
                    break;
                }
                c = a.1;
            }
        }
        let missing = [dependency_ref(99)];
        assert!(state.dependencies(&missing, &mut None).unwrap().is_empty());
    }
    #[test]
    fn candidate_success_then_early_dependency_match_still_exposes_bad_tail() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("public");
        fixture(&root);
        corrupt_input_two(&root, &temp.path().join("rewrite"));
        let mut state = indexed(&root, 1);
        let page = state
            .page(&mut CandidateQuery::default(), cursor(0), 0, &mut None)
            .unwrap();
        assert_eq!(page.0.len(), 1);
        assert!(!state.eof);
        let refs = [dependency_ref(0)];
        let old = read_dependencies(&root, &refs, 1, 1 << 20)
            .unwrap_err()
            .to_string();
        assert_eq!(
            state
                .dependencies(&refs, &mut None)
                .unwrap_err()
                .to_string(),
            old
        );
    }
    #[test]
    fn early_dependency_match_does_not_hide_eof_count_error() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("public");
        fixture(&root);
        let mut prefix = PublicRecordingReaderV1::recover(&root, 1, 1 << 20)
            .unwrap()
            .prefix
            .unwrap();
        prefix.next_canonical_position += 1;
        fs::write(
            root.join(&prefix.stable_storage_ref).join("prefix.json"),
            serde_json::to_vec(&prefix).unwrap(),
        )
        .unwrap();
        let refs = [dependency_ref(0)];
        let old = read_dependencies(&root, &refs, 1, 1 << 20)
            .unwrap_err()
            .to_string();
        assert_eq!(old, "稳定分片内容与前缀计数不符");
        let mut state = indexed(&root, 1);
        assert_eq!(
            state
                .dependencies(&refs, &mut None)
                .unwrap_err()
                .to_string(),
            old
        );
        assert!(!state.eof);
    }

    #[test]
    fn same_input_multiple_reference_members_are_not_collapsed() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("public");
        crate::public_recording::tests::historical_identity_fixture(&root);
        let refs = [dependency_ref(2), dependency_ref(2), dependency_ref(90)];
        let old = read_dependencies(&root, &refs, 1, 1 << 20).unwrap();
        let mut state = indexed(&root, 1);
        let new = state.dependencies(&refs, &mut None).unwrap();
        assert_eq!(new.len(), 2);
        assert_eq!(
            serde_json::to_value(old).unwrap(),
            serde_json::to_value(new).unwrap()
        );
    }
}
