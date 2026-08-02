use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::tempdir;

const CURRENT: &str = "data-collect/docs/CURRENT.md";
const MANIFEST: &str = "data-collect/docs/archive-manifest.tsv";
const TAG: &str = "data-collect-history-pre-current-authority-20260730";
const TAG_OBJECT: &str = "7f9a7b09cc7936cdd3c38b7d9953878985614626";
const PEELED_COMMIT: &str = "4ed31ad6abe7237d92cc99a4857979d8ae26d373";
const CURRENT_ENTRY_MARKER: &str = "> 生命周期：`CURRENT_AUTHORITY_ENTRY`";
const CURRENT_CONTRACT_MARKER: &str = "> 生命周期：`CURRENT_CONTRACT`";
const HISTORICAL_MARKER: &str = "> 生命周期：`HISTORICAL_EVIDENCE_ONLY`";
const EVIDENCE_MARKER: &str = "> 生命周期：`EVIDENCE_ONLY_INPUT`";

const MASTER_TRIPLET: [&str; 3] = [
    "demand/data-collect-demand.md",
    "plan/data-collect-plan.md",
    "changelog/data-collect-changelog.md",
];

const CURRENT_DOCS: [&str; 6] = [
    "data-collect/docs/current/contracts.md",
    "data-collect/docs/current/interfaces.md",
    "data-collect/docs/current/operations.md",
    "data-collect/docs/current/acceptance.md",
    "data-collect/docs/current/known-limitations.md",
    "data-collect/docs/current/document-authority-audit.md",
];

const ROUTE_ONLY_DOCS: [&str; 5] = [
    "data-collect/README.md",
    "data-collect/docs/acceptance-matrix.md",
    "data-collect/docs/training-collector-deploy-readme.md",
    "data-collect/docs/paper-live-hot-path-integration.md",
    "data-collect/docs/data-collect-document-authority-audit-20260730.md",
];

const GOVERNANCE_TRIPLET: [&str; 3] = [
    "demand/data-collect-current-authority-consolidation-demand.md",
    "plan/data-collect-current-authority-consolidation-plan.md",
    "changelog/data-collect-current-authority-consolidation-changelog.md",
];

#[derive(Debug)]
struct ArchiveManifest {
    meta: BTreeMap<String, String>,
    historical: BTreeMap<String, String>,
    snapshots: BTreeMap<String, String>,
}

#[test]
fn current_authority_graph_is_unique_and_closed() {
    let root = repo_root();
    validate_entry_graph(&root).unwrap();

    let current_docs = marker_paths(&root, CURRENT_CONTRACT_MARKER).unwrap();
    assert_eq!(
        current_docs,
        CURRENT_DOCS.map(str::to_owned).into_iter().collect()
    );

    for path in CURRENT_DOCS {
        let doc = read(&root.join(path)).unwrap();
        for expected in [
            "> 当前执行 authority：`data-collect/docs/CURRENT.md`",
            "> 当前需求：`demand/data-collect-demand.md`",
            "> 当前计划：`plan/data-collect-plan.md`",
            "> 当前 changelog：`changelog/data-collect-changelog.md`",
            "> 历史读取入口：`data-collect/docs/archive-manifest.tsv`",
            "> 执行授权：`none`",
        ] {
            assert!(doc.contains(expected), "{path} missing {expected}");
        }
    }

    for (path, lifecycle) in [
        (MASTER_TRIPLET[0], "CURRENT_MASTER_DEMAND"),
        (MASTER_TRIPLET[1], "CURRENT_MASTER_PLAN_CLOSED"),
        (MASTER_TRIPLET[2], "CURRENT_MASTER_CHANGELOG"),
    ] {
        let doc = read(&root.join(path)).unwrap();
        assert!(doc.contains(&format!("> 生命周期：`{lifecycle}`")));
        assert!(doc.contains("data-collect/docs/CURRENT.md"));
        for peer in MASTER_TRIPLET {
            if peer != path {
                assert!(doc.contains(peer), "{path} does not reference {peer}");
            }
        }
    }

    for path in ROUTE_ONLY_DOCS {
        let doc = read(&root.join(path)).unwrap();
        assert!(doc.contains("> 生命周期：`ROUTE_ONLY`"));
        assert!(doc.contains("> 当前执行 authority：`none`"));
        assert!(doc.contains("superseded_by"));
    }

    for path in GOVERNANCE_TRIPLET {
        let doc = read(&root.join(path)).unwrap();
        assert!(doc.contains("> 生命周期：`GOVERNANCE_RECORD`"));
        assert!(doc.contains("> 当前执行 authority：`none`"));
        assert!(doc.contains("> superseded_by：`data-collect/docs/CURRENT.md`"));
    }

    for path in ["AGENTS.md", "data-collect/README.md"] {
        assert!(
            read(&root.join(path))
                .unwrap()
                .contains("data-collect/docs/CURRENT.md"),
            "{path} does not route to CURRENT"
        );
    }

    let current = read(&root.join(CURRENT)).unwrap();
    for rule in [
        "`historical` 行是已移出活动树的历史长文",
        "`snapshot` 行是仍有活动路径",
        "旧 blob 不保证含生命周期首屏标记",
        "清单中的 `kind=snapshot`",
        "活动树同路径文件和本 `CURRENT` 的对应主题归属文档",
    ] {
        assert!(
            current.contains(rule),
            "{CURRENT} missing archive rule {rule}"
        );
    }
    let agents = read(&root.join("AGENTS.md")).unwrap();
    for rule in [
        "清单中 `historical` 行对应的历史长文含首屏标记",
        "清单中 `snapshot` 行是活动路径",
        "旧 blob 不保证含首屏标记",
        "以 `kind=snapshot` 判定其当前执行权威为",
        "清单行、活动树同路径文件和 `CURRENT.md` 对应主题",
    ] {
        assert!(
            agents.contains(rule),
            "AGENTS.md missing archive rule {rule}"
        );
    }

    let rebuild = read(&root.join("demand/rebuild-core-demand.md")).unwrap();
    assert!(rebuild.contains("data-collect/docs/CURRENT.md"));
    assert!(rebuild.contains("不是 `data-collect` 的当前权威文档"));

    let sim_system = read(&root.join("demand/sim-system-demand.md")).unwrap();
    assert!(sim_system.contains("data-collect/docs/CURRENT.md"));
    assert!(sim_system.contains("继续拥有 `data-collect` 当前事实"));

    let mut expected_related = BTreeSet::from([
        "AGENTS.md".to_owned(),
        "data-collect/README.md".to_owned(),
        CURRENT.to_owned(),
        "demand/rebuild-core-demand.md".to_owned(),
        "demand/sim-system-demand.md".to_owned(),
    ]);
    expected_related.extend(MASTER_TRIPLET.map(str::to_owned));
    expected_related.extend(CURRENT_DOCS.map(str::to_owned));
    expected_related.extend(ROUTE_ONLY_DOCS.map(str::to_owned));
    expected_related.extend(GOVERNANCE_TRIPLET.map(str::to_owned));
    expected_related.extend(tag_marker_paths(&root, EVIDENCE_MARKER).unwrap());
    assert_eq!(
        paths_containing_any(&root, &["data-collect", "data_collect", "DataCollect"]).unwrap(),
        expected_related,
        "every active Markdown that mentions data-collect must have an explicit disposition"
    );
}

#[test]
fn archive_manifest_restores_every_frozen_object_and_isolates_history() {
    let root = repo_root();
    let manifest = parse_manifest(&root.join(MANIFEST)).unwrap();
    validate_archive_objects(&root, &manifest).unwrap();
    validate_historical_absent(&root, &manifest).unwrap();

    let expected_snapshots = ROUTE_ONLY_DOCS
        .into_iter()
        .chain(MASTER_TRIPLET)
        .chain(GOVERNANCE_TRIPLET)
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        manifest.snapshots.keys().cloned().collect::<BTreeSet<_>>(),
        expected_snapshots,
        "snapshot rows must be exactly the active paths replaced by the current layer"
    );

    for (path, blob) in &manifest.snapshots {
        let current_path = root.join(path);
        assert!(
            current_path.is_file(),
            "snapshot route/current file missing: {path}"
        );
        let current_blob = git(&root, &["hash-object", path]).unwrap();
        assert_ne!(
            current_blob, *blob,
            "snapshot path was not replaced by the current layer: {path}"
        );
    }

    let current_corpus = [
        vec!["AGENTS.md", "data-collect/README.md"],
        MASTER_TRIPLET.to_vec(),
        CURRENT_DOCS.to_vec(),
        ROUTE_ONLY_DOCS.to_vec(),
        vec![CURRENT],
    ]
    .concat();
    for path in current_corpus {
        let doc = read(&root.join(path)).unwrap();
        for historical_path in manifest.historical.keys() {
            assert!(
                !doc.contains(historical_path),
                "{path} directly references archived history {historical_path}"
            );
        }
        for forbidden in [
            "systemctl --user enable --now data-collect-training-collector.service",
            "data-collect-retention-admin",
            "train-data-collect-ver1-0",
            "APPROVED_EXECUTING",
            "ROLLOVER_PENDING",
            "rm -rf",
        ] {
            assert!(
                !doc.contains(forbidden),
                "{path} retains forbidden current token {forbidden}"
            );
        }
    }
}

#[test]
fn evidence_only_inputs_remain_non_authoritative() {
    let root = repo_root();
    let expected = tag_marker_paths(&root, EVIDENCE_MARKER).unwrap();
    assert_eq!(expected.len(), 4);
    validate_evidence_tree(&root, &expected).unwrap();
}

#[test]
fn authority_guard_rejects_negative_fixtures_without_touching_the_worktree() {
    let root = repo_root();

    let entry_fixture = tempdir().unwrap();
    write_entry_fixture(entry_fixture.path(), "demand/data-collect-demand.md");
    validate_entry_graph(entry_fixture.path()).unwrap();

    let duplicate = entry_fixture
        .path()
        .join("data-collect/docs/duplicate-current.md");
    fs::write(
        &duplicate,
        format!("# duplicate\n\n{CURRENT_ENTRY_MARKER}\n"),
    )
    .unwrap();
    assert!(validate_entry_graph(entry_fixture.path()).is_err());
    fs::remove_file(duplicate).unwrap();

    write_entry_fixture(entry_fixture.path(), "demand/missing.md");
    assert!(validate_entry_graph(entry_fixture.path()).is_err());

    let manifest = parse_manifest(&root.join(MANIFEST)).unwrap();
    let bad_manifest_path = entry_fixture.path().join("bad-manifest.tsv");
    let mut bad_manifest = read(&root.join(MANIFEST)).unwrap();
    let first_blob = manifest.historical.values().next().unwrap();
    bad_manifest = bad_manifest.replacen(first_blob, &"0".repeat(40), 1);
    fs::write(&bad_manifest_path, bad_manifest).unwrap();
    let bad_manifest = parse_manifest(&bad_manifest_path).unwrap();
    assert!(validate_archive_objects(&root, &bad_manifest).is_err());

    let restored_fixture = tempdir().unwrap();
    let (historical_path, _) = manifest.historical.iter().next().unwrap();
    let restored_path = restored_fixture.path().join(historical_path);
    fs::create_dir_all(restored_path.parent().unwrap()).unwrap();
    fs::write(restored_path, "restored history").unwrap();
    assert!(validate_historical_absent(restored_fixture.path(), &manifest).is_err());

    let expected = tag_marker_paths(&root, EVIDENCE_MARKER).unwrap();
    let evidence_fixture = tempdir().unwrap();
    for path in &expected {
        let target = evidence_fixture.path().join(path);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::copy(root.join(path), target).unwrap();
    }
    let first_evidence = evidence_fixture
        .path()
        .join(expected.iter().next().unwrap());
    let damaged = read(&first_evidence)
        .unwrap()
        .replace(EVIDENCE_MARKER, "> 生命周期：`DAMAGED`");
    fs::write(first_evidence, damaged).unwrap();
    assert!(validate_evidence_tree(evidence_fixture.path(), &expected).is_err());
}

fn validate_entry_graph(root: &Path) -> Result<(), String> {
    let current_entries = marker_paths(root, CURRENT_ENTRY_MARKER)?;
    let expected = BTreeSet::from([CURRENT.to_owned()]);
    if current_entries != expected {
        return Err(format!(
            "expected exactly one CURRENT entry at {CURRENT}, found {current_entries:?}"
        ));
    }

    let current = read(&root.join(CURRENT))?;
    for (label, path) in [
        ("当前需求", MASTER_TRIPLET[0]),
        ("当前计划", MASTER_TRIPLET[1]),
        ("当前 changelog", MASTER_TRIPLET[2]),
        ("历史读取入口", MANIFEST),
    ] {
        let field = format!("> {label}：`{path}`");
        if current.matches(&field).count() != 1 {
            return Err(format!("{CURRENT} must contain exactly one {field}"));
        }
        if !root.join(path).is_file() {
            return Err(format!("{CURRENT} references missing path {path}"));
        }
    }
    Ok(())
}

fn validate_archive_objects(root: &Path, manifest: &ArchiveManifest) -> Result<(), String> {
    for (key, expected) in [
        ("version", "1"),
        ("tag", TAG),
        ("tag_object", TAG_OBJECT),
        ("peeled_commit", PEELED_COMMIT),
        ("object_format", "sha1"),
        ("historical_count", "45"),
        ("snapshot_count", "11"),
        ("entry_count", "56"),
    ] {
        if manifest.meta.get(key).map(String::as_str) != Some(expected) {
            return Err(format!("manifest meta {key} is not {expected}"));
        }
    }
    if manifest.historical.len() != 45
        || manifest.snapshots.len() != 11
        || manifest.historical.len() + manifest.snapshots.len() != 56
    {
        return Err("manifest entry counts do not match metadata".to_owned());
    }

    if git(root, &["cat-file", "-t", TAG])? != "tag" {
        return Err(format!("{TAG} is not an annotated tag"));
    }
    if git(root, &["rev-parse", TAG])? != TAG_OBJECT {
        return Err("annotated tag object mismatch".to_owned());
    }
    if git(root, &["rev-parse", &format!("{TAG}^{{}}")])? != PEELED_COMMIT {
        return Err("peeled archive commit mismatch".to_owned());
    }
    if git(root, &["rev-parse", "--show-object-format"])? != "sha1" {
        return Err("repository object format is not sha1".to_owned());
    }

    let tagged_historical = tag_marker_paths(root, HISTORICAL_MARKER)?;
    let manifest_historical = manifest.historical.keys().cloned().collect::<BTreeSet<_>>();
    if tagged_historical != manifest_historical {
        return Err("manifest historical set differs from tagged banners".to_owned());
    }

    for rows in [&manifest.historical, &manifest.snapshots] {
        for (path, expected_blob) in rows {
            let actual_blob = git(root, &["rev-parse", &format!("{TAG}:{path}")])?;
            if &actual_blob != expected_blob {
                return Err(format!(
                    "archive blob mismatch for {path}: {actual_blob} != {expected_blob}"
                ));
            }
            if git(root, &["cat-file", "-t", expected_blob])? != "blob" {
                return Err(format!("archive object is not a blob: {path}"));
            }
        }
    }
    Ok(())
}

fn validate_historical_absent(root: &Path, manifest: &ArchiveManifest) -> Result<(), String> {
    for path in manifest.historical.keys() {
        if root.join(path).exists() {
            return Err(format!("archived historical path is active: {path}"));
        }
    }
    Ok(())
}

fn validate_evidence_tree(root: &Path, expected: &BTreeSet<String>) -> Result<(), String> {
    let actual = marker_paths(root, EVIDENCE_MARKER)?;
    if &actual != expected {
        return Err(format!(
            "evidence-only path set mismatch: expected {expected:?}, found {actual:?}"
        ));
    }
    for path in expected {
        let doc = read(&root.join(path))?;
        for field in [
            "> 最终快照：`EVIDENCE_INPUT_FROZEN_AT_20260730`",
            "> 当前执行 authority：`none`",
            "> execution_superseded_by：`data-collect/docs/CURRENT.md`",
        ] {
            if !doc.contains(field) {
                return Err(format!("{path} missing evidence-only field {field}"));
            }
        }
    }
    Ok(())
}

fn parse_manifest(path: &Path) -> Result<ArchiveManifest, String> {
    let content = read(path)?;
    let mut lines = content.lines();
    if lines.next() != Some("kind\tkey_or_path\tvalue_or_blob") {
        return Err("archive manifest header is invalid".to_owned());
    }
    let mut manifest = ArchiveManifest {
        meta: BTreeMap::new(),
        historical: BTreeMap::new(),
        snapshots: BTreeMap::new(),
    };
    for (index, line) in lines.enumerate() {
        let columns = line.split('\t').collect::<Vec<_>>();
        if columns.len() != 3 || columns.iter().any(|value| value.is_empty()) {
            return Err(format!("invalid manifest row {}", index + 2));
        }
        let target = match columns[0] {
            "meta" => &mut manifest.meta,
            "historical" => {
                validate_oid(columns[2])?;
                &mut manifest.historical
            }
            "snapshot" => {
                validate_oid(columns[2])?;
                &mut manifest.snapshots
            }
            other => return Err(format!("unknown manifest row kind {other}")),
        };
        if target
            .insert(columns[1].to_owned(), columns[2].to_owned())
            .is_some()
        {
            return Err(format!("duplicate manifest key/path {}", columns[1]));
        }
    }
    Ok(manifest)
}

fn validate_oid(value: &str) -> Result<(), String> {
    if value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(format!("invalid sha1 object id {value}"))
    }
}

fn tag_marker_paths(root: &Path, marker: &str) -> Result<BTreeSet<String>, String> {
    let output = git(root, &["grep", "-l", "-F", marker, TAG, "--", "*.md"])?;
    let prefix = format!("{TAG}:");
    output
        .lines()
        .map(|line| {
            line.strip_prefix(&prefix)
                .map(str::to_owned)
                .ok_or_else(|| format!("unexpected git grep result {line}"))
        })
        .collect()
}

fn marker_paths(root: &Path, marker: &str) -> Result<BTreeSet<String>, String> {
    let mut markdown = Vec::new();
    collect_markdown(root, root, &mut markdown)?;
    let mut result = BTreeSet::new();
    for (relative, path) in markdown {
        if read(&path)?.contains(marker) {
            result.insert(relative);
        }
    }
    Ok(result)
}

fn paths_containing_any(root: &Path, needles: &[&str]) -> Result<BTreeSet<String>, String> {
    let mut markdown = Vec::new();
    collect_markdown(root, root, &mut markdown)?;
    let mut result = BTreeSet::new();
    for (relative, path) in markdown {
        let content = read(&path)?;
        if needles.iter().any(|needle| content.contains(needle)) {
            result.insert(relative);
        }
    }
    Ok(result)
}

fn collect_markdown(
    root: &Path,
    directory: &Path,
    output: &mut Vec<(String, PathBuf)>,
) -> Result<(), String> {
    for entry in fs::read_dir(directory).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| error.to_string())?;
        if file_type.is_dir() {
            let name = entry.file_name();
            if name == ".git" || name == "target" || name == "third_party" {
                continue;
            }
            collect_markdown(root, &path, output)?;
        } else if path.extension().is_some_and(|extension| extension == "md") {
            let relative = path
                .strip_prefix(root)
                .map_err(|error| error.to_string())?
                .to_string_lossy()
                .replace('\\', "/");
            output.push((relative, path));
        }
    }
    Ok(())
}

fn write_entry_fixture(root: &Path, demand_path: &str) {
    for path in [
        "data-collect/docs",
        "demand",
        "plan",
        "changelog",
        "data-collect/docs/current",
    ] {
        fs::create_dir_all(root.join(path)).unwrap();
    }
    fs::write(root.join("demand/data-collect-demand.md"), "demand").unwrap();
    fs::write(root.join("plan/data-collect-plan.md"), "plan").unwrap();
    fs::write(
        root.join("changelog/data-collect-changelog.md"),
        "changelog",
    )
    .unwrap();
    fs::write(root.join(MANIFEST), "kind\tkey_or_path\tvalue_or_blob\n").unwrap();
    fs::write(
        root.join(CURRENT),
        format!(
            "# current\n\n{CURRENT_ENTRY_MARKER}\n\n> 当前需求：`{demand_path}`\n\n> 当前计划：`{}`\n\n> 当前 changelog：`{}`\n\n> 历史读取入口：`{MANIFEST}`\n",
            MASTER_TRIPLET[1], MASTER_TRIPLET[2]
        ),
    )
    .unwrap();
}

fn git(root: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .current_dir(root)
        .args(args)
        .output()
        .map_err(|error| format!("failed to run git {args:?}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn read(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(|error| format!("failed to read {}: {error}", path.display()))
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("data-collect package must have a workspace parent")
        .to_path_buf()
}
