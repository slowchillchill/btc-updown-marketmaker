import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { at, fact, statusLabel } from "./facts";
import {
  get,
  Raw,
  Source,
  Tree,
  type Page,
  type Run,
  type Snapshot,
} from "./evidence";
import {
  list,
  publicRows,
  relatedTargets,
  reportPoints,
  rangePoints,
  traceJson,
  text,
  type Evidence,
  type Point,
} from "./browse";
import { Topology } from "./Topology";
import { Economics, Field, MarketSummary } from "./Economics";
import { EventTable } from "./EventTable";
import { Timeline } from "./Timeline";
import { ExecutionProjection } from "./ExecutionProjection";
import { NoePanel, useNoe } from "./Noe";
import { noePoints } from "./noePoints";
import { ResearchPanel, useResearch } from "./Research";
import { researchPoints } from "./researchPoints";
import {
  ExistingPanel,
  useExisting,
  existingPoints,
  existingLabel,
  existingSourceIdentity,
} from "./Existing";
import { SourceIdentity } from "./SourceIdentity";
import { usePublicBrowse } from "./PublicBrowse";
const views = [
  ["overview", "模块总览"],
  ["timeline", "多时钟时间线"],
  ["decisions", "输入与决定"],
  ["execution", "意图与执行反馈"],
  ["economics", "账户经济与生命周期"],
  ["training", "训练与实验"],
  ["evidence", "证据重放"],
] as const;
type View = (typeof views)[number][0];
export default function App() {
  const [runs, setRuns] = useState<Run[]>([]),
    [run, setRun] = useState(""),
    [snapshot, setSnapshot] = useState<Snapshot | null>(null),
    [view, setView] = useState<View>("overview");
  const [loading, setLoading] = useState(false),
    [error, setError] = useState(""),
    [page, setPage] = useState<Page | null>(null);
  const [selection, setSelection] = useState<Evidence | null>(null),
    [condition, setCondition] = useState(""),
    [start, setStart] = useState(""),
    [end, setEnd] = useState(""),
    [exportScope, setExportScope] = useState("range");
  const currentRun = useRef(run);
  currentRun.current = run;
  const downloadUrl = useRef<string | null>(null);
  useEffect(
    () => () => {
      if (downloadUrl.current) URL.revokeObjectURL(downloadUrl.current);
    },
    [],
  );
  const select = useCallback((e: Evidence) => setSelection(e), []);
  useEffect(() => {
    get<{ runs: Run[] }>("/api/runs")
      .then((d) => {
        setRuns(d.runs);
        setRun(d.runs[0]?.key ?? "");
      })
      .catch((e) => setError(String(e)));
  }, []);
  useEffect(() => {
    if (!run) return;
    const abort = new AbortController();
    setLoading(true);
    setError("");
    setSnapshot(null);
    setPage(null);
    setSelection(null);
    setCondition("");
    setStart("");
    setEnd("");

    fetch(`/api/runs/${run}/snapshot`, { signal: abort.signal })
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json() as Promise<Snapshot>;
      })
      .then((d) => {
        if (!abort.signal.aborted) setSnapshot(d);
      })
      .catch((e) => {
        if (!abort.signal.aborted) setError(String(e));
      })
      .finally(() => {
        if (!abort.signal.aborted) setLoading(false);
      });
    return () => abort.abort();
  }, [run]);
  const report =
    snapshot?.report.status === "ready"
      ? (snapshot.report.exact ?? undefined)
      : undefined;
  const metadata =
    snapshot?.metadata.status === "ready"
      ? (snapshot.metadata.exact ?? undefined)
      : undefined;
  const engineeringProjection =
    snapshot?.report_kind === "engineering_ledger_projection";
  const reportMarkets = list(at(report, "account_economics_memory", "markets"));
  const account = at(report, "ledger_consumed_memory", "account_instance_key");
  const allRows = useMemo(() => publicRows(page), [page]);
  const publicBrowse = usePublicBrowse({
    run,
    page,
    allRows,
    onPage: setPage,
    condition,
    ready: snapshot?.public_recording.status === "ready",
    onSelect: select,
  });
  const research = useResearch(run);
  const researchReady = snapshot?.research?.status === "ready";
  const loadedResearchPoints = useMemo(
    () => researchPoints(snapshot?.research, research.loaded),
    [snapshot?.research, research.loaded],
  );
  const selectResearch = (e: Evidence) =>
    select({
      ...e,
      points: loadedResearchPoints.filter(
        (p) => p.path === e.path || p.path.startsWith(e.path + "/"),
      ),
    });
  const researchPanel =
    snapshot?.research && snapshot.research.status !== "not_configured" ? (
      <ResearchPanel
        key={run}
        status={snapshot.research}
        state={research}
        onSelect={selectResearch}
      />
    ) : null;
  const existing = useExisting(run);
  const existingReady =
    !!snapshot?.existing_source &&
    snapshot.existing_source.status !== "not_configured";
  const existingStatus = snapshot?.existing_source;
  const completionDocument = existingStatus?.documents?.find(
    (d) => d.document === "completion" && d.status === "ready",
  )?.summary;
  const loadedExistingPoints = useMemo(
    () => existingPoints(existingStatus, existing.loaded),
    [existingStatus, existing.loaded],
  );
  const selectExisting = (e: Evidence) =>
    select({
      ...e,
      points: loadedExistingPoints.filter(
        (p) => p.path === e.path || p.path.startsWith(e.path + "/"),
      ),
    });
  const existingPanel =
    existingStatus && existingStatus.status !== "not_configured" ? (
      <ExistingPanel
        key={run}
        status={existingStatus}
        state={existing}
        condition={condition}
        onSelect={selectExisting}
      />
    ) : null;
  const noe = useNoe(run);
  const loadedNoePoints = useMemo(() => noePoints(noe.loaded), [noe.loaded]);
  useEffect(() => {
    setSelection((previous) =>
      previous?.source === "noe" &&
      (noe.loaded?.status !== "ready" ||
        !previous.path.startsWith(`/noe/${noe.loaded.index}/`))
        ? null
        : previous,
    );
  }, [noe.loaded]);
  const selectNoe = (e: Evidence) =>
    select({
      ...e,
      points: loadedNoePoints.filter(
        (p) => p.path === e.path || p.path.startsWith(e.path + "/"),
      ),
    });
  const stream = text(at(snapshot?.public_recording.header, "input_stream_id"));
  const publicOnly =
    !report && !text(at(metadata, "config", "session", "run_id"));
  const noeOnly =
    publicOnly &&
    snapshot?.public_recording.status !== "ready" &&
    !!snapshot?.noe_results.length;
  const marketIds = [
    ...new Set([
      ...reportMarkets.flatMap((m) =>
        text(at(m, "condition_id")) ? [text(at(m, "condition_id"))!] : [],
      ),
      ...list(
        at(report, "ledger_consumed_memory", "market_projections"),
      ).flatMap((m) =>
        text(at(m, "condition_id")) ? [text(at(m, "condition_id"))!] : [],
      ),
      ...list(
        at(completionDocument, "final_ledger", "market_projections"),
      ).flatMap((m) =>
        text(at(m, "condition_id")) ? [text(at(m, "condition_id"))!] : [],
      ),
      ...allRows.flatMap((r) => r.markets?.map((m) => m.condition) ?? []),
      ...(snapshot?.research?.catalog ?? []).flatMap((m) =>
        text(at(m, "condition_id")) ? [text(at(m, "condition_id"))!] : [],
      ),
      ...loadedNoePoints.flatMap((p) => (p.condition ? [p.condition] : [])),
      ...(selection?.markets?.map((m) => m.condition) ?? []),
    ]),
  ];
  const selectedOutsidePage = condition && !marketIds.includes(condition);
  const allPoints = useMemo(() => {
    const current = [
      ...reportPoints(report),
      ...allRows.flatMap((r) => r.points),
      ...loadedNoePoints,
      ...loadedResearchPoints,
      ...loadedExistingPoints,
    ];
    const selectedPoint = selection as Partial<Point> | null;
    const retained =
      selection?.points ??
      (typeof selectedPoint?.utcMs === "string" &&
      typeof selectedPoint.clock === "string"
        ? [selection as Point]
        : []);
    return [
      ...current,
      ...retained.filter(
        (p) =>
          p.source !== "noe" &&
          !current.some((c) => c.source === p.source && c.path === p.path),
      ),
    ];
  }, [
    report,
    allRows,
    selection,
    loadedNoePoints,
    loadedResearchPoints,
    loadedExistingPoints,
  ]);
  const rangeValid =
    (!start || /^-?\d+$/.test(start)) &&
    (!end || /^-?\d+$/.test(end)) &&
    (!start || !end || BigInt(start) <= BigInt(end));
  const points = useMemo(
    () => (rangeValid ? rangePoints(allPoints, start, end, condition) : []),
    [allPoints, start, end, condition, rangeValid],
  );
  const rows = useMemo(
    () =>
      publicBrowse.rows.filter(
        (r) =>
          (!condition ||
            r.markets?.some((m) => m.condition === condition) ||
            (!r.markets?.length && r.condition === undefined)) &&
          ((!start && !end) || r.points.some((p) => points.includes(p))),
      ),
    [publicBrowse.rows, points, start, end, condition],
  );
  const exportPoints =
    exportScope === "selection"
      ? points.filter(
          (p) =>
            selection &&
            p.source === selection.source &&
            (p.path === selection.path ||
              p.path.startsWith(selection.path + "/")),
        )
      : points;
  const sourceName = (source: Evidence["source"]) =>
    source === "existing"
      ? existingLabel(existingStatus?.kind)
      : source === "report"
        ? engineeringProjection
          ? "Ledger 工程投影"
          : "历史报告"
        : source === "metadata"
          ? "启动元数据"
          : source === "research"
            ? "真实历史描述研究"
            : source === "noe"
              ? "NOE 解码记录"
              : "公共稳定记录";
  const evidencePanel = (
    <section className="evidence-panel">
      <h2>所选事实</h2>
      {selection ? (
        <>
          <p>
            {sourceName(selection.source)} · 配置来源 {run}
          </p>
          <code>{selection.path || "/"}</code>
          {condition &&
            selection.condition !== condition &&
            !selection.markets?.some((m) => m.condition === condition) && (
              <p className="gap">
                无法将此事实直接关联所选市场。保留原证据，不替换身份。
              </p>
            )}
          {selection.existing && (
            <p>
              原文档 <code>{fact(at(selection.existing, "document"))}</code> ·
              文件 <code>{fact(at(selection.existing, "source"))}</code> ·
              原引用 <code>{fact(at(selection.existing, "reference"))}</code>
            </p>
          )}
          {selection.inputStreamId && (
            <p>
              公共流 <code>{selection.inputStreamId}</code>
            </p>
          )}
          {!!selection.references?.length && (
            <div className="native-links">
              <h3>原引用关联 · 当前页 / 保留选择</h3>
              {selection.references.map((ref, i) => {
                const targets = relatedTargets(ref, allRows, selection);
                return (
                  <div key={i}>
                    <small>
                      {ref.label} · {ref.path}
                    </small>
                    <code>{JSON.stringify(ref.value)}</code>
                    {targets.length ? (
                      targets.map((target) => (
                        <button
                          key={target.path}
                          onClick={() => select(target)}
                        >
                          {target.path}
                        </button>
                      ))
                    ) : (
                      <p className="gap">
                        目标未在当前页或保留选择中读取；未跳转或替换邻近记录。
                      </p>
                    )}
                  </div>
                );
              })}
            </div>
          )}
          <pre>{JSON.stringify(selection.value, null, 2)}</pre>
        </>
      ) : (
        <p className="empty">点击卡片、字段或事件引用查原值。</p>
      )}
      <small>
        查看市场：{condition || "全部已读市场"}
        ；字段选择跨视图保留。公共证据绑定原稳定前缀。
      </small>
    </section>
  );
  function download() {
    if (!snapshot || !exportPoints.length) return;
    const payload = traceJson(exportPoints, {
      source_key: run,
      ...(stream ? { input_stream_id: stream } : {}),
      ...(report || metadata
        ? {
            run_id:
              at(account, "run_id") ??
              at(metadata, "config", "session", "run_id") ??
              null,
            account_identity: account ?? null,
          }
        : {}),
      report_source: engineeringProjection ? null : snapshot.report.source,
      engineering_projection_source: engineeringProjection
        ? snapshot.report.source
        : null,
      report_kind: report ? snapshot.report_kind : null,
      research_source: snapshot.research,
      existing_source_identity: existingSourceIdentity(existingStatus),
      metadata_source: snapshot.metadata.source,
      public_source: snapshot.public_recording,
      public_page_range: page
        ? {
            row_start: page.row_start,
            returned_rows: page.returned_rows,
            page: page.page,
          }
        : null,
      selection_range: {
        start_utc_ms: start || null,
        end_utc_ms: end || null,
        condition_id: condition || null,
        scope: exportScope,
      },
      economics_version: at(report, "economics_version") ?? null,
    });
    if (downloadUrl.current) URL.revokeObjectURL(downloadUrl.current);
    const url = URL.createObjectURL(
      new Blob([payload], { type: "application/json" }),
    );
    downloadUrl.current = url;
    const link = document.createElement("a");
    link.href = url;
    link.download = `facts-run-${run}.trace.json`;
    document.body.append(link);
    link.click();
    link.remove();
  }
  const controls = publicBrowse.controls;
  const rangeControls = (
    <div className="range-controls">
      <label>
        区间起点 UTC ms
        <input
          aria-label="区间起点 UTC ms"
          value={start}
          onChange={(e) => setStart(e.target.value)}
          placeholder="不限"
        />
      </label>
      <label>
        区间终点 UTC ms
        <input
          aria-label="区间终点 UTC ms"
          value={end}
          onChange={(e) => setEnd(e.target.value)}
          placeholder="不限"
        />
      </label>
      <button
        onClick={() => {
          setStart("");
          setEnd("");
        }}
      >
        清除时间范围
      </button>
      <select
        aria-label="导出范围"
        value={exportScope}
        onChange={(e) => setExportScope(e.target.value)}
      >
        <option value="range">当前范围可证时间点</option>
        <option value="selection">所选事实可证时间点</option>
      </select>
      <button disabled={!rangeValid || !exportPoints.length} onClick={download}>
        导出 Perfetto
      </button>
      <span>{points.length} 个可证时间点</span>
      {!rangeValid && (
        <span role="alert">请输入整数 UTC ms，起点不得晚于终点。</span>
      )}
    </div>
  );
  function timing() {
    return (
      <section>
        <h2>有限计算时序 · 历史宿主调度</h2>
        <p className="muted">
          起止来自历史宿主显式调度，区间差值为派生调度时间，不是本机 CPU
          或网络实测耗时。配置来源：启动元数据 config；实际处理 /
          网络测量未接入。
        </p>
        {[
          "last_dispatched_calculation_timing",
          "pending_calculation_timing",
        ].map((group) => {
          const t = at(report, group);
          return (
            <div key={group}>
              <h3>{group}</h3>
              {t == null ? (
                <p>{fact(t)}</p>
              ) : (
                <>
                  <Tree
                    name={group}
                    path={"/" + group}
                    value={t}
                    onSelect={(path, value) =>
                      select({ source: "report", path, value })
                    }
                  />
                  <div className="timing-deltas">
                    {[
                      [
                        "计算区间",
                        "calculation_started_at_utc_ms",
                        "calculation_completed_at_utc_ms",
                      ],
                      [
                        "完成至实际发送",
                        "calculation_completed_at_utc_ms",
                        "actual_sent_at_utc_ms",
                      ],
                      [
                        "实际减计划发送",
                        "scheduled_send_at_utc_ms",
                        "actual_sent_at_utc_ms",
                      ],
                    ].map(([label, a, b]) => {
                      const av = text(at(t, a)),
                        bv = text(at(t, b));
                      return (
                        <p key={label}>
                          {label} · 派生：
                          {av && bv && /^-?\d+$/.test(av) && /^-?\d+$/.test(bv)
                            ? `${(BigInt(bv) - BigInt(av)).toString()} ms = ${bv} − ${av}`
                            : "无完整端点，仅显示已知点"}
                          <small>
                            {" "}
                            来源 /{group}/{b} − /{group}/{a}
                          </small>
                        </p>
                      );
                    })}
                  </div>
                </>
              )}
            </div>
          );
        })}
      </section>
    );
  }
  return (
    <div className="shell">
      <aside>
        <div className="brand">
          ◈{" "}
          <span>
            公共事实
            <br />
            <b>浏览器</b>
          </span>
        </div>
        <div className="nav-label">只读 · 历史快照</div>
        {views.map(([id, label]) => (
          <button
            key={id}
            className={view === id ? "active" : ""}
            onClick={() => setView(id)}
          >
            {label}
          </button>
        ))}
        <div className="aside-foot">
          显式来源 / 精确原值
          <br />
          静态架构 + 已保存事实
          <br />
          <span>没有实时业务心跳</span>
        </div>
      </aside>
      <main>
        <header>
          <div>
            <p className="eyebrow">公共业务事实浏览器</p>
            <h1>
              {view === "overview"
                ? existingReady
                  ? existingLabel(existingStatus?.kind)
                  : researchReady
                    ? "描述研究总览"
                    : publicOnly
                      ? noeOnly
                        ? "NOE 来源总览"
                        : "公共流总览"
                      : engineeringProjection
                        ? "Ledger 工程投影总览"
                        : "运行总览"
                : views.find(([id]) => id === view)?.[1]}{" "}
              <span>原事实 · 可回查</span>
            </h1>
          </div>
          <label>
            显式来源
            <select
              aria-label="运行选择"
              value={run}
              onChange={(e) => setRun(e.target.value)}
            >
              {runs.map((r) => (
                <option key={r.key} value={r.key}>
                  {r.label}
                </option>
              ))}
            </select>
          </label>
        </header>
        {loading && <p role="status">读取配置的来源快照…</p>}
        {error && <p role="alert">{error}</p>}
        {snapshot && (
          <>
            <div className="run-strip">
              <b>
                {existingReady
                  ? existingLabel(existingStatus?.kind)
                  : report
                    ? engineeringProjection
                      ? "Ledger 工程投影"
                      : "backtest"
                    : researchReady
                      ? "真实历史描述研究"
                      : noeOnly
                        ? "显式 NOE 引用"
                        : "公共历史材料"}
              </b>
              <span>{snapshot.label}</span>
              {existingReady ? (
                <span>原工程 / 历史摘要 · 非回测宿主 / 正式训练</span>
              ) : researchReady ? (
                <span>原研究范围 · 未运行账户 / 策略决定</span>
              ) : engineeringProjection ? (
                <span>原 Ledger 投影 · 非宿主报告 / 策略研究</span>
              ) : publicOnly ? (
                <span>
                  {noeOnly
                    ? "NOE 单引用 · 报告/公共流未接入"
                    : `公共流 · ${stream ?? "未交接 input_stream_id"}`}
                </span>
              ) : (
                <>
                  <span>历史入口 · {fact(at(report, "end_reason"))}</span>
                  <span>{fact(at(report, "economics_version"))}</span>
                  <span>计算累计 {fact(at(report, "calculation_count"))}</span>
                  <span>未闭合 {fact(at(report, "unclosed_commitments"))}</span>
                  <span>待 NOE {fact(at(report, "pending_noe_records"))}</span>
                </>
              )}
              <span>
                {researchReady
                  ? "研究目标数"
                  : noeOnly
                    ? "显式引用数"
                    : "稳定公共组边界"}{" "}
                {fact(
                  researchReady
                    ? snapshot.research.targets?.length.toString()
                    : noeOnly
                      ? snapshot.noe_results.length.toString()
                      : at(
                          snapshot.public_recording.prefix,
                          "next_group_position",
                        ),
                )}
              </span>
            </div>
            <div className="identity">
              {existingReady ? (
                <>
                  <span>
                    来源类别 <code>{existingStatus?.kind}</code>
                  </span>
                  <span>已交接摘要 / 明确原文件实例</span>
                </>
              ) : researchReady ? (
                <>
                  <span>
                    研究原代码{" "}
                    <code>
                      {fact(
                        snapshot.research.summary?.find(
                          (f) => f.path === "/build_identity/source_commit",
                        )?.exact,
                      )}
                    </code>
                  </span>
                  <span>
                    研究账户运行{" "}
                    <code>
                      {fact(
                        at(
                          snapshot.research.summary?.find(
                            (f) => f.path === "/execution",
                          )?.exact,
                          "accounts_run",
                        ),
                      )}
                      （原 /execution/accounts_run）
                    </code>
                  </span>
                </>
              ) : publicOnly ? (
                <>
                  <span>
                    {noeOnly ? "NOE 来源" : "公共流"}{" "}
                    <code>
                      {noeOnly
                        ? `${snapshot.noe_results.length} 个显式引用`
                        : (stream ?? "未交接")}
                    </code>
                  </span>
                  <span>
                    {noeOnly ? "单引用读取" : "已读范围"}{" "}
                    <code>
                      {noeOnly
                        ? noe.loaded
                          ? `配置索引 ${noe.loaded.index} · ${noe.loaded.status}`
                          : "尚未加载"
                        : page
                          ? `[${page.row_start}, ${page.total_returned_rows})`
                          : "尚未读页"}
                    </code>
                  </span>
                  <span>
                    报告 / 元数据 {statusLabel[snapshot.report.status]} /{" "}
                    {statusLabel[snapshot.metadata.status]}
                  </span>
                </>
              ) : (
                <>
                  <span>
                    运行{" "}
                    <code>
                      {fact(
                        at(account, "run_id") ??
                          at(metadata, "config", "session", "run_id"),
                      )}
                    </code>
                  </span>
                  <span>
                    账户{" "}
                    <code>
                      {fact(
                        at(account, "account_id") ??
                          at(metadata, "config", "session", "account_id"),
                      )}
                    </code>
                  </span>
                  <span>
                    策略{" "}
                    <code>
                      {fact(at(metadata, "config", "strategy", "strategy_id"))}
                    </code>
                  </span>
                  <span>
                    观察截止 UTC ms{" "}
                    <code>{fact(at(report, "last_observed_at_utc_ms"))}</code>
                  </span>
                </>
              )}
              <label>
                查看市场
                <select
                  aria-label="市场选择"
                  value={condition}
                  onChange={(e) => setCondition(e.target.value)}
                >
                  <option value="">全部已读市场</option>
                  {marketIds.map((id) => (
                    <option key={id} value={id}>
                      {id}
                    </option>
                  ))}
                  {selectedOutsidePage && (
                    <option value={condition}>
                      {condition} · 当前页无来源
                    </option>
                  )}
                </select>
              </label>
            </div>
            <SourceIdentity
              report={report}
              metadata={metadata}
              header={snapshot.public_recording.header ?? undefined}
              reportVersion={at(
                snapshot.report.exact ?? undefined,
                "economics_version",
              )}
              metadataVersion={at(
                snapshot.metadata.exact ?? undefined,
                "economics_version",
              )}
              onSelect={select}
            />
            <div className="notice">
              {existingReady
                ? "独立工程 / 历史来源；原字段与口径保留"
                : researchReady
                  ? "真实首小时描述研究 · 事后标签不回填冻结输入"
                  : engineeringProjection
                    ? "原 Ledger 工程投影；非宿主报告 / 策略研究"
                    : noeOnly
                      ? "显式 NOE 单引用；报告和公共流未接入"
                      : publicOnly
                        ? "固定公共前缀与有限已读范围"
                        : "终态快照与有限时序"}{" "}
              ·
              公共输入按固定前缀和已读范围浏览；报告不代表完整逐次诊断、执行或经济历史。查看市场是浏览选择，不代表策略当前活跃市场。
            </div>
            {engineeringProjection && (
              <Source title="Ledger 工程投影来源" doc={snapshot.report} />
            )}
            {!engineeringProjection && snapshot.report.status !== "ready" && (
              <Source
                title={
                  researchReady || existingReady
                    ? "账户回测报告状态（独立可选来源）"
                    : "报告状态"
                }
                doc={snapshot.report}
              />
            )}
            {view === "overview" && researchPanel}
            {[
              "overview",
              "execution",
              "economics",
              "training",
              "evidence",
            ].includes(view) && existingPanel}
            {view === "overview" && (
              <>
                <div className="overview-grid">
                  <section>
                    <Topology
                      report={report}
                      engineering={engineeringProjection}
                      existing={
                        existingStatus?.status === "ready"
                          ? {
                              kind: existingStatus.kind!,
                              evidence: {
                                source: "existing",
                                path:
                                  existingStatus.kind === "completion"
                                    ? "/final_ledger"
                                    : existingStatus.kind ===
                                        "collection_manifest"
                                      ? "/existing/manifest/collection_day_coverage"
                                      : existingStatus.kind === "model_timing"
                                        ? "/existing/time_results/production_revision"
                                        : existingStatus.kind ===
                                            "remeasurement_12m"
                                          ? "/existing/measurement/measurement_source_commit"
                                          : "/existing/provenance/baseline",
                                value:
                                  existingStatus.kind === "completion"
                                    ? at(completionDocument, "final_ledger")!
                                    : existingStatus.kind ===
                                        "collection_manifest"
                                      ? (at(
                                          existingStatus.documents?.find(
                                            (d) => d.document === "manifest",
                                          )?.summary,
                                          "collection_day_coverage",
                                        ) ?? null)
                                      : existingStatus.kind ===
                                            "model_timing" ||
                                          existingStatus.kind ===
                                            "remeasurement_12m"
                                        ? (at(
                                            existingStatus.documents?.find(
                                              (d) =>
                                                d.document ===
                                                (existingStatus.kind ===
                                                "model_timing"
                                                  ? "time_results"
                                                  : "measurement"),
                                            )?.summary,
                                            existingStatus.kind ===
                                              "model_timing"
                                              ? "production_revision"
                                              : "measurement_source_commit",
                                          ) ?? null)
                                        : (at(
                                            existingStatus.documents?.find(
                                              (d) =>
                                                d.document === "provenance",
                                            )?.summary,
                                            "baseline",
                                          ) ?? null),
                              },
                            }
                          : undefined
                      }
                      research={
                        researchReady
                          ? {
                              source: "research",
                              path: "/execution",
                              value:
                                snapshot.research.summary?.find(
                                  (f) => f.path === "/execution",
                                )?.exact ?? null,
                            }
                          : undefined
                      }
                      metadata={metadata}
                      selection={selection}
                      playback={
                        publicBrowse.playing ? publicBrowse.playFact : null
                      }
                      publicRows={allRows}
                      noeCount={snapshot.noe_results.length}
                      publicReady={snapshot.public_recording.status === "ready"}
                      noeSelection={
                        noe.loaded?.status === "ready"
                          ? {
                              source: "noe",
                              path: `/noe/${noe.loaded.index}/exact/record`,
                              value: at(noe.loaded.exact, "record")!,
                              points: loadedNoePoints.filter((p) =>
                                p.path.startsWith(
                                  `/noe/${noe.loaded!.index}/exact/record/`,
                                ),
                              ),
                            }
                          : undefined
                      }
                      onSelect={select}
                    />
                  </section>
                  <div className="overview-side">
                    <MarketSummary
                      report={report}
                      condition={condition}
                      onSelect={select}
                    />
                    <section>
                      <h2>
                        {researchReady
                          ? "账户事实 · 未产生"
                          : "运行事实 · 终态"}
                      </h2>
                      {[
                        ["保守 NAV", "conservative_nav_pusd"],
                        ["Maker 最坏情形 NAV", "worst_case_maker_nav_pusd"],
                        ["未闭合承诺", "unclosed_commitments"],
                        ["待账本应用 NOE", "pending_noe_records"],
                      ].map(([label, key]) => (
                        <Field
                          key={key}
                          label={label}
                          path={"/" + key}
                          report={report}
                          onSelect={select}
                        />
                      ))}
                      <p className="gap">
                        当前挂单本金 / 策略等待次数：未接入。
                      </p>
                    </section>
                    {evidencePanel}
                  </div>
                </div>
                <section>
                  <h2>公共记录 · 当前已读页</h2>
                  {controls}
                  <EventTable
                    rows={publicBrowse.rows}
                    onSelect={select}
                    selectedPath={selection?.path}
                    playbackPath={publicBrowse.playFact?.path}
                  />
                </section>
              </>
            )}
            {view === "timeline" && (
              <>
                {rangeControls}
                <section>
                  <h2>多时钟 · 有限可证事实</h2>
                  <Timeline points={points} onSelect={select} />
                  {condition && (
                    <p className="gap">
                      仅包含有明确 condition 关联的时间点；账户 /
                      容器级时间点不能凭时间猜测市场。
                    </p>
                  )}
                  <EventTable
                    rows={points.map((p) => ({
                      ...p,
                      kind: p.name,
                      identity: p.clock,
                      points: [p],
                    }))}
                    onSelect={select}
                  />
                </section>
                <section>
                  <h2>公共交付范围 · 原宿主汇总</h2>
                  <Field
                    label="经营前 / 经营范围内已交付公共组"
                    path="/delivery_summary"
                    report={report}
                    onSelect={select}
                  />
                  <p className="muted">
                    缺汇总不从稳定组边界反推已交付数量；未从累计值生成子区间历史。
                  </p>
                </section>
                {timing()}
                {evidencePanel}
              </>
            )}
            {view === "decisions" && (
              <>
                <section>
                  <h2>输入与决定 · 来源覆盖</h2>
                  <p className="gap">
                    完整 StrategyInputV1 / StrategyOutputV1 /
                    人工策略诊断未接入。公共持久记录是上游证据，不等于策略当时冻结输入；空列表不能证明等待或
                    NoTrade。
                  </p>
                  <Field
                    label="报告保存的观察截止与原组身份"
                    path="/observation_cutoff"
                    report={report}
                    onSelect={select}
                  />
                  <Field
                    label="真实计算累计 · 不能拆成子区间次数"
                    path="/calculation_count"
                    report={report}
                    onSelect={select}
                  />
                </section>
                {timing()}
                {evidencePanel}
              </>
            )}
            {view === "execution" && (
              <>
                <section>
                  <ExecutionProjection
                    engineering={engineeringProjection}
                    report={report}
                    condition={condition}
                    onSelect={select}
                  />
                </section>
                <NoePanel
                  index={snapshot.noe_results}
                  state={noe}
                  onSelect={selectNoe}
                />
                {evidencePanel}
              </>
            )}
            {view === "economics" && (
              <>
                <section>
                  <Economics
                    report={report}
                    condition={condition}
                    onSelect={select}
                  />
                </section>
                {evidencePanel}
              </>
            )}
            {view === "training" && researchPanel}
            {view === "training" && (
              <>
                <section>
                  <h2>业务模型训练 · 来源未接入</h2>
                  <p>
                    人工策略与模型复用观察、执行和账户读取位置；本次策略身份：
                    {fact(at(metadata, "config", "strategy", "strategy_id"))}。
                  </p>
                  <p className="gap">
                    模型决定、训练账户根 / 转移 / Reward / 批次 / 检查点及真实
                    MLflow
                    实验来源未接入。没有实验链接可展示；本运行不能解释为模型训练。
                  </p>
                  <p className="muted">
                    model-train / offline-evaluation / PAPER / LIVE
                    的完整宿主事实仍由所属模块交付。
                  </p>
                </section>
                {evidencePanel}
              </>
            )}
            {view === "evidence" && researchPanel}
            {view === "evidence" && (
              <>
                {rangeControls}
                <section>
                  <h2>证据范围 · 原稳定前缀</h2>
                  <p className="muted">
                    原顺序游标只保留最近一页。刷新、切回运行或其他窗口推进后取回服务端当前页，不从头扫描。只在当前页按明确字段筛选；本页外历史未定位，不冒充全历史查询。无时间记录不会进入时间范围导出。
                  </p>
                  {controls}
                  {page && (
                    <p>
                      当前可浏览行 [{page.row_start},{" "}
                      {(
                        BigInt(page.row_start) + BigInt(page.returned_rows)
                      ).toString()}
                      ) · 稳定前缀{" "}
                      {fact(
                        at(
                          snapshot.public_recording.prefix,
                          "stable_storage_ref",
                        ),
                      )}
                    </p>
                  )}
                  <EventTable
                    rows={rows}
                    onSelect={select}
                    selectedPath={selection?.path}
                    playbackPath={publicBrowse.playFact?.path}
                  />
                  {page && (
                    <details>
                      <summary>本页读取成本</summary>
                      <pre>{JSON.stringify(page.cost, null, 2)}</pre>
                    </details>
                  )}
                  <Tree
                    name="公共来源 / 版本 / 稳定前缀"
                    value={snapshot.public_recording}
                    onSelect={(path, value) =>
                      select({ source: "public", path, value })
                    }
                  />
                </section>
                {evidencePanel}
                <div className="columns">
                  <Source
                    title={
                      engineeringProjection ? "Ledger 工程投影" : "历史报告"
                    }
                    doc={snapshot.report}
                  />
                  <Source title="启动元数据" doc={snapshot.metadata} />
                </div>
                <section>
                  <h2>
                    {engineeringProjection ? "完整原工程投影" : "完整原报告"}
                  </h2>
                  {snapshot.report.exact !== null && (
                    <Tree
                      name="result.json"
                      value={snapshot.report.exact}
                      onSelect={(path, value) =>
                        select({ source: "report", path, value })
                      }
                    />
                  )}
                  <Raw
                    title={engineeringProjection ? "工程投影" : "报告"}
                    doc={snapshot.report}
                  />
                  <h2>启动元数据</h2>
                  {snapshot.metadata.exact !== null && (
                    <Tree
                      name="metadata"
                      value={snapshot.metadata.exact}
                      onSelect={(path, value) =>
                        select({ source: "metadata", path, value })
                      }
                    />
                  )}
                  <Raw title="元数据" doc={snapshot.metadata} />
                  <h2>覆盖与缺口</h2>
                  {snapshot.gaps.map((g) => (
                    <p className="gap" key={g}>
                      {g}
                    </p>
                  ))}
                </section>
              </>
            )}
            <footer>
              来源状态：{engineeringProjection ? "工程投影" : "报告"}{" "}
              {statusLabel[snapshot.report.status]} · 元数据{" "}
              {statusLabel[snapshot.metadata.status]} · 公共记录{" "}
              {statusLabel[String(snapshot.public_recording.status)]} /
              派生追踪可重建，不参加恢复或归约。
            </footer>
          </>
        )}
      </main>
    </div>
  );
}
