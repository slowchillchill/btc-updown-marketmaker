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
  reportPoints,
  rangePoints,
  traceJson,
  text,
  type Evidence,
} from "./browse";
import { Topology } from "./Topology";
import { Economics, Field, MarketSummary } from "./Economics";
import { EventTable } from "./EventTable";
import { Timeline } from "./Timeline";
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
    [page, setPage] = useState<Page | null>(null),
    [pageBusy, setPageBusy] = useState(false),
    [pageError, setPageError] = useState("");
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
    setPageError("");
    setPageBusy(false);
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
  async function loadPage(next?: string) {
    const requested = run;
    setPageBusy(true);
    setPageError("");
    try {
      const d = await get<Page>(
        `/api/runs/${run}/public${next === undefined ? "" : `?page=${next}`}`,
      );
      if (currentRun.current === requested) setPage(d);
    } catch (e) {
      if (currentRun.current === requested) setPageError(String(e));
    } finally {
      if (currentRun.current === requested) setPageBusy(false);
    }
  }
  const report =
    snapshot?.report.status === "ready"
      ? (snapshot.report.exact ?? undefined)
      : undefined;
  const metadata =
    snapshot?.metadata.status === "ready"
      ? (snapshot.metadata.exact ?? undefined)
      : undefined;
  const markets = list(at(report, "account_economics_memory", "markets"));
  const account = at(report, "ledger_consumed_memory", "account_instance_key");
  const allRows = useMemo(() => publicRows(page), [page]);
  const allPoints = useMemo(
    () => [...reportPoints(report), ...allRows.flatMap((r) => r.points)],
    [report, allRows],
  );
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
      allRows.filter(
        (r) =>
          (!condition ||
            r.condition === condition ||
            r.condition === undefined) &&
          ((!start && !end) || r.points.some((p) => points.includes(p))),
      ),
    [allRows, points, start, end, condition],
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
    source === "report"
      ? "历史报告"
      : source === "metadata"
        ? "启动元数据"
        : "公共稳定记录";
  const evidencePanel = (
    <section className="evidence-panel">
      <h2>所选事实</h2>
      {selection ? (
        <>
          <p>
            {sourceName(selection.source)} · 运行来源 {run}
          </p>
          <code>{selection.path || "/"}</code>
          {condition && selection.condition !== condition && (
            <p className="gap">
              无法将此事实直接关联所选市场。保留原证据，不替换身份。
            </p>
          )}
          <pre>{JSON.stringify(selection.value, null, 2)}</pre>
        </>
      ) : (
        <p className="empty">点击卡片、字段或事件引用查原值。</p>
      )}
      <small>
        查看市场：{condition || "全部报告市场"}
        ；字段选择跨视图保留。公共证据绑定原稳定前缀。
      </small>
    </section>
  );
  function download() {
    if (!snapshot || !exportPoints.length) return;
    const payload = traceJson(exportPoints, {
      run_source_key: run,
      run_id:
        at(account, "run_id") ??
        at(metadata, "config", "session", "run_id") ??
        null,
      account_identity: account ?? null,
      report_source: snapshot.report.source,
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
  const controls = (
    <div className="page-controls">
      <button
        disabled={
          pageBusy ||
          snapshot?.public_recording.status !== "ready" ||
          !!page?.at_end
        }
        onClick={() =>
          loadPage(page ? (BigInt(page.page) + 1n).toString() : undefined)
        }
      >
        {pageBusy ? "读取中…" : page ? "下一页" : "读取当前页"}
      </button>
      <button
        disabled={pageBusy || snapshot?.public_recording.status !== "ready"}
        onClick={() => loadPage()}
      >
        取回服务端当前页
      </button>
      <span>
        {page
          ? `页 ${page.page} · 本页 ${page.returned_rows} 条 · 已顺序读至 ${page.total_returned_rows}${page.at_end ? " · 稳定前缀末端" : ""}`
          : "公共记录尚未读取"}
      </span>
    </div>
  );
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
                ? "运行总览"
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
              <b>backtest</b>
              <span>{snapshot.label}</span>
              <span>历史入口 · {fact(at(report, "end_reason"))}</span>
              <span>{fact(at(report, "economics_version"))}</span>
              <span>计算累计 {fact(at(report, "calculation_count"))}</span>
              <span>未闭合 {fact(at(report, "unclosed_commitments"))}</span>
              <span>待 NOE {fact(at(report, "pending_noe_records"))}</span>
              <span>
                稳定公共组边界{" "}
                {fact(
                  at(snapshot.public_recording.prefix, "next_group_position"),
                )}
              </span>
            </div>
            <div className="identity">
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
              <label>
                查看市场
                <select
                  aria-label="市场选择"
                  value={condition}
                  onChange={(e) => setCondition(e.target.value)}
                >
                  <option value="">全部报告市场</option>
                  {markets.map((m, i) => (
                    <option key={i} value={text(at(m, "condition_id")) ?? ""}>
                      {fact(at(m, "condition_id"))}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <div className="notice">
              终态快照与有限时序 ·
              当前来源未保存完整逐次输入、诊断、执行或经济历史。查看市场是浏览选择，不代表策略当前活跃市场。
            </div>
            {snapshot.report.status !== "ready" && (
              <Source title="报告状态" doc={snapshot.report} />
            )}
            {view === "overview" && (
              <>
                <div className="overview-grid">
                  <section>
                    <Topology
                      report={report}
                      metadata={metadata}
                      selection={selection}
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
                      <h2>运行事实 · 终态</h2>
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
                  {pageError && <p role="alert">{pageError}</p>}
                  <EventTable rows={allRows} onSelect={select} />
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
                      仅包含有明确 condition
                      关联的时间点；报告账户级时间点不能凭时间猜测市场。
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
                  <h2>意图、预留与执行反馈</h2>
                  <p className="gap">
                    逐批意图、即时分派回执、订单 / 成交和逐条 NOE
                    未接入。没有反馈证据不等于零成交。报告的未闭合项和预留投影只代表保存边界。
                  </p>
                  {[
                    [
                      "本地预留投影",
                      "ledger_consumed_memory/local_pending_reservation_projection",
                    ],
                    [
                      "平台确认投影",
                      "ledger_consumed_memory/platform_confirmed_projection",
                    ],
                    ["NOE 耐久位置", "durable_noe_prefix"],
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
                </section>
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
            {view === "training" && (
              <>
                <section>
                  <h2>公共模型位置 · 来源未接入</h2>
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
            {view === "evidence" && (
              <>
                {rangeControls}
                <section>
                  <h2>证据范围 · 原稳定前缀</h2>
                  <p className="muted">
                    原顺序游标只保留最近一页。刷新、切回运行或其他窗口推进后取回服务端当前页，不从头扫描。只在当前页按明确字段筛选；本页外历史未定位，不冒充全历史查询。无时间记录不会进入时间范围导出。
                  </p>
                  {controls}
                  {pageError && <p role="alert">{pageError}</p>}
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
                  <EventTable rows={rows} onSelect={select} />
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
                  <Source title="历史报告" doc={snapshot.report} />
                  <Source title="启动元数据" doc={snapshot.metadata} />
                </div>
                <section>
                  <h2>完整原报告</h2>
                  {snapshot.report.exact !== null && (
                    <Tree
                      name="result.json"
                      value={snapshot.report.exact}
                      onSelect={(path, value) =>
                        select({ source: "report", path, value })
                      }
                    />
                  )}
                  <Raw title="报告" doc={snapshot.report} />
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
              来源状态：报告 {statusLabel[snapshot.report.status]} · 元数据{" "}
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
