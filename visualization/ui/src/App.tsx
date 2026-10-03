import { useEffect, useRef, useState } from "react";
import { at, fact, statusLabel, type Json } from "./facts";
type Doc = {
  source: string | null;
  status: string;
  detail: string | null;
  exact: Json | null;
  raw_json: string | null;
  bytes_read: string;
  read_attempts: string;
  decode_attempts: string;
  load_elapsed_us: string;
};
type Snapshot = {
  label: string;
  report: Doc;
  metadata: Doc;
  public_recording: Record<string, Json>;
  gaps: string[];
};
type Page = {
  page: string;
  row_start: string;
  returned_rows: string;
  total_returned_rows: string;
  at_end: boolean;
  prefix: Json;
  rows: Json[];
  cost: Json;
};
type Run = { key: string; label: string };
async function get<T>(url: string): Promise<T> {
  const response = await fetch(url);
  const body = await response.json();
  if (!response.ok) throw new Error(body.detail ?? `HTTP ${response.status}`);
  return body;
}
function Tree({
  value,
  name,
  path = "",
  onSelect,
}: {
  value: Json;
  name: string;
  path?: string;
  onSelect: (p: string, v: Json) => void;
}) {
  const [open, setOpen] = useState(false);
  if (value === null || typeof value !== "object")
    return (
      <div className="leaf">
        <button onClick={() => onSelect(path, value)}>{name}</button>
        <code>{fact(value)}</code>
      </div>
    );
  const entries = Object.entries(value);
  return (
    <details className="tree" onToggle={(e) => setOpen(e.currentTarget.open)}>
      <summary>
        <span>{name}</span>
        <small>
          {Array.isArray(value) ? "数组" : "对象"} · {entries.length}
        </small>
        <button
          onClick={(e) => {
            e.preventDefault();
            onSelect(path, value);
          }}
        >
          查原值
        </button>
      </summary>
      {open &&
        entries.map(([key, v]) => (
          <Tree
            key={key}
            name={key}
            value={v}
            path={`${path}/${key.replaceAll("~", "~0").replaceAll("/", "~1")}`}
            onSelect={onSelect}
          />
        ))}
    </details>
  );
}
function Source({ title, doc }: { title: string; doc: Doc }) {
  return (
    <div className="source">
      <strong>{title}</strong>
      <span className={`status ${doc.status === "ready" ? "ok" : "warn"}`}>
        {statusLabel[doc.status] ?? doc.status}
      </span>
      <p className="source-path">{doc.source ?? "未指定来源"}</p>
      {doc.detail && <p className="error">{doc.detail}</p>}
      <small>
        读取 {doc.read_attempts} 次 · 解码 {doc.decode_attempts} 次 ·{" "}
        {doc.bytes_read} B · 启动读取 {doc.load_elapsed_us} μs
      </small>
    </div>
  );
}
function Raw({ doc, title }: { doc: Doc; title: string }) {
  return doc.raw_json !== null ? (
    <details className="raw">
      <summary>{title} · 原 JSON 文本（数字未改写）</summary>
      <pre>{doc.raw_json}</pre>
    </details>
  ) : null;
}
export default function App() {
  const [runs, setRuns] = useState<Run[]>([]),
    [run, setRun] = useState(""),
    [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [view, setView] = useState<"report" | "public" | "source">("report"),
    [error, setError] = useState(""),
    [loading, setLoading] = useState(false);
  const [page, setPage] = useState<Page | null>(null),
    [pageError, setPageError] = useState(""),
    [pageBusy, setPageBusy] = useState(false);
  const currentRun = useRef(run);
  currentRun.current = run;
  const [selection, setSelection] = useState<{
    path: string;
    value: Json;
    source: string;
  } | null>(null);
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
    setPageError("");
    setPageBusy(false);
    setSelection(null);
    fetch(`/api/runs/${run}/snapshot`, { signal: abort.signal })
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json() as Promise<Snapshot>;
      })
      .then((data) => {
        if (!abort.signal.aborted) setSnapshot(data);
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
    const requestedRun = run;
    setPageBusy(true);
    setPageError("");
    try {
      const data = await get<Page>(
        `/api/runs/${run}/public${next === undefined ? "" : `?page=${next}`}`,
      );
      if (currentRun.current === requestedRun) setPage(data);
    } catch (e) {
      if (currentRun.current === requestedRun) setPageError(String(e));
    } finally {
      if (currentRun.current === requestedRun) setPageBusy(false);
    }
  }
  const report =
    snapshot?.report.status === "ready" ? snapshot.report.exact : undefined;
  const metadata =
    snapshot?.metadata.status === "ready" ? snapshot.metadata.exact : undefined;
  const select = (path: string, value: Json) =>
    setSelection({ path, value, source: "历史报告" });
  const selectMetadata = (path: string, value: Json) =>
    setSelection({ path, value, source: "启动元数据" });
  const selectPublic = (path: string, value: Json) =>
    setSelection({
      path,
      value,
      source:
        "公共记录 · 运行 " +
        run +
        " · 稳定前缀 " +
        fact(at(snapshot?.public_recording.prefix, "stable_storage_ref")),
    });
  const tiles: [string, Json | undefined, string][] = [
    ["结束原因", at(report, "end_reason"), "原报告"],
    ["公共记录排空", at(report, "recording_drained"), "原报告"],
    ["计算次数", at(report, "calculation_count"), "本运行累计"],
    ["耐久 NOE 前缀", at(report, "durable_noe_prefix"), "原耐久位置"],
    ["未闭合承诺", at(report, "unclosed_commitments"), "终态快照"],
    ["待账本应用 NOE", at(report, "pending_noe_records"), "终态快照"],
    [
      "保守 NAV · pUSD",
      at(report, "conservative_nav_pusd"),
      "原精确有理数 / 状态",
    ],
    [
      "Maker 最坏情形 NAV · pUSD",
      at(report, "worst_case_maker_nav_pusd"),
      "原精确有理数",
    ],
  ];
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
        {[
          ["report", "运行报告"],
          ["public", "公共记录"],
          ["source", "来源与边界"],
        ].map(([id, label]) => (
          <button
            key={id}
            className={view === id ? "active" : ""}
            onClick={() => setView(id as typeof view)}
          >
            {label}
          </button>
        ))}
        <div className="aside-foot">
          原运行耐久事实
          <br />
          终态 / 有限时序
          <br />
          <span>历史范围固定于启动时</span>
        </div>
      </aside>
      <main>
        <header>
          <div>
            <p className="eyebrow">公共业务事实浏览器</p>
            <h1>
              历史运行 <span>事实与证据</span>
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
        {error && (
          <p role="alert" className="error">
            {error}
          </p>
        )}
        {!loading && !runs.length && !error && <p>没有配置运行来源。</p>}
        {snapshot && (
          <>
            <div className="run-strip">
              <b>backtest</b>
              <span>{snapshot.label}</span>
              <span>
                {snapshot.report.status === "ready"
                  ? "HistoricalRunReportV1 · JSON 证据"
                  : "JSON 证据 · 报告尚无法解释"}
              </span>
              <span>{fact(at(report, "economics_version"))}</span>
            </div>
            <div className="identity">
              <span>
                运行{" "}
                <code>{fact(at(metadata, "config", "session", "run_id"))}</code>
              </span>
              <span>
                账户{" "}
                <code>
                  {fact(at(metadata, "config", "session", "account_id"))}
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
            </div>
            {view === "report" && (
              <>
                <div className="notice">
                  终态报告与最近计算时序。该来源没有完整诊断或经济历史，无法绘制历史曲线。缺元数据仍可浏览报告。
                </div>
                {snapshot.report.status !== "ready" && (
                  <Source title="报告状态" doc={snapshot.report} />
                )}
                <div className="tiles">
                  {tiles.map(([label, value, note]) => (
                    <article key={label}>
                      <p>{label}</p>
                      <strong>{fact(value)}</strong>
                      <small>{note}</small>
                    </article>
                  ))}
                </div>
                <div className="columns">
                  <section>
                    <h2>账户经济 · 原终态快照</h2>
                    <p className="muted">
                      现金、持仓、预留与返佣按原字段分别保留；未计算挂单价值或市场损益。
                    </p>
                    {at(report, "account_economics_memory") != null ? (
                      <Tree
                        key={run}
                        name="account_economics_memory"
                        path="/account_economics_memory"
                        value={at(report, "account_economics_memory")!}
                        onSelect={select}
                      />
                    ) : (
                      <p>无可解释的账户经济快照</p>
                    )}
                    <h2>有限计算时序 · 原字段</h2>
                    {[
                      "last_dispatched_calculation_timing",
                      "pending_calculation_timing",
                      "delivery_summary",
                    ].map((name) =>
                      at(report, name) != null ? (
                        <Tree
                          key={`${run}/${name}`}
                          name={name}
                          path={"/" + name}
                          value={at(report, name)!}
                          onSelect={select}
                        />
                      ) : (
                        <p className="muted" key={name}>
                          {name}：{fact(at(report, name))}
                        </p>
                      ),
                    )}
                  </section>
                  <section>
                    <h2>证据详情</h2>
                    <p className="muted">
                      点击字段“查原值”。路径属于原 JSON；数字保持精确文本。
                    </p>
                    {selection ? (
                      <>
                        <p className="muted">{selection.source}</p>
                        <code className="pointer">{selection.path || "/"}</code>
                        <pre>{JSON.stringify(selection.value, null, 2)}</pre>
                      </>
                    ) : (
                      <p className="empty">尚未选择事实</p>
                    )}
                  </section>
                </div>
                <section>
                  <h2>完整报告</h2>
                  {snapshot.report.exact !== null && (
                    <Tree
                      key={run}
                      name="result.json"
                      value={snapshot.report.exact}
                      onSelect={select}
                    />
                  )}
                  <Raw doc={snapshot.report} title="报告" />
                </section>
              </>
            )}
            {view === "public" && (
              <section>
                <h2>公共记录 · 指定稳定前缀</h2>
                <p className="muted">
                  复用原顺序游标。原身份在完整 JSON
                  中；行序只定位本前缀展示位置。最近一页可重试。刷新、切回运行或其他浏览窗口推进后，取回服务端当前页。
                </p>
                <Tree
                  key={run}
                  name="来源 / 版本 / 前缀"
                  value={snapshot.public_recording}
                  onSelect={selectPublic}
                />
                <div className="page-controls">
                  <button
                    disabled={
                      pageBusy ||
                      snapshot.public_recording.status !== "ready" ||
                      !!page?.at_end
                    }
                    onClick={() =>
                      loadPage(
                        page ? (BigInt(page.page) + 1n).toString() : undefined,
                      )
                    }
                  >
                    {pageBusy ? "读取中…" : page ? "下一页" : "读取当前页"}
                  </button>
                  <button
                    disabled={
                      pageBusy || snapshot.public_recording.status !== "ready"
                    }
                    onClick={() => loadPage()}
                  >
                    取回服务端当前页
                  </button>
                  <span>
                    {page
                      ? `页 ${page.page} · 本页 ${page.returned_rows} 条 · 累计 ${page.total_returned_rows} 条${page.at_end ? " · 稳定前缀末端" : ""}`
                      : "尚未读取记录"}
                  </span>
                </div>
                {pageError && (
                  <p role="alert" className="error">
                    {pageError}
                  </p>
                )}
                {page && (
                  <>
                    <div className="records">
                      {page.rows.map((row, index) => (
                        <Tree
                          key={`${run}/${page.row_start}/${index}`}
                          name={`行 ${(BigInt(page.row_start) + BigInt(index)).toString()} · ${Object.keys(row ?? {})[0] ?? "记录"}`}
                          value={row}
                          path={`/public/${page.row_start}/${index}`}
                          onSelect={selectPublic}
                        />
                      ))}
                    </div>
                    <details>
                      <summary>本页读取成本</summary>
                      <pre>{JSON.stringify(page.cost, null, 2)}</pre>
                    </details>
                    {selection && (
                      <div className="page-evidence">
                        <p className="muted">{selection.source}</p>
                        <code>{selection.path}</code>
                        <pre>{JSON.stringify(selection.value, null, 2)}</pre>
                      </div>
                    )}
                  </>
                )}
              </section>
            )}
            {view === "source" && (
              <>
                <div className="columns">
                  <Source title="历史报告" doc={snapshot.report} />
                  <Source title="启动元数据" doc={snapshot.metadata} />
                </div>
                <section>
                  <h2>启动元数据 · 独立来源</h2>
                  {snapshot.metadata.exact !== null && (
                    <Tree
                      key={run}
                      name="metadata"
                      value={snapshot.metadata.exact}
                      onSelect={selectMetadata}
                    />
                  )}
                  <Raw doc={snapshot.metadata} title="元数据" />
                </section>
                <section>
                  <h2>当前覆盖与缺口</h2>
                  {snapshot.gaps.map((g) => (
                    <p className="gap" key={g}>
                      {g}
                    </p>
                  ))}
                  <p className="muted">
                    报告仅在启动时读取/解析一次。公共记录由显式根目录与写入方返回凭据建立。浏览状态不驱动交易。
                  </p>
                </section>
              </>
            )}
            <footer>
              来源状态：报告 {statusLabel[snapshot.report.status]} · 元数据{" "}
              {statusLabel[snapshot.metadata.status]} · 公共记录{" "}
              {statusLabel[String(snapshot.public_recording.status)]} /
              技术验证与人工视觉验收分别记录
            </footer>
          </>
        )}
      </main>
    </div>
  );
}
