import { useEffect, useRef, useState } from "react";
import { at, fact, type Json } from "./facts";
import { get, Tree } from "./evidence";
import { list, text, type Evidence, type Point } from "./browse";
import { ExecutionProjection } from "./ExecutionProjection";
import { Field } from "./Economics";
export type Fragment = {
  document: string;
  source: string;
  kind: string;
  path: string;
  exact?: Json;
  rows?: {
    path: string;
    exact: Json;
    csv_record_index?: string;
    csv_line?: string;
  }[];
  start?: string;
  end?: string;
  total?: string;
  omitted_arrays: { path: string; length: string }[];
};
export type ExistingStatus = {
  status: string;
  kind?: string;
  detail?: string;
  documents?: {
    document: string;
    source?: string;
    status: string;
    detail?: string;
    summary?: Json;
    arrays?: { path: string; length: string }[];
    bytes_read?: string | null;
    file_bytes?: string;
    read_attempts?: string;
    decode_attempts?: string;
    load_elapsed_us?: string;
  }[];
};
export const existingLabel = (kind?: string) =>
  kind === "completion"
    ? "已停止工程容量诊断 · 最终 Ledger 快照"
    : kind === "continuous"
      ? "原生输入优化 · 有限连续调用成本证据"
      : kind === "collection_manifest"
        ? "历史采集摘要"
        : "工程 / 历史来源";
export function useExisting(run: string) {
  const [loaded, setLoaded] = useState<Fragment | null>(null),
    [error, setError] = useState(""),
    [loading, setLoading] = useState(false);
  const generation = useRef(0);
  useEffect(() => {
    generation.current++;
    setLoaded(null);
    setError("");
    setLoading(false);
  }, [run]);
  const load = async (
    document: string,
    path: string,
    start?: string,
    count?: string,
  ) => {
    const g = ++generation.current;
    setLoading(true);
    setError("");
    const q = new URLSearchParams({ document, path });
    if (start !== undefined) q.set("start", start);
    if (count !== undefined) q.set("count", count);
    try {
      const v = await get<Fragment>(`/api/runs/${run}/existing?${q}`);
      if (g === generation.current) setLoaded(v);
    } catch (e) {
      if (g === generation.current) {
        setError(String(e));
        setLoaded(null);
      }
    } finally {
      if (g === generation.current) setLoading(false);
    }
  };
  return { loaded, error, loading, load };
}
const pointer = (document: string, path: string) =>
  document === "completion" ? path : `/existing/${document}${path}`;
export function existingPoints(
  status: ExistingStatus | undefined,
  loaded: Fragment | null,
): Point[] {
  if (status?.status !== "ready") return [];
  const out: Point[] = [];
  const walk = (
    value: Json,
    path: string,
    document: string,
    source?: string,
    condition?: string,
  ) => {
    if (value === null || typeof value !== "object") return;
    if (Array.isArray(value)) {
      value.forEach((v, i) =>
        walk(v, `${path}/${i}`, document, source, condition),
      );
      return;
    }
    const own = text(at(value, "condition_id")) ?? condition;
    for (const [k, v] of Object.entries(value)) {
      const p = `${path}/${k.replaceAll("~", "~0").replaceAll("/", "~1")}`;
      const clock = k.endsWith("_utc_ms")
        ? "原 UTC ms"
        : document === "calls" && k === "now"
          ? "宿主逻辑 UTC ms"
          : undefined;
      if (clock && typeof v === "string" && /^-?\d+$/.test(v))
        out.push({
          source: "existing",
          path: pointer(document, p),
          value: v,
          utcMs: v,
          name: k,
          clock,
          condition: own,
          existing: {
            kind: status.kind ?? null,
            document,
            source: source ?? null,
            reference: p,
            original_identity: value,
          },
        });
      else walk(v, p, document, source, own);
    }
  };
  for (const d of status.documents ?? [])
    if (d.status === "ready" && d.summary !== undefined)
      walk(d.summary, "", d.document, d.source);
  if (loaded) {
    if (loaded.exact !== undefined)
      walk(loaded.exact, loaded.path, loaded.document, loaded.source);
    for (const r of loaded.rows ?? [])
      walk(r.exact, r.path, loaded.document, loaded.source);
  }
  return out.filter((p, i) => out.findIndex((q) => q.path === p.path) === i);
}
export function ExistingPanel({
  status,
  state,
  condition,
  onSelect,
}: {
  status: ExistingStatus;
  state: ReturnType<typeof useExisting>;
  condition: string;
  onSelect: (e: Evidence) => void;
}) {
  const [document, setDocument] = useState(
      status.documents?.find((d) => d.status === "ready")?.document ?? "",
    ),
    [path, setPath] = useState(""),
    [start, setStart] = useState("0"),
    [count, setCount] = useState("5"),
    [summaryPath, setSummaryPath] = useState("");
  const completion = status.kind === "completion";
  const select = (role: string, p: string, value: Json, condition?: string) =>
    onSelect({
      source: "existing",
      path: pointer(role, p),
      value,
      condition: condition ?? text(at(value, "condition_id")),
      existing: {
        kind: status.kind ?? null,
        document: role,
        source:
          status.documents?.find((d) => d.document === role)?.source ?? null,
        reference: p,
      },
    });
  const arrayButton = (role: string, a: { path: string; length: string }) => (
    <button
      key={role + a.path}
      onClick={() => {
        setDocument(role);
        setPath(a.path);
        setStart("0");
        void state.load(role, a.path, "0", count);
      }}
    >
      {role}
      {a.path || "/"} · 原数组 {a.length} 条
    </button>
  );
  const original = status.documents?.find(
    (d) => d.document === "completion" && d.status === "ready",
  )?.summary;
  const manifest = status.documents?.find(
    (d) => d.document === "manifest" && d.status === "ready",
  )?.summary;
  const counters = at(manifest, "source_counters");
  const counterEntries =
    counters && typeof counters === "object" && !Array.isArray(counters)
      ? Object.entries(counters)
      : [];
  const summaryRows =
    state.loaded && ["summary", "pooled"].includes(state.loaded.document)
      ? (state.loaded.rows ?? [])
      : [];
  const currentSummary =
    summaryRows.find((r) => r.path === summaryPath) ?? summaryRows[0];
  const summaryValue = currentSummary?.exact;
  const pooled = state.loaded?.document === "pooled";
  const statistics = pooled ? at(summaryValue, "phases_ms") : summaryValue;
  const metrics = pooled
    ? Object.keys(statistics ?? {})
    : [
        "service_ms",
        "native_ms",
        "cpu_ms",
        "packing_ms",
        "state_ms",
        "object_preparation_ms",
        "action_steps_ms",
      ];
  const loadSummary = (role: string) => {
    const total = status.documents
      ?.find((d) => d.document === role)
      ?.arrays?.find((a) => a.path === "")?.length;
    if (total !== undefined) {
      setDocument(role);
      setPath("");
      setStart("0");
      setSummaryPath("");
      void state.load(role, "", "0", total);
    }
  };
  return (
    <section className="existing-panel" data-existing-kind={status.kind}>
      <h2>{existingLabel(status.kind)}</h2>
      <p className="muted">
        独立显式来源 · {status.status}
        。仅载入以下摘要文件；下钻沿原字段和原行序，最近相同请求复用响应，不驱动业务或打开源端声明的数据文件。
      </p>
      <div className="fact-grid">
        {(status.kind === "collection_manifest"
          ? [
              ["原采集日期", "/utc_day"],
              ["原结束状态", "/state"],
              ["原结束原因", "/closure_reason"],
              ["原构建提交", "/build_commit"],
              ["原覆盖状态", "/collection_day_coverage/state"],
              [
                "首次开始 · UTC ms",
                "/collection_day_coverage/first_started_utc_ms",
              ],
              [
                "最近开始 · UTC ms",
                "/collection_day_coverage/last_started_utc_ms",
              ],
              ["原结束 · UTC ms", "/collection_day_coverage/ended_utc_ms"],
              [
                "日内重新激活",
                "/collection_day_coverage/reactivated_within_day",
              ],
            ]
          : completion
            ? [
                ["原窗口完成", "/complete"],
                ["工程组数", "/groups"],
                ["原排空秒", "/drain_seconds"],
                ["原业务服务累计 ns", "/service_total_ns"],
                ["原证据累计 ns", "/evidence_total_ns"],
                ["原装配提交", "/online/dpt/session/git_commit"],
              ]
            : []
        ).map(([label, p]) => {
          const role = completion ? "completion" : "manifest",
            v = status.documents?.find((d) => d.document === role)?.summary;
          return (
            <Field
              key={p}
              label={label}
              path={p}
              source="existing"
              report={v}
              onSelect={(e) => select(role, e.path, e.value, e.condition)}
            />
          );
        })}
      </div>
      {status.detail && <p role="alert">{status.detail}</p>}
      {completion ? (
        <p className="gap">
          complete / online 是工程窗口与排空诊断，非经济闭合；caller_role=PAPER
          不证明联网 PAPER。平台确认现金 / 可用额不是策略统一最终资金、NAV
          或收益验收。缺 outcome 映射，不猜 UP/DOWN；不同容量运行不连成曲线。
        </p>
      ) : status.kind === "continuous" ? (
        <p className="gap">
          all / entry / continuation 是同一 run 的重叠视图，count 不能相加。
          pooled
          的两轮合并分位数不能平均；各原摘要与明细的实际条数见原数组及区段。
          service 从 native.consume 至 Actor 执行结束；CPU 可超过墙钟，进程 RSS
          / user / system 包含图加载与前史，嵌套探针不能相加当总墙钟。Debug
          动作保留原文，不是权威 ModelDecision / NoTrade
          历史。工程图与初始权重不表示训练完成，源端成本不是 C 服务成本。
        </p>
      ) : (
        <p className="gap">
          只读原 manifest，不打开所列 Parquet 或复验文件哈希。空 gaps
          不证明无遗漏，snapshot 一次不判23小时缺口；source_timestamp
          没有统一单位，不猜
          UTC。历史摘要不代表实时在线、稳定公共前缀或账户经营。
        </p>
      )}
      {status.kind === "collection_manifest" && (
        <>
          <h3>
            来源原统计 · {counterEntries.length} 类（已读 source_counters）
          </h3>
          <div className="source-table">
            <table>
              <thead>
                <tr>
                  <th>原来源</th>
                  {[
                    "files",
                    "rows",
                    "written_rows",
                    "dropped_rows",
                    "backfill_rows",
                    "reconnect_events",
                    "lag_events",
                    "first_recv_time_utc_ms",
                    "last_recv_time_utc_ms",
                  ].map((k) => (
                    <th key={k}>{k}</th>
                  ))}
                  <th>原详情</th>
                </tr>
              </thead>
              <tbody>
                {counterEntries.map(([source, v]) => (
                  <tr key={source}>
                    <td>{source}</td>
                    {[
                      "files",
                      "rows",
                      "written_rows",
                      "dropped_rows",
                      "backfill_rows",
                      "reconnect_events",
                      "lag_events",
                      "first_recv_time_utc_ms",
                      "last_recv_time_utc_ms",
                    ].map((k) => (
                      <td key={k}>{fact(at(v, k))}</td>
                    ))}
                    <td>
                      <button
                        onClick={() =>
                          select("manifest", `/source_counters/${source}`, v)
                        }
                      >
                        查原值
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
      {status.kind === "continuous" && (
        <>
          <div className="fact-grid">
            <Field
              label="原工程基线提交"
              source="existing"
              path="/baseline"
              report={
                status.documents?.find((d) => d.document === "provenance")
                  ?.summary
              }
              onSelect={(e) => select("provenance", e.path, e.value)}
            />
          </div>
          <h3>原摘要对照 · ms / 原样本数</h3>
          <div className="research-controls">
            <button onClick={() => loadSummary("summary")}>浏览单轮摘要</button>
            <button onClick={() => loadSummary("pooled")}>
              浏览两轮合并摘要
            </button>
          </div>
          {currentSummary && (
            <>
              <label>
                原摘要记录
                <select
                  aria-label="原摘要记录"
                  value={currentSummary.path}
                  onChange={(e) => setSummaryPath(e.target.value)}
                >
                  {summaryRows.map((r) => (
                    <option key={r.path} value={r.path}>
                      {[
                        "run",
                        "model",
                        "segment",
                        "window",
                        "variant",
                        "round",
                        "phase",
                      ]
                        .map((k) =>
                          at(r.exact, k) === undefined
                            ? null
                            : `${k}=${fact(at(r.exact, k))}`,
                        )
                        .filter(Boolean)
                        .join(" · ")}{" "}
                      · 原{r.path}
                    </option>
                  ))}
                </select>
              </label>
              <p>
                {pooled ? "两轮合并原统计" : "单轮原统计"} ·{" "}
                {state.loaded?.source} · 原{currentSummary.path}
                ；所有量均来自该原记录，不计算或平均分位数。
              </p>
              <div className="source-table">
                <table>
                  <thead>
                    <tr>
                      <th>原指标 · ms</th>
                      {["count", "mean", "p50", "p90", "p95", "p99", "max"].map(
                        (k) => (
                          <th key={k}>{k}</th>
                        ),
                      )}
                    </tr>
                  </thead>
                  <tbody>
                    {metrics.map((m) => (
                      <tr key={m}>
                        <td>
                          <button
                            onClick={() =>
                              select(
                                state.loaded!.document,
                                currentSummary.path +
                                  (pooled ? "/phases_ms" : "") +
                                  `/${m}`,
                                at(statistics, m) ?? null,
                              )
                            }
                          >
                            {m}
                          </button>
                        </td>
                        {[
                          "count",
                          "mean",
                          "p50",
                          "p90",
                          "p95",
                          "p99",
                          "max",
                        ].map((k) => (
                          <td key={k}>{fact(at(statistics, m, k))}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="fact-grid">
                {(pooled
                  ? ["calls"]
                  : ["action_families", "over_4ms", "over_1ms"]
                ).map((k) => (
                  <button
                    className="fact-field"
                    key={k}
                    onClick={() =>
                      select(
                        state.loaded!.document,
                        `${currentSummary.path}/${k}`,
                        at(summaryValue, k) ?? null,
                      )
                    }
                  >
                    <span>{k} · 原值</span>
                    <strong>{fact(at(summaryValue, k))}</strong>
                  </button>
                ))}
              </div>
              <button
                onClick={() =>
                  select(
                    state.loaded!.document,
                    currentSummary.path,
                    currentSummary.exact,
                  )
                }
              >
                选择此原摘要
              </button>
            </>
          )}
        </>
      )}
      {completion && original && (
        <>
          <div className="fact-grid">
            {[
              ["平台确认现金 · pUSD", "cash_pusd"],
              ["平台确认可用额 · pUSD", "available_pusd"],
              ["平台确认费用 · pUSD", "fees_pusd"],
              ["平台确认已实现 PnL · pUSD", "realized_pnl_pusd"],
            ].map(([label, key]) => (
              <Field
                key={key}
                label={label}
                path={`/final_ledger/platform_confirmed_projection/${key}`}
                report={original}
                source="existing"
                onSelect={(e) =>
                  select("completion", e.path, e.value, e.condition)
                }
              />
            ))}
          </div>
          <details className="projection-details">
            <summary>展开最终 Ledger 执行与预留投影</summary>
            <ExecutionProjection
              report={original}
              engineering
              root="/final_ledger"
              source="existing"
              condition={condition}
              onSelect={(e) =>
                select("completion", e.path, e.value, e.condition)
              }
            />
          </details>
        </>
      )}
      {status.documents?.map((d) => (
        <details
          key={d.document}
          className="source-document"
          open={d.document === "manifest"}
        >
          <summary>
            {d.document} · {d.source ?? "未指定"} · {d.status}
          </summary>
          {d.detail && <p role="alert">{d.detail}</p>}
          <p className="muted">
            读取 {d.read_attempts ?? "未测"} 次 / 解码{" "}
            {d.decode_attempts ?? "未测"} 次 ·{" "}
            {d.bytes_read === null
              ? "原入口未返回读取字节"
              : `${d.bytes_read ?? "未测"} B`}{" "}
            {d.file_bytes ? `· 文件长度 ${d.file_bytes} B（非 I/O）` : ""} ·
            原加载+解释 {d.load_elapsed_us ?? "未测"} μs
          </p>
          {d.summary !== undefined && (
            <Tree
              name="原摘要字段"
              value={d.summary}
              onSelect={(p, v) => select(d.document, p, v)}
            />
          )}
          <div className="research-controls">
            {d.arrays?.map((a) => arrayButton(d.document, a))}
          </div>
        </details>
      ))}
      <div className="research-controls">
        <label>
          原文档
          <select
            aria-label="工程历史文档"
            value={document}
            onChange={(e) => {
              setDocument(e.target.value);
              setPath("");
            }}
          >
            {status.documents?.map((d) => (
              <option key={d.document} value={d.document}>
                {d.document} · {d.status}
              </option>
            ))}
          </select>
        </label>
        <label>
          原字段路径
          <input
            aria-label="工程历史路径"
            value={path}
            onChange={(e) => setPath(e.target.value)}
            placeholder="例如 /files；根数组留空"
          />
        </label>
        <label>
          原数组起点
          <input
            aria-label="工程历史起点"
            value={start}
            onChange={(e) => setStart(e.target.value)}
          />
        </label>
        <label>
          区段条数
          <input
            aria-label="工程历史条数"
            value={count}
            onChange={(e) => setCount(e.target.value)}
          />
        </label>
        <button
          disabled={state.loading || !document}
          onClick={() => void state.load(document, path, start, count)}
        >
          读取原区段
        </button>
      </div>
      {state.loading && <p role="status">从已载入文档选择原区段…</p>}
      {state.error && <p role="alert">{state.error}</p>}
      {state.loaded && (
        <div data-existing-loaded={state.loaded.document}>
          <h3>
            {state.loaded.source} · {state.loaded.path || "/"}{" "}
            {state.loaded.total !== undefined
              ? `原区段 [${state.loaded.start}, ${state.loaded.end}) / ${state.loaded.total}`
              : "原字段"}
          </h3>
          {state.loaded.exact !== undefined && (
            <Tree
              name="原字段详情"
              path={state.loaded.path}
              value={state.loaded.exact}
              onSelect={(p, v) => select(state.loaded!.document, p, v)}
            />
          )}
          {(summaryRows.length ? [currentSummary!] : state.loaded.rows)?.map(
            (r) => (
              <div key={r.path} className="research-fragment">
                <button
                  onClick={() =>
                    select(state.loaded!.document, r.path, r.exact)
                  }
                >
                  选择原记录 {r.path} {r.csv_line ? `· CSV行${r.csv_line}` : ""}
                </button>
                <Tree
                  name="原记录字段"
                  path={r.path}
                  value={r.exact}
                  onSelect={(p, v) => select(state.loaded!.document, p, v)}
                />
              </div>
            ),
          )}
          <div className="research-controls">
            {state.loaded.omitted_arrays?.map((a) =>
              arrayButton(state.loaded!.document, a),
            )}
          </div>
        </div>
      )}
    </section>
  );
}
