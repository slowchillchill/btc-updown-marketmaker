import { displayError } from "./i18n";
import { displayFact as fact } from "./i18n";
import { useTranslation } from "react-i18next";
import { useEffect, useRef, useState } from "react";
import { at, type Json } from "./facts";
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
        : kind === "model_timing"
          ? "有限模型宿主工程诊断"
          : kind === "remeasurement_12m"
            ? "十二分钟完整流程测量报告"
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
// 原工程冻结/报告身份随导出；不以当前C提交或SIM夹具身份替代。
export function existingSourceIdentity(
  status: ExistingStatus | undefined,
): Json {
  const document =
    status?.kind === "model_timing"
      ? "time_results"
      : status?.kind === "remeasurement_12m"
        ? "measurement"
        : undefined;
  if (!document) return null;
  const original = status?.documents?.find(
    (d) => d.document === document && d.status === "ready",
  );
  if (!original) return null;
  const fields =
    document === "time_results"
      ? ["production_revision", "diagnostic_revision", "artifact_sha256"]
      : [
          "schema",
          "measurement_source_commit",
          "historical_baseline_source_commit",
        ];
  return {
    document,
    source: original.source ?? null,
    fields: Object.fromEntries(
      fields
        .filter((k) => at(original.summary, k) !== undefined)
        .map((k) => [k, at(original.summary, k)!]),
    ),
  };
}
export function existingPoints(
  status: ExistingStatus | undefined,
  loaded: Fragment | null,
): Point[] {
  if (status?.status !== "ready") return [];
  const out: Point[] = [];
  const push = (
    value: Json,
    path: string,
    document: string,
    source: string | undefined,
    condition: string | undefined,
    identity: Json,
  ) => {
    const field = path
      .split("/")
      .at(-1)
      ?.replaceAll("~1", "/")
      .replaceAll("~0", "~");
    const logical = status.kind === "model_timing";
    const traceField =
      [
        "at",
        "observation",
        "compute_start",
        "completion_at",
        "send",
        "arrival",
        "planned_completion",
        "send_attempt",
        "drain_until",
      ].includes(field ?? "") &&
      (document === "trace" || document === "time_results");
    const isoClock =
      status.kind === "remeasurement_12m" &&
      document === "measurement" &&
      /^\/execution\/timeline\/(material|research)_(launcher_utc|final_gnu_record_modified_utc)$/.test(
        path,
      );
    if (isoClock && typeof value === "string") {
      // 此原报告明确为 +00:00、六位微秒；不泛化解析未知来源时间文本。
      const match =
        /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d{6})\+00:00$/.exec(value);
      if (!match) return;
      const ms = Date.parse(match[1] + "Z");
      if (!Number.isSafeInteger(ms)) return;
      const us = BigInt(ms) * 1000n + BigInt(match[2]);
      out.push({
        source: "existing",
        path: pointer(document, path),
        value,
        name: field!,
        clock: field!.includes("launcher")
          ? "启动器原记录 UTC · 微秒"
          : "完成计量文件 mtime UTC · 微秒",
        utcMs: (us / 1000n).toString(),
        utcUs: us.toString(),
        originalUtcText: value,
        existing: {
          kind: status.kind ?? null,
          document,
          source: source ?? null,
          reference: path,
          original_identity: identity,
        },
      });
      return;
    }
    const clock =
      logical && (traceField || field?.endsWith("_utc_ms"))
        ? path.includes("/next_required/")
          ? "未来必需工程逻辑 ms · 非已发生"
          : "工程运行逻辑 ms · 非真实日期"
        : field?.endsWith("_utc_ms")
          ? "原 UTC ms"
          : document === "calls" && field === "now"
            ? "宿主逻辑 UTC ms"
            : undefined;
    if (!clock || typeof value !== "string" || !/^-?\d+$/.test(value)) return;
    out.push({
      source: "existing",
      path: pointer(document, path),
      value,
      utcMs: value,
      name: field!,
      clock,
      condition,
      existing: {
        kind: status.kind ?? null,
        document,
        source: source ?? null,
        reference: path,
        original_identity: identity,
      },
    });
  };
  const walk = (
    value: Json,
    path: string,
    document: string,
    source?: string,
    condition?: string,
    identity: Json = value,
  ) => {
    if (value === null || typeof value !== "object") {
      push(value, path, document, source, condition, identity);
      return;
    }
    if (Array.isArray(value)) {
      value.forEach((v, i) =>
        walk(v, `${path}/${i}`, document, source, condition),
      );
      return;
    }
    const own = text(at(value, "condition_id")) ?? condition;
    for (const [k, v] of Object.entries(value)) {
      const p = `${path}/${k.replaceAll("~", "~0").replaceAll("/", "~1")}`;
      walk(v, p, document, source, own, value);
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
  const { t: tr } = useTranslation();
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
      {a.path || "/"}
      {tr(" · 原数组 ")}
      {a.length}
      {tr(" 条")}
    </button>
  );
  const original = status.documents?.find(
    (d) => d.document === "completion" && d.status === "ready",
  )?.summary;
  const manifest = status.documents?.find(
    (d) => d.document === "manifest" && d.status === "ready",
  )?.summary;
  const measurement = status.documents?.find(
    (d) => d.document === "measurement" && d.status === "ready",
  )?.summary;
  const measurementField = (parts: string[]) => {
    const value = at(measurement, ...parts);
    const path = "/" + parts.join("/");
    return (
      <button
        data-original-field={path}
        disabled={value === undefined}
        onClick={() =>
          value !== undefined && select("measurement", path, value)
        }
      >
        {value === undefined ? tr("未保存") : fact(value)}
      </button>
    );
  };
  const timing = status.documents?.find(
    (d) => d.document === "time_results" && d.status === "ready",
  )?.summary;
  const rawRun = status.documents?.find(
    (d) => d.document === "run" && d.status === "ready",
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
      <h2>{tr(existingLabel(status.kind))}</h2>
      <p className="muted">
        {tr("独立显式来源 · ")}
        {status.status}
        {tr(
          "。仅载入以下摘要文件；下钻沿原字段和原行序，最近相同请求复用响应，不驱动业务或打开源端声明的数据文件。",
        )}
      </p>
      <div className="fact-grid">
        {(status.kind === "collection_manifest"
          ? [
              [tr("原采集日期"), "/utc_day"],
              [tr("原结束状态"), "/state"],
              [tr("原结束原因"), "/closure_reason"],
              [tr("原构建提交"), "/build_commit"],
              [tr("原覆盖状态"), "/collection_day_coverage/state"],
              [
                tr("首次开始 · UTC ms"),
                "/collection_day_coverage/first_started_utc_ms",
              ],
              [
                tr("最近开始 · UTC ms"),
                "/collection_day_coverage/last_started_utc_ms",
              ],
              [tr("原结束 · UTC ms"), "/collection_day_coverage/ended_utc_ms"],
              [
                tr("日内重新激活"),
                "/collection_day_coverage/reactivated_within_day",
              ],
            ]
          : completion
            ? [
                [tr("原窗口完成"), "/complete"],
                [tr("工程组数"), "/groups"],
                [tr("原排空秒"), "/drain_seconds"],
                [tr("原业务服务累计 ns"), "/service_total_ns"],
                [tr("原证据累计 ns"), "/evidence_total_ns"],
                [tr("原装配提交"), "/online/dpt/session/git_commit"],
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
      {status.detail && <p role="alert">{displayError(status.detail)}</p>}
      {completion ? (
        <p className="gap">
          {tr(
            "complete / online 是工程窗口与排空诊断，非经济闭合；caller_role=PAPER 不证明联网 PAPER。平台确认现金 / 可用额不是策略统一最终资金、NAV 或收益验收。缺 outcome 映射，不猜 UP/DOWN；不同容量运行不连成曲线。",
          )}
        </p>
      ) : status.kind === "continuous" ? (
        <p className="gap">
          {tr(
            "all / entry / continuation 是同一 run 的重叠视图，count 不能相加。 pooled 的两轮合并分位数不能平均；各原摘要与明细的实际条数见原数组及区段。 service 从 native.consume 至 Actor 执行结束；CPU 可超过墙钟，进程 RSS / user / system 包含图加载与前史，嵌套探针不能相加当总墙钟。Debug 动作保留原文，不是权威 ModelDecision / NoTrade 历史。工程图与初始权重不表示训练完成，源端成本不是 C 服务成本。",
          )}
        </p>
      ) : status.kind === "model_timing" ? (
        <p className="gap">
          {tr(
            "工程逻辑毫秒不绑定研究日期；50/250 ms为预声明经济计算延迟，compute_wall_ns为实测时长。原Debug反馈、intent、receipt不解析为NOE或权威决定。exit0 / CLEAN_RUN / stopped_without_future不证明经济闭合；下一必需PublicVisibility 86401011仍未知。SIM内aaaa/bbbb身份与冻结生产修订分别保留。",
          )}
        </p>
      ) : status.kind === "remeasurement_12m" ? (
        <p className="gap">
          {tr(
            "一次跨版本历史观察，未控制共享主机/缓存，不归因单项优化或保证稳定加速。阶段秒/CPU/proc_io与GNU整进程RSS KiB、512B计数分开；研究独占CPU/RSS null不填整进程值。材料范围与启动器/完成文件mtime不同；原微秒文本与导出精确保留，页面范围输入为毫秒。内部phase没有UTC端点，不用wall拼区间；公开省略数组不补算，不拼接首小时或账户收益。",
          )}
        </p>
      ) : (
        <p className="gap">
          {tr(
            "只读原 manifest，不打开所列 Parquet 或复验文件哈希。空 gaps 不证明无遗漏，snapshot 一次不判23小时缺口；source_timestamp 没有统一单位，不猜 UTC。历史摘要不代表实时在线、稳定公共前缀或账户经营。",
          )}
        </p>
      )}
      {status.kind === "model_timing" && (
        <>
          <div className="fact-grid">
            {[
              [tr("生产修订"), "/production_revision"],
              [tr("工程诊断修订"), "/diagnostic_revision"],
              [tr("下一必需工程逻辑 ms · 未知未来"), "/stop/next_required/at"],
              [tr("下一必需位置"), "/stop/next_required/path"],
            ].map(([label, p]) => (
              <Field
                key={p}
                label={label}
                path={p}
                source="existing"
                report={timing}
                onSelect={(e) => select("time_results", e.path, e.value)}
              />
            ))}
            {[
              [tr("原进程退出码"), "/exit_code"],
              [tr("原进程墙钟 ns"), "/wall_ns"],
            ].map(([label, p]) => (
              <Field
                key={p}
                label={label}
                path={p}
                source="existing"
                report={rawRun}
                onSelect={(e) => select("run", e.path, e.value)}
              />
            ))}
          </div>
          <div className="research-controls">
            {["trace", "time_sim", "feedback"].map((role) => (
              <button
                key={role}
                onClick={() => {
                  setDocument(role);
                  setPath("");
                  setStart("0");
                  void state.load(role, "", "0", count);
                }}
              >
                {tr("浏览 {{role}} 原序记录", { role })}
              </button>
            ))}
          </div>
        </>
      )}
      {status.kind === "remeasurement_12m" && (
        <>
          <div className="fact-grid">
            {[
              [tr("原报告版本"), "/schema"],
              [tr("原状态"), "/status"],
              [tr("实际测量源码"), "/measurement_source_commit"],
              [tr("历史基准源码"), "/historical_baseline_source_commit"],
              [tr("旧阶段合计 · 秒"), "/execution/phase_wall_seconds_sum/old"],
              [tr("新阶段合计 · 秒"), "/execution/phase_wall_seconds_sum/new"],
            ].map(([label, p]) => (
              <Field
                key={p}
                label={label}
                path={p}
                source="existing"
                report={measurement}
                onSelect={(e) => select("measurement", e.path, e.value)}
              />
            ))}
          </div>
          <h3>{tr("原阶段对照 · 秒 / proc_io原计数（非GNU整进程）")}</h3>
          <div className="source-table">
            <table>
              <thead>
                <tr>
                  <th>{tr("原阶段")}</th>
                  <th>{tr("旧/新")}</th>
                  {[
                    "wall_seconds",
                    "user_seconds",
                    "system_seconds",
                    "exclusive_cpu_seconds",
                    "exclusive_max_rss_kib",
                  ].map((k) => (
                    <th key={k}>{k}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {[
                  "prepare",
                  "general_statistics",
                  "two_market_research",
                ].flatMap((phase) =>
                  ["old", "new"].map((side) => (
                    <tr key={phase + side}>
                      <td>{phase}</td>
                      <td>{side}</td>
                      {[
                        "wall_seconds",
                        "user_seconds",
                        "system_seconds",
                        "exclusive_cpu_seconds",
                        "exclusive_max_rss_kib",
                      ].map((k) => (
                        <td key={k}>
                          {measurementField([
                            "execution",
                            "phases",
                            phase,
                            side,
                            k,
                          ])}
                        </td>
                      ))}
                    </tr>
                  )),
                )}
              </tbody>
            </table>
          </div>
          <h3>{tr("原阶段 proc_io · 原字节 / 系统调用次数（不互相加总）")}</h3>
          <div className="source-table">
            <table>
              <thead>
                <tr>
                  <th>{tr("原阶段")}</th>
                  <th>{tr("旧/新")}</th>
                  {[
                    "rchar",
                    "wchar",
                    "read_bytes",
                    "write_bytes",
                    "syscr",
                    "syscw",
                    "cancelled_write_bytes",
                  ].map((k) => (
                    <th key={k}>{k}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {[
                  "prepare",
                  "general_statistics",
                  "two_market_research",
                ].flatMap((phase) =>
                  ["old", "new"].map((side) => (
                    <tr key={phase + side}>
                      <td>{phase}</td>
                      <td>{side}</td>
                      {[
                        "rchar",
                        "wchar",
                        "read_bytes",
                        "write_bytes",
                        "syscr",
                        "syscw",
                        "cancelled_write_bytes",
                      ].map((k) => (
                        <td key={k}>
                          {measurementField([
                            "execution",
                            "phases",
                            phase,
                            side,
                            "proc_io",
                            k,
                          ])}
                        </td>
                      ))}
                    </tr>
                  )),
                )}
              </tbody>
            </table>
          </div>
          <h3>{tr("GNU原整进程统计 · 秒 / RSS KiB / 512B输入输出计数")}</h3>
          <div className="source-table">
            <table>
              <thead>
                <tr>
                  <th>{tr("原进程")}</th>
                  <th>{tr("旧/新")}</th>
                  {[
                    "wall_seconds",
                    "user_seconds",
                    "system_seconds",
                    "max_rss_kib",
                    "file_system_inputs_512b",
                    "file_system_outputs_512b",
                    "exit_code",
                  ].map((k) => (
                    <th key={k}>{k}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {["material_trial", "two_market_research"].flatMap((proc) =>
                  ["old", "new"].map((side) => (
                    <tr key={proc + side}>
                      <td>{proc}</td>
                      <td>{side}</td>
                      {[
                        "wall_seconds",
                        "user_seconds",
                        "system_seconds",
                        "max_rss_kib",
                        "file_system_inputs_512b",
                        "file_system_outputs_512b",
                        "exit_code",
                      ].map((k) => (
                        <td key={k}>
                          {measurementField([
                            "execution",
                            "gnu_processes",
                            proc,
                            side,
                            k,
                          ])}
                        </td>
                      ))}
                    </tr>
                  )),
                )}
              </tbody>
            </table>
          </div>
          <div className="research-controls">
            {[
              "/execution/timeline",
              "/input_identity",
              "/configuration",
              "/business",
              "/comparison_contract",
              "/public_projection_omissions",
            ].map((p) => (
              <button
                key={p}
                onClick={() => {
                  setDocument("measurement");
                  setPath(p);
                  void state.load("measurement", p, "0", count);
                }}
              >
                {tr("读取")}
                {p}
              </button>
            ))}
            <button
              onClick={() => {
                setDocument("measurement");
                setPath("/complete_result_comparison");
                void state.load(
                  "measurement",
                  "/complete_result_comparison",
                  "0",
                  count,
                );
              }}
            >
              {tr("浏览完整比较与排除规则")}
            </button>
          </div>
        </>
      )}
      {status.kind === "collection_manifest" && (
        <>
          <h3>
            {tr("来源原统计 · ")}
            {counterEntries.length}
            {tr(" 类（已读 source_counters）")}
          </h3>
          <div className="source-table">
            <table>
              <thead>
                <tr>
                  <th>{tr("原来源")}</th>
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
                  <th>{tr("原详情")}</th>
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
                        {tr("查原值")}
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
              label={tr("原工程基线提交")}
              source="existing"
              path="/baseline"
              report={
                status.documents?.find((d) => d.document === "provenance")
                  ?.summary
              }
              onSelect={(e) => select("provenance", e.path, e.value)}
            />
          </div>
          <h3>{tr("原摘要对照 · ms / 原样本数")}</h3>
          <div className="research-controls">
            <button onClick={() => loadSummary("summary")}>
              {tr("浏览单轮摘要")}
            </button>
            <button onClick={() => loadSummary("pooled")}>
              {tr("浏览两轮合并摘要")}
            </button>
          </div>
          {currentSummary && (
            <>
              <label>
                {tr("原摘要记录")}
                <select
                  aria-label={tr("原摘要记录")}
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
                      {tr("· 原")}
                      {r.path}
                    </option>
                  ))}
                </select>
              </label>
              <p>
                {pooled ? tr("两轮合并原统计") : tr("单轮原统计")} ·{" "}
                {state.loaded?.source}
                {tr(" · 原")}
                {currentSummary.path}
                {tr("；所有量均来自该原记录，不计算或平均分位数。")}
              </p>
              <div className="source-table">
                <table>
                  <thead>
                    <tr>
                      <th>{tr("原指标 · ms")}</th>
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
                    <span>
                      {k}
                      {tr(" · 原值")}
                    </span>
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
                {tr("选择此原摘要")}
              </button>
            </>
          )}
        </>
      )}
      {completion && original && (
        <>
          <div className="fact-grid">
            {[
              [tr("平台确认现金 · pUSD"), "cash_pusd"],
              [tr("平台确认可用额 · pUSD"), "available_pusd"],
              [tr("平台确认费用 · pUSD"), "fees_pusd"],
              [tr("平台确认已实现 PnL · pUSD"), "realized_pnl_pusd"],
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
            <summary>{tr("展开最终 Ledger 执行与预留投影")}</summary>
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
            {d.document} · {d.source ?? tr("未指定")} · {d.status}
          </summary>
          {d.detail && <p role="alert">{displayError(d.detail)}</p>}
          <p className="muted">
            {tr("读取 ")}
            {d.read_attempts ?? tr("未测")}
            {tr(" 次 / 解码")} {d.decode_attempts ?? tr("未测")}
            {tr(" 次 ·")}{" "}
            {d.bytes_read === null
              ? tr("原入口未返回读取字节")
              : `${d.bytes_read ?? tr("未测")} B`}{" "}
            {d.file_bytes
              ? tr("· 文件长度 {{v0}} B（非 I/O）", { v0: d.file_bytes })
              : ""}
            {tr(" · 原加载+解释 ")}
            {d.load_elapsed_us ?? tr("未测")} μs
          </p>
          {d.summary !== undefined && (
            <Tree
              name={tr("原摘要字段")}
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
          {tr("原文档")}
          <select
            aria-label={tr("工程历史文档")}
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
          {tr("原字段路径")}
          <input
            aria-label={tr("工程历史路径")}
            value={path}
            onChange={(e) => setPath(e.target.value)}
            placeholder={tr("例如 /files；根数组留空")}
          />
        </label>
        <label>
          {tr("原数组起点")}
          <input
            aria-label={tr("工程历史起点")}
            value={start}
            onChange={(e) => setStart(e.target.value)}
          />
        </label>
        <label>
          {tr("区段条数")}
          <input
            aria-label={tr("工程历史条数")}
            value={count}
            onChange={(e) => setCount(e.target.value)}
          />
        </label>
        <button
          disabled={state.loading || !document}
          onClick={() => void state.load(document, path, start, count)}
        >
          {tr("读取原区段")}
        </button>
      </div>
      {state.loading && <p role="status">{tr("从已载入文档选择原区段…")}</p>}
      {state.error && <p role="alert">{displayError(state.error)}</p>}
      {state.loaded && (
        <div data-existing-loaded={state.loaded.document}>
          <h3>
            {state.loaded.source} · {state.loaded.path || "/"}{" "}
            {state.loaded.total !== undefined
              ? tr("原区段 [{{v0}}, {{v1}}) / {{v2}}", {
                  v0: state.loaded.start,
                  v1: state.loaded.end,
                  v2: state.loaded.total,
                })
              : tr("原字段")}
          </h3>
          {state.loaded.exact !== undefined && (
            <Tree
              name={tr("原字段详情")}
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
                  {tr("选择原记录 ")}
                  {r.path}{" "}
                  {r.csv_line ? tr("· CSV行{{v0}}", { v0: r.csv_line }) : ""}
                </button>
                <Tree
                  name={tr("原记录字段")}
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
