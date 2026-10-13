import { displayError } from "./i18n";
import { displayFact as fact } from "./i18n";
import { useTranslation } from "react-i18next";
import { useEffect, useRef, useState } from "react";
import { at, statusLabel, type Json } from "./facts";
import { get, Tree } from "./evidence";
import { type Evidence } from "./browse";
export type ResearchFragment = {
  path: string;
  exact?: Json;
  rows?: { path: string; exact: Json }[];
  omitted_arrays: { path: string; length: string }[];
  start?: string;
  end?: string;
  total?: string;
};
export type ResearchStatus = {
  status: string;
  detail?: string;
  source?: string;
  summary?: ResearchFragment[];
  catalog?: Json[] | null;
  targets?: Json[] | null;
  global_update_count?: string | null;
  bytes_read?: string;
  load_elapsed_us?: string;
  read_attempts?: string;
  decode_attempts?: string;
};
export function useResearch(run: string) {
  const [loaded, setLoaded] = useState<ResearchFragment | null>(null),
    [busy, setBusy] = useState(false),
    [error, setError] = useState("");
  const [market, setMarket] = useState("0"),
    [freeze, setFreeze] = useState("0"),
    [window, setWindow] = useState("0"),
    [offset, setOffset] = useState("0"),
    [count, setCount] = useState("20");
  const epoch = useRef(0),
    activeRun = useRef(run);
  activeRun.current = run;
  useEffect(() => {
    epoch.current++;
    setLoaded(null);
    setBusy(false);
    setError("");
    setMarket("0");
    setFreeze("0");
    setWindow("0");
    setOffset("0");
  }, [run]);
  async function load(path: string, start?: string, count?: string) {
    const request = ++epoch.current,
      owner = run;
    setBusy(true);
    setError("");
    try {
      const q = new URLSearchParams({ path });
      if (start !== undefined) q.set("start", start);
      if (count !== undefined) q.set("count", count);
      const data = await get<ResearchFragment>(
        `/api/runs/${run}/research?${q}`,
      );
      if (request === epoch.current && owner === activeRun.current)
        setLoaded(data);
    } catch (e) {
      if (request === epoch.current && owner === activeRun.current) {
        setError(String(e));
        setLoaded(null);
      }
    } finally {
      if (request === epoch.current && owner === activeRun.current)
        setBusy(false);
    }
  }
  return {
    loaded,
    busy,
    error,
    load,
    market,
    setMarket,
    freeze,
    setFreeze,
    window,
    setWindow,
    offset,
    setOffset,
    count,
    setCount,
  };
}
export function ResearchPanel({
  status,
  state,
  onSelect,
}: {
  status: ResearchStatus;
  state: ReturnType<typeof useResearch>;
  onSelect: (e: Evidence) => void;
}) {
  const { t: tr } = useTranslation();
  const {
    market,
    setMarket,
    freeze,
    setFreeze,
    window,
    setWindow,
    offset,
    setOffset,
    count,
    setCount,
  } = state;
  const projection = status.summary?.find(
    (f) => f.path === "/projection",
  )?.exact;
  const pairColumns = at(projection, "price_pair_update_indices_columns"),
    holdColumns = at(projection, "hold_segment_rows_columns");
  const target = status.targets?.[Number(market)],
    base = `/research/markets/${market}`,
    frozen = `${base}/frozen_sampling/${freeze}`,
    w = `${frozen}/descriptive_windows/${window}`;
  const select = (path: string, value: Json) =>
    onSelect({ source: "research", path, value });
  const tree = (f: ResearchFragment) => (
    <div key={f.path} data-research-path={f.path}>
      <code>{f.path}</code>
      {f.exact !== undefined && (
        <Tree
          name={tr("原字段")}
          path={f.path}
          value={f.exact}
          onSelect={select}
        />
      )}
      {f.rows && (
        <>
          <p>
            {tr("原数组 [")}
            {f.start}, {f.end}) / {f.total}
            {tr(" · 原序与原索引")}
          </p>
          {f.rows.map((row) => (
            <div key={row.path}>
              <Tree
                name={row.path.split("/").at(-1)!}
                path={row.path}
                value={row.exact}
                onSelect={select}
              />
              {Array.isArray(row.exact) &&
                (/price_pair_update_indices$/.test(f.path) ||
                  /segment_rows$/.test(f.path)) && (
                  <div className="native-links">
                    {(f.path.endsWith("price_pair_update_indices")
                      ? row.exact.slice(0, 2)
                      : [row.exact[3]]
                    )
                      .filter((i) => typeof i === "string" && /^\d+$/.test(i))
                      .map((i, j) => (
                        <button
                          key={j}
                          onClick={() =>
                            state.load(
                              "/research/global_spot/updates_original_visible_order",
                              i as string,
                              "1",
                            )
                          }
                        >
                          {tr("查原全局 update index ")}
                          {fact(i)}
                        </button>
                      ))}
                  </div>
                )}
            </div>
          ))}
        </>
      )}
      {!!f.omitted_arrays.length && (
        <details>
          <summary>
            {tr("未读取的原数组 · ")}
            {f.omitted_arrays.length}
            {tr(" 项")}
          </summary>
          {f.omitted_arrays.map((a) => (
            <div key={a.path}>
              <code>{a.path}</code>
              <span>
                {" "}
                · {a.length}
                {tr(" 原项 ")}
              </span>
              <button onClick={() => state.load(a.path, offset, count)}>
                {tr("读取原区段")}
              </button>
            </div>
          ))}
        </details>
      )}
    </div>
  );
  return (
    <section className="research-panel" data-research-status={status.status}>
      <h2>{tr("真实历史描述研究 · 只读原产物")}</h2>
      <p className={status.status === "ready" ? "muted" : "gap"}>
        {status.source ?? tr("未指定")} ·{" "}
        {tr(statusLabel[status.status] ?? status.status)}{" "}
        {displayError(status.detail)}
      </p>
      {status.status === "ready" && (
        <>
          <p>
            {fact(status.catalog?.length.toString())}
            {tr(" 目录成员 /")} {fact(status.targets?.length.toString())}
            {tr(" 完整目标 ·")} {fact(status.global_update_count)}
            {tr(" 原全局 spot 状态更新（含初始化）")}
          </p>
          <p className="muted">
            {tr(
              "冻结采样不是策略触发。事后标签独立保存，不回填冻结 特征；窗口描述量不是 sigma、概率、官方 TWAP 或盈利结果。USD/BTC 与 USDT/BTC 保留原字段单位。",
            )}
          </p>
          <div className="timing-deltas">
            {[
              [tr("冻结采样点"), "/result_summary", "sampling_points"],
              [tr("描述窗口"), "/result_summary", "window_rows"],
              [tr("独立右边界"), "/result_summary", "right_boundary_points"],
              [tr("初始化组"), "/research/counts", "initialization/groups"],
              [
                tr("初始化观察"),
                "/research/counts",
                "initialization/observations",
              ],
              [tr("研究组"), "/research/counts", "research/groups"],
              [tr("研究观察"), "/research/counts", "research/observations"],
            ].map(([label, path, key]) => {
              const value = at(
                status.summary?.find((f) => f.path === path)?.exact,
                key,
              );
              return (
                <button
                  key={key}
                  disabled={value === undefined}
                  onClick={() =>
                    value !== undefined &&
                    select(path + "/" + key.replaceAll("/", "~1"), value)
                  }
                >
                  {label} · {fact(value)}
                </button>
              );
            })}
          </div>
          <details>
            <summary>
              {tr("摘要与原来源 · 单文档 ")}
              {status.bytes_read}
              {tr(" B · 读取")} {status.read_attempts}
              {tr(" 次 / 解码 ")}
              {status.decode_attempts}
              {tr(" 次 · 初始化 ")}
              {status.load_elapsed_us} μs
            </summary>
            {status.summary?.map(tree)}
          </details>
          <details>
            <summary>
              {tr("实际市场目录 · ")}
              {status.catalog?.length}
              {tr(" 成员（不等于完整目标数）")}
            </summary>
            {status.catalog?.map((m, i) => (
              <Tree
                key={i}
                name={tr("目录 {{v0}} · {{v1}}", {
                  v0: i,
                  v1: fact(at(m, "clob_market_id")),
                })}
                value={m}
                path={`/research/actual_market_catalog/${i}`}
                onSelect={select}
              />
            ))}
          </details>
          <div className="range-controls">
            <label>
              {tr("完整目标")}
              <select
                aria-label={tr("研究目标")}
                value={market}
                onChange={(e) => {
                  setMarket(e.target.value);
                  setFreeze("0");
                }}
              >
                {status.targets?.map((m, i) => (
                  <option key={i} value={i}>
                    {i} · {fact(at(m, "identity", "clob_market_id"))}
                  </option>
                ))}
              </select>
            </label>
            <label>
              {tr("原冻结索引")}
              <select
                aria-label={tr("研究冻结索引")}
                value={freeze}
                onChange={(e) => setFreeze(e.target.value)}
              >
                {Array.from(
                  { length: Number(at(target, "freeze_count") ?? 0) },
                  (_, i) => (
                    <option key={i} value={i}>
                      {i}
                    </option>
                  ),
                )}
              </select>
            </label>
            <label>
              {tr("描述窗")}
              <select
                aria-label={tr("研究窗口索引")}
                value={window}
                onChange={(e) => setWindow(e.target.value)}
              >
                {[15, 30, 60, 120, 300].map((seconds, i) => (
                  <option key={i} value={i}>
                    {seconds}
                    {tr(" s（原索引 ")}
                    {i}）
                  </option>
                ))}
              </select>
            </label>
            <button onClick={() => state.load(base)}>
              {tr("市场 / 事后标签")}
            </button>
            <button onClick={() => state.load(frozen)}>{tr("冻结特征")}</button>
            <button onClick={() => state.load(w)}>{tr("描述窗口")}</button>
            <button
              onClick={() =>
                state.load(base + "/right_boundary_hold_diagnostic")
              }
            >
              {tr("独立右边界")}
            </button>
          </div>
          <div className="range-controls">
            <label>
              {tr("原数组起点")}
              <input
                aria-label={tr("研究数组起点")}
                value={offset}
                onChange={(e) => setOffset(e.target.value)}
              />
            </label>
            <label>
              {tr("区段长度")}
              <input
                aria-label={tr("研究数组长度")}
                value={count}
                onChange={(e) => setCount(e.target.value)}
              />
            </label>
            <button
              onClick={() =>
                state.load(w + "/price_pair_update_indices", offset, count)
              }
            >
              {tr("价格对原索引列")}
            </button>
            <button
              onClick={() =>
                state.load(w + "/hold_coverage/segment_rows", offset, count)
              }
            >
              {tr("持价原区段列")}
            </button>
            <button
              onClick={() =>
                state.load(
                  "/research/global_spot/updates_original_visible_order",
                  offset,
                  count,
                )
              }
            >
              {tr("全局更新原区段")}
            </button>
            <button onClick={() => state.load("/measurement")}>
              {tr("源端资源测量（非 C 成本）")}
            </button>
            <button onClick={() => state.load("/storage_snapshot")}>
              {tr("源端存储快照")}
            </button>
            <button
              onClick={() => state.load("/first_two_market_finite_comparison")}
            >
              {tr("原有限对照")}
            </button>
          </div>
          <button onClick={() => state.load("/build_identity")}>
            {tr("原代码与构建身份")}
          </button>
          <p className="muted">
            {tr("price_pair_update_indices 原列：")}
            {fact(pairColumns)}
            {tr("；segment_rows 原列：")}
            {fact(holdColumns)}
            {tr(
              "。索引指原全局更新数组，不是 group_position；未公开 classification 等字段不能补算。详情只保留最近一份响应，所选事实独立保留。",
            )}
          </p>
          {state.busy && <p role="status">{tr("读取明确原字段区段…")}</p>}
          {state.error && <p role="alert">{displayError(state.error)}</p>}
          {state.loaded && tree(state.loaded)}
        </>
      )}
    </section>
  );
}
