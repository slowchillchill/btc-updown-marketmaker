import { useEffect, useMemo, useRef, useState } from "react";
import { type Evidence, type Row } from "./browse";
import type { Page } from "./evidence";
import {
  emptyQuery,
  filterBatch,
  positionDomains,
  queryValid,
  type PublicQuery,
} from "./publicQuery";

type Result = {
  matching_reused?: boolean;
  matching?: {
    matched_offsets: string[];
    scanned_rows: string;
    unknown_rows: string;
    mismatch_rows: string;
    query_elapsed_us: string;
  };
  batch?: Page;
  query: PublicQuery & { page: string | null };
  cursor: {
    current_page: string | null;
    next_page: string;
    total_returned_rows: string;
    at_end: boolean;
    failed: boolean;
  };
  detail?: string;
};
type Expected = { run: string; page?: string; query: PublicQuery };
export function usePublicBrowse({
  run,
  page,
  allRows,
  onPage,
  condition,
  ready,
  onSelect,
}: {
  run: string;
  page: Page | null;
  allRows: Row[];
  onPage: (page: Page) => void;
  condition: string;
  ready: boolean;
  onSelect: (e: Evidence) => void;
}) {
  const [fields, setFields] = useState(emptyQuery),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [notice, setNotice] = useState(""),
    [echo, setEcho] = useState<Result | null>(null),
    [retry, setRetry] = useState<Expected | null>(null),
    [progress, setProgress] = useState({
      stage: "尚未查找",
      scanned: 0n,
      matched: 0n,
      unknown: 0n,
      elapsed: 0,
    }),
    [playing, setPlaying] = useState(false),
    [speed, setSpeed] = useState("2"),
    [playIndex, setPlayIndex] = useState(-1),
    [playFact, setPlayFact] = useState<Row | null>(null);
  const query = useMemo(() => ({ ...fields, condition }), [fields, condition]);
  const valid = queryValid(query);
  const filtered = useMemo(() => filterBatch(allRows, query), [allRows, query]);
  const currentRun = useRef(run),
    stopped = useRef(false),
    inFlight = useRef(false),
    started = useRef(0);
  currentRun.current = run;
  useEffect(() => {
    stopped.current = true;
    setFields(emptyQuery);
    setBusy(inFlight.current);
    setError("");
    setNotice("");
    setRetry(null);
    setEcho(null);
    setProgress({
      stage: "尚未查找",
      scanned: 0n,
      matched: 0n,
      unknown: 0n,
      elapsed: 0,
    });
    setPlaying(false);
    setPlayIndex(-1);
    setPlayFact(null);
  }, [run]);
  useEffect(() => {
    stopped.current = true;
    setPlaying(false);
    setPlayIndex(-1);
    setPlayFact(null);
  }, [query]);
  useEffect(() => {
    setPlaying(false);
    setPlayIndex(-1);
    setPlayFact(null);
  }, [page]);
  useEffect(() => {
    if (!busy) return;
    const timer = setInterval(
      () =>
        setProgress((p) => ({
          ...p,
          elapsed: performance.now() - started.current,
        })),
      250,
    );
    return () => clearInterval(timer);
  }, [busy]);
  useEffect(() => {
    if (!playing) return;
    const timer = setTimeout(
      () => {
        const next = playIndex + 1;
        if (next >= filtered.rows.length) {
          setPlaying(false);
          return;
        }
        const fact = filtered.rows[next];
        setPlayIndex(next);
        setPlayFact(fact);
        onSelect(fact);
        if (next === filtered.rows.length - 1) setPlaying(false);
      },
      1000 / Number(speed),
    );
    return () => clearTimeout(timer);
  }, [playing, playIndex, speed, filtered.rows, onSelect]);
  async function read(expected: Expected, search: boolean) {
    if (inFlight.current) return;
    inFlight.current = true;
    stopped.current = false;
    started.current = performance.now();
    setBusy(true);
    setError("");
    setNotice("");
    setPlaying(false);
    let scanned = 0n,
      matched = 0n,
      unknown = 0n,
      wanted = expected,
      previous = page;
    setProgress({
      stage: search ? "查找中" : "读取当前批",
      scanned,
      matched,
      unknown,
      elapsed: 0,
    });
    try {
      for (;;) {
        setRetry(wanted);
        const params = new URLSearchParams({
          ...wanted.query,
          ...(wanted.page === undefined ? {} : { page: wanted.page }),
        });
        const response = await fetch(
            `/api/runs/${wanted.run}/public-query?${params}`,
          ),
          d = (await response.json()) as Result;
        if (currentRun.current !== wanted.run) return;
        setEcho(d);
        if (!response.ok)
          throw new Error(
            `${d.detail ?? `HTTP ${response.status}`}；期望页 ${wanted.page ?? "当前"}，服务端最近页 ${d.cursor?.current_page ?? "无"}，下一页 ${d.cursor?.next_page ?? "未知"}。请显式取回当前批核对覆盖。`,
          );
        if (!d.batch) throw new Error("响应没有原批");
        const batch = d.batch;
        if (
          wanted.page === undefined &&
          ((previous &&
            batch.page !== previous.page &&
            BigInt(batch.page) !== BigInt(previous.page) + 1n) ||
            (!previous && batch.page !== "0"))
        )
          setNotice(
            `服务端覆盖已改变：取回页 ${batch.page}；此前中间页未在此窗口保留。`,
          );
        onPage(batch);
        previous = batch;
        setRetry(null);
        if (!d.matching) throw new Error("响应没有服务端本批匹配结果");
        const matching = d.matching;
        scanned += BigInt(matching.scanned_rows);
        matched += BigInt(matching.matched_offsets.length);
        unknown += BigInt(matching.unknown_rows);
        const done =
          !search ||
          matching.matched_offsets.length > 0 ||
          batch.at_end ||
          stopped.current;
        setProgress({
          stage: stopped.current
            ? "已取消后续读取 · 在途批已保留"
            : batch.at_end
              ? "稳定前缀末端"
              : matching.matched_offsets.length
                ? "已找到匹配批"
                : search
                  ? "空匹配批 · 继续查找"
                  : "本批已保留",
          scanned,
          matched,
          unknown,
          elapsed: performance.now() - started.current,
        });
        if (done) break;
        // 让进度与取消输入进入一次浏览帧；不加入业务时间或后台读取任务。
        await new Promise<void>((resolve) =>
          requestAnimationFrame(() => resolve()),
        );
        if (stopped.current) break;
        wanted = { ...wanted, page: (BigInt(batch.page) + 1n).toString() };
      }
    } catch (e) {
      if (currentRun.current === expected.run) {
        setError(String(e));
        setProgress((p) => ({
          ...p,
          stage: "读取未确认 · 可重试/取回",
          elapsed: performance.now() - started.current,
        }));
      }
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  }
  function field(name: keyof typeof emptyQuery, value: string) {
    stopped.current = true;
    setFields((f) => ({ ...f, [name]: value }));
  }
  const clocks = [
    "源消息 · 来源事件",
    "源消息 · 接收",
    "请求时钟",
    "HTTP 接收",
    "锚价源计算",
    "采集接纳",
    "历史补查接收",
    "历史来源事件",
    "历史身份就绪",
    "历史生效",
    "规范事件 · 来源事件",
    "规范事件 · 接收",
    "直接来源 · 来源事件",
    "直接来源 · 接收",
    "依赖来源 · 来源事件",
    "依赖来源 · 接收",
    "来源证据 · 来源事件",
    "来源证据 · 接收",
  ];
  const cursorFailed = !!echo?.cursor.failed;
  const next = () => ({
    run,
    page: page ? (BigInt(page.page) + 1n).toString() : undefined,
    query,
  });
  const controls = (
    <div className="public-browse">
      <div className="range-controls public-query-fields">
        <label>
          原记录类型
          <select
            aria-label="公共记录类型"
            value={fields.kind}
            onChange={(e) => field("kind", e.target.value)}
          >
            <option value="">全部原类型</option>
            {[
              "Input",
              "Canonical",
              "Report",
              "PmReport",
              "PmStepBinding",
              "Group",
              "Observation",
              "SourceBinding",
            ].map((k) => (
              <option key={k}>{k}</option>
            ))}
          </select>
        </label>
        <label>
          位置域
          <select
            aria-label="公共位置域"
            value={fields.position_domain}
            onChange={(e) => field("position_domain", e.target.value)}
          >
            {positionDomains.map(([v, t]) => (
              <option key={v} value={v}>
                {t}
              </option>
            ))}
          </select>
        </label>
        <label>
          位置起点（含）
          <input
            aria-label="位置起点"
            value={fields.position_start}
            onChange={(e) => field("position_start", e.target.value)}
            placeholder="不限"
          />
        </label>
        <label>
          位置终点（含）
          <input
            aria-label="位置终点"
            value={fields.position_end}
            onChange={(e) => field("position_end", e.target.value)}
            placeholder="不限"
          />
        </label>
        <label>
          原时钟
          <select
            aria-label="公共原时钟"
            value={fields.clock}
            onChange={(e) => field("clock", e.target.value)}
          >
            <option value="">不限定时钟</option>
            {clocks.map((c) => (
              <option key={c}>{c}</option>
            ))}
          </select>
        </label>
        <label>
          原 UTC ms 起点（含）
          <input
            aria-label="公共时间起点"
            value={fields.time_start}
            onChange={(e) => field("time_start", e.target.value)}
            placeholder="不限"
          />
        </label>
        <label>
          原 UTC ms 终点（含）
          <input
            aria-label="公共时间终点"
            value={fields.time_end}
            onChange={(e) => field("time_end", e.target.value)}
            placeholder="不限"
          />
        </label>
        <button onClick={() => setFields(emptyQuery)}>清除公共条件</button>
      </div>
      {!valid && (
        <p role="alert">
          范围需为整数、起点不大于终点；设置端点时须选择对应位置域或原时钟。
        </p>
      )}
      <p className="muted">
        当前保留批客户端筛选；下一匹配由服务器判断 · 市场 {condition || "不限"}{" "}
        · 原序保留 · 不减少原批 HTTP 响应量 · 端点均包含。
      </p>
      <div className="page-controls">
        <button
          disabled={busy || !ready || !!page?.at_end || cursorFailed || !valid}
          onClick={() => read(next(), false)}
        >
          {busy ? "读取中…" : page ? "下一页" : "读取当前页"}
        </button>
        <button
          disabled={busy || !ready || !!page?.at_end || cursorFailed || !valid}
          onClick={() => read(next(), true)}
        >
          从当前游标查找下一批匹配
        </button>
        <button
          disabled={busy || !ready || !valid}
          onClick={() => read({ run, query }, false)}
        >
          取回服务端当前页
        </button>
        {busy && (
          <button
            onClick={() => {
              stopped.current = true;
              setProgress((p) => ({
                ...p,
                stage: "已请求取消 · 等待在途批保存",
              }));
            }}
          >
            取消后续查找
          </button>
        )}
        {retry && !busy && (
          <button onClick={() => read(retry, false)}>重试最近期望批</button>
        )}
        <span>
          {page
            ? `页 ${page.page} · 本页 ${page.returned_rows} 条 · 已顺序读至 ${page.total_returned_rows}${page.at_end ? " · 稳定前缀末端" : ""}`
            : "公共记录尚未读取"}
        </span>
      </div>
      <p role="status" className="query-progress">
        {progress.stage} · 本次扫描 {progress.scanned.toString()} · 匹配{" "}
        {progress.matched.toString()} · 无法判断 {progress.unknown.toString()} ·
        耗时 {progress.elapsed.toFixed(0)} ms · 查找总量未知
      </p>
      {cursorFailed && (
        <p role="alert">
          所属读取接口已失败并停止游标；可取回最近成功批，不能继续推进。
        </p>
      )}
      <p className="batch-summary">
        当前保留原批 {allRows.length} 条 · 当前条件匹配 {filtered.rows.length} ·
        已知不匹配 {filtered.mismatch} · 缺字段无法判断 {filtered.unknown}
      </p>
      {notice && <p role="alert">{notice}</p>}
      {error && <p role="alert">{error}</p>}
      <p className="muted">
        取消不撤销同步底层读取；网络中断不证明已取消。在途批完成后保留，恢复仅取回最近批。修改条件或切换视图不自动扫描。
      </p>
      {echo && (
        <details>
          <summary>服务器回显 · 条件 / 固定前缀 / 当前游标</summary>
          <pre>
            {JSON.stringify(
              {
                query: echo.query,
                cursor: echo.cursor,
                matching: echo.matching,
                matching_reused: echo.matching_reused,
              },
              null,
              2,
            )}
          </pre>
        </details>
      )}
      <div className="page-controls history-playback">
        <b>历史浏览 · 当前匹配事实</b>
        <label>
          浏览速度
          <select
            aria-label="浏览记录每秒"
            value={speed}
            onChange={(e) => setSpeed(e.target.value)}
          >
            {["1", "2", "5", "10"].map((s) => (
              <option key={s} value={s}>
                {s} 记录/秒
              </option>
            ))}
          </select>
        </label>
        <button
          disabled={!filtered.rows.length || busy}
          onClick={() => {
            if (playIndex >= filtered.rows.length - 1) setPlayIndex(-1);
            setPlaying(true);
          }}
        >
          播放已读事实
        </button>
        <button disabled={!playing} onClick={() => setPlaying(false)}>
          暂停浏览
        </button>
        <span>
          {playing
            ? "播放中"
            : playIndex === filtered.rows.length - 1 && playIndex >= 0
              ? "已到本范围末尾"
              : "已暂停"}{" "}
          · {Math.max(0, playIndex + 1)}/{filtered.rows.length} ·
          不取页、不驱动业务
        </span>
        {playFact && (
          <code data-playback-path={playFact.path}>{playFact.path}</code>
        )}
      </div>
    </div>
  );
  return { controls, rows: filtered.rows, query, playFact, playing };
}
