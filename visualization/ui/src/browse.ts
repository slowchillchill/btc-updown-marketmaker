import Decimal from "decimal.js";
import { stringify } from "lossless-json";
import { at, type Json } from "./facts";
import type { Page } from "./evidence";
// 展示引用与泳道标签；原领域记录仍是完整 rows，不建立事件归约或领域主键。
export type Evidence = {
  source: "report" | "metadata" | "public";
  path: string;
  value: Json;
  condition?: string;
};
export type Point = Evidence & { name: string; clock: string; utcMs: string };
export type Row = Evidence & {
  kind: string;
  identity: string;
  points: Point[];
};
export function text(v: Json | undefined): string | undefined {
  return typeof v === "string" ? v : undefined;
}
export function list(v: Json | undefined): Json[] {
  return Array.isArray(v) ? v : [];
}
export function cashTotal(
  a: Json | undefined,
  b: Json | undefined,
): string | undefined {
  if (typeof a !== "string" || typeof b !== "string") return undefined;
  // ExactDecimalV1 的普通十进制文本；精度覆盖两项全部位数及加法进位。
  if (!/^-?\d+(\.\d+)?$/.test(a) || !/^-?\d+(\.\d+)?$/.test(b))
    return undefined;
  const D = Decimal.clone({ precision: a.length + b.length + 2 });
  return new D(a).plus(b).toFixed();
}
export function assetsByOutcome(
  market: Json | undefined,
  label: string,
): number | undefined {
  const matches = list(at(market, "assets")).flatMap((asset, i) =>
    at(asset, "outcome", "label") === label ? [i] : [],
  );
  return matches.length === 1 ? matches[0] : undefined;
}
function point(
  source: Evidence["source"],
  path: string,
  value: Json | undefined,
  name: string,
  clock: string,
  condition?: string,
): Point[] {
  return typeof value === "string" && /^-?\d+$/.test(value)
    ? [{ source, path, value, name, clock, utcMs: value, condition }]
    : [];
}
export function reportPoints(report: Json | undefined): Point[] {
  const out: Point[] = [];
  out.push(
    ...point(
      "report",
      "/last_observed_at_utc_ms",
      at(report, "last_observed_at_utc_ms"),
      "报告观察截止",
      "观察截止",
    ),
  );
  for (const group of [
    "last_dispatched_calculation_timing",
    "pending_calculation_timing",
  ]) {
    for (const [field, name, clock] of [
      ["observation_at_utc_ms", "计算观察", "观察截止"],
      ["calculation_started_at_utc_ms", "计算开始", "计算时钟"],
      ["calculation_completed_at_utc_ms", "计算完成", "计算时钟"],
      ["scheduled_send_at_utc_ms", "计划发送", "调度时钟"],
      ["actual_sent_at_utc_ms", "实际发送", "提交时钟"],
    ])
      out.push(
        ...point(
          "report",
          `/${group}/${field}`,
          at(report, group, field),
          `${group === "pending_calculation_timing" ? "待计算" : "最近分派"} · ${name}`,
          clock,
        ),
      );
  }
  for (const [branch, name] of [
    ["decision_time", "决定"],
    ["receipt/execution_time", "执行观察"],
  ]) {
    const path = "/observation_cutoff/" + branch + "/logical_time_utc_ms";
    out.push(
      ...point(
        "report",
        path,
        at(
          report,
          "observation_cutoff",
          ...branch.split("/"),
          "logical_time_utc_ms",
        ),
        name,
        "运行逻辑时钟",
      ),
    );
  }
  out.push(
    ...point(
      "report",
      "/ledger_consumed_memory/available_through_utc_ms",
      at(report, "ledger_consumed_memory", "available_through_utc_ms"),
      "账本可读截止",
      "账本截止",
    ),
  );
  return out;
}
export function publicRows(page: Page | null): Row[] {
  if (!page) return [];
  return page.rows.map((row, i) => {
    const kind = Object.keys(row ?? {})[0] ?? "未知记录";
    const v = at(row, kind);
    const path = `/public/${page.row_start}/${i}`;
    const points: Point[] = [];
    let condition: string | undefined;
    let identity =
      text(at(v, "input_stream_id")) ??
      text(at(v, "binding", "event_id")) ??
      text(at(v, "group_id")) ??
      "";
    if (kind === "Input") {
      const ev = at(v, "ingress", "Message", "source_evidence");
      const native = at(v, "ingress", "Message", "row", "Existing");
      condition = text(
        at(native, Object.keys(native ?? {})[0] ?? "", "condition_id"),
      );
      const base = path + "/Input/ingress/Message/source_evidence";
      identity = text(at(ev, "source_message_id")) ?? identity;
      points.push(
        ...point(
          "public",
          base + "/received_at_utc_ms",
          at(ev, "received_at_utc_ms"),
          "源消息接收",
          "来源接收",
          condition,
        ),
      );
      points.push(
        ...point(
          "public",
          base + "/source_event_at_utc_ms",
          at(ev, "source_event_at_utc_ms"),
          "平台原事件",
          "来源事件",
          condition,
        ),
      );
    } else if (kind === "Canonical") {
      const record = at(v, "record");
      const recordKind = Object.keys(record ?? {})[0] ?? "";
      const native = at(record, recordKind);
      condition = text(at(native, "market", "condition_id"));
      for (const field of ["received_at_utc_ms", "source_event_at_utc_ms"]) {
        points.push(
          ...point(
            "public",
            `${path}/Canonical/record/${recordKind}/direct_time/${field}`,
            at(native, "direct_time", field),
            `归约事实 · ${field}`,
            field === "received_at_utc_ms" ? "来源接收" : "来源事件",
            condition,
          ),
        );
      }
    } else if (kind === "Observation") {
      condition = text(at(v, "market", "condition_id"));
      identity = [
        text(at(v, "visible_prefix", "input_stream_id")),
        text(at(v, "observation_position")),
      ]
        .filter(Boolean)
        .join(" / ");
    } else if (kind === "Report")
      identity = text(at(v, "report", "position", "input_stream_id")) ?? "";
    return {
      source: "public",
      path,
      value: row,
      kind,
      condition,
      identity,
      points,
    };
  });
}
export function rangePoints(
  points: Point[],
  start: string,
  end: string,
  condition: string,
): Point[] {
  return points.filter(
    (p) =>
      (!condition || p.condition === condition) &&
      (!start || BigInt(p.utcMs) >= BigInt(start)) &&
      (!end || BigInt(p.utcMs) <= BigInt(end)),
  );
}
export function traceJson(
  points: Point[],
  context: Record<string, unknown>,
): string {
  const clocks = [...new Set(points.map((p) => p.clock))];
  return stringify(
    {
      displayTimeUnit: "ms",
      metadata: {
        ...context,
        derived: true,
        generation:
          "可证实时间点映射为 Chrome Trace instant；原 UTC ms 精确保留；不补因果或持续时间",
      },
      traceEvents: [
        ...clocks.map((clock, i) => ({
          ph: "M",
          pid: 1,
          tid: i + 1,
          name: "thread_name",
          args: { name: clock },
        })),
        ...points.map((p) => ({
          ph: "i",
          s: "t",
          pid: 1,
          tid: clocks.indexOf(p.clock) + 1,
          ts: BigInt(p.utcMs) * 1000n,
          name: p.name,
          cat: p.clock,
          args: {
            original_utc_ms: p.utcMs,
            clock_kind: p.clock,
            source: p.source,
            reference: p.path,
            condition_id: p.condition ?? null,
            derived: true,
            ...context,
          },
        })),
      ],
    },
    null,
    2,
  )!;
}
