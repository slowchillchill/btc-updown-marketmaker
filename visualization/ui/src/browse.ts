import Decimal from "decimal.js";
import { stringify } from "lossless-json";
import { at, type Json } from "./facts";
import type { Page } from "./evidence";
// 展示引用与泳道标签；原领域记录仍是完整 rows，不建立事件归约或领域主键。
export type Evidence = {
  source: "report" | "metadata" | "public" | "noe" | "research";
  path: string;
  value: Json;
  condition?: string;
  inputStreamId?: string;
  references?: PublicLink[];
  identities?: PublicIdentity[];
  markets?: PublicMarket[];
  points?: Point[];
  // NOE 原引用/上下文/身份；不混为公共关联键。
  noe?: Json;
  research?: Json;
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
// 浏览关联只索引当前页与明确保留的所选记录；键由原流和原位置组成，不归约业务。
export type PublicLink = {
  label: string;
  key: string;
  path: string;
  value: Json;
};
export type PublicIdentity = { label: string; key: string };
export type PublicMarket = { condition: string; path: string; value: Json };
function key(
  kind: string,
  ...parts: (string | undefined)[]
): string | undefined {
  return parts.every((p) => p !== undefined)
    ? JSON.stringify([kind, ...parts])
    : undefined;
}
function factKey(v: Json | undefined): string | undefined {
  return key(
    "fact",
    text(at(v, "input_stream_id")),
    text(at(v, "closed_at_input_position")),
    text(at(v, "fact_ordinal")),
  );
}
function inputKey(v: Json | undefined): string | undefined {
  return key(
    "input",
    text(at(v, "input_stream_id")),
    text(at(v, "input_position")),
  );
}
export function publicRows(page: Page | null): Row[] {
  if (!page) return [];
  const stream = text(at(page.prefix, "input_stream_id"));
  return page.rows.map((row, i) => {
    const kind = Object.keys(row ?? {})[0] ?? "未知记录";
    const v = at(row, kind);
    const path = `/public/${page.row_start}/${i}`;
    const base = path + "/" + kind;
    const points: Point[] = [],
      references: PublicLink[] = [],
      identities: PublicIdentity[] = [],
      markets: PublicMarket[] = [];
    const own = (label: string, k: string | undefined) => {
      if (k) identities.push({ label, key: k });
    };
    const link = (
      label: string,
      k: string | undefined,
      p: string,
      value: Json | undefined,
    ) => {
      if (k && value !== undefined)
        references.push({ label, key: k, path: p, value });
    };
    const market = (value: Json | undefined, p: string) => {
      const id =
        typeof value === "string" ? value : text(at(value, "condition_id"));
      if (id) markets.push({ condition: id, path: p, value: value! });
      return id;
    };
    const time = (
      value: Json | undefined,
      p: string,
      label: string,
      clock: string,
      condition?: string,
    ) => points.push(...point("public", p, value, label, clock, condition));
    const eventTime = (
      value: Json | undefined,
      p: string,
      label: string,
      condition?: string,
    ) => {
      time(
        at(value, "source_event_at_utc_ms"),
        p + "/source_event_at_utc_ms",
        label + " · 平台事件",
        label + " · 来源事件",
        condition,
      );
      time(
        at(value, "received_at_utc_ms"),
        p + "/received_at_utc_ms",
        label + " · 接收",
        label + " · 接收",
        condition,
      );
    };
    const inputRef = (value: Json | undefined, p: string) =>
      link("原输入位置", inputKey(value), p, value);
    const factRef = (value: Json | undefined, p: string) =>
      link("原规范事实位置", factKey(value), p, value);
    const sourceRef = (ev: Json | undefined, p: string, condition?: string) => {
      link(
        "来源消息",
        key("source", stream, text(at(ev, "source_message_id"))),
        p,
        ev,
      );
      eventTime(ev, p, "来源证据", condition);
    };
    const sourceRefs = (
      values: Json | undefined,
      p: string,
      condition?: string,
    ) => list(values).forEach((ev, j) => sourceRef(ev, p + "/" + j, condition));
    let identity = stream ?? "未交接 input_stream_id";
    if (kind === "Input") {
      own("输入", inputKey(v));
      identity = `${text(at(v, "input_stream_id")) ?? ""} / input_position ${text(at(v, "input_position")) ?? ""}`;
      const message = at(v, "ingress", "Message"),
        ev = at(message, "source_evidence"),
        raw = at(message, "row"),
        rp = base + "/ingress/Message/row";
      own(
        "来源消息",
        key(
          "source",
          text(at(v, "input_stream_id")),
          text(at(ev, "source_message_id")),
        ),
      );
      link(
        "来源产物",
        key("artifact", stream, text(at(ev, "source_message_id"))),
        base + "/ingress/Message/source_evidence",
        ev,
      );
      list(at(v, "dependency_record_refs")).forEach((ref, j) =>
        inputRef(ref, base + "/dependency_record_refs/" + j),
      );
      let native: Json | undefined,
        np = rp;
      if (at(raw, "Existing") !== undefined) {
        const wrapped = at(raw, "Existing"),
          variant = Object.keys(wrapped ?? {})[0] ?? "";
        native = at(wrapped, variant);
        np += `/Existing/${variant}`;
        if (variant === "PolymarketOrderbook")
          market(at(native, "market"), np + "/market");
      } else if (at(raw, "PolymarketMarketMetadataV6") !== undefined) {
        const wrapped = at(raw, "PolymarketMarketMetadataV6"),
          variant = Object.keys(wrapped ?? {})[0] ?? "";
        native = at(wrapped, variant);
        np += `/PolymarketMarketMetadataV6/${variant}`;
      }
      market(at(native, "condition_id"), np + "/condition_id");
      const condition = markets.length === 1 ? markets[0].condition : undefined;
      eventTime(
        ev,
        base + "/ingress/Message/source_evidence",
        "源消息",
        condition,
      );
      if (native) {
        for (const [field, label, clock] of [
          ["metadata_fetch_start_utc", "元数据请求开始", "请求时钟"],
          ["http_received_at_utc", "HTTP 响应接收", "HTTP 接收"],
          ["anchor_source_computed_at_utc", "锚价源计算", "锚价源计算"],
          ["collector_admission_time_utc_ms", "采集接纳", "采集接纳"],
        ])
          time(at(native, field), np + "/" + field, label, clock, condition);
      }
      const repair = at(raw, "HistoricalPriceAnchorRepairBatch"),
        hp = rp + "/HistoricalPriceAnchorRepairBatch";
      time(
        at(repair, "response", "received_at_utc_ms"),
        hp + "/response/received_at_utc_ms",
        "历史补查响应接收",
        "历史补查接收",
      );
      list(at(repair, "rows")).forEach((r, j) => {
        const p = hp + "/rows/" + j,
          c = market(at(r, "condition_id"), p + "/condition_id");
        for (const [field, label, clock] of [
          ["source_event_at_utc_ms", "历史锚价源事件", "历史来源事件"],
          ["response_received_at_utc_ms", "历史补查响应接收", "历史补查接收"],
          [
            "historical_identity_ready_at_utc_ms",
            "历史身份证据就绪",
            "历史身份就绪",
          ],
        ])
          time(at(r, field), p + "/" + field, label, clock, c);
      });
    } else if (kind === "Canonical") {
      own("规范事实", factKey(at(v, "binding", "position")));
      own(
        "规范记录位置",
        key("canonical", stream, text(at(v, "canonical_position"))),
      );
      own("规范事件", key("event", stream, text(at(v, "binding", "event_id"))));
      identity += ` / canonical_position ${text(at(v, "canonical_position")) ?? ""} / event_id ${text(at(v, "binding", "event_id")) ?? ""}`;
      const record = at(v, "record"),
        variant = Object.keys(record ?? {})[0] ?? "",
        native = at(record, variant),
        np = base + "/record/" + variant;
      let condition = market(at(native, "market"), np + "/market");
      if (variant === "ReferenceEvent")
        condition = market(
          at(native, "payload", "reference", "market"),
          np + "/payload/reference/market",
        );
      for (const field of ["time", "direct_time", "dependency_time"])
        eventTime(
          at(native, field),
          np + "/" + field,
          field === "time"
            ? "规范事件"
            : field === "direct_time"
              ? "直接来源"
              : "依赖来源",
          condition,
        );
      link(
        "原公共组身份",
        key("group-id", stream, text(at(native, "group_id"))),
        np + "/group_id",
        at(native, "group_id"),
      );
      inputRef(at(native, "input_record"), np + "/input_record");
      list(at(native, "source_refs")).forEach((ref, j) =>
        inputRef(ref, np + "/source_refs/" + j),
      );
      sourceRefs(
        at(native, "lineage", "source_evidence"),
        np + "/lineage/source_evidence",
        condition,
      );
      list(at(native, "direct_source_message_ids")).forEach((id, j) =>
        link(
          "直接来源消息",
          key("source", stream, text(id)),
          np + "/direct_source_message_ids/" + j,
          id,
        ),
      );
      const app = at(native, "payload", "reference", "anchor", "application"),
        ap = np + "/payload/reference/anchor/application";
      if (at(app, "kind") === "historical_repair") {
        time(
          at(app, "historical_identity_ready_at_utc_ms"),
          ap + "/historical_identity_ready_at_utc_ms",
          "历史身份证据就绪",
          "历史身份就绪",
          condition,
        );
        time(
          at(app, "effective_from_utc_ms"),
          ap + "/effective_from_utc_ms",
          "历史锚价生效",
          "历史生效",
          condition,
        );
        sourceRefs(
          at(app, "historical_identity_evidence"),
          ap + "/historical_identity_evidence",
          condition,
        );
      }
    } else if (kind === "Group") {
      own("公共组身份", key("group-id", stream, text(at(v, "group_id"))));
      own("公共组", key("group", stream, text(at(v, "group_position"))));
      identity += ` / group_position ${text(at(v, "group_position")) ?? ""} / group_id ${text(at(v, "group_id")) ?? ""}`;
      list(at(v, "member_input_positions")).forEach((pos, j) =>
        link(
          "成员输入",
          key("input", stream, text(pos)),
          base + "/member_input_positions/" + j,
          pos,
        ),
      );
      list(at(v, "records")).forEach((ref, j) =>
        factRef(ref, base + "/records/" + j),
      );
      list(at(v, "observation_positions")).forEach((pos, j) =>
        link(
          "原观察位置",
          key("observation", stream, text(pos)),
          base + "/observation_positions/" + j,
          pos,
        ),
      );
    } else if (kind === "Observation") {
      market(at(v, "market"), base + "/market");
      const originalStream = text(at(v, "visible_prefix", "input_stream_id"));
      own(
        "公共观察",
        key("observation", originalStream, text(at(v, "observation_position"))),
      );
      identity = `${originalStream ?? ""} / observation_position ${text(at(v, "observation_position")) ?? ""} / group_position ${text(at(v, "group_position")) ?? ""}`;
      link(
        "原公共组",
        key("group", originalStream, text(at(v, "group_position"))),
        base + "/group_position",
        at(v, "group_position"),
      );
      list(at(v, "group_events")).forEach((ref, j) =>
        factRef(ref, base + "/group_events/" + j),
      );
    } else if (kind === "SourceBinding") {
      own(
        "来源产物",
        key("artifact", stream, text(at(v, "source_message_id"))),
      );
      identity += ` / source_message_id ${text(at(v, "source_message_id")) ?? ""}`;
      link(
        "来源消息",
        key("source", stream, text(at(v, "source_message_id"))),
        base + "/source_message_id",
        at(v, "source_message_id"),
      );
    } else if (kind === "PmStepBinding" || kind === "PmReport") {
      if (kind === "PmStepBinding")
        own("派生步骤", key("derived", stream, text(at(v, "derived_id"))));
      else {
        link(
          "原规范事件",
          key("event", stream, text(at(v, "report", "canonical_fact_id"))),
          base + "/report/canonical_fact_id",
          at(v, "report", "canonical_fact_id"),
        );
        link(
          "原派生步骤",
          key(
            "derived",
            stream,
            text(at(v, "report", "disposition", "Produced", "derived_id")),
          ),
          base + "/report/disposition/Produced/derived_id",
          at(v, "report", "disposition", "Produced", "derived_id"),
        );
        for (const disposition of ["NoOutput", "Suppressed"])
          list(
            at(v, "report", "disposition", disposition, "operation_evidence"),
          ).forEach((e, j) =>
            sourceRef(
              at(e, "source_evidence"),
              base +
                "/report/disposition/" +
                disposition +
                "/operation_evidence/" +
                j +
                "/source_evidence",
            ),
          );
      }
      const position = at(v, "position"),
        canonical = text(
          kind === "PmStepBinding"
            ? at(position, "canonical_record_position")
            : at(v, "canonical_position"),
        );
      identity +=
        kind === "PmStepBinding"
          ? ` / consumer_stream_id ${text(at(position, "consumer_stream_id")) ?? ""} / derived_id ${text(at(v, "derived_id")) ?? ""} / ${JSON.stringify(position)}`
          : ` / canonical_position ${canonical ?? ""} / report_ordinal ${text(at(v, "report_ordinal")) ?? ""}`;
      link(
        "原规范记录",
        key("canonical", stream, canonical),
        base +
          (kind === "PmStepBinding"
            ? "/position/canonical_record_position"
            : "/canonical_position"),
        canonical,
      );
      if (kind === "PmReport")
        link(
          "原输入",
          key("input", stream, text(at(v, "input_position"))),
          base + "/input_position",
          at(v, "input_position"),
        );
    } else if (kind === "Report") {
      identity += ` / input_position ${text(at(v, "input_position")) ?? ""}`;
      link(
        "原输入",
        key("input", stream, text(at(v, "input_position"))),
        base + "/input_position",
        at(v, "input_position"),
      );
      const position = at(v, "report", "position");
      identity += " / report_position " + JSON.stringify(position);
      own(
        "归约报告",
        key(
          "report",
          text(at(position, "input_stream_id")),
          text(at(position, "closed_at_input_position")),
          text(at(position, "report_ordinal")),
        ),
      );
      link(
        "产出的规范事件",
        key(
          "event",
          stream,
          text(at(v, "report", "report", "disposition", "result_event_id")),
        ),
        base + "/report/report/disposition/result_event_id",
        at(v, "report", "report", "disposition", "result_event_id"),
      );
      list(
        at(v, "report", "report", "disposition", "operation_evidence"),
      ).forEach((e, j) =>
        sourceRef(
          at(e, "source_evidence"),
          base +
            "/report/report/disposition/operation_evidence/" +
            j +
            "/source_evidence",
        ),
      );
    }
    const conditions = [...new Set(markets.map((m) => m.condition))];
    const evidence = {
      source: "public" as const,
      path,
      value: row,
      condition: conditions.length === 1 ? conditions[0] : undefined,
      inputStreamId: stream,
      references,
      identities,
      markets,
    };
    // 每个绘图点仍带原记录身份与精确引用；它们不构成因果边。
    return {
      ...evidence,
      kind,
      identity,
      points: points.map((p) => ({
        ...p,
        inputStreamId: stream,
        references,
        identities,
      })),
    };
  });
}
export function relatedTargets(
  link: PublicLink,
  rows: Row[],
  selection: Evidence | null,
): Evidence[] {
  const retained =
    selection?.source === "public" &&
    /^\/public\/\d+\/\d+$/.test(selection.path) &&
    selection.identities
      ? [selection]
      : [];
  return [...rows, ...retained].filter(
    (r, i, all) =>
      all.findIndex((x) => x.path === r.path) === i &&
      r.identities?.some((id) => id.key === link.key),
  );
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
            input_stream_id: p.inputStreamId ?? null,
            native_identities: p.identities ?? [],
            native_references: p.references ?? [],
            noe_provenance: p.noe ?? null,
            research_provenance: p.research ?? null,
            ...context,
          },
        })),
      ],
    },
    null,
    2,
  )!;
}
