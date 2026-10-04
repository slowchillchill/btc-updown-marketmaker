import { at } from "./facts";
import { text, type Row } from "./browse";

export const positionDomains = [
  ["", "不限定位置"],
  ["storage_row", "固定前缀存储行偏移"],
  ["input_position", "原 input_position"],
  ["canonical_position", "原 canonical_position"],
  ["group_position", "原 group_position"],
  ["observation_position", "原 observation_position"],
] as const;
export type PublicQuery = {
  kind: string;
  condition: string;
  position_domain: string;
  position_start: string;
  position_end: string;
  clock: string;
  time_start: string;
  time_end: string;
};
export const emptyQuery: Omit<PublicQuery, "condition"> = {
  kind: "",
  position_domain: "",
  position_start: "",
  position_end: "",
  clock: "",
  time_start: "",
  time_end: "",
};
export function queryValid(q: PublicQuery): boolean {
  const valid = (start: string, end: string) =>
    (!start || /^-?\d+$/.test(start)) &&
    (!end || /^-?\d+$/.test(end)) &&
    (!start || !end || BigInt(start) <= BigInt(end));
  return (
    valid(q.position_start, q.position_end) &&
    valid(q.time_start, q.time_end) &&
    (!(q.position_start || q.position_end) || !!q.position_domain) &&
    (!(q.time_start || q.time_end) || !!q.clock)
  );
}
function inRange(value: string, start: string, end: string): boolean {
  return (
    (!start || BigInt(value) >= BigInt(start)) &&
    (!end || BigInt(value) <= BigInt(end))
  );
}
function position(row: Row, domain: string): string | undefined {
  if (domain === "storage_row") {
    const match = /^\/public\/(\d+)\/(\d+)$/.exec(row.path);
    return match ? (BigInt(match[1]) + BigInt(match[2])).toString() : undefined;
  }
  const own = text(at(row.value, row.kind, domain));
  if (own !== undefined) return own;
  // PmStepBinding 明确保存 canonical_record_position，未借用其他位置域。
  return domain === "canonical_position" && row.kind === "PmStepBinding"
    ? text(at(row.value, row.kind, "position", "canonical_record_position"))
    : undefined;
}
export function classify(
  row: Row,
  q: PublicQuery,
): "match" | "mismatch" | "unknown" {
  const tests: (boolean | undefined)[] = [];
  if (q.kind) tests.push(row.kind === q.kind);
  if (q.condition)
    tests.push(
      row.markets?.length
        ? row.markets.some((m) => m.condition === q.condition)
        : undefined,
    );
  if (q.position_domain) {
    const value = position(row, q.position_domain);
    tests.push(
      value === undefined
        ? undefined
        : inRange(value, q.position_start, q.position_end),
    );
  }
  if (q.clock) {
    const times = row.points.filter(
      (p) =>
        p.clock === q.clock && (!q.condition || p.condition === q.condition),
    );
    tests.push(
      times.length
        ? times.some((p) => inRange(p.utcMs, q.time_start, q.time_end))
        : undefined,
    );
  }
  // 已知不匹配优先；只有其余条件均可能满足、却缺字段时才无法判断。
  return tests.includes(false)
    ? "mismatch"
    : tests.includes(undefined)
      ? "unknown"
      : "match";
}
export function filterBatch(rows: Row[], q: PublicQuery) {
  const result = { rows: [] as Row[], mismatch: 0, unknown: 0 };
  if (!queryValid(q)) return result;
  for (const row of rows) {
    const state = classify(row, q);
    if (state === "match")
      result.rows.push(row); // 保留原对象与原偏移，不重新编号。
    else result[state]++;
  }
  return result;
}
