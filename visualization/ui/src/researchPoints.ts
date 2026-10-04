import { at, type Json } from "./facts";
import { text, type Point } from "./browse";
import type { ResearchStatus, ResearchFragment } from "./Research";
export function researchPoints(
  status: ResearchStatus | undefined,
  loaded: ResearchFragment | null,
): Point[] {
  if (status?.status !== "ready") return [];
  const summary = status.summary ?? [],
    out: Point[] = [];
  const sourceCommit = summary.find(
    (f) => f.path === "/build_identity/source_commit",
  )?.exact;
  const prefix = summary.find(
    (f) => f.path === "/research/normal_prefix",
  )?.exact;
  const fragments = [...summary, ...(loaded ? [loaded] : [])];
  const push = (
    value: Json,
    path: string,
    clock: string,
    name: string,
    own: Json,
    condition?: string,
  ) => {
    if (typeof value !== "string" || !/^\-?\d+$/.test(value)) return;
    out.push({
      source: "research",
      value,
      path,
      utcMs: value,
      clock,
      name,
      condition,
      inputStreamId: text(at(prefix, "input_stream_id")),
      research: {
        schema: "complementary_pair_first_fit_hour_budget_trial_v1",
        research_schema: "complementary_pair_first_fit_hour_v1",
        source_commit: sourceCommit ?? null,
        normal_prefix: prefix ?? null,
        original_record: own,
        reference: path,
      },
    });
  };
  const walk = (v: Json, path: string, condition?: string) => {
    if (v === null || typeof v !== "object") return;
    if (Array.isArray(v)) {
      v.forEach((r, i) => walk(r, `${path}/${i}`, condition));
      return;
    }
    const ownCondition = text(at(v, "condition_id")) ?? condition;
    for (const [k, a] of Object.entries(v)) {
      const p = path + "/" + k.replaceAll("~", "~0").replaceAll("/", "~1");
      if (k === "source_event_at_utc_ms") {
        if (Array.isArray(a))
          a.forEach((t, i) =>
            push(
              t,
              `${p}/${i}`,
              "源事件时钟",
              "研究 · 源事件",
              v,
              ownCondition,
            ),
          );
        else push(a, p, "源事件时钟", "研究 · 源事件", v, ownCondition);
      } else if (k === "received_at_utc_ms") {
        const fallback =
          at(v, "latest_received_anchor") === null ||
          at(v, "direct_anchor_count") === "0" ||
          (Array.isArray(at(v, "source_ids")) &&
            (at(v, "source_ids") as Json[]).length === 0);
        push(
          a,
          p,
          fallback ? "接收轴 fallback（无实测 anchor）" : "本地接收时钟",
          fallback ? "研究 · fallback 接收轴（非实测）" : "研究 · 接收",
          v,
          ownCondition,
        );
      } else if (k === "first_visible_at_utc_ms")
        push(
          a,
          p,
          path.includes("posthoc_label") ? "事后标签公开可见" : "首次公开可见",
          path.includes("posthoc_label")
            ? "研究 · 事后标签"
            : "研究 · 首次公开可见",
          v,
          ownCondition,
        );
      else if (k === "frozen_visible_cutoff_utc_ms")
        push(
          a,
          p,
          path.includes("right_boundary") ? "独立右边界" : "研究冻结时钟",
          path.includes("right_boundary")
            ? "研究 · 右边界诊断（非采样）"
            : "研究 · 冻结（非策略触发）",
          v,
          ownCondition,
        );
      else if (
        [
          "received_start_utc_ms",
          "received_end_utc_ms",
          "start_at_utc_ms",
          "end_at_utc_ms",
        ].includes(k)
      )
        push(a, p, "研究范围端点", "研究 · 范围端点", v, ownCondition);
      else walk(a, p, ownCondition);
    }
  };
  for (const f of fragments) {
    // 只用原市场数组索引取身份；全局更新无市场，不继承界面选择。
    const match = f.path.match(/^\/research\/markets\/(\d+)(?:\/|$)/);
    const condition = match
      ? text(at(status.targets?.[Number(match[1])], "identity", "condition_id"))
      : undefined;
    if (f.exact !== undefined) walk(f.exact, f.path, condition);
    for (const row of f.rows ?? []) {
      if (Array.isArray(row.exact) && f.path.endsWith("/segment_rows")) {
        push(
          row.exact[0],
          row.path + "/0",
          "持价区段端点",
          "持价区段 · from",
          row.exact,
          condition,
        );
        push(
          row.exact[1],
          row.path + "/1",
          "持价区段端点",
          "持价区段 · to",
          row.exact,
          condition,
        );
      } else walk(row.exact, row.path, condition);
    }
  }
  return out.filter(
    (p, i, all) => all.findIndex((a) => a.path === p.path) === i,
  );
}
