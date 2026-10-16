// 原版本化 DTO 的明确时钟路径；只映射当前已读引用，不读取/重放/推断关联。
import { at, type Json } from "./facts";
import { text, type Point } from "./browse";
import type { LoadedNoe } from "./Noe";
export function noePoints(loaded: LoadedNoe | null): Point[] {
  if (
    !loaded ||
    loaded.status !== "ready" ||
    !["v1", "v2", "v3", "v4"].includes(loaded.version)
  )
    return [];
  const exact = loaded.exact,
    base = `/noe/${loaded.index}/exact`,
    record = at(exact, "record");
  const variant =
    record && typeof record === "object" && !Array.isArray(record)
      ? Object.keys(record)[0]
      : undefined;
  const payload = variant ? at(record, variant) : undefined;
  const out: Point[] = [];
  const provenance: Json = {
    index: loaded.index,
    version: loaded.version,
    reference: loaded.reference,
    context: at(exact, "context") ?? null,
    context_reference: at(exact, "boundary", "context_reference") ?? null,
    source_watermark: at(exact, "boundary", "source_watermark") ?? null,
    record_variant: variant ?? null,
    // 复用当前 DTO 对象，保留原请求/子项/订单/成交与别名身份；不另抽权威schema。
    record: record ?? null,
    logical_identity:
      at(payload, "logical_identity") ??
      at(payload, "fact", "logical_identity") ??
      null,
    source_order: at(payload, "source_order") ?? null,
    economic_identity:
      at(payload, "economic_identity") ??
      at(payload, "fact", "economic_identity") ??
      null,
  };
  const add = (
    root: Json | undefined,
    path: string,
    field: string,
    name: string,
    clock: string,
    condition?: string,
    sourceReference?: Json,
  ) => {
    const value = at(root, field);
    if (typeof value === "string" && /^-?\d+$/.test(value))
      out.push({
        source: "noe",
        path: path + "/" + field,
        value,
        utcMs: value,
        name,
        clock,
        condition,
        noe: { ...provenance, source_reference: sourceReference ?? null },
      });
  };
  add(
    at(exact, "boundary"),
    base + "/boundary",
    "durable_available_at_utc_ms",
    "NOE · 容器发布可用",
    "NOE 发布可用",
  );
  if (!variant || !payload) return out;
  const root = base + "/record/" + variant;
  const transition = (value: Json | undefined, path: string, label: string) => {
    const p = at(value, "AccountTransition");
    add(
      p,
      path + "/AccountTransition",
      "economic_effective_at_utc_ms",
      label + " · 经济生效",
      "经济生效",
      text(at(p, "condition_id")),
    );
  };
  // 市场只来自本记录的明确载荷身份；容器边界与无市场的引用没有 condition。
  const marketOf = (evidence: Json | undefined): string | undefined => {
    for (const name of ["OrderLifecycle", "TradeLifecycle"]) {
      const c = text(at(evidence, name, "result", "condition_id"));
      if (c) return c;
    }
    const accepted = text(
      at(
        evidence,
        "Control",
        "result",
        "Submission",
        "outcome",
        "Accepted",
        "order",
        "condition_id",
      ),
    );
    return (
      text(
        at(
          evidence,
          "SettlementLifecycleV2",
          "result",
          "MakerAccrualFinalized",
          "condition_id",
        ),
      ) ??
      text(
        at(
          evidence,
          "SettlementLifecycleV2",
          "result",
          "Contribution",
          "condition_id",
        ),
      ) ??
      text(
        at(
          evidence,
          "SettlementLifecycleV2",
          "result",
          "Request",
          "settlement_request_id",
          "condition_id",
        ),
      ) ??
      text(
        at(
          evidence,
          "SettlementLifecycleV2",
          "result",
          "Cost",
          "settlement_request_id",
          "condition_id",
        ),
      ) ??
      accepted ??
      text(
        at(evidence, "TradeLifecycleV3", "result", "result", "condition_id"),
      ) ??
      text(
        at(
          evidence,
          "Economic",
          "evidence",
          "AccountTransition",
          "condition_id",
        ),
      ) ??
      text(
        at(
          evidence,
          "EconomicV2",
          "evidence",
          "disposition",
          "AccountTransition",
          "condition_id",
        ),
      ) ??
      text(
        at(
          evidence,
          "SettlementLifecycle",
          "result",
          "AutoRedeemApplied",
          "condition_id",
        ),
      ) ??
      text(
        at(
          evidence,
          "SettlementLifecycle",
          "result",
          "MakerRebateAccrualFinalized",
          "condition_id",
        ),
      ) ??
      text(
        at(
          evidence,
          "SettlementLifecycle",
          "result",
          "TakerRebateAccrualApplied",
          "condition_id",
        ),
      ) ??
      text(
        at(
          evidence,
          "SettlementLifecycleV2",
          "result",
          "Resolved",
          "condition_id",
        ),
      )
    );
  };
  const source = (
    reference: Json | undefined,
    path: string,
    label: string,
    condition?: string,
  ) => {
    const direct = at(reference, "record", "DirectResponse");
    if (direct) {
      const p = path + "/record/DirectResponse";
      add(
        direct,
        p,
        "platform_responded_at_utc_ms",
        label + " · 平台响应",
        "平台响应",
        condition,
        reference,
      );
      add(
        direct,
        p,
        "response_received_at_utc_ms",
        label + " · 本地接收",
        "本地接收",
        condition,
        reference,
      );
    }
    for (const kind of [
      "OrderLifecycle",
      "TradeLifecycle",
      "SettlementLifecycle",
    ]) {
      const p = at(reference, "record", kind);
      if (!p) continue;
      add(
        p,
        path + "/record/" + kind,
        "platform_event_at_utc_ms",
        label + " · 平台事件",
        "平台事件",
        condition,
        reference,
      );
      add(
        p,
        path + "/record/" + kind,
        "feedback_received_at_utc_ms",
        label + " · 反馈接收",
        "反馈接收",
        condition,
        reference,
      );
    }
  };
  const evidence = (value: Json | undefined, path: string, label: string) => {
    const condition = marketOf(value);
    add(
      at(value, "Control", "result", "Submission", "outcome", "Accepted"),
      path + "/Control/result/Submission/outcome/Accepted",
      "effective_processing_at_utc_ms",
      label + " · 有效处理",
      "平台有效处理",
      condition,
    );
    add(
      at(value, "TradeLifecycle", "result"),
      path + "/TradeLifecycle/result",
      "matched_at_utc_ms",
      label + " · 原成交时刻",
      "成交时钟",
      condition,
    );
    add(
      at(value, "TradeLifecycleV3", "result", "result"),
      path + "/TradeLifecycleV3/result/result",
      "matched_at_utc_ms",
      label + " · 原成交时刻",
      "成交时钟",
      condition,
    );
    add(
      at(value, "Economic", "evidence", "AccountTransition"),
      path + "/Economic/evidence/AccountTransition",
      "economic_effective_at_utc_ms",
      label + " · 经济生效",
      "经济生效",
      condition,
    );
    transition(
      at(value, "EconomicV2", "evidence", "disposition"),
      path + "/EconomicV2/evidence/disposition",
      label,
    );
    add(
      at(value, "SettlementLifecycle", "result", "AutoRedeemApplied"),
      path + "/SettlementLifecycle/result/AutoRedeemApplied",
      "resolution_source_event_at_utc_ms",
      label + " · 判定源事件",
      "判定源事件",
      condition,
    );
    add(
      at(value, "SettlementLifecycle", "result", "AutoRedeemApplied"),
      path + "/SettlementLifecycle/result/AutoRedeemApplied",
      "resolution_public_consumed_at_utc_ms",
      label + " · 判定公开消费（模拟依据）",
      "判定公开消费",
      condition,
    );
    // v2 Settlement.Resolved 不含活动UTC；返佣归属日/保留边界/编码到期不当事件点。
  };
  switch (variant) {
    case "Evidence":
    case "Pending":
    case "Conflict": {
      const field =
        variant === "Conflict" ? "conflicting_evidence" : "evidence";
      const value = at(payload, field);
      const label =
        variant === "Evidence"
          ? "NOE Evidence"
          : variant === "Pending"
            ? "NOE Pending · 待决证据"
            : "NOE Conflict · 冲突证据";
      source(
        at(payload, "source_reference"),
        root + "/source_reference",
        label,
        marketOf(value),
      );
      evidence(value, root + "/" + field, label);
      break;
    }
    case "EconomicFact":
    case "EconomicFactV2": {
      const fact = at(payload, "fact"),
        disposition = at(fact, "disposition");
      source(
        at(fact, "source_reference"),
        root + "/fact/source_reference",
        "NOE " + variant,
        text(at(disposition, "AccountTransition", "condition_id")),
      );
      transition(disposition, root + "/fact/disposition", "NOE " + variant);
      break;
    }
    case "Duplicate":
      source(
        at(payload, "source_reference"),
        root + "/source_reference",
        "NOE Duplicate · 本次引用",
      );
      source(
        at(payload, "original_source_reference"),
        root + "/original_source_reference",
        "NOE Duplicate · 原引用",
      );
      break;
    case "EconomicAlias":
      source(
        at(payload, "source_reference"),
        root + "/source_reference",
        "NOE EconomicAlias · 本次引用",
      );
      break;
  }
  return out;
}
