import { useTranslation } from "react-i18next";
import type { ReactNode } from "react";
import { at, type Json } from "./facts";
import { list, type Evidence } from "./browse";
import { Field } from "./Economics";
import { Source, Tree, type Snapshot } from "./evidence";
import { displayFact } from "./i18n";
import {
  fraction,
  rounded,
  navChange,
  decimalFraction,
  utc,
} from "./snapshotValues";

export function HourlySnapshot({
  snapshot,
  report,
  hour,
  onHour,
  onSelect,
  evidence,
  view,
}: {
  snapshot: Snapshot;
  report: Json;
  hour: string;
  onHour: (s: string) => void;
  onSelect: (e: Evidence) => void;
  evidence: ReactNode;
  view: string;
}) {
  const { t: tr } = useTranslation();
  const hours = list(at(report, "research", "hours"));
  const base = `/research/hours/${hour}/cumulative`;
  const cumulative = at(report, "research", "hours", hour, "cumulative");
  const provenance =
    snapshot.report_provenance?.status === "ready"
      ? (snapshot.report_provenance.exact ?? undefined)
      : undefined;
  const initial = at(provenance, "initial_available_pusd");
  const selectedNav = at(cumulative, "metrics", "conservative_nav_pusd");
  const change = navChange(selectedNav, initial);
  function stateDisplay(value: Json | undefined): string {
    return value === undefined
      ? tr("未提供字段")
      : value === null
        ? tr("空值（null）")
        : displayFact(value);
  }
  const changeGap = stateDisplay(
    decimalFraction(initial) ? selectedNav : initial,
  );
  function field(label: string, relative: string, unit = "", percent = false) {
    const path = base + "/" + relative;
    const value = at(report, ...path.slice(1).split("/"));
    const f = fraction(value);
    const display = f
      ? rounded(f, 2, percent ? 100n : 1n)
      : value === undefined
        ? tr("未提供字段")
        : value === null
          ? tr("空值（null）")
          : displayFact(value);
    return (
      <Field
        key={path}
        label={tr(label) + (unit ? ` · ${unit}` : "")}
        path={path + (at(value, "Known") !== undefined ? "/Known" : "")}
        report={report}
        display={display}
        onSelect={onSelect}
      />
    );
  }
  function scalar(
    label: string,
    relative: string,
    unit = "",
    display?: string,
  ) {
    const path = base + "/" + relative;
    const value = at(report, ...path.slice(1).split("/"));
    return (
      <Field
        key={path}
        label={tr(label) + (unit ? ` · ${unit}` : "")}
        path={path}
        report={report}
        display={display ?? stateDisplay(value)}
        onSelect={onSelect}
      />
    );
  }
  const cutoff = at(
    cumulative,
    "observation_cutoff",
    "decision_time",
    "logical_time_utc_ms",
  );
  const cumulativeStart = at(report, "research", "hours", "0", "start_utc_ms");
  const boundary = at(cumulative, "boundary_utc_ms");
  const originFile = at(provenance, "original_file");
  const showEconomics = ["overview", "economics"].includes(view);
  const showExecution = ["execution", "decisions"].includes(view);
  return (
    <div className="hourly-snapshot" data-hour={hour}>
      <section className="snapshot-identity">
        <div className="snapshot-heading">
          <div>
            <p className="eyebrow">BACKTEST · {tr("独立模拟账户")}</p>
            <h2>{snapshot.label}</h2>
          </div>
          <label>
            {tr("已保存快照")}
            <select
              aria-label={tr("已保存快照")}
              value={hour}
              onChange={(e) => onHour(e.target.value)}
            >
              {hours.map((h, i) => (
                <option key={i} value={i}>
                  {tr("累计至 {{hours}}h", { hours: i + 1 })} ·{" "}
                  {utc(at(h, "cumulative", "boundary_utc_ms")) ??
                    displayFact(at(h, "end_utc_ms"))}
                </option>
              ))}
            </select>
          </label>
        </div>
        <div className="snapshot-boundary">
          <span>
            {tr("累计历史区间")}: {utc(cumulativeStart) ?? tr("未提供字段")} →{" "}
            {utc(boundary) ?? tr("未提供字段")}
          </span>
          <span>
            {tr("实际观察截止")}: {utc(cutoff) ?? tr("未提供字段")}
          </span>
        </div>
        <div className="snapshot-state">
          <span>
            {tr("原保存状态")}: <b>{displayFact(at(report, "end_reason"))}</b>
          </span>
          <span>
            recording_drained:{" "}
            <b>{displayFact(at(report, "recording_drained"))}</b>
          </span>
          <span>{displayFact(at(report, "economics_version"))}</span>
        </div>
        <p className="muted">
          {tr(
            "NAV 包含剩余头寸价值。保存状态与所选小时边界分开；返佣不当作已到账现金。",
          )}
        </p>
        <details className="snapshot-source">
          <summary>{tr("来源与运行身份")}</summary>
          <Source title={tr("实际加载的提取报告")} doc={snapshot.report} />
          {snapshot.report_provenance && (
            <Source
              title={tr("提取来源映射")}
              doc={snapshot.report_provenance}
            />
          )}
          <div className="fact-grid">
            {[
              "candidate",
              "run_id",
              "comparison_identity_sha256",
              "original_file",
              "report_pointer",
              "extracted_sha256",
            ].map((k) => (
              <Field
                key={k}
                source="provenance"
                label={k}
                path={`/${k}`}
                report={provenance}
                onSelect={onSelect}
              />
            ))}
            <Field
              source="provenance"
              label={tr("原运行实现")}
              path="/runtime_binding/binary/runtime_commit"
              report={provenance}
              onSelect={onSelect}
            />
            <Field
              source="provenance"
              label={tr("发布代次")}
              path="/published_generation"
              report={provenance}
              onSelect={onSelect}
            />
          </div>
          <p className="muted">
            {tr("原包装文件")}: {displayFact(originFile)} · {tr("原报告路径")}:{" "}
            {displayFact(at(provenance, "report_pointer"))}
          </p>
        </details>
      </section>
      <div className="snapshot-workspace">
        <div className="snapshot-content">
          {showEconomics && (
            <>
              <section>
                <h2>{tr("现金、NAV 与风险")}</h2>
                <p className="muted">
                  {tr("所选小时的累计摘要；金额以 pUSD 计。")}
                </p>
                <div className="snapshot-primary">
                  {field(
                    "实际可用现金",
                    "metrics/actual_available_pusd",
                    "pUSD",
                  )}
                  {field("保守 NAV", "metrics/conservative_nav_pusd", "pUSD")}
                  {field(
                    "累计最大公共 NAV 回撤",
                    "day_economics/maximum_nav_drawdown_fraction",
                    "%",
                    true,
                  )}
                </div>
                <div className="fact-grid">
                  {field(
                    "确认净结果",
                    "metrics/confirmed_net_pnl_pusd",
                    "pUSD",
                  )}
                  {field(
                    "累计最大公共 NAV 回撤金额",
                    "day_economics/maximum_nav_drawdown_pusd",
                    "pUSD",
                  )}
                  {field(
                    "Maker 最坏情形 NAV",
                    "metrics/worst_case_maker_nav_pusd",
                    "pUSD",
                  )}
                </div>
                <div className="snapshot-derived">
                  <div>
                    <span>{tr("NAV 增量 · 派生显示")}</span>
                    <strong>
                      {change ? rounded(change.amount) : changeGap}{" "}
                      <small>pUSD</small>
                    </strong>
                  </div>
                  <div>
                    <span>{tr("NAV 变化比例 · 派生显示")}</span>
                    <strong>
                      {change?.percent ? rounded(change.percent) : changeGap}{" "}
                      <small>%</small>
                    </strong>
                  </div>
                  <p>
                    {tr(
                      "依据：所选 NAV 减原初始资金；比例除以原初始资金。确认净结果使用独立保存字段。",
                    )}
                  </p>
                  <Field
                    source="provenance"
                    label={tr("原初始资金") + " · pUSD"}
                    path="/initial_available_pusd"
                    report={provenance}
                    display={
                      decimalFraction(initial)
                        ? rounded(decimalFraction(initial)!)
                        : stateDisplay(initial)
                    }
                    onSelect={onSelect}
                  />
                </div>
              </section>
              <section>
                <h2>{tr("库存、资金与返佣")}</h2>
                <div className="fact-grid">
                  {field("配对份额", "metrics/paired_shares", "shares")}
                  {field("孤腿份额", "metrics/orphan_shares", "shares")}
                  {field(
                    "平均资金占用",
                    "day_economics/average_committed_capital_pusd",
                    "pUSD",
                  )}
                  {field(
                    "累计最大资金占用",
                    "day_economics/maximum_committed_capital_pusd",
                    "pUSD",
                  )}
                  {field(
                    "实际预留现金",
                    "metrics/actual_reserved_pusd",
                    "pUSD",
                  )}
                  {scalar(
                    "测量时长",
                    "day_economics/measured_duration_ms",
                    "ms",
                  )}
                </div>
                <h3>{tr("返佣合计 · Maker 与 Taker")}</h3>
                <div className="fact-grid">
                  {[
                    ["未成熟返佣", "accrued_unmatured"],
                    ["确认未到账返佣", "confirmed_unpaid"],
                    ["已到账返佣", "paid"],
                    ["低于门槛返佣", "below_threshold"],
                    ["锁定剩余返佣", "locked_remainder"],
                  ].map(([label, k]) =>
                    field(label, `metrics/${k}_rebates_pusd`, "pUSD"),
                  )}
                </div>
                <h3>{tr("累计执行份额、本金及实际费用")}</h3>
                <div className="fact-grid">
                  {["maker", "taker"].flatMap((role) =>
                    [
                      [
                        `${role === "maker" ? "Maker" : "Taker"} 份额`,
                        "shares",
                        "shares",
                      ],
                      [
                        `${role === "maker" ? "Maker" : "Taker"} 本金`,
                        "principal_pusd",
                        "pUSD",
                      ],
                      [
                        `${role === "maker" ? "Maker" : "Taker"} 实际费用`,
                        "fee_pusd",
                        "pUSD",
                      ],
                    ].map(([label, k, u]) =>
                      field(label, `metrics/${role}_${k}`, u),
                    ),
                  )}
                </div>
              </section>
            </>
          )}
          {(showEconomics || showExecution) && (
            <section id="snapshot-execution">
              <h2>{tr("执行与决策 · 累计摘要")}</h2>
              <p className="muted">
                {tr(
                  "模拟 trade、请求、请求腿和决策使用各自计数；无逐笔历史或成交率。",
                )}
              </p>
              <div className="fact-grid">
                {scalar(
                  "Maker 成功模拟成交",
                  "counters/maker_success_trade_count",
                  "trades",
                )}
                {scalar(
                  "Maker 经济应用成交",
                  "counters/maker_economic_trade_count",
                  "trades",
                )}
                {scalar(
                  "Taker 成功模拟成交",
                  "counters/taker_success_trade_count",
                  "trades",
                )}
                {scalar(
                  "Taker 经济应用成交",
                  "counters/taker_economic_trade_count",
                  "trades",
                )}
                {scalar("开放订单", "counters/open_order_count", "orders")}
                {scalar("待 NOE", "counters/pending_noe_records", "records")}
                {scalar(
                  "未闭合承诺",
                  "counters/unclosed_commitments",
                  "commitments",
                )}
              </div>
              <h3>{tr("已回执请求及响应")}</h3>
              <div className="fact-grid">
                {[
                  ["提交请求", "receipted_submission_requests", "requests"],
                  ["提交请求腿", "receipted_submission_requested_legs", "legs"],
                  ["撤单请求", "receipted_cancellation_requests", "requests"],
                  [
                    "已完成撤单请求",
                    "completed_receipted_cancellation_requests",
                    "requests",
                  ],
                  [
                    "待完成撤单请求",
                    "pending_receipted_cancellation_requests",
                    "requests",
                  ],
                  ["已撤响应", "receipted_canceled_responses", "responses"],
                  ["未撤响应", "receipted_not_canceled_responses", "responses"],
                  [
                    "未知撤单响应",
                    "receipted_unknown_cancel_responses",
                    "responses",
                  ],
                  [
                    "登记但无回执请求",
                    "registered_requests_without_receipt",
                    "requests",
                  ],
                ].map(([label, k, u]) => scalar(label, `counters/${k}`, u))}
              </div>
              <h3>{tr("已保存决策")}</h3>
              <div className="fact-grid">
                {[
                  ["成功计算", "successful"],
                  ["NoTrade 决策", "no_trade"],
                  ["Actions 决策", "actions"],
                  ["Exit 决策", "exits"],
                ].map(([label, k]) =>
                  scalar(label, `decisions/${k}`, "decisions"),
                )}
              </div>
              <h3>{tr("待计算与原观察边界")}</h3>
              {scalar(
                "待计算",
                "pending_calculation",
                "",
                at(cumulative, "pending_calculation") === undefined
                  ? tr("未提供字段")
                  : at(cumulative, "pending_calculation") === null
                    ? tr("空值（null）")
                    : tr("已保存 · 点击查看原时序"),
              )}
              <div className="fact-grid">
                {[
                  "observation_at_utc_ms",
                  "calculation_started_at_utc_ms",
                  "calculation_completed_at_utc_ms",
                  "scheduled_send_at_utc_ms",
                  "actual_sent_at_utc_ms",
                ].map((k) => scalar(k, `pending_calculation/${k}`, "UTC ms"))}
              </div>
              {scalar(
                "原观察截止与组身份",
                "observation_cutoff",
                "",
                utc(cutoff) ?? stateDisplay(cutoff),
              )}
            </section>
          )}
          {!showEconomics && !showExecution && view !== "evidence" && (
            <section>
              <h2>{tr("此快照的历史详情未提供")}</h2>
              <p className="gap">
                {tr(
                  "Unavailable · 小时快照没有完整逐市場、Ledger、逐笔执行、连续 NAV 或训练历史；未借用最终保存状态。",
                )}
              </p>
            </section>
          )}
          <section className="snapshot-gap">
            <h3>{tr("详情边界")}</h3>
            <p className="gap">
              {tr(
                "Unavailable · 小时快照没有完整逐市場、Ledger、逐笔执行、连续 NAV 或训练历史；未借用最终保存状态。",
              )}
            </p>
          </section>
          {view === "evidence" && (
            <section>
              <h2>{tr("所选累计快照 · 原字段")}</h2>
              {cumulative === undefined ? (
                <p className="empty">{tr("未提供字段")}</p>
              ) : (
                <Tree
                  key={`${snapshot.label}-${hour}`}
                  name={base}
                  path={base}
                  value={cumulative}
                  onSelect={(path, value) =>
                    onSelect({ source: "report", path, value })
                  }
                />
              )}
              <p className="muted">
                {tr("仅浏览所选 cumulative；单小时变化与最终保存状态不混入。")}
              </p>
            </section>
          )}
        </div>
        <div className="snapshot-evidence">{evidence}</div>
      </div>
    </div>
  );
}
