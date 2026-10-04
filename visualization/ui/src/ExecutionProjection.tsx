import { at, type Json } from "./facts";
import { list, type Evidence } from "./browse";
import { Field } from "./Economics";
export function ExecutionProjection({
  report,
  root = "/ledger_consumed_memory",
  source = "report",
  engineering = false,
  condition,
  onSelect,
}: {
  report: Json | undefined;
  root?: string;
  source?: Evidence["source"];
  engineering?: boolean;
  condition: string;
  onSelect: (e: Evidence) => void;
}) {
  if (!report)
    return (
      <>
        <h2>执行与预留 · 报告投影未接入</h2>
        <p className="gap">
          没有可解释报告；不构造账户、市场预留或消费位置。显式 NOE
          单引用独立读取，不证明报告 Ledger 消费。
        </p>
      </>
    );
  const base = root,
    markets = list(
      at(report, ...root.slice(1).split("/"), "market_projections"),
    );
  const selected = markets
    .map((m, i) => ({ m, i }))
    .filter(({ m }) => !condition || at(m, "condition_id") === condition);
  const fields = (root: string, entries: [string, string][]) =>
    entries.map(([label, key]) => (
      <Field
        source={source}
        key={root + key}
        label={label}
        path={root + "/" + key}
        report={report}
        onSelect={onSelect}
      />
    ));
  const platform: [string, string][] = [
    ["平台本金占用 · pUSD（含确认待应用）", "principal_encumbrance_pusd"],
    [
      "即时 taker 费用占用 · pUSD（含确认待应用）",
      "immediate_taker_fee_encumbrance_pusd",
    ],
    [
      "平台确认、经济待应用 · 项",
      "platform_confirmation_pending_economic_count",
    ],
    [
      "确认待应用本金 · pUSD（已含）",
      "platform_confirmation_pending_economic_principal_pusd",
    ],
    [
      "确认待应用即时费用 · pUSD（已含）",
      "platform_confirmation_pending_economic_immediate_fee_pusd",
    ],
    ["开放订单 · 单", "open_order_count"],
    ["待定成交 · 笔（含已应用）", "pending_fill_count"],
  ];
  return (
    <>
      <h2>
        执行与预留 · {engineering ? "工程原 Ledger 投影" : "报告内存投影"}
      </h2>
      <p className="muted">
        {engineering
          ? "原 Ledger 入口生成的工程快照，非宿主运行报告"
          : "同一报告保存边界"}
        ，账户汇总覆盖其全部市场；市场明细按完整
        condition_id。占用包含确认待经济应用部分，不能再相加，也不是有效挂单价值。待定成交含
        PendingUnapplied / PendingApplied，不等于尚未入账数。
      </p>
      <div className="fact-grid projection-boundaries">
        {fields(base, [
          ["完整账户身份", "account_instance_key"],
          ["Ledger 条目序号", "ledger_entry_seq"],
          ["平台来源前缀", "platform_source_prefix"],
          ["账本本地前缀", "ledger_local_prefix"],
          ["可用截止 · 原 UTC ms", "available_through_utc_ms"],
        ])}
        {source === "report" &&
          fields("", [
            ["报告观察截止", "observation_cutoff"],
            ["NOE 耐久前缀", "durable_noe_prefix"],
            ["待账本应用 NOE · 条", "pending_noe_records"],
          ])}
      </div>
      <p className="gap">
        以上是不同位置域与截止，不能相减推导积压。待 NOE
        为零、账户相同或配置相同都不证明任意单引用 NOE 已被 Ledger 消费。
      </p>
      <h3>账户 · 本地未决预留</h3>
      <div className="fact-grid" data-projection-scope="account-local">
        {fields(base + "/local_pending_reservation_projection", [
          ["提交前 · 项", "before_send_count"],
          ["提交前本金 · pUSD", "before_send_principal_pusd"],
          ["提交前即时费用预留 · pUSD", "before_send_immediate_fee_pusd"],
          ["发送后效果未知 · 项", "after_send_unknown_count"],
          ["效果未知本金 · pUSD", "after_send_unknown_principal_pusd"],
          [
            "效果未知即时费用预留 · pUSD",
            "after_send_unknown_immediate_fee_pusd",
          ],
        ])}
      </div>
      <h3>账户 · 平台确认投影</h3>
      <div className="fact-grid" data-projection-scope="account-platform">
        {fields(base + "/platform_confirmed_projection", platform)}
      </div>
      {selected.map(({ m, i }) => (
        <section
          className="market-projection"
          data-projection-condition={String(at(m, "condition_id"))}
          key={i}
        >
          <h3>市场执行投影 · {String(at(m, "condition_id"))}</h3>
          <div className="fact-grid">
            {fields(base + `/market_projections/${i}`, [
              ["完整市场身份", "condition_id"],
              ["提交前 · 项", "local_before_send_count"],
              ["提交前本金 · pUSD", "local_before_send_principal_pusd"],
              [
                "提交前即时费用预留 · pUSD",
                "local_before_send_immediate_fee_pusd",
              ],
              ["发送后效果未知 · 项", "local_after_send_unknown_count"],
              [
                "效果未知本金 · pUSD",
                "local_after_send_unknown_principal_pusd",
              ],
              [
                "效果未知即时费用预留 · pUSD",
                "local_after_send_unknown_immediate_fee_pusd",
              ],
              ...platform,
            ])}
          </div>
        </section>
      ))}
      {!selected.length && (
        <p className="gap">
          {engineering ? "工程原投影" : "报告"}
          没有所选市场的执行投影；不补零，不借用经济仓位或其他市场。
        </p>
      )}
      <p className="gap">
        逐批意图/即时回执/完整订单与成交历史仍未交接。
        {engineering ? "工程投影" : "报告投影"}
        不是这些历史，也不重新计算账户经济或 NAV。
      </p>
    </>
  );
}
