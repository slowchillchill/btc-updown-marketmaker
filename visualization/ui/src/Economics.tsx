import { displayFact as fact } from "./i18n";
import { useTranslation } from "react-i18next";
import { at, type Json } from "./facts";
import { assetsByOutcome, cashTotal, list, type Evidence } from "./browse";
export function Field({
  source = "report",
  label,
  path,
  report,
  onSelect,
}: {
  source?: Evidence["source"];
  label: string;
  path: string;
  report: Json | undefined;
  onSelect: (e: Evidence) => void;
}) {
  const { t: tr } = useTranslation();
  const segments = path.slice(1).split("/");
  const value = at(report, ...segments);
  const condition =
    (segments[0] === "account_economics_memory" && segments[1] === "markets") ||
    (["ledger_consumed_memory", "final_ledger"].includes(segments[0]) &&
      segments[1] === "market_projections")
      ? at(report, ...segments.slice(0, 3), "condition_id")
      : undefined;
  return (
    <button
      className="fact-field"
      disabled={value === undefined}
      onClick={() =>
        value !== undefined &&
        onSelect({
          source,
          path,
          value,
          condition: typeof condition === "string" ? condition : undefined,
        })
      }
    >
      <span>{label}</span>
      <strong>{fact(value)}</strong>
      <small>{path}</small>
    </button>
  );
}
export function Economics({
  report,
  condition,
  onSelect,
  compact = false,
}: {
  report: Json | undefined;
  condition: string;
  onSelect: (e: Evidence) => void;
  compact?: boolean;
}) {
  const { t: tr } = useTranslation();
  const cash = "/account_economics_memory/cash";
  const a = at(
      report,
      "account_economics_memory",
      "cash",
      "actual_available_pusd",
    ),
    b = at(report, "account_economics_memory", "cash", "actual_reserved_pusd");
  const total = cashTotal(a, b);
  const markets = list(at(report, "account_economics_memory", "markets"));
  const selected = markets
    .map((m, i) => ({ m, i }))
    .filter(({ m }) => !condition || at(m, "condition_id") === condition);
  return (
    <>
      <h2>{tr("账户经济 · 原终态快照")}</h2>
      <p className="muted">
        {tr(
          "账户连续跨市场；以下是报告保存时的同一账户读数。单位 pUSD；没有完整经济历史或当前活跃市场来源。",
        )}
      </p>
      <div className="cash-total">
        <span>{tr("实际总现金 · 派生显示")}</span>
        <strong>{total ?? tr("未接入")}</strong>
        <small>
          {fact(a)}
          {tr(" 可用 + ")}
          {fact(b)}
          {tr(" 预留；只做此合同口径加法。两项原引用可查。")}
        </small>
      </div>
      <div className="fact-grid">
        {[
          [tr("实际可用现金"), "actual_available_pusd"],
          [tr("实际预留现金"), "actual_reserved_pusd"],
          [tr("Maker 可用现金"), "maker_available_pusd"],
          [tr("Maker 预留现金"), "maker_reserved_pusd"],
        ].map(([label, key]) => (
          <Field
            key={key}
            label={label}
            path={cash + "/" + key}
            report={report}
            onSelect={onSelect}
          />
        ))}
        {[
          [tr("保守 NAV"), "conservative_nav_pusd"],
          [tr("Maker 最坏情形 NAV"), "worst_case_maker_nav_pusd"],
          [
            tr("方向残余"),
            "account_economics_memory/directional_residual_pusd",
          ],
          [
            tr("其他账户残余"),
            "account_economics_memory/other_account_residual_pusd",
          ],
          [tr("其他应收"), "account_economics_memory/other_receivable_pusd"],
          [tr("其他应付"), "account_economics_memory/other_liability_pusd"],
        ].map(([label, key]) => (
          <Field
            key={key}
            label={label}
            path={"/" + key}
            report={report}
            onSelect={onSelect}
          />
        ))}
      </div>
      <p className="gap">
        {tr(
          "有效挂单未成交本金：未接入。原本金占用还包含确认待经济应用本金，不能当挂单价值。策略等待 / NoTrade 聚合：未接入。",
        )}
      </p>
      {selected.map(({ m, i }) => {
        const base = "/account_economics_memory/markets/" + i;
        return (
          <section className="market-economics" key={base}>
            <h3>
              {tr("市场终态 · ")}
              <code>{fact(at(m, "condition_id"))}</code>
            </h3>
            <Field
              label={tr("结算状态")}
              path={base + "/settlement_state"}
              report={report}
              onSelect={onSelect}
            />
            <div className="asset-grid">
              {["UP", "DOWN"].map((label) => {
                const index = assetsByOutcome(m, label);
                return (
                  <article key={label} data-outcome={label}>
                    <h3 className={label === "UP" ? "up" : "down"}>{label}</h3>
                    {index === undefined ? (
                      <p>{tr("无唯一资产方向映射 · 未接入")}</p>
                    ) : (
                      <>
                        <Field
                          label={tr("原资产身份")}
                          path={`${base}/assets/${index}`}
                          report={report}
                          onSelect={onSelect}
                        />
                        <Field
                          label={tr("剩余成功份额 · shares")}
                          path={`${base}/success_shares/${index}`}
                          report={report}
                          onSelect={onSelect}
                        />
                        <Field
                          label={tr("剩余成功本金成本 · pUSD")}
                          path={`${base}/success_principal_cost_pusd/${index}`}
                          report={report}
                          onSelect={onSelect}
                        />
                      </>
                    )}
                  </article>
                );
              })}
            </div>
            {at(m, "settlement_state") === "Unresolved" && (
              <p className="muted">
                {tr("未判定持仓剩余本金成本 · 派生表达：")}
                {fact(at(m, "success_principal_cost_pusd", "0"))} +{" "}
                {fact(at(m, "success_principal_cost_pusd", "1"))}{" "}
                {tr("pUSD。原各资产成本见上方；不含挂单。")}
              </p>
            )}
            {!compact && (
              <>
                <div className="fact-grid">
                  {[
                    [tr("配对份额"), "paired_shares"],
                    [tr("孤腿份额 · 原资产顺序"), "orphan_shares"],
                    [
                      tr("未判定孤腿本金成本"),
                      "unresolved_orphan_principal_cost_pusd",
                    ],
                    [tr("配对结算应收"), "paired_resolution_receivable_pusd"],
                    [tr("赢家孤腿应收"), "winner_orphan_receivable_pusd"],
                    [tr("未付 Taker 费用"), "unpaid_taker_fee_pusd"],
                    [tr("未付结算费用"), "unpaid_settlement_fee_pusd"],
                    [tr("保证持仓价值"), "guaranteed_position_value_pusd"],
                    [tr("已确认净损益"), "confirmed_net_pnl_pusd"],
                  ].map(([label, key]) => (
                    <Field
                      key={key}
                      label={label}
                      path={base + "/" + key}
                      report={report}
                      onSelect={onSelect}
                    />
                  ))}
                </div>
                <h3>{tr("已执行累计 · 按流动性角色")}</h3>
                <p className="muted">
                  {tr(
                    "原 executed 按 Maker / Taker 汇总，不提供 UP / DOWN 执行分解。",
                  )}
                </p>
                <div className="columns">
                  {["Maker", "Taker"].map((role, index) => (
                    <div key={role} data-liquidity-role={role}>
                      <h4>{role}</h4>
                      {[
                        [tr("执行份额 · shares"), "shares"],
                        [tr("执行本金 · pUSD"), "principal_pusd"],
                        [tr("实际费用 · pUSD"), "actual_fee_pusd"],
                      ].map(([label, key]) => (
                        <Field
                          key={key}
                          label={label}
                          path={`${base}/executed/${index}/${key}`}
                          report={report}
                          onSelect={onSelect}
                        />
                      ))}
                    </div>
                  ))}
                </div>
                <h3>{tr("返佣 · 原精确状态与金额")}</h3>
                <div className="columns">
                  {["Maker", "Taker"].map((role, index) => (
                    <div key={role}>
                      <h4>{role}</h4>
                      {[
                        [tr("估计值"), "estimated_pusd"],
                        [tr("已计提未成熟"), "accrued_unmatured_pusd"],
                        [tr("门槛以下"), "below_threshold_pusd"],
                        [tr("确认未付"), "confirmed_unpaid_pusd"],
                        [tr("已付"), "paid_pusd"],
                        [tr("锁定余量"), "locked_remainder_pusd"],
                      ].map(([label, key]) => (
                        <Field
                          key={key}
                          label={label}
                          path={`${base}/rebates/${index}/${key}`}
                          report={report}
                          onSelect={onSelect}
                        />
                      ))}
                    </div>
                  ))}
                </div>
              </>
            )}
          </section>
        );
      })}
      {!!condition && !selected.length && (
        <p className="gap">
          {tr(
            "此账户报告没有所选 condition 的经济读数，无法关联；不替换为邻近市场。",
          )}
        </p>
      )}
      {!markets.length && <p className="gap">{tr("市场经济读数未接入。")}</p>}
      {!compact && (
        <>
          <h2>{tr("预留与运行生命周期")}</h2>
          <div className="fact-grid">
            {[
              [
                tr("本地提交前 / 发送后未知预留"),
                "ledger_consumed_memory/local_pending_reservation_projection",
              ],
              [
                tr("平台确认投影 · 原范围"),
                "ledger_consumed_memory/platform_confirmed_projection",
              ],
              [
                tr("账本内存消费位置"),
                "ledger_consumed_memory/ledger_entry_seq",
              ],
              [
                tr("账本可读截止 UTC ms"),
                "ledger_consumed_memory/available_through_utc_ms",
              ],
              [tr("结束原因"), "end_reason"],
              [tr("公共记录排空"), "recording_drained"],
              [tr("NOE 耐久前缀"), "durable_noe_prefix"],
              [tr("未闭合承诺"), "unclosed_commitments"],
              [tr("待账本应用 NOE"), "pending_noe_records"],
              [tr("待计算"), "pending_calculation"],
              [tr("下一必需时间 UTC ms"), "next_required_at_utc_ms"],
            ].map(([label, key]) => (
              <Field
                key={key}
                label={label}
                path={"/" + key}
                report={report}
                onSelect={onSelect}
              />
            ))}
          </div>
        </>
      )}
    </>
  );
}
// 总览详情仅随显式查看市场，未提供策略当前活跃市场时不猜选首项。
export function MarketSummary({
  report,
  condition,
  onSelect,
}: {
  report: Json | undefined;
  condition: string;
  onSelect: (e: Evidence) => void;
}) {
  const { t: tr } = useTranslation();
  const markets = list(at(report, "account_economics_memory", "markets"));
  const i = markets.findIndex((m) => at(m, "condition_id") === condition);
  const m = markets[i];
  const base = `/account_economics_memory/markets/${i}`;
  return (
    <section>
      <h2>{tr("所看市场 · 终态经济")}</h2>
      {!condition ? (
        <p className="muted">
          {tr(
            "在顶栏选择报告市场查看 UP / DOWN。策略当前活跃市场未接入，不猜首个市场。",
          )}
        </p>
      ) : i < 0 ? (
        <p className="gap">{tr("无法关联所选市场。")}</p>
      ) : (
        <>
          <code>{condition}</code>
          {["UP", "DOWN"].map((label) => {
            const n = assetsByOutcome(m, label);
            return (
              <div key={label}>
                <h3 className={label === "UP" ? "up" : "down"}>{label}</h3>
                {n === undefined ? (
                  <p>{tr("未接入资产方向映射")}</p>
                ) : (
                  <>
                    <Field
                      label={tr("剩余成功份额 · shares")}
                      path={`${base}/success_shares/${n}`}
                      report={report}
                      onSelect={onSelect}
                    />
                    <Field
                      label={tr("持仓价值 · 剩余本金成本 pUSD")}
                      path={`${base}/success_principal_cost_pusd/${n}`}
                      report={report}
                      onSelect={onSelect}
                    />
                  </>
                )}
              </div>
            );
          })}
          <Field
            label={tr("配对份额")}
            path={`${base}/paired_shares`}
            report={report}
            onSelect={onSelect}
          />
          <p className="gap">
            {tr("所选市场有效挂单 / 等待次数 / 最近决定：未接入。")}
          </p>
        </>
      )}
    </section>
  );
}
