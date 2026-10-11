import { displayError } from "./i18n";
import { displayFact as fact } from "./i18n";
import { useTranslation } from "react-i18next";
import { useEffect, useRef, useState } from "react";
import { at, statusLabel, type Json } from "./facts";
import type { Evidence } from "./browse";
import { get, Tree } from "./evidence";
export type NoeIndex = {
  index: string;
  label: string;
  version: string;
  reference_file: string;
  status: string;
  detail: string;
};
export type LoadedNoe = {
  index: string;
  label: string;
  version: string;
  status: string;
  detail: string | null;
  reference: Json;
  exact: Json;
  cost: Json;
  representation: string;
  association: string;
};
export function useNoe(run: string) {
  const [entry, setEntry] = useState<{
      run: string;
      epoch: number;
      data: LoadedNoe;
    } | null>(null),
    [busyEntry, setBusyEntry] = useState<{
      run: string;
      epoch: number;
      token: number;
    } | null>(null),
    [error, setError] = useState("");
  const active = useRef({ run, epoch: 0 }),
    flight = useRef<{ run: string; epoch: number; token: number } | null>(null),
    sequence = useRef(0);
  if (active.current.run !== run)
    active.current = { run, epoch: active.current.epoch + 1 };
  const loaded =
    entry?.run === run && entry.epoch === active.current.epoch
      ? entry.data
      : null;
  const busy =
    busyEntry?.run === run && busyEntry.epoch === active.current.epoch;
  useEffect(() => {
    setEntry(null);
    setError("");
  }, [run]);
  async function load(index: string) {
    const requested = { ...active.current, token: ++sequence.current };
    if (
      (flight.current?.run === run &&
        flight.current.epoch === requested.epoch) ||
      loaded?.index === index
    )
      return;
    flight.current = requested;
    setBusyEntry(requested);
    setError("");
    const current = () =>
      active.current.run === requested.run &&
      active.current.epoch === requested.epoch &&
      flight.current?.token === requested.token;
    try {
      const data = await get<LoadedNoe>(`/api/runs/${run}/noe/${index}`);
      if (current()) setEntry({ ...requested, data });
    } catch (e) {
      if (current()) {
        setEntry(null);
        setError(String(e));
      }
    } finally {
      if (flight.current?.token === requested.token) {
        flight.current = null;
        setBusyEntry(null);
      }
    }
  }
  return { loaded, busy, error, load };
}
export function NoePanel({
  index,
  state,
  onSelect,
}: {
  index: NoeIndex[];
  state: ReturnType<typeof useNoe>;
  onSelect: (e: Evidence) => void;
}) {
  const { t: tr } = useTranslation();
  const { loaded, busy, error, load } = state;
  const field = (
    label: string,
    path: string,
    value: Json | undefined,
    summary?: string,
  ) => (
    <button
      className="fact-field"
      disabled={value === undefined}
      onClick={() =>
        value !== undefined &&
        onSelect({ source: "noe", path: `/noe/${loaded?.index}${path}`, value })
      }
    >
      <span>{label}</span>
      <strong>{summary ?? fact(value)}</strong>
      <small>{path}</small>
    </button>
  );
  const record = at(loaded?.exact, "record"),
    variant =
      record && typeof record === "object" ? Object.keys(record)[0] : undefined;
  return (
    <section className="noe-panel">
      <h2>{tr("NOE · 显式单引用详情")}</h2>
      <p className="muted">
        {tr(
          "仅列配置交接引用，按需使用明确版本读取；不枚举历史，不自动逐条读同容器。已加载单引用跨视图保留，组件重渲染不重读。",
        )}
      </p>
      {!index.length && (
        <p className="gap">
          {tr("未交接原写入方返回的 NOE 单引用；不从目录或报告前缀反造。")}
        </p>
      )}
      <div className="noe-index">
        {index.map((item) => (
          <button
            data-noe-index={item.index}
            key={item.index}
            disabled={busy || loaded?.index === item.index}
            onClick={() => load(item.index)}
          >
            {item.label} · {item.version} ·{" "}
            {loaded?.index === item.index
              ? tr("已加载")
              : item.status === "unknown_version"
                ? tr("未知版本")
                : tr("读取原引用")}
          </button>
        ))}
      </div>
      {busy && (
        <p role="status">
          {tr(
            "读取单引用与所属整容器… 总底层 I/O / 解码量待接口返回，不承诺逐条成本。",
          )}
        </p>
      )}
      {error && <p role="alert">{displayError(error)}</p>}
      {loaded && (
        <>
          <p
            className={`status ${loaded.status === "ready" ? "ok" : "warn"}`}
            data-noe-status={loaded.status}
          >
            {tr(statusLabel[loaded.status] ?? loaded.status)} · {loaded.version}{" "}
            · {loaded.label}
          </p>
          {loaded.detail && <p role="alert">{displayError(loaded.detail)}</p>}
          <p>{tr(loaded.representation)}</p>
          <p className="gap">{tr(loaded.association)}</p>
          <div className="fact-grid">
            {field(
              tr("原 NoeResultReferenceV1"),
              "/reference",
              loaded.reference,
            )}
            {field(
              tr("原上下文 / 账户 / 合同 / 实现"),
              "/exact/context",
              at(loaded.exact, "context"),
              tr("{{v0}} · 原上下文可下钻", {
                v0: fact(at(loaded.exact, "context", "contract_version")),
              }),
            )}
            {field(
              tr("发布边界 / 来源水位 / 发布 UTC ms"),
              "/exact/boundary",
              at(loaded.exact, "boundary"),
              tr("{{v0}} UTC ms · 原水位可下钻", {
                v0: fact(
                  at(loaded.exact, "boundary", "durable_available_at_utc_ms"),
                ),
              }),
            )}
            {field(
              tr("真实记录变体 · {{v0}}", { v0: variant ?? tr("未解码") }),
              "/exact/record",
              record,
              tr("{{v0}} · 原内容可下钻", { v0: variant ?? tr("未解码") }),
            )}
          </div>
          <p className="muted">
            {tr(
              "Evidence / EconomicFact / EconomicFactV2 / Duplicate / EconomicAlias / Pending / Conflict 按原变体保留，均不自动等于成交或账本应用。请求、子项、订单和成交身份只沿原字段。",
            )}
          </p>
          {loaded.exact !== null && (
            <Tree
              name={tr("所属读取器解码原记录")}
              value={loaded.exact}
              path={`/noe/${loaded.index}/exact`}
              onSelect={(path, value) =>
                onSelect({ source: "noe", path, value })
              }
            />
          )}
          <details>
            <summary>{tr("单引用读取成本与口径")}</summary>
            <pre data-noe-cost>{JSON.stringify(loaded.cost, null, 2)}</pre>
          </details>
        </>
      )}
    </section>
  );
}
