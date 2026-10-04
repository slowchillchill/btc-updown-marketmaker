import { at, fact, type Json } from "./facts";
import type { Evidence } from "./browse";
export function SourceIdentity({
  report,
  metadata,
  header,
  reportVersion,
  metadataVersion,
  onSelect,
}: {
  report: Json | undefined;
  metadata: Json | undefined;
  header: Json | undefined;
  reportVersion: Json | undefined;
  metadataVersion: Json | undefined;
  onSelect: (e: Evidence) => void;
}) {
  const field = (
    source: Evidence["source"],
    root: Json | undefined,
    path: string,
    label: string,
    originalValue?: Json,
  ) => {
    const value =
      originalValue === undefined
        ? at(root, ...path.slice(1).split("/"))
        : originalValue;
    return (
      <button
        className="fact-field"
        key={source + path}
        disabled={value === undefined}
        onClick={() => value !== undefined && onSelect({ source, path, value })}
      >
        <span>{label}</span>
        <strong>{fact(value)}</strong>
        <small>
          {source}
          {path}
        </small>
      </button>
    );
  };
  return (
    <details className="source-identity">
      <summary>来源与合同版本 · 业务触发模式未保存 / 未交接</summary>
      <div className="fact-grid">
        {field(
          "metadata",
          metadata,
          "/config/implementation_identity/git_commit",
          "原运行代码提交",
        )}
        {field(
          "metadata",
          metadata,
          "/config/implementation_identity/source_identity_sha256",
          "原源码身份 SHA-256",
        )}
        {field(
          "metadata",
          metadata,
          "/config/implementation_identity/cargo_lock_sha256",
          "原依赖锁 SHA-256",
        )}
        {field(
          "metadata",
          metadata,
          "/config/strategy/strategy_rule_version",
          "策略规则版本",
        )}
        {field(
          "public",
          header === undefined ? undefined : { header },
          "/header/canonical_rule_version",
          "公共规范规则版本",
        )}
        {field(
          "public",
          header === undefined ? undefined : { header },
          "/header/observation_version",
          "公共观察版本",
        )}
        {field(
          "public",
          header === undefined ? undefined : { header },
          "/header/ordering_version",
          "公共排序版本",
        )}
        {field(
          "report",
          report,
          "/economics_version",
          "报告经济版本（原值）",
          reportVersion,
        )}
        {field(
          "metadata",
          metadata,
          "/economics_version",
          "元数据经济版本（原值）",
          metadataVersion,
        )}
        {field(
          "metadata",
          metadata,
          "/caller_role",
          "原宿主角色（非触发模式）",
        )}
        {field(
          "metadata",
          metadata,
          "/recording_role",
          "原耐久角色（非触发模式）",
        )}
        {(
          [
            "public_delivery_delay_ms",
            "calculation_start_delay_ms",
            "calculation_duration_ms",
            "dispatch_delay_ms",
          ] as const
        ).map((key) =>
          field(
            "metadata",
            metadata,
            "/config/timing/" + key,
            "历史配置延迟 · " + key,
          ),
        )}
      </div>
      <p className="gap">
        Fixed / Sequence 是历史宿主延迟配置，非 CPU
        实测或策略计算周期。未保存独立业务触发模式，不从配置、次数或策略名称猜逐帧
        /
        100ms。版本分别保留所属来源，缺失不补零；运行提交不使用本页面代码版本代替。
      </p>
    </details>
  );
}
