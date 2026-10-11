import { displayFact as fact } from "./i18n";
import { useTranslation } from "react-i18next";
import { useMemo, useState } from "react";
import {
  Background,
  Controls,
  Handle,
  MarkerType,
  Position,
  ReactFlow,
  type NodeProps,
  type Node,
  type Edge,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { cashTotal, relatedTargets, type Row } from "./browse";
import { at, type Json } from "./facts";
import type { Evidence } from "./browse";
const modules = [
  ["data-collect", "外部事实采集", 0, 0],
  ["data-pipeline", "公共归约 / 历史输入", 360, 0],
  ["strategy", "策略 / 模型公共位置", 720, 0],
  ["ledger", "连续账户经济", 0, 240],
  ["shared-run", "观察、预留与执行协调", 360, 240],
  ["intent-bridge", "意图提交适配", 720, 240],
  ["execution-normalization", "执行与经济反馈规范化", 360, 480],
  ["sim-system", "公共消费 / 隐藏撮合", 720, 480],
] as const;
export const relations = [
  [
    "data-collect",
    "data-pipeline",
    "采集事实",
    "来源记录 → 公共归约",
    "right",
    "left",
  ],
  [
    "data-pipeline",
    "sim-system",
    "隐藏推进 · 撮合",
    "隐藏输入不进入策略观察",
    "right",
    "left",
  ],
  [
    "data-pipeline",
    "sim-system",
    "公共消费",
    "公共观察经 SIM 形成可消费结果",
    "bottom",
    "top",
  ],
  [
    "sim-system",
    "shared-run",
    "公共观察结果",
    "SIM 公共消费 → shared-run 读取",
    "left",
    "right",
  ],
  [
    "shared-run",
    "strategy",
    "冻结输入",
    "StrategyInputV1，含当时观察、执行与账户读取",
    "top",
    "bottom",
  ],
  [
    "strategy",
    "shared-run",
    "输出 · 宿主调度",
    "StrategyOutputV1 → backtest 调度 → shared-run",
    "left",
    "top",
  ],
  [
    "shared-run",
    "ledger",
    "预留 / 经济应用",
    "shared-run 调用 Ledger；NOE 通过 apply_closed 尝试入账",
    "left",
    "right",
  ],
  [
    "ledger",
    "shared-run",
    "账户读取",
    "AccountEconomicsReadV2 / 账本消费位置",
    "top",
    "left",
  ],
  [
    "shared-run",
    "intent-bridge",
    "按序提交",
    "整批预留成功后按序发送；不是平台原子执行",
    "right",
    "left",
  ],
  [
    "intent-bridge",
    "shared-run",
    "即时分派回程",
    "提交回执 → shared-run → 策略 dispatched；不是完整成交",
    "top",
    "bottom",
  ],
  [
    "intent-bridge",
    "sim-system",
    "SIM 提交",
    "公共意图经 Bridge 适配到 SIM 后端",
    "bottom",
    "top",
  ],
  [
    "sim-system",
    "execution-normalization",
    "后续执行 / 经济",
    "SIM → execution-normalization → NOE",
    "left",
    "right",
  ],
  [
    "execution-normalization",
    "shared-run",
    "NOE · apply_closed",
    "执行登记更新与尝试 Ledger 应用分别保留",
    "top",
    "bottom",
  ],
] as const;
type ModuleData = {
  title: string;
  subtitle: string;
  summary: string;
  evidence?: Evidence;
  selected: boolean;
  moduleId: string;
  onSelect: (e: Evidence) => void;
} & Record<string, unknown>;
type ModuleNode = Node<ModuleData, "module">;
function ModuleCard({ data }: NodeProps<ModuleNode>) {
  const { t: tr } = useTranslation();
  return (
    <div
      className={`module-card ${data.moduleId} ${data.selected ? "selected" : ""}`}
    >
      {(["left", "right", "top", "bottom"] as const).map((p) => (
        <span key={p}>
          <Handle
            id={p}
            type="target"
            position={
              Position[
                (p[0].toUpperCase() + p.slice(1)) as keyof typeof Position
              ]
            }
          />
          <Handle
            id={p + "-out"}
            type="source"
            position={
              Position[
                (p[0].toUpperCase() + p.slice(1)) as keyof typeof Position
              ]
            }
          />
        </span>
      ))}
      <div className="module-head">
        <strong>{data.title}</strong>
        <span>{tr("架构职责")}</span>
      </div>
      <p>{tr(data.subtitle)}</p>
      <div className="module-fact">{data.summary}</div>
      <button
        className="nodrag"
        disabled={!data.evidence}
        onClick={() => data.evidence && data.onSelect(data.evidence)}
      >
        {tr("查看已有证据")}
      </button>
      {data.title === "sim-system" && (
        <small>{tr("公共观察消费　│　隐藏撮合状态")}</small>
      )}
    </div>
  );
}
const nodeTypes = { module: ModuleCard };
export function Topology({
  report,
  engineering = false,
  research,
  existing,
  metadata,
  selection,
  onSelect,
  playback = null,
  publicRows = [],
  noeSelection,
  noeCount = 0,
  publicReady = false,
}: {
  report: Json | undefined;
  engineering?: boolean;
  research?: Evidence;
  existing?: { kind: string; evidence: Evidence };
  metadata: Json | undefined;
  selection: Evidence | null;
  onSelect: (e: Evidence) => void;
  playback?: Evidence | null;
  publicRows?: Row[];
  noeSelection?: Evidence;
  noeCount?: number;
  publicReady?: boolean;
}) {
  const { t: tr } = useTranslation();
  const [relation, setRelation] = useState<(typeof relations)[number] | null>(
    null,
  );
  const strategy = fact(at(metadata, "config", "strategy", "strategy_id"));
  const sourceAssociation =
    !!playback &&
    at(playback.value, "Input") !== undefined &&
    !!playback.references?.some((ref) =>
      relatedTargets(ref, publicRows, playback).some(
        (target) => at(target.value, "SourceBinding") !== undefined,
      ),
    );
  const evidence = (
    source: Evidence["source"],
    path: string,
    value: Json | undefined,
  ): Evidence | undefined =>
    value === undefined ? undefined : { source, path, value };
  const facts: Record<string, { summary: string; evidence?: Evidence }> = {
    "data-collect": {
      summary: publicReady
        ? tr("原消息在公共记录按需浏览；采集运行历史未接入")
        : tr("采集 / 原消息来源未接入"),
    },
    "data-pipeline": {
      summary: publicReady
        ? tr("指定公共稳定前缀 · 顺序读取")
        : tr("公共记录未接入"),
    },
    ledger: {
      summary: engineering
        ? tr("原 Ledger 工程投影 · 非宿主 / 经济报告")
        : !report
          ? tr("账户经济未接入")
          : tr("实际总现金 ") +
            (cashTotal(
              at(
                report,
                "account_economics_memory",
                "cash",
                "actual_available_pusd",
              ),
              at(
                report,
                "account_economics_memory",
                "cash",
                "actual_reserved_pusd",
              ),
            ) ?? tr("未接入")) +
            tr(" pUSD · 终态；成本 / 应收见市场详情"),
      evidence: evidence(
        "report",
        engineering ? "/ledger_consumed_memory" : "/account_economics_memory",
        engineering
          ? at(report, "ledger_consumed_memory")
          : at(report, "account_economics_memory"),
      ),
    },
    "shared-run": {
      summary:
        !report || engineering
          ? tr("宿主运行证据未接入")
          : tr("待 NOE ") +
            fact(at(report, "pending_noe_records")) +
            tr(" · 待计算 ") +
            fact(at(report, "pending_calculation")),
      evidence: evidence(
        "report",
        "/observation_cutoff",
        at(report, "observation_cutoff"),
      ),
    },
    "intent-bridge": { summary: tr("逐次分派回执未接入") },
    "execution-normalization": {
      summary:
        (!report
          ? tr("报告耐久 NOE 前缀未接入 · ")
          : engineering
            ? tr("宿主耐久 NOE 前缀 ")
            : tr("报告耐久前缀 ")) +
        fact(at(report, "durable_noe_prefix")) +
        tr("；显式引用 {{v0}} 条{{v1}}；无消费关联", {
          v0: noeCount,
          v1: noeSelection ? tr(" · 单引用已加载") : tr(" · 尚未读取"),
        }),
      evidence:
        noeSelection ??
        evidence(
          "report",
          "/durable_noe_prefix",
          at(report, "durable_noe_prefix"),
        ),
    },
    "sim-system": {
      summary: metadata
        ? tr("装配参数可查；隐藏状态不作策略输入")
        : tr("SIM 装配 / 运行证据未接入"),
      evidence: evidence("metadata", "/config", at(metadata, "config")),
    },
  };
  facts.strategy = {
    summary: research
      ? tr("baseline-mm 描述研究已交接 · 非策略决定 / 账户运行")
      : !metadata && (!report || engineering)
        ? tr("策略 / 运行计算证据未接入")
        : strategy +
          tr(" · 计算累计 ") +
          fact(at(report, "calculation_count")) +
          tr("（本运行）"),
    evidence:
      research ??
      evidence(
        "metadata",
        "/config/strategy",
        at(metadata, "config", "strategy"),
      ),
  };
  if (existing?.kind === "completion")
    facts.ledger = {
      summary: tr("已停止工程最终 Ledger 投影 · 非 NAV / 经营验收"),
      evidence: existing.evidence,
    };
  if (existing?.kind === "collection_manifest")
    facts["data-collect"] = {
      summary: tr("历史采集清单已交接 · 非实时 / 公共前缀"),
      evidence: existing.evidence,
    };
  if (existing?.kind === "model_timing")
    facts.strategy = {
      summary: tr("有限模型宿主工程时序 · 非正式训练 / 联网PAPER"),
      evidence: existing.evidence,
    };
  if (existing?.kind === "remeasurement_12m")
    facts.strategy = {
      summary: tr("十二分钟历史描述与测量 · 非账户经营"),
      evidence: existing.evidence,
    };
  if (existing?.kind === "continuous")
    facts.strategy = {
      summary: tr("原生调用工程成本已交接 · 非训练 / 策略决定"),
      evidence: existing.evidence,
    };
  const nodes: ModuleNode[] = modules.map(([id, subtitle, x, y]) => ({
    id,
    type: "module",
    position: { x, y },
    data: {
      moduleId: id,
      title:
        id === "strategy"
          ? strategy === tr("未接入")
            ? tr("策略 / 模型")
            : strategy
          : id,
      subtitle:
        id === "strategy"
          ? tr("baseline-mm / model-train 公共位置")
          : tr(subtitle),
      ...facts[id],
      selected:
        (id === "data-pipeline" && playback?.source === "public") ||
        (id === "data-collect" && sourceAssociation) ||
        (!!selection &&
          facts[id].evidence?.source === selection.source &&
          facts[id].evidence?.path === selection.path),
      onSelect,
    },
    draggable: false,
  }));
  const edges: Edge[] = useMemo(
    () =>
      relations.map((r, i) => ({
        id: String(i),
        source: r[0],
        target: r[1],
        label: tr(r[2]),
        sourceHandle: r[4] + "-out",
        targetHandle: r[5],
        type: "smoothstep",
        animated: false,
        className:
          i === 0 && sourceAssociation ? "historical-association" : undefined,
        markerEnd: { type: MarkerType.ArrowClosed },
        style: {
          stroke:
            i === 0 && sourceAssociation
              ? "#68d8c3"
              : r[2].includes("隐藏")
                ? "#897eb8"
                : "#527f9b",
          strokeWidth: i === 0 && sourceAssociation ? 2.5 : 1.2,
          strokeDasharray: r[2].includes("隐藏") ? "5 5" : undefined,
        },
        labelStyle: { fill: "#abc1d3", fontSize: 10 },
        labelBgStyle: { fill: "#0e1724" },
        labelBgPadding: [5, 4],
      })),
    [sourceAssociation, tr],
  );
  return (
    <div>
      <div className="canvas-title">
        <h2>{tr("模块合同与实际路径")}</h2>
        <span>
          {tr("实线：公共关系　虚线：隐藏撮合输入　箭头：消费者方向")}
        </span>
      </div>
      <p className="muted">
        {tr(
          "架构关系层 · 所有边静态。卡片显示本次可读快照；累计数不代表当前流量。策略位置复用公共合同，本次策略：",
        )}
        <b>{strategy}</b>。
      </p>
      {playback && (
        <p className="history-path">
          {tr("历史浏览原引用 ")}
          {playback.path}
          {tr(
            " · 仅突出原身份支持的公共关联，静态架构方向不代表实时消费或业务执行。",
          )}
        </p>
      )}
      <div className="topology">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          fitView
          minZoom={0.35}
          maxZoom={1.5}
          nodesConnectable={false}
          nodesDraggable={false}
          ariaLabelConfig={{
            "controls.zoomIn.ariaLabel": tr("放大"),
            "controls.zoomOut.ariaLabel": tr("缩小"),
            "controls.fitView.ariaLabel": tr("适应视图"),
            "controls.interactive.ariaLabel": tr("交互开关"),
          }}
          onEdgeClick={(_, edge) => setRelation(relations[Number(edge.id)])}
        >
          <Background color="#28384b" gap={22} />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>
      {relation && (
        <div className="relation-detail">
          <b>
            {relation[0]} → {relation[1]}
          </b>
          <p>{tr(relation[3])}</p>
          <small>
            {tr(
              "合同关系说明 · 当前运行逐次路径证据不足，未宣布检查通过或违约。来源：Issue #215 第一部分及相关公共合同。",
            )}
          </small>
        </div>
      )}
      <p className="muted">
        {tr(
          "PAPER / LIVE 完整宿主、模型训练与独立评价未接入；复用位置不代表本运行已调用这些模块。",
        )}
      </p>
    </div>
  );
}
