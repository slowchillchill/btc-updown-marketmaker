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
import { cashTotal } from "./browse";
import { at, fact, type Json } from "./facts";
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
        <span>架构职责</span>
      </div>
      <p>{data.subtitle}</p>
      <div className="module-fact">{data.summary}</div>
      <button
        className="nodrag"
        disabled={!data.evidence}
        onClick={() => data.evidence && data.onSelect(data.evidence)}
      >
        查看已有证据
      </button>
      {data.title === "sim-system" && (
        <small>公共观察消费　│　隐藏撮合状态</small>
      )}
    </div>
  );
}
const nodeTypes = { module: ModuleCard };
export function Topology({
  report,
  metadata,
  selection,
  onSelect,
}: {
  report: Json | undefined;
  metadata: Json | undefined;
  selection: Evidence | null;
  onSelect: (e: Evidence) => void;
}) {
  const [relation, setRelation] = useState<(typeof relations)[number] | null>(
    null,
  );
  const strategy = fact(at(metadata, "config", "strategy", "strategy_id"));
  const evidence = (
    source: Evidence["source"],
    path: string,
    value: Json | undefined,
  ): Evidence | undefined =>
    value === undefined ? undefined : { source, path, value };
  const facts: Record<string, { summary: string; evidence?: Evidence }> = {
    "data-collect": { summary: "原消息在公共记录按需浏览；采集运行历史未接入" },
    "data-pipeline": { summary: "指定公共稳定前缀 · 顺序读取" },
    ledger: {
      summary:
        "实际总现金 " +
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
        ) ?? "未接入") +
        " pUSD · 终态；成本 / 应收见市场详情",
      evidence: evidence(
        "report",
        "/account_economics_memory",
        at(report, "account_economics_memory"),
      ),
    },
    "shared-run": {
      summary:
        "待 NOE " +
        fact(at(report, "pending_noe_records")) +
        " · 待计算 " +
        fact(at(report, "pending_calculation")),
      evidence: evidence(
        "report",
        "/observation_cutoff",
        at(report, "observation_cutoff"),
      ),
    },
    "intent-bridge": { summary: "逐次分派回执未接入" },
    "execution-normalization": {
      summary:
        "报告耐久前缀 " +
        fact(at(report, "durable_noe_prefix")) +
        "；逐条 NOE 未接入",
      evidence: evidence(
        "report",
        "/durable_noe_prefix",
        at(report, "durable_noe_prefix"),
      ),
    },
    "sim-system": {
      summary: "装配参数可查；隐藏状态不作策略输入",
      evidence: evidence("metadata", "/config", at(metadata, "config")),
    },
  };
  facts.strategy = {
    summary:
      strategy +
      " · 计算累计 " +
      fact(at(report, "calculation_count")) +
      "（本运行）",
    evidence: evidence(
      "metadata",
      "/config/strategy",
      at(metadata, "config", "strategy"),
    ),
  };
  const nodes: ModuleNode[] = modules.map(([id, subtitle, x, y]) => ({
    id,
    type: "module",
    position: { x, y },
    data: {
      moduleId: id,
      title:
        id === "strategy"
          ? strategy === "未接入"
            ? "策略 / 模型"
            : strategy
          : id,
      subtitle:
        id === "strategy" ? "baseline-mm / model-train 公共位置" : subtitle,
      ...facts[id],
      selected:
        !!selection &&
        facts[id].evidence?.source === selection.source &&
        facts[id].evidence?.path === selection.path,
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
        label: r[2],
        sourceHandle: r[4] + "-out",
        targetHandle: r[5],
        type: "smoothstep",
        animated: false,
        markerEnd: { type: MarkerType.ArrowClosed },
        style: {
          stroke: r[2].includes("隐藏") ? "#897eb8" : "#527f9b",
          strokeWidth: 1.2,
          strokeDasharray: r[2].includes("隐藏") ? "5 5" : undefined,
        },
        labelStyle: { fill: "#abc1d3", fontSize: 10 },
        labelBgStyle: { fill: "#0e1724" },
        labelBgPadding: [5, 4],
      })),
    [],
  );
  return (
    <div>
      <div className="canvas-title">
        <h2>模块合同与实际路径</h2>
        <span>实线：公共关系　虚线：隐藏撮合输入　箭头：消费者方向</span>
      </div>
      <p className="muted">
        架构关系层 ·
        所有边静态。卡片显示本次可读快照；累计数不代表当前流量。策略位置复用公共合同，本次策略：
        <b>{strategy}</b>。
      </p>
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
          <p>{relation[3]}</p>
          <small>
            合同关系说明 ·
            当前运行逐次路径证据不足，未宣布检查通过或违约。来源：Issue #215
            第一部分及相关公共合同。
          </small>
        </div>
      )}
      <p className="muted">
        PAPER / LIVE
        完整宿主、模型训练与独立评价未接入；复用位置不代表本运行已调用这些模块。
      </p>
    </div>
  );
}
