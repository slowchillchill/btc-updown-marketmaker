import { useEffect, useRef } from "react";
import * as echarts from "echarts/core";
import { ScatterChart } from "echarts/charts";
import { GridComponent, TooltipComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import { pointUs, type Evidence, type Point } from "./browse";
echarts.use([ScatterChart, GridComponent, TooltipComponent, CanvasRenderer]);
export function Timeline({
  points,
  onSelect,
}: {
  points: Point[];
  onSelect: (e: Evidence) => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!ref.current || !points.length) return;
    const chart = echarts.init(ref.current);
    const origin = points.reduce(
      (a, p) => (pointUs(p) < a ? pointUs(p) : a),
      pointUs(points[0]),
    );
    const clocks = [...new Set(points.map((p) => p.clock))];
    chart.setOption({
      backgroundColor: "transparent",
      grid: { left: 115, right: 25, top: 25, bottom: 45 },
      tooltip: {
        trigger: "item",
        formatter: (params: unknown) => {
          const i = (params as { dataIndex: number }).dataIndex;
          const p = points[i];
          // 纯文本显示，避免来源字符串成为 HTML。
          return p ? `${p.clock} · ${p.originalUtcText ?? p.utcMs}` : "";
        },
      },
      xAxis: {
        type: "value",
        name: "相对起点 ms（仅绘图近似）",
        nameLocation: "middle",
        nameGap: 28,
        axisLabel: { color: "#8aa2b8" },
        splitLine: { lineStyle: { color: "#233449" } },
        nameTextStyle: { color: "#8aa2b8" },
      },
      yAxis: {
        type: "category",
        data: clocks,
        axisLabel: { color: "#a9c2d7" },
        splitLine: { show: true, lineStyle: { color: "#233449" } },
      },
      series: [
        {
          type: "scatter",
          symbolSize: 10,
          itemStyle: { color: "#72dbc5" },
          data: points.map((p) => [
            Number(pointUs(p) - origin) / 1000,
            p.clock,
          ]),
        },
      ],
    });
    chart.on("click", (params) => {
      const p = points[params.dataIndex];
      if (p) onSelect(p);
    });
    const resize = new ResizeObserver(() => chart.resize());
    resize.observe(ref.current);
    return () => {
      resize.disconnect();
      chart.dispose();
    };
  }, [points, onSelect]);
  return (
    <>
      <div className="timeline" ref={ref} aria-label="有限多时钟事实图" />
      <p className="muted">
        仅显示已知点；没有完整诊断历史，不画经济曲线或补造持续时间。图上坐标为近似位移，选点查看原精确
        时间（毫秒整数或原微秒 UTC 文本）。
      </p>
    </>
  );
}
