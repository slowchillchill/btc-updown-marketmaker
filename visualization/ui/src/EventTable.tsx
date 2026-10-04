import { useMemo, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
} from "@tanstack/react-table";
import type { Evidence, Row } from "./browse";
export function EventTable({
  rows,
  onSelect,
  selectedPath,
  playbackPath,
}: {
  rows: Row[];
  onSelect: (e: Evidence) => void;
  selectedPath?: string;
  playbackPath?: string;
}) {
  const columns = useMemo<ColumnDef<Row>[]>(
    () => [
      { accessorKey: "kind", header: "事实类型" },
      {
        id: "clock",
        header: "时钟 / 原 UTC ms",
        accessorFn: (r) =>
          r.points.map((p) => p.clock + " · " + p.utcMs).join("；") ||
          "未接入时间映射",
      },
      {
        id: "identity",
        header: "原身份 / 市场",
        accessorFn: (r) =>
          r.identity +
          (r.markets?.length
            ? " · " + [...new Set(r.markets.map((m) => m.condition))].join("；")
            : r.condition
              ? " · " + r.condition
              : " · 无明确市场关联"),
      },
      {
        id: "reference",
        header: "原引用",
        cell: ({ row }) => (
          <button
            className="text-button"
            onClick={() => onSelect(row.original)}
          >
            {row.original.path}
          </button>
        ),
      },
    ],
    [onSelect],
  );
  const table = useReactTable({
    data: rows,
    columns,
    getCoreRowModel: getCoreRowModel(),
    getRowId: (row) => row.source + ":" + row.path,
  });
  const scroll = useRef<HTMLDivElement>(null);
  const tableRows = table.getRowModel().rows;
  const virtual = useVirtualizer({
    count: tableRows.length,
    getScrollElement: () => scroll.current,
    estimateSize: () => 100,
    getItemKey: (i) => tableRows[i].id,
  });
  const items = virtual.getVirtualItems();
  return (
    <div>
      <small className="virtual-summary">
        当前保留显示集合 {rows.length} 条 · DOM 行 {items.length} ·
        虚拟列表不减少底层读取 / 原批保留量
      </small>
      <div
        className="table-scroll"
        ref={scroll}
        data-retained-count={rows.length}
      >
        <table className="virtual-table">
          <thead>
            {table.getHeaderGroups().map((g) => (
              <tr key={g.id}>
                {g.headers.map((h) => (
                  <th key={h.id}>
                    {flexRender(h.column.columnDef.header, h.getContext())}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody
            style={{ height: virtual.getTotalSize(), position: "relative" }}
          >
            {items.map((item) => {
              const r = tableRows[item.index];
              return (
                <tr
                  key={r.id}
                  ref={virtual.measureElement}
                  data-index={item.index}
                  data-row-path={r.original.path}
                  className={
                    r.original.path === playbackPath
                      ? "playback-row"
                      : r.original.path === selectedPath
                        ? "selected-row"
                        : ""
                  }
                  style={{
                    position: "absolute",
                    width: "100%",
                    transform: `translateY(${item.start}px)`,
                  }}
                >
                  {r.getVisibleCells().map((c) => (
                    <td key={c.id}>
                      {flexRender(
                        c.column.columnDef.cell ??
                          (() => String(c.getValue() ?? "")),
                        c.getContext(),
                      )}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
        {!rows.length && (
          <p className="empty">当前来源 / 范围没有可显示事实；不补零事件。</p>
        )}
      </div>
    </div>
  );
}
