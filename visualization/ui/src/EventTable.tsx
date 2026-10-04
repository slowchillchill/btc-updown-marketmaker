import { useMemo } from "react";
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
}: {
  rows: Row[];
  onSelect: (e: Evidence) => void;
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
  });
  return (
    <div className="table-scroll">
      <table>
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
        <tbody>
          {table.getRowModel().rows.map((r) => (
            <tr key={r.id}>
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
          ))}
        </tbody>
      </table>
      {!rows.length && (
        <p className="empty">当前来源 / 范围没有可显示事实；不补零事件。</p>
      )}
    </div>
  );
}
