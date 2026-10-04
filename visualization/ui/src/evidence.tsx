import type { ResearchStatus } from "./Research";
import type { NoeIndex } from "./Noe";
import { useState } from "react";
import { fact, statusLabel, type Json } from "./facts";
export type Doc = {
  source: string | null;
  status: string;
  detail: string | null;
  exact: Json | null;
  raw_json: string | null;
  bytes_read: string;
  read_attempts: string;
  decode_attempts: string;
  load_elapsed_us: string;
};
export type Snapshot = {
  label: string;
  report: Doc;
  research: ResearchStatus;
  report_kind: "historical_backtest" | "engineering_ledger_projection";
  metadata: Doc;
  public_recording: Record<string, Json>;
  gaps: string[];
  noe_results: NoeIndex[];
};
export type Page = {
  page: string;
  row_start: string;
  returned_rows: string;
  total_returned_rows: string;
  at_end: boolean;
  prefix: Json;
  rows: Json[];
  cost: Json;
};
export type Run = { key: string; label: string };
export async function get<T>(url: string): Promise<T> {
  const response = await fetch(url);
  const body = await response.json();
  if (!response.ok) throw new Error(body.detail ?? `HTTP ${response.status}`);
  return body;
}
export function Tree({
  value,
  name,
  path = "",
  onSelect,
}: {
  value: Json;
  name: string;
  path?: string;
  onSelect: (p: string, v: Json) => void;
}) {
  const [open, setOpen] = useState(false);
  if (value === null || typeof value !== "object")
    return (
      <div className="leaf">
        <button onClick={() => onSelect(path, value)}>{name}</button>
        <code>{fact(value)}</code>
      </div>
    );
  const entries = Object.entries(value);
  return (
    <details className="tree" onToggle={(e) => setOpen(e.currentTarget.open)}>
      <summary>
        <span>{name}</span>
        <small>
          {Array.isArray(value) ? "数组" : "对象"} · {entries.length}
        </small>
        <button
          onClick={(e) => {
            e.preventDefault();
            onSelect(path, value);
          }}
        >
          查原值
        </button>
      </summary>
      {open &&
        entries.map(([key, v]) => (
          <Tree
            key={key}
            name={key}
            value={v}
            path={`${path}/${key.replaceAll("~", "~0").replaceAll("/", "~1")}`}
            onSelect={onSelect}
          />
        ))}
    </details>
  );
}
export function Source({ title, doc }: { title: string; doc: Doc }) {
  return (
    <div className="document-source">
      <strong>{title}</strong>
      <span className={`status ${doc.status === "ready" ? "ok" : "warn"}`}>
        {statusLabel[doc.status] ?? doc.status}
      </span>
      <p className="source-path">{doc.source ?? "未指定来源"}</p>
      {doc.detail && <p className="error">{doc.detail}</p>}
      <small>
        读取 {doc.read_attempts} 次 · 解码 {doc.decode_attempts} 次 ·{" "}
        {doc.bytes_read} B · 启动读取 {doc.load_elapsed_us} μs
      </small>
    </div>
  );
}
export function Raw({ doc, title }: { doc: Doc; title: string }) {
  return doc.raw_json !== null ? (
    <details className="raw">
      <summary>{title} · 原 JSON 文本（数字未改写）</summary>
      <pre>{doc.raw_json}</pre>
    </details>
  ) : null;
}
