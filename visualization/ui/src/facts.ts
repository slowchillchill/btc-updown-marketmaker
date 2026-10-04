// API 数字全部为精确文本，不转 JS Number、不重算经济事实。
export type Json = null | boolean | string | Json[] | { [key: string]: Json };
export function at(
  value: Json | undefined,
  ...path: string[]
): Json | undefined {
  for (const key of path) {
    if (value == null || typeof value !== "object") return undefined;
    value = Array.isArray(value) ? value[Number(key)] : value[key];
  }
  return value;
}
export function fact(value: Json | undefined): string {
  if (value === undefined) return "未接入";
  if (value === null) return "无数据";
  if (typeof value === "object" && !Array.isArray(value)) {
    if ("Known" in value) return `Known(${fact(value.Known)})`;
    if (
      typeof value.numerator === "string" &&
      typeof value.denominator === "string"
    )
      return `${value.numerator} / ${value.denominator}`;
  }
  return typeof value === "object" ? JSON.stringify(value) : String(value);
}
export const statusLabel: Record<string, string> = {
  ready: "已读取",
  not_configured: "未配置",
  read_error: "读取失败",
  invalid_json: "JSON 损坏",
  unsupported_version: "未知版本",
  unknown_version: "未知版本",
  unsupported_report: "报告无法解释",
};
