import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import english from "./locales/en.json";
import { fact, type Json } from "./facts";

export type Language = "zh-CN" | "en";
export const languageStorageKey = "visualization.language";

function initialLanguage(): Language {
  try {
    return localStorage.getItem(languageStorageKey) === "en" ? "en" : "zh-CN";
  } catch {
    // 浏览器禁用本地存储时，仍可在当前页面切换。
    return "zh-CN";
  }
}

void i18n.use(initReactI18next).init({
  lng: initialLanguage(),
  fallbackLng: "zh-CN",
  supportedLngs: ["zh-CN", "en"],
  resources: {
    "zh-CN": {
      translation: Object.fromEntries(
        Object.keys(english).map((key) => [key, key]),
      ),
    },
    en: { translation: english },
  },
  keySeparator: false,
  nsSeparator: false,
  interpolation: { escapeValue: false },
});

// 仅用于本应用生成的显示标签；原始值、字段名、引用和查询值不经过翻译。
export function displayLabel(label: string): string {
  if (i18n.exists(label)) return i18n.t(label);
  return label
    .split(" · ")
    .map((part) => i18n.t(part))
    .join(" · ");
}

export function displayFact(value: Json | undefined): string {
  if (value === undefined) return i18n.t("未接入");
  if (value === null) return i18n.t("无数据");
  return fact(value);
}

export function displayError(detail?: string | null): string {
  if (!detail) return "";
  const prefix = detail.startsWith("Error: ") ? "Error: " : "";
  const original = detail.slice(prefix.length);
  const templates: [RegExp, string][] = [
    [
      /^(.*)；期望页 (.*)，服务端最近页 (.*)，下一页 (.*)。请显式取回当前批核对覆盖。$/s,
      "{{v0}}；期望页 {{v1}}，服务端最近页 {{v2}}，下一页 {{v3}}。请显式取回当前批核对覆盖。",
    ],
    [
      /^顺序游标下一页为 (.*)；未跳转、未从头扫描$/,
      "顺序游标下一页为 {{v0}}；未跳转、未从头扫描",
    ],
    [
      /^未知 NOE 载荷版本 (.*)；未尝试其他版本$/,
      "未知 NOE 载荷版本 {{v0}}；未尝试其他版本",
    ],
    [
      /^读取页 (.*) 失败（已返回 (.*) 条）：(.*)；游标停止，原产物未恢复$/s,
      "读取页 {{v0}} 失败（已返回 {{v1}} 条）：{{v2}}；游标停止，原产物未恢复",
    ],
  ];
  for (const [pattern, message] of templates) {
    const match = original.match(pattern);
    if (match) {
      return (
        prefix +
        i18n.t(
          message,
          Object.fromEntries(
            match.slice(1).map((value, i) => [`v${i}`, displayError(value)]),
          ),
        )
      );
    }
  }
  return prefix + i18n.t(original);
}

export default i18n;
