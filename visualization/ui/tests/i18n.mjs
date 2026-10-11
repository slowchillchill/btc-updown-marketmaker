// 回归真实 NOE 动态标签；原始值即使与界面词条相同也不能翻译。
import assert from "node:assert/strict";
import {
  mkdtemp,
  mkdir,
  readFile,
  writeFile,
  copyFile,
  rm,
} from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import ts from "typescript";

const ui = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cache = path.join(ui, "node_modules/.cache");
await mkdir(cache, { recursive: true });
const temp = await mkdtemp(path.join(cache, "i18n-"));
try {
  for (const name of ["facts", "i18n"]) {
    const source = await readFile(path.join(ui, "src", name + ".ts"), "utf8");
    const js = ts
      .transpileModule(source, {
        compilerOptions: {
          target: ts.ScriptTarget.ES2022,
          module: ts.ModuleKind.ES2022,
        },
      })
      .outputText.replace('from "./facts"', 'from "./facts.mjs"')
      .replace(
        'from "./locales/en.json"',
        'from "./en.json" with { type: "json" }',
      );
    await writeFile(path.join(temp, name + ".mjs"), js);
  }
  await copyFile(
    path.join(ui, "src/locales/en.json"),
    path.join(temp, "en.json"),
  );
  const {
    default: i18n,
    displayLabel,
    displayFact,
  } = await import(pathToFileURL(path.join(temp, "i18n.mjs")));
  const labels = [
    ["NOE Evidence · 有效处理", "NOE Evidence · Effective processing"],
    ["NOE Evidence · 原成交时刻", "NOE Evidence · Original fill time"],
    [
      "NOE Pending · 待决证据 · 平台响应",
      "NOE Pending · Pending evidence · Platform response",
    ],
    [
      "NOE Conflict · 冲突证据 · 本地接收",
      "NOE Conflict · Conflicting evidence · Local receipt",
    ],
    [
      "NOE Duplicate · 本次引用 · 本地接收",
      "NOE Duplicate · Current reference · Local receipt",
    ],
  ];
  await i18n.changeLanguage("en");
  for (const [original, english] of labels)
    assert.equal(displayLabel(original), english);
  assert.equal(displayFact(undefined), "Not connected");
  assert.equal(displayFact(null), "No data");
  assert.equal(displayFact("未接入"), "未接入");
  assert.equal(displayFact("无数据"), "无数据");
  assert.equal(displayFact("9007199254740993.01"), "9007199254740993.01");
  assert.equal(
    i18n.t("浏览 {{role}} 原序记录", { role: "trace" }),
    "Browse trace records in original order",
  );
  await i18n.changeLanguage("zh-CN");
  for (const [original] of labels)
    assert.equal(displayLabel(original), original);
  console.log("语言回归通过：NOE 动态标签双向切换，原始中文值与精确金额保持。");
} finally {
  await rm(temp, { recursive: true });
}
