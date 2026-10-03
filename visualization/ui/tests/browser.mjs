// 使用已构建 Rust 服务与现有 Chrome，验证真正的 API/浏览器边界。
import assert from "node:assert/strict";
import { chromium } from "playwright-core";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, writeFile, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
const workspace = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);
const sourceConfig = process.argv[2]
  ? path.resolve(workspace, process.argv[2])
  : undefined;
if (!sourceConfig)
  throw new Error(
    "用法：npm run test:browser -- <合成 sources.json> [截图目录]",
  );
const output = process.argv[3]
  ? path.resolve(workspace, process.argv[3])
  : undefined;
if (output) await mkdir(output, { recursive: true });
const temp = await mkdtemp(path.join(tmpdir(), "visualization-browser-"));
const configured = JSON.parse(await readFile(sourceConfig, "utf8"));
const original = await readFile(configured.runs[0].report);
const exact = path.join(temp, "exact.json");
await writeFile(
  exact,
  '{"end_reason":"RangeEnd","economics_version":"account_economics_v2","calculation_count":18446744073709551615,"conservative_nav_pusd":{"Known":{"numerator":"0","denominator":"1"}},"worst_case_maker_nav_pusd":{"numerator":"9007199254740993","denominator":"7"}}',
);
const unknown = path.join(temp, "unknown.json");
await writeFile(
  unknown,
  '{"end_reason":"RangeEnd","economics_version":"account_economics_v99","calculation_count":18446744073709551615}',
);
const truncated = path.join(temp, "truncated.json");
await writeFile(truncated, '{"end_reason":');
configured.runs.push(
  { label: "JSON 传输验证 · 非研究", report: exact },
  { label: "未来版本验证", report: unknown },
  { label: "截断验证", report: truncated },
);
const configPath = path.join(temp, "sources.json");
await writeFile(configPath, JSON.stringify(configured));
const address = "http://127.0.0.1:4319";
const server = spawn(
  path.join(workspace, "target/debug/visualization"),
  [configPath, "127.0.0.1:4319", path.join(workspace, "visualization/ui/dist")],
  { cwd: workspace, stdio: ["ignore", "ignore", "pipe"] },
);
let serverLog = "";
server.stderr.on("data", (bytes) => (serverLog += bytes));
const errors = [];
let browser;
try {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (server.exitCode !== null) throw new Error(serverLog);
    try {
      if ((await fetch(address + "/api/runs")).ok) break;
    } catch {}
    await new Promise((r) => setTimeout(r, 50));
  }
  browser = await chromium.launch({
    executablePath: process.env.CHROME_EXECUTABLE ?? "/usr/bin/google-chrome",
    headless: true,
  });
  const page = await browser.newPage({
    viewport: { width: 1440, height: 1050 },
  });
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto(address);
  await page
    .locator(".run-strip span")
    .filter({ hasText: "合成工程验证 · 非策略研究" })
    .waitFor();
  assert.equal(
    await page
      .locator(".tiles article")
      .first()
      .locator("strong")
      .textContent(),
    "RangeEnd",
  );
  if (output)
    await page.screenshot({
      path: path.join(output, "c1-report-desktop.png"),
      fullPage: true,
    });
  const snapshotBefore = await (
    await fetch(address + "/api/runs/0/snapshot")
  ).json();
  await page.getByRole("button", { name: "公共记录", exact: true }).click();
  await page.getByRole("button", { name: "读取当前页", exact: true }).click();
  await page.getByText(/页 0 · 本页/).waitFor();
  await page
    .locator(".records .tree")
    .first()
    .locator("summary button")
    .first()
    .click();
  const selected = await page.locator(".page-evidence pre").textContent();
  await page.getByRole("button", { name: "下一页", exact: true }).click();
  await page.getByText(/页 1 · 本页/).waitFor();
  assert.equal(
    await page.locator(".page-evidence pre").textContent(),
    selected,
  );
  await page
    .getByRole("button", { name: "取回服务端当前页", exact: true })
    .click();
  await page.getByText(/页 1 · 本页/).waitFor();
  if (output)
    await page.screenshot({
      path: path.join(output, "c1-public-desktop.png"),
      fullPage: true,
    });
  // 切换运行再切回、刷新后取回当前页，不重新读取 page 0。
  await page.getByLabel("运行选择").selectOption("1");
  await page.getByRole("button", { name: "运行报告", exact: true }).click();
  await page.getByText("18446744073709551615", { exact: true }).waitFor();
  await page.getByText("Known(0 / 1)", { exact: true }).waitFor();
  await page.getByText("9007199254740993 / 7", { exact: true }).waitFor();
  await page.getByRole("button", { name: "来源与边界", exact: true }).click();
  assert.match(await page.locator(".source").nth(1).textContent(), /未配置/);
  await page.getByLabel("运行选择").selectOption("2");
  await page.getByRole("button", { name: "运行报告", exact: true }).click();
  await page.getByText("未知版本", { exact: true }).waitFor();
  assert.ok(
    !(await page.locator(".tiles").textContent()).includes(
      "18446744073709551615",
    ),
  );
  await page
    .getByText("报告 · 原 JSON 文本（数字未改写）", { exact: true })
    .click();
  assert.ok(
    (await page.locator(".raw pre").textContent()).includes(
      "18446744073709551615",
    ),
  );
  await page.getByLabel("运行选择").selectOption("3");
  await page.getByText("JSON 损坏", { exact: true }).waitFor();
  await page.getByLabel("运行选择").selectOption("0");
  await page.getByRole("button", { name: "公共记录", exact: true }).click();
  await page.getByRole("button", { name: "读取当前页", exact: true }).click();
  await page.getByText(/页 1 · 本页/).waitFor();
  await page.reload();
  await page.getByRole("button", { name: "公共记录", exact: true }).click();
  await page.getByRole("button", { name: "读取当前页", exact: true }).click();
  await page.getByText(/页 1 · 本页/).waitFor();
  const snapshotAfter = await (
    await fetch(address + "/api/runs/0/snapshot")
  ).json();
  assert.deepEqual(snapshotBefore, snapshotAfter);
  // 完整消费小型合成稳定前缀，记录返回量与实际 HTTP 响应成本。
  let next = 2,
    rows = 0,
    responseBytes = 0,
    responses = 0;
  const started = performance.now();
  for (; ; next++) {
    const response = await fetch(address + "/api/runs/0/public?page=" + next);
    const text = await response.text();
    assert.equal(response.status, 200, text);
    const data = JSON.parse(text);
    rows += Number(data.returned_rows);
    responseBytes += Buffer.byteLength(text);
    responses++;
    if (data.at_end) {
      console.log(
        JSON.stringify({
          remainingPageResponses: responses,
          remainingReturnedRows: rows,
          responseBytes,
          httpElapsedMs: performance.now() - started,
          lastPageCost: data.cost,
        }),
      );
      break;
    }
  }
  await page.getByRole("button", { name: "运行报告", exact: true }).click();
  await page.setViewportSize({ width: 390, height: 844 });
  if (output)
    await page.screenshot({
      path: path.join(output, "c1-report-mobile.png"),
      fullPage: true,
    });
  assert.ok(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  );
  assert.deepEqual(errors, []);
  assert.deepEqual(await readFile(configured.runs[0].report), original);
  console.log(
    "浏览器验证通过：真实 RangeEnd、精确值、独立缺失元数据、未知/截断证据、跨页选择、切换/刷新游标、窄屏布局。",
  );
} finally {
  await browser?.close();
  server.kill("SIGINT");
  await new Promise((resolve) => {
    if (server.exitCode !== null || server.signalCode !== null) resolve();
    else server.once("exit", resolve);
  });
  // 截图保留，临时测试输入在失败路径也清理。
  await rm(temp, { recursive: true });
}
