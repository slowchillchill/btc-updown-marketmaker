// 真实 Rust / Chrome 联调；独立展示夹具只检验映射，不冒充生产研究。
import assert from "node:assert/strict";
import { chromium } from "playwright-core";
import { spawn, execFileSync } from "node:child_process";
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
const independent = JSON.parse(original.toString());
independent.account_economics_memory.cash.actual_available_pusd =
  "9007199254740993.01";
independent.account_economics_memory.cash.actual_reserved_pusd = "0.02";
const m = independent.account_economics_memory.markets[0];
assert.deepEqual(
  m.assets.map((a) => a.outcome.label),
  ["DOWN", "UP"],
);
m.success_shares = ["7.125", "2.375"];
m.success_principal_cost_pusd = [
  { numerator: "79", denominator: "10" },
  { numerator: "23", denominator: "10" },
];
m.executed = [
  { shares: "11", principal_pusd: "5.5", actual_fee_pusd: "0" },
  { shares: "22", principal_pusd: "12.1", actual_fee_pusd: "0.2" },
];
independent.conservative_nav_pusd = "Unavailable";
independent.worst_case_maker_nav_pusd = {
  numerator: "9007199254740993",
  denominator: "7",
};
independent.last_dispatched_calculation_timing = {
  observation_at_utc_ms: 1787529610301,
  calculation_started_at_utc_ms: 1787529610400,
  calculation_completed_at_utc_ms: 1787529610390,
  scheduled_send_at_utc_ms: 1787529610800,
  actual_sent_at_utc_ms: null,
};
const display = path.join(temp, "display-boundary.json");
await writeFile(display, JSON.stringify(independent));
// 仅置换资产身份与同域数组，角色域 executed / rebates 不随资产置换。
const permuted = structuredClone(independent);
const permutedMarket = permuted.account_economics_memory.markets[0];
for (const key of [
  "assets",
  "success_shares",
  "success_principal_cost_pusd",
  "repair_gap_shares",
  "orphan_shares",
])
  permutedMarket[key].reverse();
assert.deepEqual(permutedMarket.executed, m.executed);
const displayPermuted = path.join(temp, "display-boundary-permuted.json");
await writeFile(displayPermuted, JSON.stringify(permuted));
const unknown = path.join(temp, "unknown.json");
await writeFile(
  unknown,
  '{"end_reason":"RangeEnd","economics_version":"account_economics_v99","calculation_count":18446744073709551615}',
);
const truncated = path.join(temp, "truncated.json");
await writeFile(truncated, '{"end_reason":');
configured.runs.push(
  { label: "独立展示边界夹具 · 非生产研究", report: display },
  { label: "未来版本验证", report: unknown },
  { label: "截断验证", report: truncated },
  { label: "资产置换展示夹具 · 非生产研究", report: displayPermuted },
);
const config = path.join(temp, "sources.json");
await writeFile(config, JSON.stringify(configured));
const server = spawn(
  path.join(
    process.env.CARGO_TARGET_DIR ?? path.join(workspace, "target"),
    "debug/visualization",
  ),
  [config, "127.0.0.1:4319", path.join(workspace, "visualization/ui/dist")],
  { cwd: workspace, stdio: ["ignore", "ignore", "pipe"] },
);
let serverLog = "";
server.stderr.on("data", (b) => (serverLog += b));
let browser;
const errors = [];
const apiResponses = [];
try {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (server.exitCode !== null) throw new Error(serverLog);
    try {
      if ((await fetch("http://127.0.0.1:4319/api/runs")).ok) break;
    } catch {}
    await new Promise((r) => setTimeout(r, 50));
  }
  browser = await chromium.launch({
    executablePath: process.env.CHROME_EXECUTABLE ?? "/usr/bin/google-chrome",
    headless: true,
  });
  const page = await browser.newPage({
    viewport: { width: 1600, height: 1150 },
  });
  page.on("response", async (r) => {
    if (r.url().includes("/api/runs/0/public")) {
      const body = await r.body();
      apiResponses.push({ status: r.status(), bytes: body.length });
    }
  });
  page.on("pageerror", (e) => {
    errors.push(e.message);
    console.log("浏览器错误：" + e.message);
  });
  const started = performance.now();
  await page.goto("http://127.0.0.1:4319");
  await page.locator(".module-card").nth(7).waitFor();
  assert.equal(await page.locator(".module-card").count(), 8);
  assert.equal(await page.locator("aside > button").count(), 7);
  assert.match(
    await page.locator(".module-card").nth(2).textContent(),
    /engineering-observer/,
  );
  assert.equal(await page.locator(".react-flow__edge.animated").count(), 0);
  // 必要真实消费/回程方向来自独立合同期望，不能变成 Bridge 直接回策略成交。
  // React Flow 的边身份可从 aria-label 核对，不依赖内部未承诺 data 属性。
  const edgeLabels = await page
    .locator(".react-flow__edge")
    .evaluateAll((nodes) => nodes.map((n) => n.getAttribute("aria-label")));
  assert.ok(edgeLabels.includes("Edge from shared-run to ledger"));
  assert.ok(
    edgeLabels.includes("Edge from execution-normalization to shared-run"),
  );
  assert.ok(
    !edgeLabels.some(
      (s) => s?.includes("intent-bridge") && s?.includes("strategy"),
    ),
  );
  const snapshotBefore = await (
    await fetch("http://127.0.0.1:4319/api/runs/0/snapshot")
  ).json();
  await page.getByRole("button", { name: "读取当前页", exact: true }).click();
  await page.getByText(/页 0 · 本页/).waitFor();
  await page.locator(".text-button").first().click();
  const chosen = await page.locator(".evidence-panel pre").textContent();
  const nav = async (name) =>
    page.getByRole("button", { name, exact: true }).click();
  await nav("输入与决定");
  assert.equal(await page.locator(".evidence-panel pre").textContent(), chosen);
  await nav("证据重放");
  await page.getByRole("button", { name: "下一页", exact: true }).click();
  await page.getByText(/页 1 · 本页/).waitFor();
  assert.equal(await page.locator(".evidence-panel pre").textContent(), chosen);
  await page
    .getByRole("button", { name: "取回服务端当前页", exact: true })
    .click();
  await page.getByText(/页 1 · 本页/).waitFor();
  await nav("模块总览");
  if (output) await page.evaluate(() => window.scrollTo(0, 0));
  if (output)
    await page.screenshot({
      path: path.join(output, "overview-desktop.png"),
      fullPage: true,
    });
  await nav("账户经济与生命周期");
  await page.getByLabel("市场选择").selectOption(m.condition_id);
  assert.equal(await page.locator('[data-outcome="UP"]').count(), 1);
  assert.match(
    await page.locator('[data-outcome="UP"]').textContent(),
    /"token_id":"20"/,
  );
  assert.match(
    await page.locator('[data-outcome="DOWN"]').textContent(),
    /"token_id":"3"/,
  );
  if (output) await page.evaluate(() => window.scrollTo(0, 0));
  if (output)
    await page.screenshot({
      path: path.join(output, "economics-desktop.png"),
      fullPage: true,
    });
  await page.locator('[data-outcome="UP"] .fact-field').nth(2).click();
  const costEvidence = await page.locator(".evidence-panel pre").textContent();
  await nav("意图与执行反馈");
  assert.equal(await page.getByLabel("市场选择").inputValue(), m.condition_id);
  assert.equal(
    await page.locator(".evidence-panel pre").textContent(),
    costEvidence,
  );
  await page.getByLabel("市场选择").selectOption("");
  await nav("多时钟时间线");
  await page.getByLabel("区间起点 UTC ms").fill("1787529610400");
  await page.getByLabel("区间终点 UTC ms").fill("1787529610800");
  assert.ok(
    await page
      .getByRole("button", { name: "导出 Perfetto", exact: true })
      .isEnabled(),
  );
  const downloadPromise = page.waitForEvent("download", { timeout: 10000 });
  await page
    .getByRole("button", { name: "导出 Perfetto", exact: true })
    .click();
  const download = await downloadPromise;
  const trace = output
    ? path.join(output, "selected-facts.trace.json")
    : path.join(temp, "facts.trace.json");
  await download.saveAs(trace);
  const traceText = await readFile(trace, "utf8");
  const parsed = JSON.parse(traceText);
  const instants = parsed.traceEvents.filter((e) => e.ph === "i");
  assert.equal(instants.length, 4);
  assert.deepEqual(
    instants.map((e) => e.args.original_utc_ms),
    ["1787529610400", "1787529610600", "1787529610800", "1787529610800"],
  );
  assert.ok(
    instants.every(
      (e) =>
        e.ts === Number(BigInt(e.args.original_utc_ms) * 1000n) &&
        e.args.derived === true &&
        e.args.reference,
    ),
  );
  if (process.env.TRACE_PROCESSOR) {
    const result = execFileSync(
      process.env.TRACE_PROCESSOR,
      [
        "query",
        trace,
        "SELECT COUNT(*) AS count, MIN(ts) AS min_ns, MAX(ts) AS max_ns FROM slice;",
      ],
      { encoding: "utf8" },
    );
    assert.match(result, /4,1787529610400000000,1787529610800000000/);
    console.log("官方 Perfetto Trace Processor：" + result.trim());
    const argsResult = execFileSync(
      process.env.TRACE_PROCESSOR,
      [
        "query",
        trace,
        "SELECT name, extract_arg(arg_set_id, 'args.original_utc_ms') AS original_utc_ms, extract_arg(arg_set_id, 'args.clock_kind') AS clock_kind, extract_arg(arg_set_id, 'args.reference') AS reference FROM slice ORDER BY ts, name;",
      ],
      { encoding: "utf8" },
    );
    assert.match(argsResult, /1787529610400/);
    assert.match(argsResult, /计算时钟/);
    assert.match(
      argsResult,
      /\/last_dispatched_calculation_timing\/calculation_started_at_utc_ms/,
    );
    console.log("官方解析原时间 / 时钟 / 引用：" + argsResult.trim());
    if (output)
      await writeFile(
        path.join(output, "perfetto-parse.txt"),
        result + "\n" + argsResult,
      );
  } else
    console.log(
      "官方 Perfetto 解析未运行；可通过 TRACE_PROCESSOR 指定官方工具。",
    );
  if (output) await page.evaluate(() => window.scrollTo(0, 0));
  if (output)
    await page.screenshot({
      path: path.join(output, "timeline-desktop.png"),
      fullPage: true,
    });
  // 所选事实导出只承接该引用，缺时间端点无事件导出。
  await page.getByLabel("导出范围").selectOption("selection");
  assert.equal(
    await page
      .getByRole("button", { name: "导出 Perfetto", exact: true })
      .isEnabled(),
    false,
  );
  await page.getByRole("button", { name: "清除时间范围", exact: true }).click();
  await page.getByLabel("运行选择").selectOption("1");
  await nav("账户经济与生命周期");
  await page
    .locator(".cash-total strong")
    .filter({ hasText: "9007199254740993.03" })
    .waitFor();
  await page.getByLabel("市场选择").selectOption(m.condition_id);
  assert.match(
    await page.locator('[data-outcome="UP"]').textContent(),
    /2.375/,
  );
  assert.match(
    await page.locator('[data-outcome="UP"]').textContent(),
    /23 \/ 10/,
  );
  assert.match(
    await page.locator('[data-outcome="DOWN"]').textContent(),
    /7.125/,
  );
  assert.match(
    await page.locator('[data-outcome="DOWN"]').textContent(),
    /79 \/ 10/,
  );
  assert.match(
    await page.locator(".fact-grid:visible").first().textContent(),
    /Unavailable/,
  );
  assert.match(
    await page.locator(".fact-grid:visible").first().textContent(),
    /9007199254740993 \/ 7/,
  );
  assert.match(
    await page.locator("main").textContent(),
    /有效挂单未成交本金：未接入/,
  );
  const assertRoleTotals = async () => {
    for (const [index, role, expected] of [
      [0, "Maker", ["11", "5.5", "0"]],
      [1, "Taker", ["22", "12.1", "0.2"]],
    ]) {
      const roleCard = page.locator(`[data-liquidity-role="${role}"]`);
      assert.deepEqual(
        await roleCard.locator(".fact-field strong").allTextContents(),
        expected,
      );
      assert.deepEqual(
        await roleCard.locator(".fact-field small").allTextContents(),
        ["shares", "principal_pusd", "actual_fee_pusd"].map(
          (field) =>
            `/account_economics_memory/markets/0/executed/${index}/${field}`,
        ),
      );
    }
    for (const label of ["UP", "DOWN"])
      assert.ok(
        !(
          await page.locator(`[data-outcome="${label}"]`).textContent()
        ).includes("/executed/"),
      );
  };
  await assertRoleTotals();
  if (output) {
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.screenshot({
      path: path.join(output, "economics-role-boundary.png"),
      fullPage: true,
    });
  }
  await page.getByLabel("运行选择").selectOption("4");
  await page
    .locator(".run-strip span")
    .filter({ hasText: "资产置换展示夹具 · 非生产研究" })
    .waitFor();
  await page
    .locator(".cash-total strong")
    .filter({ hasText: "9007199254740993.03" })
    .waitFor();
  await page.getByLabel("市场选择").selectOption(m.condition_id);
  await assertRoleTotals();
  assert.deepEqual(
    await page
      .locator('[data-outcome="UP"] .fact-field small')
      .allTextContents(),
    ["assets", "success_shares", "success_principal_cost_pusd"].map(
      (field) => `/account_economics_memory/markets/0/${field}/0`,
    ),
  );
  assert.deepEqual(
    await page
      .locator('[data-outcome="DOWN"] .fact-field small')
      .allTextContents(),
    ["assets", "success_shares", "success_principal_cost_pusd"].map(
      (field) => `/account_economics_memory/markets/0/${field}/1`,
    ),
  );
  assert.match(
    await page.locator('[data-outcome="UP"]').textContent(),
    /"token_id":"20"/,
  );
  assert.match(
    await page.locator('[data-outcome="UP"]').textContent(),
    /2.375/,
  );
  assert.match(
    await page.locator('[data-outcome="UP"]').textContent(),
    /23 \/ 10/,
  );
  assert.match(
    await page.locator('[data-outcome="DOWN"]').textContent(),
    /"token_id":"3"/,
  );
  assert.match(
    await page.locator('[data-outcome="DOWN"]').textContent(),
    /7.125/,
  );
  assert.match(
    await page.locator('[data-outcome="DOWN"]').textContent(),
    /79 \/ 10/,
  );
  console.log(
    "角色 / 资产置换回归通过：Maker 11 / 5.5 / 0，Taker 22 / 12.1 / 0.2；资产置换不改 executed，UP/DOWN 身份份额成本映射不变。",
  );
  await page.getByLabel("市场选择").selectOption("");
  await nav("多时钟时间线");
  assert.match(
    await page.locator(".timing-deltas").textContent(),
    /-10 ms = 1787529610390 − 1787529610400/,
  );
  assert.match(
    await page.locator(".timing-deltas").textContent(),
    /无完整端点/,
  );
  assert.ok(!(await page.locator("tbody").textContent()).includes("实际发送"));
  await page.getByLabel("运行选择").selectOption("2");
  await nav("证据重放");
  await page
    .getByText("报告 · 原 JSON 文本（数字未改写）", { exact: true })
    .click();
  assert.match(
    await page.locator(".raw pre").first().textContent(),
    /18446744073709551615/,
  );
  assert.match(
    await page.locator(".document-source").first().textContent(),
    /未知版本/,
  );
  await page.getByLabel("运行选择").selectOption("3");
  await page.getByText("JSON 损坏", { exact: true }).first().waitFor();
  await page.getByLabel("运行选择").selectOption("0");
  await page.getByRole("button", { name: "读取当前页", exact: true }).click();
  await page.getByText(/页 1 · 本页/).waitFor();
  await page.reload();
  await nav("证据重放");
  await page.getByRole("button", { name: "读取当前页", exact: true }).click();
  await page.getByText(/页 1 · 本页/).waitFor();
  assert.deepEqual(
    await (await fetch("http://127.0.0.1:4319/api/runs/0/snapshot")).json(),
    snapshotBefore,
  );
  await nav("训练与实验");
  assert.match(
    await page.locator("main").textContent(),
    /真实 MLflow 实验来源未接入/,
  );
  await nav("模块总览");
  await page.setViewportSize({ width: 390, height: 844 });
  await page.locator(".module-card").nth(7).waitFor();
  if (output) await page.evaluate(() => window.scrollTo(0, 0));
  if (output)
    await page.screenshot({
      path: path.join(output, "overview-mobile.png"),
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
    JSON.stringify({
      browserTotalElapsedMs: performance.now() - started,
      publicHttpResponses: apiResponses,
      publicStablePrefix: snapshotBefore.public_recording.prefix,
      officialPerfetto: !!process.env.TRACE_PROCESSOR,
    }),
  );
  console.log(
    "浏览器验证通过：真实合成来源、八模块/七导航/静态边、跨视图/跨页身份、UP/DOWN非对称映射、现金精确加法、零/Unavailable、负时差/缺端点、范围导出、未知/损坏、游标刷新与窄屏。",
  );
} finally {
  await browser?.close();
  server.kill("SIGINT");
  await new Promise((resolve) => {
    if (server.exitCode !== null || server.signalCode !== null) resolve();
    else server.once("exit", resolve);
  });
  await rm(temp, { recursive: true });
}
