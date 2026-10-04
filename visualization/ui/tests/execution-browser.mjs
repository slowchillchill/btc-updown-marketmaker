// 原 writer 引用 / typed Ledger 工程投影 + 实际 Chrome，不冒充研究产物。
import assert from "node:assert/strict";
import { chromium } from "playwright-core";
import { spawn, execFileSync } from "node:child_process";
import { readFile, writeFile, mkdtemp, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
const workspace = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);
const [noePath, projectionPath, reportPath, output] = process.argv.slice(2);
if (!output)
  throw Error(
    "execution-browser.mjs <NOE sources.json> <Ledger展示 sources.json> <原回测工程 sources.json> <截图目录>",
  );
const noe = JSON.parse(await readFile(noePath)).runs[0];
const projection = JSON.parse(await readFile(projectionPath)).runs[0];
const original = JSON.parse(await readFile(reportPath)).runs[0];
const temp = await mkdtemp(path.join(tmpdir(), "execution-browser-"));
await mkdir(output, { recursive: true });
const unknownMetadata = path.join(temp, "unknown-metadata.json");
await writeFile(
  unknownMetadata,
  JSON.stringify({ economics_version: "account_economics_v99" }),
);
const config = path.join(temp, "sources.json");
await writeFile(
  config,
  JSON.stringify({
    runs: [
      projection,
      noe,
      { ...original, noe_results: [noe.noe_results[0]] },
      {
        label: "未知版本工程引用",
        noe_results: [{ ...noe.noe_results[0], version: "v999" }],
      },
      {
        ...original,
        label: "原报告 + 未知元数据版本展示边界",
        metadata: unknownMetadata,
      },
    ],
  }),
);
const server = spawn(
  path.join(
    process.env.CARGO_TARGET_DIR ?? path.join(workspace, "target"),
    "debug/visualization",
  ),
  [config, "127.0.0.1:4322", path.join(workspace, "visualization/ui/dist")],
  { stdio: ["ignore", "ignore", "pipe"] },
);
let browser,
  log = "";
server.stderr.on("data", (b) => (log += b));
const result = {
  errors: [],
  requests: [],
  responses: [],
  projection: {},
  versions: [],
  source_identity: {},
  noe_only: {},
};
try {
  for (let i = 0; i < 200; i++) {
    if (server.exitCode !== null) throw Error(log);
    try {
      if ((await fetch("http://127.0.0.1:4322/api/runs")).ok) break;
    } catch {}
    await new Promise((r) => setTimeout(r, 25));
  }
  browser = await chromium.launch({
    executablePath: process.env.CHROME_EXECUTABLE ?? "/usr/bin/google-chrome",
    headless: true,
  });
  const page = await browser.newPage({
    viewport: { width: 1600, height: 1150 },
  });
  page.on("pageerror", (e) => result.errors.push(e.message));
  page.on("request", (r) => {
    if (r.url().includes("/noe/"))
      result.requests.push(r.url().split("4322")[1]);
  });
  page.on("response", async (r) => {
    if (r.url().includes("/noe/") && r.ok()) {
      const body = await r.body();
      result.responses.push({ bytes: body.length, data: JSON.parse(body) });
    }
  });
  const nav = async (name) =>
    page.getByRole("button", { name, exact: true }).click();
  const run = async (key) => {
    await page.getByLabel("运行选择").selectOption(key);
    await page.waitForFunction(
      (k) =>
        document.querySelector('select[aria-label="运行选择"]')?.value === k,
      key,
    );
  };
  await page.goto("http://127.0.0.1:4322");
  await page.locator(".module-card").nth(7).waitFor();
  assert.match(
    await page.locator(".run-strip").textContent(),
    /Ledger 工程投影/,
  );
  assert.doesNotMatch(
    await page.locator(".run-strip").textContent(),
    /backtest|RangeEnd|历史入口/,
  );
  assert.match(
    await page.locator(".module-card").nth(2).textContent(),
    /策略 \/ 运行计算证据未接入/,
  );
  assert.match(
    await page.locator(".module-card.shared-run").textContent(),
    /宿主运行证据未接入/,
  );
  const projectedSnapshot = await (
    await fetch("http://127.0.0.1:4322/api/runs/0/snapshot")
  ).json();
  assert.equal(projectedSnapshot.report_kind, "engineering_ledger_projection");
  assert.equal(projectedSnapshot.report.exact.end_reason, undefined);
  assert.equal(projectedSnapshot.report.exact.economics_version, undefined);
  await nav("意图与执行反馈");
  const accountField = (key) =>
    page
      .locator('[data-projection-scope="account-platform"] .fact-field')
      .filter({
        has: page.locator("small", {
          hasText: "/platform_confirmed_projection/" + key,
        }),
      });
  const expected = {
    principal_encumbrance_pusd: "3.3",
    immediate_taker_fee_encumbrance_pusd: "0.03",
    platform_confirmation_pending_economic_count: "1",
    open_order_count: "1",
    pending_fill_count: "1",
  };
  for (const [k, v] of Object.entries(expected))
    assert.equal(await accountField(k).locator("strong").textContent(), v);
  assert.equal(await page.locator(".market-projection").count(), 2);
  const second = "0x" + "2".repeat(64);
  assert.ok(
    (
      await page.getByLabel("市场选择").locator("option").allTextContents()
    ).includes(second),
  );
  await page.getByLabel("市场选择").selectOption(second);
  assert.equal(await page.locator(".market-projection").count(), 1);
  const field = page.locator(".market-projection .fact-field").filter({
    has: page.locator("small", {
      hasText: "local_before_send_principal_pusd",
    }),
  });
  assert.equal(await field.locator("strong").textContent(), "4.4");
  await field.click();
  assert.match(
    await page.locator(".evidence-panel").textContent(),
    /market_projections\/1\/local_before_send_principal_pusd/,
  );
  assert.ok(
    !(await page.locator("main").textContent()).includes(
      "current_all_in_commitment",
    ),
  );
  result.projection = {
    ...expected,
    projection_only_condition: second,
    projection_only_principal: "4.4",
    selected_original_path:
      "/ledger_consumed_memory/market_projections/1/local_before_send_principal_pusd",
  };
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({
    path: path.join(output, "execution-projection-desktop.png"),
    fullPage: true,
  });
  await run("1");
  await page.locator('[data-noe-index="0"]').waitFor();
  assert.equal(result.requests.length, 0);
  assert.match(await page.locator(".run-strip").textContent(), /NOE/);
  assert.ok(
    !(await page.locator(".run-strip").textContent()).includes("backtest"),
  );
  const expectedVariants = [
    "EconomicFactV2",
    "EconomicAlias",
    "Evidence",
    "Duplicate",
    "Conflict",
    "Pending",
  ];
  const downloadTrace = async (name, expectedTimes) => {
    await nav("多时钟时间线");
    await page.getByLabel("导出范围").selectOption("selection");
    const dp = page.waitForEvent("download");
    await page
      .getByRole("button", { name: "导出 Perfetto", exact: true })
      .click();
    const trace = path.join(output, name + ".trace.json");
    await (await dp).saveAs(trace);
    const events = JSON.parse(await readFile(trace, "utf8")).traceEvents.filter(
      (e) => e.ph === "i",
    );
    assert.deepEqual(
      events.map((e) => e.args.original_utc_ms),
      expectedTimes,
    );
    assert.ok(
      events.every(
        (e) =>
          e.args.source === "noe" &&
          e.args.noe_provenance.version === "v4" &&
          e.args.noe_provenance.reference &&
          e.args.reference.startsWith("/noe/2/exact/") &&
          e.ts === Number(BigInt(e.args.original_utc_ms) * 1000n),
      ),
    );
    if (!process.env.TRACE_PROCESSOR)
      throw Error("NOE 导出要求官方 TRACE_PROCESSOR");
    const parsed = execFileSync(
      process.env.TRACE_PROCESSOR,
      [
        "query",
        trace,
        "SELECT COUNT(*) AS count, MIN(ts) AS min_ns, MAX(ts) AS max_ns FROM slice;",
      ],
      { encoding: "utf8" },
    );
    const ns = expectedTimes.map((t) => BigInt(t) * 1000000n);
    const expected = `${ns.length},${ns.reduce((a, b) => (a < b ? a : b))},${ns.reduce((a, b) => (a > b ? a : b))}`;
    assert.ok(parsed.includes(expected), parsed);
    const args = execFileSync(
      process.env.TRACE_PROCESSOR,
      [
        "query",
        trace,
        "SELECT extract_arg(arg_set_id, 'args.original_utc_ms') AS original_utc_ms, extract_arg(arg_set_id, 'args.reference') AS reference FROM slice ORDER BY ts;",
      ],
      { encoding: "utf8" },
    );
    assert.ok(
      args.includes(expectedTimes[0]) && args.includes("/noe/2/exact/"),
    );
    result.noe_time ??= {};
    result.noe_time[name] = {
      points: events.length,
      original_times: expectedTimes,
      official_result: parsed.trim(),
      original_args: args.trim(),
    };
    return events;
  };
  for (let i = 0; i < 6; i++) {
    if (i === 2) await page.evaluate(() => performance.clearResourceTimings());
    const clickStarted = await page.evaluate(() => performance.now());
    await page.locator(`[data-noe-index="${i}"]`).click();
    await page.locator('[data-noe-status="ready"]').waitFor();
    await page.waitForFunction(
      (n) =>
        document
          .querySelector('[data-noe-index="' + n + '"]')
          ?.textContent.includes("已加载"),
      String(i),
    );
    if (i === 2)
      result.noe_detail_interaction = await page.evaluate(async (start) => {
        await new Promise((r) =>
          requestAnimationFrame(() => requestAnimationFrame(r)),
        );
        const entry = performance
          .getEntriesByType("resource")
          .find((e) => e.name.includes("/noe/2"));
        return {
          click_to_detail_two_frames_ms: performance.now() - start,
          http_resource: entry?.toJSON(),
          note: "自动化点击/HTTP/解析/React/调度/两帧综合耗时，非CPU",
        };
      }, clickStarted);
    const data = await (
      await fetch("http://127.0.0.1:4322/api/runs/1/noe/" + i)
    ).json();
    assert.match(
      await page
        .locator(".noe-panel .fact-field")
        .filter({ has: page.locator("span", { hasText: "真实记录变体" }) })
        .locator("strong")
        .textContent(),
      new RegExp(expectedVariants[i]),
    );
    assert.equal(Object.keys(data.exact.record)[0], expectedVariants[i]);
    assert.equal(data.cost.native_read_calls_total, String(i + 1));
    assert.match(JSON.stringify(data.exact), /9007199254740993/);
    result.versions.push({
      index: i,
      version: "v4",
      variant: expectedVariants[i],
      cost: data.cost,
    });
    if (i === 3)
      assert.doesNotMatch(
        await page.locator(".evidence-panel").textContent(),
        /\/noe\/2\//,
      );
    const expectedPoints = [1, 1, 3, 5, 3, 3][i];
    await nav("多时钟时间线");
    await page.waitForFunction(
      (n) =>
        document
          .querySelector("main")
          ?.textContent.includes(n + " 个可证时间点"),
      expectedPoints,
    );
    assert.equal(
      await page
        .locator('.table-scroll[data-retained-count="' + expectedPoints + '"]')
        .count(),
      1,
    );
    await nav("意图与执行反馈");
    if (i === 2) {
      const before = result.requests.length;
      const timelineStarted = await page.evaluate(() => performance.now());
      await nav("多时钟时间线");
      await page.waitForFunction(
        () =>
          document
            .querySelector(".table-scroll")
            ?.getAttribute("data-retained-count") === "3",
      );
      result.noe_interaction = await page.evaluate(async (start) => {
        await new Promise((r) =>
          requestAnimationFrame(() => requestAnimationFrame(r)),
        );
        const entry = performance
          .getEntriesByType("resource")
          .find((e) => e.name.includes("/noe/2"));
        return {
          timeline_switch_to_dom_two_frames_ms: performance.now() - start,
          http_resource: entry?.toJSON(),
          points: 3,
          note: "已加载事实导航/React/调度与两帧综合耗时；原Resource Timing单列，非CPU",
        };
      }, timelineStarted);
      assert.ok(
        (
          await page
            .locator("tr[data-row-path]")
            .first()
            .getAttribute("data-row-path")
        ).startsWith("/noe/2/exact/"),
      );
      await nav("意图与执行反馈");
      await page
        .locator(".noe-panel .fact-field")
        .filter({
          has: page.locator("span", { hasText: "发布边界 / 来源水位" }),
        })
        .click();
      const boundary = await downloadTrace("noe-boundary", ["1788840000002"]);
      assert.equal(boundary[0].args.condition_id, null);
      await nav("意图与执行反馈");
      await page
        .locator(".noe-panel .fact-field")
        .filter({ has: page.locator("span", { hasText: "真实记录变体" }) })
        .click();
      await downloadTrace("noe-record", ["1788840000000", "1788840000001"]);
      const leaf = page
        .locator("tr[data-row-path]")
        .filter({ hasText: "平台响应" });
      await leaf.getByRole("button").click();
      await downloadTrace("noe-leaf", ["1788840000000"]);
      assert.equal(result.requests.length, before);
      result.noe_time.cross_view_export_native_requests = 0;
      await page.getByLabel("导出范围").selectOption("range");
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.screenshot({
        path: path.join(output, "noe-timeline-desktop.png"),
        fullPage: true,
      });
      await nav("意图与执行反馈");
    }
    if (i === 0) {
      assert.match(JSON.stringify(data.exact), /9007199254740993\.01/);
      const count = result.requests.length;
      await nav("模块总览");
      assert.match(
        await page.locator(".module-card").nth(1).textContent(),
        /公共记录未接入/,
      );
      assert.match(
        await page.locator(".module-card").nth(2).textContent(),
        /策略 \/ 运行计算证据未接入/,
      );
      await nav("意图与执行反馈");
      assert.equal(result.requests.length, count);
      assert.equal(await page.locator('[data-noe-status="ready"]').count(), 1);
    }
  }
  for (const [index, version] of [
    [6, "v1"],
    [12, "v2"],
    [18, "v3"],
  ]) {
    await page.locator(`[data-noe-index="${index}"]`).click();
    await page.waitForFunction(
      (n) =>
        document
          .querySelector('[data-noe-index="' + n + '"]')
          ?.textContent.includes("已加载"),
      String(index),
    );
    const data = await (
      await fetch("http://127.0.0.1:4322/api/runs/1/noe/" + index)
    ).json();
    assert.equal(data.status, "ready");
    assert.equal(Object.keys(data.exact.record)[0], "EconomicFact");
    result.versions.push({ index, version, cost: data.cost });
  }
  result.noe_only = {
    configured: 24,
    browser_native_requests: result.requests.length,
    automatic_requests: 0,
    cross_view_reread: 0,
  };
  await page.locator('[data-noe-index="0"]').click();
  await page.waitForFunction(() =>
    document
      .querySelector('[data-noe-index="0"]')
      ?.textContent.includes("已加载"),
  );
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({
    path: path.join(output, "noe-v4-desktop.png"),
    fullPage: true,
  });
  await run("3");
  await page.locator('[data-noe-index="0"]').click();
  await page.locator('[data-noe-status="unknown_version"]').waitFor();
  assert.match(
    await page.locator(".noe-panel").textContent(),
    /未尝试其他版本/,
  );
  await nav("多时钟时间线");
  await page.waitForFunction(() =>
    document.querySelector("main")?.textContent.includes("0 个可证时间点"),
  );
  assert.doesNotMatch(
    await page.locator(".evidence-panel").textContent(),
    /\/noe\//,
  );
  await run("2");
  await nav("意图与执行反馈");
  assert.match(await page.locator(".run-strip").textContent(), /backtest/);
  const actualSnapshot = await (
    await fetch("http://127.0.0.1:4322/api/runs/2/snapshot")
  ).json();
  assert.equal(actualSnapshot.report_kind, "historical_backtest");
  assert.equal(actualSnapshot.report.exact.end_reason, "RangeEnd");
  await page.locator(".source-identity summary").click();
  const snapshot = await (
    await fetch("http://127.0.0.1:4322/api/runs/2/snapshot")
  ).json();
  const metadata = snapshot.metadata.exact;
  const commit = metadata.config.implementation_identity.git_commit;
  const commitField = page.locator(".source-identity .fact-field").filter({
    has: page.locator("small", {
      hasText: "/config/implementation_identity/git_commit",
    }),
  });
  assert.equal(await commitField.locator("strong").textContent(), commit);
  await commitField.click();
  assert.match(
    await page.locator(".evidence-panel").textContent(),
    /implementation_identity\/git_commit/,
  );
  const reportVersion = page.locator(".source-identity .fact-field").filter({
    has: page.locator("small", { hasText: "report/economics_version" }),
  });
  const metaVersion = page.locator(".source-identity .fact-field").filter({
    has: page.locator("small", { hasText: "metadata/economics_version" }),
  });
  assert.equal(
    await reportVersion.locator("strong").textContent(),
    snapshot.report.exact.economics_version,
  );
  assert.equal(
    await metaVersion.locator("strong").textContent(),
    metadata.economics_version,
  );
  result.source_identity = {
    original_run_git_commit: commit,
    report_economics_version: snapshot.report.exact.economics_version,
    metadata_economics_version: metadata.economics_version,
    business_trigger: "未保存/未交接",
  };
  assert.match(
    await page.locator(".source-identity").textContent(),
    /非 CPU 实测/,
  );
  await run("4");
  await page.waitForFunction(() =>
    document
      .querySelector(".source-identity")
      ?.textContent.includes("account_economics_v99"),
  );
  assert.equal(
    await reportVersion.locator("strong").textContent(),
    "account_economics_v2",
  );
  assert.equal(
    await metaVersion.locator("strong").textContent(),
    "account_economics_v99",
  );
  result.source_identity.unknown_metadata_version_preserved =
    "account_economics_v99";
  // 保留非NOE选择；失败清旧点，run A→B→A 后旧异步结果不可回填。
  await run("2");
  await nav("意图与执行反馈");
  await page.locator(".source-identity summary").click();
  await commitField.click();
  const originalSelection = await page.locator(".evidence-panel").textContent();
  await page.locator('[data-noe-index="0"]').click();
  await page.locator('[data-noe-status="ready"]').waitFor();
  assert.equal(
    await page.locator(".evidence-panel").textContent(),
    originalSelection,
  );
  await run("1");
  await page.locator('[data-noe-index="2"]').click();
  await page.locator('[data-noe-status="ready"]').waitFor();
  await nav("多时钟时间线");
  await page.locator("tr[data-row-path]").first().getByRole("button").click();
  await nav("意图与执行反馈");
  await page.route("**/api/runs/1/noe/3", (route) =>
    route.fulfill({
      status: 503,
      body: JSON.stringify({ detail: "HTTP 503 工程验证失败" }),
    }),
  );
  await page.locator('[data-noe-index="3"]').click();
  await page.getByRole("alert").filter({ hasText: "503" }).waitFor();
  await nav("多时钟时间线");
  await page.waitForFunction(() =>
    document.querySelector("main")?.textContent.includes("0 个可证时间点"),
  );
  assert.doesNotMatch(
    await page.locator(".evidence-panel").textContent(),
    /\/noe\//,
  );
  await page.unroute("**/api/runs/1/noe/3");
  await nav("意图与执行反馈");
  let held, captured;
  const arrived = new Promise((resolve) => (captured = resolve));
  await page.route("**/api/runs/1/noe/2", (route) => {
    held = route;
    captured();
  });
  await page.locator('[data-noe-index="2"]').click();
  await arrived;
  await run("3");
  await page.locator('[data-noe-index="0"]').click();
  await page.locator('[data-noe-status="unknown_version"]').waitFor();
  await run("1");
  await page.locator('[data-noe-index="2"]').waitFor();
  const oldResponse = page.waitForResponse((r) =>
    r.url().includes("/api/runs/1/noe/2"),
  );
  await held.continue();
  await (await oldResponse).finished();
  await page.evaluate(
    () =>
      new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r))),
  );
  assert.equal(await page.locator("[data-noe-status]").count(), 0);
  await nav("多时钟时间线");
  await page.waitForFunction(() =>
    document.querySelector("main")?.textContent.includes("0 个可证时间点"),
  );
  result.noe_lifecycle = {
    non_noe_selection_preserved: true,
    failure_clears_points: true,
    run_aba_stale_result_ignored: true,
  };
  assert.deepEqual(result.errors, []);
  await writeFile(
    path.join(output, "execution-browser-results.json"),
    JSON.stringify(result, null, 2),
  );
  console.log(
    JSON.stringify(
      {
        projection: result.projection,
        noe_only: result.noe_only,
        versions: result.versions.map((v) => ({
          index: v.index,
          version: v.version,
        })),
        responses: result.responses.map((r) => ({
          bytes: r.bytes,
          status: r.data.status,
        })),
        source_identity: result.source_identity,
        errors: result.errors,
      },
      null,
      2,
    ),
  );
} finally {
  if (browser) await browser.close();
  server.kill("SIGINT");
  await new Promise((r) =>
    server.exitCode !== null ? r() : server.once("exit", r),
  );
  await rm(temp, { recursive: true, force: true });
}
