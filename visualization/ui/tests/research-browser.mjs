// 真实公开研究产物的独立常量期待 + 实际 Chrome；不运行研究生产器。
import assert from "node:assert/strict";
import { chromium } from "playwright-core";
import { spawn } from "node:child_process";
import { readFile, writeFile, mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
const workspace = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);
const [configPath, output] = process.argv.slice(2);
if (!output) throw Error("research-browser.mjs <sources.json> <输出目录>");
await mkdir(output, { recursive: true });
const temp = await mkdtemp(path.join(tmpdir(), "research-browser-"));
const source = JSON.parse(await readFile(configPath)).runs[0];
await writeFile(
  path.join(temp, "unknown.json"),
  JSON.stringify({ schema: "unsupported_research" }),
);
await writeFile(path.join(temp, "corrupt.json"), "{broken");
const config = path.join(temp, "sources.json");
await writeFile(
  config,
  JSON.stringify({
    runs: [
      source,
      {
        ...source,
        label: "独立描述研究（无报告/公共）",
        public_recording: null,
      },
      { label: "未知研究合同", research: path.join(temp, "unknown.json") },
      { label: "损坏研究", research: path.join(temp, "corrupt.json") },
    ],
  }),
);
const server = spawn(
  path.join(
    process.env.CARGO_TARGET_DIR ?? path.join(workspace, "target"),
    "debug/visualization",
  ),
  [config, "127.0.0.1:4323", path.join(workspace, "visualization/ui/dist")],
  { stdio: ["ignore", "ignore", "pipe"] },
);
let browser,
  log = "";
server.stderr.on("data", (b) => (log += b));
const result = {
  errors: [],
  responses: [],
  window: {},
  spot: {},
  failure: {},
  cost: {},
};
const responseTasks = [];
try {
  for (let i = 0; i < 400; i++) {
    if (server.exitCode !== null) throw Error(log);
    try {
      if ((await fetch("http://127.0.0.1:4323/api/runs")).ok) break;
    } catch {}
    await new Promise((r) => setTimeout(r, 25));
  }
  browser = await chromium.launch({
    executablePath: process.env.CHROME_EXECUTABLE ?? "/usr/bin/google-chrome",
    headless: true,
  });
  const page = await browser.newPage({
    viewport: { width: 1600, height: 1150 },
    acceptDownloads: true,
  });
  page.on("pageerror", (e) => result.errors.push(e.message));
  page.on("response", (r) => {
    if (r.url().includes("/research?"))
      responseTasks.push(
        (async () =>
          result.responses.push({
            url: r.url().split("4323")[1],
            bytes: (await r.body()).length,
            status: r.status(),
          }))(),
      );
  });
  const nav = async (name) =>
    page.getByRole("button", { name, exact: true }).click();
  const selected = (path) => page.locator(`[data-research-path="${path}"]`);
  const expand = async (locator) => {
    for (let i = 0; i < 6; i++) {
      await locator
        .locator("details.tree")
        .evaluateAll((ds) => ds.forEach((d) => (d.open = true)));
      await page.evaluate(
        () =>
          new Promise((r) =>
            requestAnimationFrame(() => requestAnimationFrame(r)),
          ),
      );
    }
  };
  await page.goto("http://127.0.0.1:4323");
  await page
    .getByRole("heading", { name: "真实历史描述研究 · 只读原产物" })
    .waitFor();
  assert.match(
    await page.locator(".run-strip").textContent(),
    /真实历史描述研究/,
  );
  assert.doesNotMatch(
    await page.locator(".run-strip").textContent(),
    /backtest|RangeEnd|Ledger 工程/,
  );
  assert.match(
    await page.locator(".identity").textContent(),
    /22b7738cc5f44c5f4b18bb358a91ac23f81a7a15/,
  );
  const snapshotResponse = await fetch(
      "http://127.0.0.1:4323/api/runs/0/snapshot",
    ),
    snapshotBytes = (await snapshotResponse.arrayBuffer()).byteLength;
  const snap = await (
    await fetch("http://127.0.0.1:4323/api/runs/0/snapshot")
  ).json();
  assert.equal(snap.research.catalog.length, 14);
  assert.equal(snap.research.targets.length, 12);
  assert.equal(snap.research.bytes_read, "15253473");
  assert.equal(
    snap.research.summary.find((f) => f.path === "/result_summary").exact
      .sampling_points,
    "96",
  );
  assert.equal(
    snap.research.summary.find((f) => f.path === "/result_summary").exact
      .window_rows,
    "480",
  );
  assert.equal(snap.report.status, "not_configured");
  assert.equal(snap.research.raw_json, undefined);
  assert.equal(snap.research.exact, undefined);
  await page.evaluate(() => scrollTo(0, 0));
  await page.screenshot({
    path: path.join(output, "research-overview-desktop.png"),
    fullPage: true,
  });
  await page.getByLabel("研究目标").selectOption("1");
  await page.getByLabel("研究窗口索引").selectOption("4");
  const clock = await page.evaluate(() => performance.now());
  await nav("描述窗口");
  const windowPath =
    "/research/markets/1/frozen_sampling/0/descriptive_windows/4";
  await selected(windowPath).waitFor();
  await expand(selected(windowPath));
  const leaf = (key) =>
    selected(windowPath)
      .locator(".leaf")
      .filter({ has: page.getByRole("button", { name: key, exact: true }) })
      .locator("code");
  assert.equal(await leaf("price_pairs").textContent(), "280");
  assert.equal(await leaf("same_received_time_pairs").textContent(), "1");
  assert.equal(
    await leaf("squared_price_change_sum").textContent(),
    "510.3332819460326887224024",
  );
  assert.equal(
    await leaf("squared_change_per_second_description").textContent(),
    "无数据",
  );
  await page.evaluate(
    () =>
      new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r))),
  );
  result.cost.click_to_details_dom_and_two_frames_ms = await page.evaluate(
    (t) => performance.now() - t,
    clock,
  );
  result.window = {
    path: windowPath,
    price_pairs: "280",
    same_received_time_pairs: "1",
    squared_sum: "510.3332819460326887224024",
    descriptive_rate: null,
  };
  await page.evaluate(() => scrollTo(0, 0));
  await page.screenshot({
    path: path.join(output, "research-window-desktop.png"),
    fullPage: true,
  });
  await page.getByLabel("研究数组长度").fill("2");
  await nav("价格对原索引列");
  const pairPath = windowPath + "/price_pair_update_indices";
  await selected(pairPath).waitFor();
  assert.match(
    await selected(pairPath).textContent(),
    /原数组 \[0, 2\) \/ 280/,
  );
  await selected(pairPath)
    .getByText(/未读取的原数组/)
    .count();
  await nav("持价原区段列");
  const holdPath = windowPath + "/hold_coverage/segment_rows";
  await selected(holdPath).waitFor();
  await expand(selected(holdPath));
  assert.match(await selected(holdPath).textContent(), /1788912000000/);
  assert.match(await selected(holdPath).textContent(), /1788912002228/);
  assert.match(await selected(holdPath).textContent(), /2228/);
  assert.match(await selected(holdPath).textContent(), /false/);
  result.hold = {
    path: holdPath,
    first_original_row: ["1788912000000", "1788912002228", "2228", "0", false],
  };
  await page.getByLabel("研究数组起点").fill("1");
  await page.getByLabel("研究数组长度").fill("1");
  await nav("全局更新原区段");
  const updates = "/research/global_spot/updates_original_visible_order";
  await selected(updates).waitFor();
  await expand(selected(updates));
  const body = JSON.parse(
    await (
      await fetch(
        "http://127.0.0.1:4323/api/runs/0/research?" +
          new URLSearchParams({ path: updates, start: "1", count: "1" }),
      )
    ).text(),
  );
  const spot = body.rows[0].exact;
  assert.equal(body.rows[0].path, updates + "/1");
  assert.deepEqual(spot.source_event_at_utc_ms, ["1788911999000"]);
  assert.equal(spot.received_at_utc_ms, "1788912002228");
  assert.equal(spot.first_visible_at_utc_ms, "1788912002251");
  assert.equal(spot.group_position, "334");
  assert.match(await selected(updates).textContent(), /1788911999000/);
  assert.match(await selected(updates).textContent(), /1788912002228/);
  assert.match(await selected(updates).textContent(), /1788912002251/);
  await selected(updates)
    .locator("details.tree > summary")
    .first()
    .getByRole("button", { name: "查原值", exact: true })
    .click();
  await Promise.all(responseTasks);
  const reads = result.responses.length;
  await nav("多时钟时间线");
  await page.getByLabel("导出范围").selectOption("selection");
  const downloadPromise = page.waitForEvent("download");
  await nav("导出 Perfetto");
  const download = await downloadPromise;
  const tracePath = path.join(output, "research-spot.trace.json");
  await download.saveAs(tracePath);
  const trace = JSON.parse(await readFile(tracePath));
  const events = trace.traceEvents.filter((e) => e.ph === "i");
  assert.equal(events.length, 3);
  assert.deepEqual(
    events.map((e) => e.args.original_utc_ms).sort(),
    ["1788911999000", "1788912002228", "1788912002251"].sort(),
  );
  for (const e of events) {
    assert.equal(e.args.source, "research");
    assert.ok(e.args.reference.startsWith(updates + "/1/"));
    assert.equal(
      e.args.research_provenance.source_commit,
      "22b7738cc5f44c5f4b18bb358a91ac23f81a7a15",
    );
    assert.equal(e.args.condition_id, null);
    assert.equal(e.ph, "i");
  }
  result.spot = {
    group_position: "334",
    source_ms: "1788911999000",
    received_ms: "1788912002228",
    public_visible_ms: "1788912002251",
    trace_points: events.length,
  };
  await page.evaluate(() => scrollTo(0, 0));
  await page.screenshot({
    path: path.join(output, "research-timeline-desktop.png"),
    fullPage: true,
  });
  await nav("证据重放");
  assert.match(
    await page.locator(".evidence-panel").textContent(),
    /updates_original_visible_order\/1/,
  );
  assert.equal(await page.getByLabel("研究目标").inputValue(), "1");
  assert.equal(await page.getByLabel("研究窗口索引").inputValue(), "4");
  await nav("模块总览");
  assert.equal(result.responses.length, reads);
  await page.getByLabel("研究目标").selectOption("11");
  await nav("市场 / 事后标签");
  await selected("/research/markets/11").waitFor();
  await expand(selected("/research/markets/11"));
  assert.match(
    await selected("/research/markets/11").textContent(),
    /first_usable_terminal无数据/,
  );
  assert.match(
    await selected("/research/markets/11").textContent(),
    /latest_terminal_usablefalse/,
  );
  await page.getByLabel("研究数组起点").fill("0");
  await page.getByLabel("研究数组长度").fill("1");
  await selected("/research/markets/11")
    .locator("details:not(.tree) > summary")
    .click();
  const labelPath =
    "/research/markets/11/posthoc_label_qualification/changes_original_visible_order";
  await selected("/research/markets/11")
    .locator("code")
    .filter({ hasText: labelPath })
    .locator("..")
    .getByRole("button", { name: "读取原区段", exact: true })
    .click();
  await selected(labelPath).waitFor();
  await expand(selected(labelPath));
  assert.match(await selected(labelPath).textContent(), /unknown_not_observed/);
  result.terminal = {
    market: "4352956",
    first_usable_terminal: null,
    last_quality: "unknown_not_observed",
  };
  await page.getByLabel("运行选择").selectOption("1");
  await page.waitForFunction(() =>
    document.querySelector(".run-strip")?.textContent.includes("独立描述研究"),
  );
  assert.match(
    await page.locator(".run-strip").textContent(),
    /真实历史描述研究/,
  );
  assert.doesNotMatch(
    await page.locator(".run-strip").textContent(),
    /公共历史材料|backtest/,
  );
  for (const [index, status] of [
    ["2", "unsupported_version"],
    ["3", "read_error"],
  ]) {
    await page.getByLabel("运行选择").selectOption(index);
    await page.waitForFunction(
      (s) =>
        document.querySelector(".research-panel")?.dataset.researchStatus === s,
      status,
    );
    assert.equal(await page.locator("[data-research-path]").count(), 0);
    await nav("多时钟时间线");
    assert.match(await page.locator("main").textContent(), /0 个可证时间点/);
    await nav("模块总览");
    result.failure[index] = status;
  }
  result.cost.snapshot_bytes = snapshotBytes;
  result.cost.resource_timing = await page.evaluate(() =>
    performance
      .getEntriesByType("resource")
      .filter((e) => e.name.includes("/research?"))
      .map((e) => ({
        path: e.name.split("4323")[1],
        duration_ms: e.duration,
        transfer_bytes: e.transferSize,
        encoded_body_bytes: e.encodedBodySize,
        decoded_body_bytes: e.decodedBodySize,
      })),
  );
  assert.deepEqual(result.errors, []);
  await writeFile(
    path.join(output, "browser-result.json"),
    JSON.stringify(result, null, 2),
  );
  console.log(
    JSON.stringify(
      {
        errors: result.errors,
        window: result.window,
        spot: result.spot,
        cost: result.cost,
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
  await writeFile(path.join(output, "server.log"), log);
}
