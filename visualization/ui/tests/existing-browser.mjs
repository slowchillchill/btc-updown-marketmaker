// 显式真实工程/历史文件；独立原字段期待，Chrome/下载/综合交互成本。
import assert from "node:assert/strict";
import { chromium } from "playwright-core";
import { spawn } from "node:child_process";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
const workspace = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);
const [config, output] = process.argv.slice(2);
if (!output) throw Error("existing-browser.mjs sources.json output");
await mkdir(output, { recursive: true });
const server = spawn(
  path.join(
    process.env.CARGO_TARGET_DIR ?? path.join(workspace, "target"),
    "debug/visualization",
  ),
  [config, "127.0.0.1:4324", path.join(workspace, "visualization/ui/dist")],
  { stdio: ["ignore", "ignore", "pipe"] },
);
let browser,
  log = "";
server.stderr.on("data", (b) => (log += b));
const result = { errors: [], sources: [], cost: {}, downloads: [] };
try {
  for (let i = 0; i < 300; i++) {
    if (server.exitCode !== null) throw Error(log);
    try {
      if ((await fetch("http://127.0.0.1:4324/api/runs")).ok) break;
    } catch {}
    await new Promise((r) => setTimeout(r, 25));
  }
  browser = await chromium.launch({
    executablePath: "/usr/bin/google-chrome",
    headless: true,
  });
  const page = await browser.newPage({
    viewport: { width: 1600, height: 1050 },
    acceptDownloads: true,
  });
  page.on("pageerror", (e) => result.errors.push(String(e)));
  const nav = async (name) =>
    page.getByRole("button", { name, exact: true }).click();
  const panel = () => page.locator(".existing-panel");
  const expand = async (loc) => {
    for (let i = 0; i < 5; i++) {
      await loc
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
  const snapshot = async (index) => {
    const r = await fetch(`http://127.0.0.1:4324/api/runs/${index}/snapshot`);
    const b = await r.text();
    const s = JSON.parse(b);
    assert.equal(s.report.status, "not_configured");
    assert.equal(s.metadata.status, "not_configured");
    assert.equal(s.research.status, "not_configured");
    assert.equal(s.existing_source.status, "ready");
    result.sources.push({
      index,
      kind: s.existing_source.kind,
      snapshot_bytes: Buffer.byteLength(b),
      documents: s.existing_source.documents.map((d) => ({
        document: d.document,
        status: d.status,
        bytes_read: d.bytes_read,
      })),
    });
    return s;
  };
  const initialNavigation = performance.now();
  await page.goto("http://127.0.0.1:4324");
  await panel().waitFor();
  await page.evaluate(
    () =>
      new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r))),
  );
  result.cost.completion = {
    navigation_to_cards_two_frames_ms: performance.now() - initialNavigation,
    resource: await page.evaluate(() =>
      performance
        .getEntriesByType("resource")
        .filter((e) => e.name.includes("/snapshot"))
        .map((e) => ({
          duration_ms: e.duration,
          encoded_body_bytes: e.encodedBodySize,
          transfer_bytes: e.transferSize,
        })),
    ),
  };
  const large = await snapshot(0),
    l = large.existing_source.documents[0].summary.final_ledger;
  assert.equal(l.ledger_entry_seq, "10262");
  assert.equal(l.platform_source_prefix, "499");
  assert.equal(l.ledger_local_prefix, "4806");
  assert.equal(l.available_through_utc_ms, "1789517400990");
  assert.equal(l.platform_confirmed_projection.cash_pusd, "996.56973");
  assert.equal(l.platform_confirmed_projection.available_pusd, "965.37913");
  assert.equal(l.platform_confirmed_projection.fees_pusd, "0.21907");
  assert.equal(l.platform_confirmed_projection.realized_pnl_pusd, "0.0182196");
  assert.equal(
    l.local_pending_reservation_projection.after_send_unknown_count,
    "119",
  );
  assert.equal(
    l.platform_confirmed_projection
      .platform_confirmation_pending_economic_count,
    "49",
  );
  assert.equal(l.platform_confirmed_projection.open_order_count, "116");
  assert.equal(l.market_projections.length, 2);
  assert.doesNotMatch(
    await page.locator(".run-strip").textContent(),
    /backtest|RangeEnd|真实历史描述研究/,
  );
  assert.match(await panel().textContent(), /996.56973/);
  assert.equal(await page.getByLabel("市场选择").locator("option").count(), 3);
  await page.evaluate(() => scrollTo(0, 0));
  await page.screenshot({
    path: path.join(output, "large-completion-desktop.png"),
    fullPage: true,
  });
  await panel().locator(".projection-details > summary").click();
  await panel()
    .getByRole("button", { name: /可用截止 · 原 UTC ms/ })
    .click();
  assert.match(
    await page.locator(".evidence-panel").textContent(),
    /\/final_ledger\/available_through_utc_ms/,
  );
  await nav("多时钟时间线");
  await page.getByLabel("导出范围").selectOption("selection");
  const dlPromise = page.waitForEvent("download");
  await nav("导出 Perfetto");
  const dl = await dlPromise;
  await dl.saveAs(path.join(output, "completion-trace.json"));
  const trace = JSON.parse(
    await readFile(path.join(output, "completion-trace.json")),
  );
  const instants = trace.traceEvents.filter((e) => e.ph === "i");
  assert.equal(instants.length, 1);
  assert.equal(instants[0].args.original_utc_ms, "1789517400990");
  assert.equal(String(instants[0].ts), "1789517400990000");
  result.downloads.push({
    source: "completion",
    points: instants.length,
    original_utc_ms: instants[0].args.original_utc_ms,
  });
  await page.getByLabel("运行选择").selectOption("1");
  await nav("模块总览");
  await panel().waitFor();
  const small = await snapshot(1);
  assert.equal(
    small.existing_source.documents[0].summary.final_ledger
      .platform_confirmed_projection.cash_pusd,
    "1000",
  );
  assert.equal(
    small.existing_source.documents[0].summary.final_ledger
      .platform_confirmed_projection.available_pusd,
    "1000",
  );
  assert.equal(
    small.existing_source.documents[0].summary.final_ledger
      .platform_confirmed_projection.fees_pusd,
    "0",
  );
  await page.getByLabel("运行选择").selectOption("2");
  await panel().waitFor();
  const continuous = await snapshot(2);
  assert.equal(
    continuous.existing_source.documents.find((d) => d.document === "summary")
      .arrays[0].length,
    "48",
  );
  assert.equal(
    continuous.existing_source.documents.find((d) => d.document === "pooled")
      .arrays[0].length,
    "8",
  );
  assert.equal(
    continuous.existing_source.documents.find((d) => d.document === "calls")
      .arrays[0].length,
    "1024",
  );
  await nav("浏览单轮摘要");
  await page.locator('[data-existing-loaded="summary"]').waitFor();
  assert.equal(
    await page.getByLabel("原摘要记录").locator("option").count(),
    48,
  );
  const statisticCount = () =>
    panel().locator(".source-table tbody tr").first().locator("td").nth(1);
  assert.equal(await statisticCount().textContent(), "64");
  assert.match(
    await panel().locator(".source-table").textContent(),
    /18.86497059375/,
  );
  await page.getByLabel("原摘要记录").selectOption("/1");
  assert.equal(await statisticCount().textContent(), "1");
  await page.getByLabel("原摘要记录").selectOption("/2");
  assert.equal(await statisticCount().textContent(), "63");
  await nav("浏览两轮合并摘要");
  await page.locator('[data-existing-loaded="pooled"]').waitFor();
  assert.equal(
    await page.getByLabel("原摘要记录").locator("option").count(),
    8,
  );
  assert.equal(await statisticCount().textContent(), "128");
  assert.match(
    await panel().locator(".source-table").textContent(),
    /18.7270144375/,
  );
  await page.evaluate(() => scrollTo(0, 0));
  await page.screenshot({
    path: path.join(output, "continuous-summary-desktop.png"),
    fullPage: true,
  });
  await page.getByLabel("工程历史文档").selectOption("calls");
  await page.getByLabel("工程历史条数").fill("1");
  await page.evaluate(() => performance.clearResourceTimings());
  const click = await page.evaluate(() => performance.now());
  const responsePromise = page.waitForResponse(
    (r) => r.url().includes("/existing?") && r.url().includes("document=calls"),
  );
  await nav("读取原区段");
  const response = await responsePromise;
  await page.locator('[data-existing-loaded="calls"]').waitFor();
  await page.evaluate(
    () =>
      new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r))),
  );
  const ready = await page.evaluate(() => performance.now());
  await expand(page.locator('[data-existing-loaded="calls"]'));
  assert.match(
    await page.locator('[data-existing-loaded="calls"]').textContent(),
    /624252762/,
  );
  assert.match(
    await page.locator('[data-existing-loaded="calls"]').textContent(),
    /1789517395414/,
  );
  assert.match(
    await page.locator('[data-existing-loaded="calls"]').textContent(),
    /CSV行2/,
  );
  result.cost.continuous = {
    click_to_dom_two_frames_ms: ready - click,
    response_bytes: (await response.body()).length,
    resource: await page.evaluate(() =>
      performance
        .getEntriesByType("resource")
        .filter((e) => e.name.includes("/existing?"))
        .map((e) => ({
          duration_ms: e.duration,
          encoded_body_bytes: e.encodedBodySize,
          decoded_body_bytes: e.decodedBodySize,
          transfer_bytes: e.transferSize,
        })),
    ),
    rendered_records: await page
      .locator('[data-existing-loaded="calls"] .research-fragment')
      .count(),
  };
  let calls = 0;
  const countRequests = (r) => {
    if (r.url().includes("/existing?")) calls++;
  };
  page.on("request", countRequests);
  await nav("训练与实验");
  await nav("证据重放");
  assert.equal(calls, 0);
  page.off("request", countRequests);
  await page.evaluate(() => scrollTo(0, 0));
  await page.screenshot({
    path: path.join(output, "continuous-calls-desktop.png"),
    fullPage: true,
  });
  await page.getByLabel("运行选择").selectOption("3");
  await nav("模块总览");
  await panel().waitFor();
  const manifest = await snapshot(3),
    m = manifest.existing_source.documents[0].summary;
  assert.equal(m.utc_day, "2026-09-09");
  assert.equal(m.state, "closed");
  assert.equal(await panel().locator(".source-table tbody tr").count(), 7);
  assert.equal(
    await panel()
      .locator(".source-table tbody tr")
      .filter({ hasText: "polymarket_rtds_chainlink_btc_usd" })
      .locator("td")
      .nth(6)
      .textContent(),
    "11",
  );
  assert.equal(m.collection_day_coverage.state, "ended_known");
  assert.equal(
    m.source_counters.polymarket_rtds_chainlink_btc_usd.reconnect_events,
    "11",
  );
  assert.equal(
    m.source_counters.polymarket_rtds_chainlink_twap.reconnect_events,
    "11",
  );
  assert.deepEqual(m.gaps, []);
  assert.equal(
    manifest.existing_source.documents[0].arrays.find(
      (a) => a.path === "/files",
    ).length,
    "145",
  );
  await page.getByLabel("工程历史路径").fill("/files");
  await page.getByLabel("工程历史条数").fill("5");
  await page.evaluate(() => performance.clearResourceTimings());
  const before = await page.evaluate(() => performance.now());
  const wait = page.waitForResponse((r) => r.url().includes("/existing?"));
  await nav("读取原区段");
  const resp = await wait;
  await page.locator('[data-existing-loaded="manifest"]').waitFor();
  await page.evaluate(
    () =>
      new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r))),
  );
  result.cost.manifest = {
    click_to_dom_two_frames_ms:
      (await page.evaluate(() => performance.now())) - before,
    response_bytes: (await resp.body()).length,
    rendered_records: await page
      .locator('[data-existing-loaded="manifest"] .research-fragment')
      .count(),
    resource: await page.evaluate(() =>
      performance
        .getEntriesByType("resource")
        .filter((e) => e.name.includes("/existing?"))
        .map((e) => ({
          duration_ms: e.duration,
          encoded_body_bytes: e.encodedBodySize,
          transfer_bytes: e.transferSize,
        })),
    ),
  };
  assert.equal(result.cost.manifest.rendered_records, 5);
  const b = await resp.body();
  const retry = await fetch(resp.url());
  assert.equal(await retry.text(), b.toString());
  await page.evaluate(() => scrollTo(0, 0));
  await page.screenshot({
    path: path.join(output, "collection-manifest-desktop.png"),
    fullPage: true,
  });
  assert.deepEqual(result.errors, []);
} finally {
  if (browser) await browser.close();
  server.kill("SIGTERM");
  await new Promise((r) => server.once("exit", r));
  result.own_server_stopped = true;
  result.own_browser_closed = true;
  await writeFile(
    path.join(output, "browser.json"),
    JSON.stringify(result, null, 2),
  );
  await writeFile(path.join(output, "server.log"), log);
}
