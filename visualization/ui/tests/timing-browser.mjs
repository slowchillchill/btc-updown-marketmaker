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

  let detailRequests = 0;
  page.on("request", (r) => {
    if (r.url().includes("/existing?")) detailRequests++;
  });
  const ready = async () =>
    page.evaluate(
      () =>
        new Promise((r) =>
          requestAnimationFrame(() => requestAnimationFrame(r)),
        ),
    );
  const read = async (document, p, start = "0", count = "2") => {
    await nav("证据重放");
    await page.getByLabel("工程历史文档").selectOption(document);
    await page.getByLabel("工程历史路径").fill(p);
    await page.getByLabel("工程历史起点").fill(start);
    await page.getByLabel("工程历史条数").fill(count);
    const before = await page.evaluate(() => performance.now());
    const wait = page.waitForResponse((r) => r.url().includes("/existing?"));
    await nav("读取原区段");
    const response = await wait;
    const data = await response.json();
    assert.equal(data.path, p);
    await page.waitForFunction(
      (p) =>
        document
          .querySelector("[data-existing-loaded] h3")
          ?.textContent.includes((p || "/") + " "),
      p,
    );
    await ready();
    const elapsed = await page.evaluate(
      (before) => performance.now() - before,
      before,
    );
    const resource = await page.evaluate((url) => {
      const e = performance.getEntriesByName(url).at(-1);
      return e
        ? {
            duration_ms: e.duration,
            encoded_body_bytes: e.encodedBodySize,
            decoded_body_bytes: e.decodedBodySize,
            transfer_bytes: e.transferSize,
          }
        : null;
    }, response.url());
    return { data, cost: { click_to_detail_two_frames_ms: elapsed, resource } };
  };
  const selectedTrace = async (name) => {
    const requests = detailRequests;
    await nav("多时钟时间线");
    await page.getByLabel("导出范围").selectOption("selection");
    const wait = page.waitForEvent("download");
    await nav("导出 Perfetto");
    const d = await wait;
    const file = path.join(output, name + ".json");
    await d.saveAs(file);
    const trace = JSON.parse(await readFile(file));
    const points = trace.traceEvents.filter((e) => e.ph === "i");
    assert.equal(detailRequests, requests);
    result.downloads.push({ name, points: points.length });
    return points;
  };
  await page.goto("http://127.0.0.1:4324");
  await panel().waitFor();
  for (let i = 0; i < 2; i++) {
    const response = await fetch(
      `http://127.0.0.1:4324/api/runs/${i}/snapshot`,
    );
    const raw = await response.text();
    const s = JSON.parse(raw);
    assert.equal(s.existing_source.status, "ready");
    assert.equal(s.report.status, "not_configured");
    assert.equal(s.public_recording.status, "not_configured");
    result.sources.push({
      index: i,
      kind: s.existing_source.kind,
      snapshot_bytes: Buffer.byteLength(raw),
      documents: s.existing_source.documents.map((d) => ({
        document: d.document,
        status: d.status,
        bytes_read: d.bytes_read,
        arrays: d.arrays,
      })),
    });
  }
  assert.match(
    await page.locator("header").innerText(),
    /有限模型宿主工程诊断/,
  );
  await page.evaluate(() => scrollTo(0, 0));
  await page.screenshot({
    path: path.join(output, "model-timing-desktop.png"),
    fullPage: true,
  });
  let r = await read("trace", "", "6", "1");
  assert.equal(r.data.total, "49");
  assert.equal(r.data.rows[0].path, "/6");
  assert.equal(r.data.rows[0].exact.compute_wall_ns, "10084179");
  assert.equal(r.data.rows[0].exact.economic_delay_ms, "50");
  assert.equal(r.data.rows[0].exact.send, null);
  result.cost.timing = r.cost;
  await page
    .getByRole("button", { name: "选择原记录 /6", exact: true })
    .click();
  let points = await selectedTrace("model-completion");
  assert.equal(points.length, 3);
  assert.equal(
    points[0].args.existing_source_identity.fields.production_revision,
    "7cdb5e971571817239e95af50b5e97bb4def53e9",
  );
  assert.deepEqual(
    new Set(points.map((p) => p.args.original_utc_ms)),
    new Set(["86100000", "86100050"]),
  );
  assert.ok(
    points.every((p) => p.args.clock_kind === "工程运行逻辑 ms · 非真实日期"),
  );
  assert.ok(
    points.every(
      (p) =>
        !p.args.reference.includes("compute_wall_ns") &&
        !p.args.reference.includes("economic_delay_ms"),
    ),
  );
  r = await read("trace", "", "47", "1");
  assert.equal(r.data.rows[0].exact.next_required.at, "86401011");
  await page
    .getByRole("button", { name: "选择原记录 /47", exact: true })
    .click();
  points = await selectedTrace("future-required");
  assert.ok(
    points.find(
      (p) =>
        p.args.reference.endsWith("/next_required/at") &&
        p.args.clock_kind === "未来必需工程逻辑 ms · 非已发生",
    ),
  );
  r = await read("feedback", "", "0", "1");
  assert.equal(r.data.total, "4");
  assert.equal(r.data.rows[0].exact.request_id, "time-controlled-buy");
  assert.match(r.data.rows[0].exact.source_reference, /86100068/);
  await page
    .getByRole("button", { name: "选择原记录 /0", exact: true })
    .click();
  await nav("多时钟时间线");
  await page.getByLabel("导出范围").selectOption("selection");
  assert.ok(
    await page
      .getByRole("button", { name: "导出 Perfetto", exact: true })
      .isDisabled(),
  );
  r = await read("time_sim", "", "0", "1");
  assert.equal(r.data.total, "84");
  assert.equal(r.data.rows[0].exact.body.projection_version, "V4");
  assert.equal(
    r.data.rows[0].exact.body.implementation_identity.git_commit,
    "a".repeat(40),
  );
  r = await read("time_sim", "/32/body/inputs", "0", "1");
  assert.equal(r.data.rows[0].exact.input, "PUBLIC_REQUEST");
  assert.equal(
    r.data.rows[0].exact.body.send_time.logical_time_utc_ms,
    "86100050",
  );
  await page
    .getByRole("button", { name: "选择原记录 /32/body/inputs/0", exact: true })
    .click();
  points = await selectedTrace("sim-request");
  assert.equal(
    points.filter((p) =>
      p.args.reference.endsWith("/send_time/logical_time_utc_ms"),
    ).length,
    1,
  );
  await page.getByLabel("运行选择").selectOption("1");
  await nav("证据重放");
  await panel().waitFor();
  assert.match(await panel().innerText(), /十二分钟完整流程测量报告/);
  assert.match(await panel().innerText(), /199112/);
  assert.match(await panel().innerText(), /exclusive_cpu_seconds/);
  r = await read("measurement", "/execution/timeline");
  assert.equal(
    r.data.exact.material_launcher_utc,
    "2026-10-04T04:16:05.737613+00:00",
  );
  result.cost.measurement = r.cost;
  await page
    .locator("[data-existing-loaded] summary")
    .first()
    .getByRole("button", { name: "查原值", exact: true })
    .click();
  points = await selectedTrace("remeasurement-timeline");
  assert.equal(points.length, 4);
  const first = points.find((p) => p.name === "material_launcher_utc");
  assert.equal(
    first.args.existing_source_identity.fields.measurement_source_commit,
    "10a8ce588f2b2d6e192253ef90e4205733a54160",
  );
  assert.equal(
    first.args.original_utc_text,
    "2026-10-04T04:16:05.737613+00:00",
  );
  assert.equal(first.args.original_utc_us, "1791087365737613");
  assert.equal(String(first.ts), "1791087365737613");
  assert.equal(first.args.original_utc_ms, null);
  await nav("证据重放");
  await page.evaluate(() => scrollTo(0, 0));
  await page.screenshot({
    path: path.join(output, "remeasurement-desktop.png"),
    fullPage: true,
  });
  r = await read("measurement", "/execution/timeline/material_launcher_utc");
  await page
    .locator("[data-existing-loaded]")
    .getByRole("button", { name: "原字段详情", exact: true })
    .click();
  points = await selectedTrace("remeasurement-time-leaf");
  assert.equal(points.length, 1);
  assert.equal(points[0].args.original_utc_us, first.args.original_utc_us);
  assert.equal(points[0].args.reference, first.args.reference);
  r = await read(
    "measurement",
    "/execution/phases/two_market_research/new/exclusive_cpu_seconds",
  );
  assert.equal(r.data.exact, null);
  r = await read("measurement", "/complete_result_comparison", "1", "1");
  assert.equal(r.data.rows[0].path, "/complete_result_comparison/1");
  assert.equal(r.data.rows[0].exact.difference_count, "0");
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
