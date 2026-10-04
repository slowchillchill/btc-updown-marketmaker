// 真实固定前缀仅读两页；其他变体来自所属核心及原写入接口生成的合法有限材料。
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
const [realPath, fixturePath, outPath] = process.argv.slice(2);
if (!realPath || !fixturePath)
  throw new Error(
    "用法：node tests/public-browser.mjs <真实 sources.json> <合法变体 sources.json> [截图目录]",
  );
const output = outPath && path.resolve(outPath);
if (output) await mkdir(output, { recursive: true });
const real = JSON.parse(await readFile(realPath, "utf8")),
  fixture = JSON.parse(await readFile(fixturePath, "utf8"));
assert.equal(real.runs[0].report, undefined);
const temp = await mkdtemp(path.join(tmpdir(), "public-evidence-browser-"));
const config = path.join(temp, "sources.json");
await writeFile(
  config,
  JSON.stringify({ runs: [...real.runs, ...fixture.runs] }),
);
const server = spawn(
  path.join(
    process.env.CARGO_TARGET_DIR ?? path.join(workspace, "target"),
    "debug/visualization",
  ),
  [config, "127.0.0.1:4319", path.join(workspace, "visualization/ui/dist")],
  { cwd: workspace, stdio: ["ignore", "ignore", "pipe"] },
);
let log = "";
server.stderr.on("data", (b) => (log += b));
let browser;
const errors = [],
  responses = [];
const api = async (p) => {
  const r = await fetch("http://127.0.0.1:4319" + p);
  assert.ok(r.ok, await r.clone().text());
  return r.json();
};
try {
  for (let n = 0; n < 200; n++) {
    if (server.exitCode !== null) throw new Error(log);
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
    acceptDownloads: true,
  });
  page.on("pageerror", (e) => errors.push(e.message));
  page.on("response", async (r) => {
    if (r.url().match(/\/api\/runs\/0\/public/))
      responses.push({ status: r.status(), bytes: (await r.body()).length });
  });
  await page.goto("http://127.0.0.1:4319");
  await page.locator(".module-card").nth(7).waitFor();
  if (!process.env.SKIP_REAL_PUBLIC) {
    const started = performance.now();
    await page.goto("http://127.0.0.1:4319");
    await page.locator(".module-card").nth(7).waitFor();
    const snapshot = await api("/api/runs/0/snapshot");
    assert.equal(snapshot.report.status, "not_configured");
    assert.equal(snapshot.metadata.status, "not_configured");
    assert.equal(snapshot.public_recording.status, "ready");
    const stream = snapshot.public_recording.header.input_stream_id;
    assert.match(await page.locator("h1").textContent(), /公共流总览/);
    assert.ok((await page.locator(".identity").textContent()).includes(stream));
    assert.ok(
      !(await page.locator(".run-strip").textContent()).includes("backtest"),
    );
    await page.getByRole("button", { name: "读取当前页", exact: true }).click();
    await page.getByText(/页 0 · 本页/).waitFor();
    const first = await api("/api/runs/0/public");
    assert.equal(first.cost.next_page_calls, "1");
    assert.equal(first.cost.reader_open_count, "1");
    assert.equal(first.returned_rows, "100");
    const initialV6 = first.rows.findIndex(
      (r) =>
        r.Input?.ingress?.Message?.row?.PolymarketMarketMetadataV6?.Existing,
    );
    assert.ok(initialV6 >= 0, "真实有限页应包含 V6 Existing");
    const market =
      first.rows[initialV6].Input.ingress.Message.row.PolymarketMarketMetadataV6
        .Existing.condition_id;
    assert.ok(
      (
        await page.getByLabel("市场选择").locator("option").allTextContents()
      ).includes(market),
    );
    assert.ok(
      (
        await page.locator("table tbody tr").nth(initialV6).textContent()
      ).includes(market),
    );
    await page
      .locator("table tbody tr")
      .nth(initialV6)
      .getByRole("button")
      .click();
    const retained = await page.locator(".evidence-panel pre").textContent();
    await page.getByRole("button", { name: "下一页", exact: true }).click();
    await page.getByText(/页 1 · 本页/).waitFor();
    const second = await api("/api/runs/0/public");
    assert.equal(second.row_start, "100");
    assert.equal(second.cost.next_page_calls, "2");
    assert.equal(second.cost.reader_open_count, "1");
    assert.equal(second.returned_rows, "100");
    assert.equal(
      await page.locator(".evidence-panel pre").textContent(),
      retained,
    );
    const old = await fetch("http://127.0.0.1:4319/api/runs/0/public?page=0");
    assert.equal(old.status, 409);
    assert.deepEqual(await api("/api/runs/0/public"), second);
    await page.getByRole("button", { name: "证据重放", exact: true }).click();
    await page.evaluate(() => window.scrollTo(0, 0));
    if (output)
      await page.screenshot({
        path: path.join(output, "real-public-desktop.png"),
        fullPage: false,
      });
    console.log(
      JSON.stringify({
        real_read: {
          pages: 2,
          rows: 200,
          first_v6_market: market,
          input_stream_id: stream,
          reader_open_count: second.cost.reader_open_count,
          next_page_calls: second.cost.next_page_calls,
          first_rows_json_bytes: first.cost.rows_json_bytes,
          second_rows_json_bytes: second.cost.rows_json_bytes,
          browser_elapsed_ms: Math.round(performance.now() - started),
          underlying_io_and_allocations: "未实测",
        },
      }),
    );
  }
  await page.getByLabel("运行选择").selectOption("1");
  await page
    .locator(".run-strip")
    .getByText("公共变体验证 · 非研究/策略运行", { exact: true })
    .waitFor();
  await page.getByRole("button", { name: "读取当前页", exact: true }).click();
  await page.getByText(/页 0 · 本页/).waitFor();
  const finite = await api("/api/runs/1/public");
  assert.equal(finite.at_end, true);
  assert.equal(finite.prefix.next_input_position, "8");
  assert.equal(finite.prefix.next_canonical_position, "17");
  assert.equal(
    await page.locator("table tbody tr").count(),
    finite.rows.length,
  );
  assert.deepEqual(
    await page.locator("table tbody tr td:first-child").allTextContents(),
    finite.rows.map((r) => Object.keys(r)[0]),
  );
  const refs = finite.rows
    .filter((r) => r.Canonical?.record?.ReferenceEvent)
    .map((r) => r.Canonical.record.ReferenceEvent);
  assert.ok(
    refs.some(
      (r) =>
        r.payload.reference_type === "chainlink_btc_usd_twap" &&
        !r.payload.reference.market,
    ),
  );
  assert.ok(
    refs.some(
      (r) =>
        r.payload.reference_type === "market_price_anchor_update" &&
        r.payload.reference.market.condition_id,
    ),
  );
  const historical = refs.filter(
    (r) => r.payload.reference.anchor?.application.kind === "historical_repair",
  );
  assert.equal(historical.length, 2);
  assert.deepEqual(
    historical.map(
      (r) => r.payload.reference.anchor.application.effective_from_utc_ms,
    ),
    ["20000", "20000"],
  );
  await page.getByRole("button", { name: "多时钟时间线", exact: true }).click();
  await page.locator(".timeline canvas").waitFor();
  const dp = page.waitForEvent("download");
  await page
    .getByRole("button", { name: "导出 Perfetto", exact: true })
    .click();
  const download = await dp;
  const tracePath = path.join(output ?? temp, "public-finite.trace.json");
  await download.saveAs(tracePath);
  const trace = JSON.parse(await readFile(tracePath, "utf8"));
  const points = trace.traceEvents.filter((e) => e.ph === "i");
  assert.ok(points.length > 0);
  assert.equal(trace.metadata.input_stream_id, "engineering-public-evidence");
  assert.equal(trace.metadata.run_id, undefined);
  assert.equal(trace.metadata.account_identity, undefined);
  // 每个导出点与引用必须真实回查到原 row 路径，不能制造展示字段位置。
  const originalValue = (p) => {
    const segments = p.split("/").filter(Boolean);
    assert.deepEqual(segments.slice(0, 2), ["public", "0"]);
    return segments
      .slice(3)
      .reduce((v, k) => v?.[k], finite.rows[Number(segments[2])]);
  };
  for (const event of points) {
    assert.equal(
      originalValue(event.args.reference),
      event.args.original_utc_ms,
    );
    for (const ref of event.args.native_references)
      assert.deepEqual(originalValue(ref.path), ref.value);
  }
  const pmEvents = finite.rows
    .filter((r) => r.Canonical?.record?.PolymarketMessage)
    .map((r) => r.Canonical.record.PolymarketMessage);
  assert.equal(pmEvents.length, 2);
  assert.deepEqual(
    pmEvents.map((r) => r.time.received_at_utc_ms),
    ["22000", "22000"],
  );
  assert.deepEqual(
    pmEvents.map((r) => r.time.source_event_at_utc_ms),
    ["21997", "21996"],
  );
  assert.notEqual(pmEvents[0].group_id, pmEvents[1].group_id);
  const byPath = (p) => points.filter((e) => e.args.reference === p);
  for (let i = 0; i < finite.rows.length; i++) {
    const c = finite.rows[i].Canonical;
    if (!c) continue;
    const variant = Object.keys(c.record)[0],
      native = c.record[variant],
      base = `/public/0/${i}/Canonical/record/${variant}`;
    for (const field of ["time", "direct_time", "dependency_time"])
      for (const time of ["source_event_at_utc_ms", "received_at_utc_ms"]) {
        if (native[field]?.[time] == null) continue;
        const matches = byPath(`${base}/${field}/${time}`);
        assert.equal(matches.length, 1);
        assert.equal(matches[0].args.original_utc_ms, native[field][time]);
        assert.equal(
          BigInt(matches[0].ts),
          BigInt(native[field][time]) * 1000n,
        );
      }
    if (variant === "ReferenceEvent")
      assert.equal(
        byPath(`${base}/time/received_at_utc_ms`)[0].args.condition_id,
        native.payload.reference.market?.condition_id ?? null,
      );
  }
  const effective = points.filter((e) => e.cat === "历史生效");
  assert.equal(effective.length, 2);
  assert.ok(effective.every((e) => e.args.original_utc_ms === "20000"));
  const receipt = points.filter((e) => e.cat === "历史补查接收");
  assert.equal(receipt.length, 3);
  assert.ok(receipt.every((e) => e.args.original_utc_ms === "1788840000123"));
  assert.ok(
    points.some(
      (e) => e.cat === "请求时钟" && e.args.original_utc_ms === "21990",
    ),
  );
  assert.ok(!points.some((e) => e.ph === "X"));
  if (process.env.TRACE_PROCESSOR) {
    const result = execFileSync(
      process.env.TRACE_PROCESSOR,
      [
        "query",
        tracePath,
        "SELECT COUNT(*) AS points, MIN(ts) AS first_ns, MAX(ts) AS last_ns FROM slice;",
      ],
      { encoding: "utf8" },
    );
    assert.match(result, new RegExp(`\\n${points.length},`));
    assert.ok(result.includes("9990000000"));
    assert.ok(result.includes("1788840000123000000"));
    const original = execFileSync(
      process.env.TRACE_PROCESSOR,
      [
        "query",
        tracePath,
        "SELECT COUNT(*) AS rows FROM slice s JOIN args a USING(arg_set_id) WHERE a.key='args.original_utc_ms' AND a.string_value='20000';",
      ],
      { encoding: "utf8" },
    );
    assert.match(original, /\n2\s*$/);
    if (output)
      await writeFile(
        path.join(output, "perfetto-public.txt"),
        result + "\n" + original,
      );
    console.log(
      "官方 Perfetto：" + result.trim() + " 原生效 args " + original.trim(),
    );
  }
  await page.getByLabel("区间起点 UTC ms").fill("22000");
  await page.getByLabel("区间终点 UTC ms").fill("22000");
  const rp = page.waitForEvent("download");
  await page
    .getByRole("button", { name: "导出 Perfetto", exact: true })
    .click();
  const rd = await rp;
  const rangePath = path.join(temp, "range.trace.json");
  await rd.saveAs(rangePath);
  const range = JSON.parse(
    await readFile(rangePath, "utf8"),
  ).traceEvents.filter((e) => e.ph === "i");
  assert.ok(range.length > 0);
  assert.ok(range.every((e) => e.args.original_utc_ms === "22000"));
  assert.ok(
    new Set(range.flatMap((e) => e.args.native_identities.map((i) => i.key)))
      .size > 1,
  );
  await page.getByRole("button", { name: "清除时间范围", exact: true }).click();
  await page
    .getByLabel("市场选择")
    .selectOption(
      "0x1111111111111111111111111111111111111111111111111111111111111111",
    );
  const mp = page.waitForEvent("download");
  await page
    .getByRole("button", { name: "导出 Perfetto", exact: true })
    .click();
  const md = await mp;
  const marketPath = path.join(temp, "market.trace.json");
  await md.saveAs(marketPath);
  const mt = JSON.parse(await readFile(marketPath, "utf8")).traceEvents.filter(
    (e) => e.ph === "i",
  );
  assert.ok(
    mt.every(
      (e) =>
        e.args.condition_id ===
        "0x1111111111111111111111111111111111111111111111111111111111111111",
    ),
  );
  await page.getByLabel("市场选择").selectOption("");
  // ECharts 默认入场动画结束后核验真实可见点；不改变来源或推进游标。
  await page.waitForTimeout(1100);
  await page.evaluate(() => window.scrollTo(0, 0));
  if (output)
    await page.screenshot({
      path: path.join(output, "finite-public-timeline.png"),
      fullPage: false,
    });
  await page.getByRole("button", { name: "证据重放", exact: true }).click();
  const obs = finite.rows.findIndex((r) => r.Observation);
  await page.locator("table tbody tr").nth(obs).getByRole("button").click();
  assert.ok((await page.locator(".native-links button").count()) > 0);
  await page.locator(".native-links button").first().click();
  assert.match(
    await page.locator(".evidence-panel pre").textContent(),
    /Group/,
  );
  const pm = finite.rows.findIndex((r) => r.PmStepBinding);
  assert.ok(pm >= 0);
  await page.locator("table tbody tr").nth(pm).getByRole("button").click();
  assert.match(
    await page.locator(".evidence-panel").textContent(),
    /consumer_stream_id/,
  );
  await page.locator(".native-links button").first().click();
  assert.match(
    await page.locator(".evidence-panel pre").textContent(),
    /Canonical/,
  );
  const binding = finite.rows.findIndex((r) => r.SourceBinding);
  await page.locator("table tbody tr").nth(binding).getByRole("button").click();
  await page.locator(".native-links button").first().click();
  assert.match(
    await page.locator(".evidence-panel pre").textContent(),
    /Input/,
  );
  await page.getByLabel("运行选择").selectOption("2");
  await page
    .locator(".run-strip")
    .getByText("公共跨页验证 · 非研究/策略运行", { exact: true })
    .waitFor();
  await page.getByRole("button", { name: "读取当前页", exact: true }).click();
  await page.getByText(/页 0 · 本页/).waitFor();
  const small = await api("/api/runs/2/public");
  const group = small.rows.findIndex((r) => r.Input?.ingress?.Message);
  assert.ok(group >= 0);
  await page.locator("table tbody tr").nth(group).getByRole("button").click();
  const selected = await page.locator(".evidence-panel pre").textContent();
  await page.getByRole("button", { name: "下一页", exact: true }).click();
  await page.getByText(/页 1 · 本页/).waitFor();
  assert.equal(
    await page.locator(".evidence-panel pre").textContent(),
    selected,
  );
  assert.ok((await page.locator(".native-links .gap").count()) > 0);
  assert.match(
    await page.locator(".native-links").textContent(),
    /目标未在当前页或保留选择中读取/,
  );
  const reload = await api("/api/runs/2/public");
  assert.equal(reload.cost.next_page_calls, "2");
  assert.equal(reload.cost.reader_open_count, "1");
  await page.evaluate(() => window.scrollTo(0, 0));
  if (output)
    await page.screenshot({
      path: path.join(output, "public-cross-page-reference.png"),
      fullPage: false,
    });
  if (output)
    await page
      .locator(".evidence-panel")
      .screenshot({ path: path.join(output, "public-reference-detail.png") });
  await page.getByRole("button", { name: "多时钟时间线", exact: true }).click();
  await page.getByLabel("导出范围").selectOption("selection");
  const selectedDownload = page.waitForEvent("download");
  await page
    .getByRole("button", { name: "导出 Perfetto", exact: true })
    .click();
  const sd = await selectedDownload,
    selectedPath = path.join(temp, "retained-selection.trace.json");
  await sd.saveAs(selectedPath);
  const selectedPoints = JSON.parse(
    await readFile(selectedPath, "utf8"),
  ).traceEvents.filter((e) => e.ph === "i");
  assert.equal(selectedPoints.length, 3);
  assert.deepEqual(
    selectedPoints.map((e) => e.args.original_utc_ms),
    ["10000", "9990", "10001"],
  );
  assert.ok(
    selectedPoints.every((e) =>
      e.args.reference.startsWith(`/public/0/${group}/Input/`),
    ),
  );
  assert.deepEqual(errors, []);
  console.log(
    JSON.stringify({
      finite_rows: finite.rows.length,
      finite_trace_points: points.length,
      same_ms_range_points: range.length,
      real_http_responses: responses,
      errors,
    }),
  );
} finally {
  await browser?.close();
  server.kill("SIGTERM");
  await new Promise((r) =>
    server.exitCode !== null ? r() : server.once("exit", r),
  );
  await rm(temp, { recursive: true, force: true });
}
