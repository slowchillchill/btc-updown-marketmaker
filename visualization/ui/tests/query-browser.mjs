// 真实读取接口 + #219 原合法有限材料；查询/取消/虚拟卸载/播放的实际浏览器反例。
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
const [fixture, output] = process.argv.slice(2);
if (!fixture || !output)
  throw new Error("用法：query-browser.mjs <合法 sources.json> <忽略截图目录>");
await mkdir(output, { recursive: true });
const input = JSON.parse(await readFile(fixture, "utf8")),
  source = input.runs[0].public_recording;
const temp = await mkdtemp(path.join(tmpdir(), "public-query-browser-"));
const config = path.join(temp, "sources.json");
await writeFile(
  config,
  JSON.stringify({
    runs: [10, 200, 10, 10, 10].map((page_rows, i) => ({
      label: `查询工程验证 ${i} · 非研究/策略运行`,
      public_recording: { ...source, page_rows },
    })),
  }),
);
const server = spawn(
  path.join(
    process.env.CARGO_TARGET_DIR ?? path.join(workspace, "target"),
    "debug/visualization",
  ),
  [config, "127.0.0.1:4321", path.join(workspace, "visualization/ui/dist")],
  { stdio: ["ignore", "ignore", "pipe"] },
);
let browser,
  log = "";
server.stderr.on("data", (b) => (log += b));
const results = {
  errors: [],
  requests: [],
  search: [],
  virtual: {},
  playback: {},
  cancel: {},
  retry: {},
  multiple_windows: {},
};
const api = async (url) => {
  const r = await fetch("http://127.0.0.1:4321" + url);
  return { status: r.status, body: await r.json() };
};
try {
  for (let i = 0; i < 200; i++) {
    if (server.exitCode !== null) throw new Error(log);
    try {
      if ((await api("/api/runs")).status === 200) break;
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
  page.on("pageerror", (e) => results.errors.push(e.message));
  page.on("request", (r) => {
    if (r.url().includes("/public"))
      results.requests.push(r.url().split("4321")[1]);
  });
  page.on("response", async (r) => {
    if (r.url().includes("/api/runs/0/public-query") && r.status() === 200) {
      const d = await r.json();
      results.search.push({
        page: d.batch.page,
        scanned: d.matching.scanned_rows,
        offsets: d.matching.matched_offsets,
        unknown: d.matching.unknown_rows,
        source_events: d.batch.rows.flatMap(
          (r) =>
            r.Input?.ingress?.Message?.source_evidence
              ?.source_event_at_utc_ms ?? [],
        ),
      });
    }
  });
  await page.goto("http://127.0.0.1:4321");
  await page.locator(".module-card").nth(7).waitFor();
  await page.getByRole("button", { name: "读取当前页", exact: true }).click();
  await page.getByText(/页 0 · 本页/).waitFor();
  await page.getByLabel("公共记录类型").selectOption("Input");
  await page.getByLabel("公共原时钟").selectOption("源消息 · 来源事件");
  await page.getByLabel("公共时间起点").fill("21996");
  await page.getByLabel("公共时间终点").fill("21996");
  await page
    .getByRole("button", { name: "从当前游标查找下一批匹配", exact: true })
    .click();
  await page
    .locator(".query-progress")
    .filter({ hasText: "已找到匹配批" })
    .waitFor();
  await page
    .getByRole("button", { name: "下一页", exact: true })
    .waitFor({ state: "visible" });
  const found = (await api("/api/runs/0/public")).body;
  const expected = found.rows.findIndex(
    (r) =>
      r.Input?.ingress?.Message?.source_evidence?.source_event_at_utc_ms ===
      "21996",
  );
  assert.ok(expected >= 0);
  assert.ok(
    results.search.slice(1, -1).some((r) => r.offsets.length === 0),
    "须实际经过空匹配批",
  );
  assert.ok(
    results.search
      .slice(1, -1)
      .some((r) => r.source_events.some((v) => BigInt(v) > 21996n)),
    "时间超过上界不能早停，后续倒退仍命中",
  );
  assert.equal(
    await page.locator("tbody tr").first().getAttribute("data-row-path"),
    `/public/${found.row_start}/${expected}`,
  );
  results.search_summary = {
    found_page: found.page,
    original_offset: expected,
    reader_open_count: found.cost.reader_open_count,
    next_page_calls: found.cost.next_page_calls,
  };
  assert.equal(found.cost.reader_open_count, "1");
  // 整批当前保留 + 虚拟卸载后选择仍绑定原对象。
  async function choose(run) {
    await page.getByLabel("运行选择").selectOption(run);
    await page
      .locator(".run-strip")
      .getByText(`查询工程验证 ${run} · 非研究/策略运行`, { exact: true })
      .waitFor();
  }
  await choose("1");
  await page.getByRole("button", { name: "读取当前页", exact: true }).click();
  await page.getByText(/页 0 · 本页/).waitFor();
  const whole = (await api("/api/runs/1/public")).body;
  assert.equal(whole.rows.length, 63);
  const count = await page.locator("tbody tr").count();
  assert.ok(count > 0 && count < 63);
  await page.locator('tr[data-row-path="/public/0/0"] button').click();
  const retained = await page.locator(".evidence-panel pre").textContent();
  await page
    .locator(".table-scroll")
    .evaluate((e) => (e.scrollTop = e.scrollHeight));
  await page.waitForTimeout(80);
  assert.equal(
    await page.locator('tr[data-row-path="/public/0/0"]').count(),
    0,
  );
  assert.equal(
    await page.locator(".evidence-panel pre").textContent(),
    retained,
  );
  results.virtual = {
    retained: 63,
    dom_rows: count,
    unmounted_selection_preserved: true,
  };
  await page.getByLabel("公共记录类型").selectOption("Input");
  await page.locator(".table-scroll").evaluate((e) => (e.scrollTop = 0));
  await page.waitForTimeout(50);
  const firstInput = whole.rows.findIndex((r) => r.Input);
  assert.ok(firstInput > 0);
  assert.equal(
    await page.locator("tbody tr").first().getAttribute("data-row-path"),
    `/public/0/${firstInput}`,
    "过滤保留原偏移而非压缩 i",
  );
  results.virtual.filtered_original_offset = firstInput;
  await page.getByLabel("浏览记录每秒").selectOption("10");
  const requestCount = results.requests.length;
  await page.getByRole("button", { name: "播放已读事实", exact: true }).click();
  await page.locator("[data-playback-path]").waitFor();
  await page.getByRole("button", { name: "暂停浏览", exact: true }).click();
  const paused = await page
    .locator("[data-playback-path]")
    .getAttribute("data-playback-path");
  await page.waitForTimeout(220);
  assert.equal(
    await page
      .locator("[data-playback-path]")
      .getAttribute("data-playback-path"),
    paused,
  );
  await page.getByRole("button", { name: "播放已读事实", exact: true }).click();
  await page
    .locator(".history-playback")
    .filter({ hasText: "已到本范围末尾" })
    .waitFor();
  assert.equal(
    results.requests.length,
    requestCount,
    "播放/暂停/末尾不产生网络请求",
  );
  assert.equal(
    await page.locator(".react-flow__edge.animated").count(),
    0,
    "无证据业务路径不能脉冲",
  );
  results.playback = {
    records: whole.rows.filter((r) => r.Input).length,
    requests_during_playback: 0,
    pause_preserved: true,
    last_path: await page
      .locator("[data-playback-path]")
      .getAttribute("data-playback-path"),
  };
  await page.getByRole("button", { name: "清除公共条件", exact: true }).click();
  await page.locator(".table-scroll").evaluate((e) => (e.scrollTop = 0));
  await page.waitForTimeout(50);
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({
    path: path.join(output, "query-overview-desktop.png"),
    fullPage: true,
  });
  // 服务器已完成这一批，浏览器尚未收到：取消只停止下一请求，保留在途结果。
  await choose("2");
  let release, received;
  const waiting = new Promise((r) => (received = r)),
    gate = new Promise((r) => (release = r));
  await page.route("**/api/runs/2/public-query?**", async (route) => {
    const r = await route.fetch();
    received();
    await gate;
    await route.fulfill({ response: r });
  });
  await page.getByLabel("公共记录类型").selectOption("Input");
  await page.getByLabel("公共位置域").selectOption("input_position");
  await page.getByLabel("位置起点").fill("6");
  const beforeCancel = results.requests.length;
  await page
    .getByRole("button", { name: "从当前游标查找下一批匹配", exact: true })
    .click();
  await waiting;
  await page.getByRole("button", { name: "取消后续查找", exact: true }).click();
  release();
  await page
    .locator(".query-progress")
    .filter({ hasText: "在途批已保留" })
    .waitFor();
  await page.waitForTimeout(80);
  assert.equal(results.requests.length, beforeCancel + 1);
  const cancelled = (await api("/api/runs/2/public")).body;
  assert.equal(cancelled.cost.next_page_calls, "1");
  results.cancel = {
    request_count: 1,
    retained_page: cancelled.page,
    next_page_calls: cancelled.cost.next_page_calls,
  };
  await page.unroute("**/api/runs/2/public-query?**");
  // 网络失败发生在服务端完成后，同一期望页重试应只回最近编码批。
  await choose("3");
  await page.route(
    "**/api/runs/3/public-query?**",
    async (route) => {
      await route.fetch();
      await route.abort("failed");
    },
    { times: 1 },
  );
  await page.getByRole("button", { name: "读取当前页", exact: true }).click();
  await page
    .getByRole("button", { name: "重试最近期望批", exact: true })
    .waitFor();
  const interrupted = (await api("/api/runs/3/public")).body;
  await page
    .getByRole("button", { name: "重试最近期望批", exact: true })
    .click();
  await page.getByText(/页 0 · 本页/).waitFor();
  const retried = (await api("/api/runs/3/public")).body;
  assert.deepEqual(retried, interrupted);
  assert.equal(retried.cost.next_page_calls, "1");
  const retryMeta = (await api("/api/runs/3/public-query?page=0")).body;
  assert.equal(retryMeta.matching_reused, true);
  results.retry = {
    same_original_batch: true,
    next_page_calls: retried.cost.next_page_calls,
    matching_reused: retryMeta.matching_reused,
  };
  // 第二窗口顺序推进两批，本窗口的下一页已旧，必须显示冲突而非重开。
  await choose("4");
  await page.getByRole("button", { name: "读取当前页", exact: true }).click();
  await page.getByText(/页 0 · 本页/).waitFor();
  const other = await browser.newPage();
  await other.goto("http://127.0.0.1:4321");
  await other.getByLabel("运行选择").selectOption("4");
  await other
    .getByRole("button", { name: "取回服务端当前页", exact: true })
    .click();
  await other.getByText(/页 0 · 本页/).waitFor();
  for (let n = 1; n <= 2; n++) {
    await other.getByRole("button", { name: "下一页", exact: true }).click();
    await other.getByText(new RegExp(`页 ${n} · 本页`)).waitFor();
  }
  await page.getByRole("button", { name: "下一页", exact: true }).click();
  await page.getByRole("alert").filter({ hasText: "服务端最近页 2" }).waitFor();
  await page
    .getByRole("button", { name: "取回服务端当前页", exact: true })
    .click();
  await page.getByRole("alert").filter({ hasText: "覆盖已改变" }).waitFor();
  const advanced = (await api("/api/runs/4/public")).body;
  assert.equal(advanced.cost.next_page_calls, "3");
  assert.equal(advanced.cost.reader_open_count, "1");
  results.multiple_windows = {
    recovered_page: advanced.page,
    next_page_calls: advanced.cost.next_page_calls,
    reader_open_count: advanced.cost.reader_open_count,
  };
  await other.close();
  // 独立原字段期望：明确 Canonical time / 市场与历史生效，不混时间或市场。
  const check = async (query, expect) => {
    const d = (
      await api(`/api/runs/1/public-query?page=0&${new URLSearchParams(query)}`)
    ).body;
    assert.deepEqual(d.matching.matched_offsets, expect.map(String));
    assert.equal(d.batch.cost.next_page_calls, "1");
    return d;
  };
  const rollback = whole.rows.flatMap((r, i) =>
    r.Canonical?.record?.PolymarketMessage?.time?.source_event_at_utc_ms ===
    "21996"
      ? [i]
      : [],
  );
  assert.ok(rollback.length);
  await check(
    {
      kind: "Canonical",
      clock: "规范事件 · 来源事件",
      time_start: "21996",
      time_end: "21996",
    },
    rollback,
  );
  const second =
    "0x2222222222222222222222222222222222222222222222222222222222222222";
  const repair = whole.rows.flatMap((r, i) =>
    r.Canonical?.record?.ReferenceEvent?.payload?.reference?.market
      ?.condition_id === second &&
    r.Canonical.record.ReferenceEvent.payload.reference.anchor?.application
      ?.effective_from_utc_ms === "20000"
      ? [i]
      : [],
  );
  assert.equal(repair.length, 1);
  await check(
    {
      kind: "Canonical",
      condition: second,
      clock: "历史生效",
      time_start: "20000",
      time_end: "20000",
    },
    repair,
  );
  const missing = await check(
    { kind: "Group", clock: "规范事件 · 来源事件" },
    [],
  );
  assert.equal(
    missing.matching.unknown_rows,
    String(whole.rows.filter((r) => r.Group).length),
  );
  assert.deepEqual(results.errors, []);
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({
    path: path.join(output, "query-recovery-desktop.png"),
    fullPage: true,
  });
  await writeFile(
    path.join(output, "query-browser.json"),
    JSON.stringify(results, null, 2),
  );
  console.log(JSON.stringify(results));
} finally {
  await browser?.close();
  server.kill("SIGINT");
  await new Promise((r) =>
    server.exitCode !== null ? r() : server.once("exit", r),
  );
  await rm(temp, { recursive: true, force: true });
}
