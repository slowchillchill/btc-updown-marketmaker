// 原 writer 引用 / typed Ledger 工程投影 + 实际 Chrome，不冒充研究产物。
import assert from "node:assert/strict";
import { chromium } from "playwright-core";
import { spawn } from "node:child_process";
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
  for (let i = 0; i < 6; i++) {
    await page.locator(`[data-noe-index="${i}"]`).click();
    await page.locator('[data-noe-status="ready"]').waitFor();
    await page.waitForFunction(
      (n) =>
        document
          .querySelector('[data-noe-index="' + n + '"]')
          ?.textContent.includes("已加载"),
      String(i),
    );
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
  await run("2");
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
