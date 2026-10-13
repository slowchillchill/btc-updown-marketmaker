// 实际 Rust / Chrome 联调。私有报告只复制，状态注入仅在独立临时副本。
import assert from "node:assert/strict";
import { chromium } from "playwright-core";
import { spawn } from "node:child_process";
import { mkdtemp, mkdir, readFile, writeFile, rm } from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
const workspace = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);
if (!process.argv[2])
  throw new Error(
    "Usage: test:snapshots -- <private seven-run sources.json> [evidence directory]",
  );
const sourceConfig = JSON.parse(
  await readFile(path.resolve(process.argv[2]), "utf8"),
);
const output = process.argv[3] ? path.resolve(process.argv[3]) : null;
if (output) await mkdir(output, { recursive: true });
const temp = await mkdtemp(
  path.join(output ?? tmpdir(), "visualization-hour-snapshots-"),
);
const before = await Promise.all(
  sourceConfig.runs.flatMap((r) => [
    readFile(r.report),
    readFile(r.report_provenance),
  ]),
);
const errors = [],
  responses = [],
  screenshotNames = [],
  checks = [];
let server, browser;
const started = performance.now();
try {
  const runs = [];
  for (let i = 0; i < sourceConfig.runs.length; i++) {
    const r = sourceConfig.runs[i],
      report = path.join(temp, `${i}.report.json`),
      provenance = path.join(temp, `${i}.provenance.json`);
    await writeFile(report, before[i * 2]);
    const p = JSON.parse(before[i * 2 + 1].toString());
    p.extracted_file = report;
    p.test_copy_of = r.report;
    await writeFile(provenance, JSON.stringify(p));
    runs.push({ ...r, report, report_provenance: provenance });
  }
  // 原完整对象的可达状态变形；不补领域状态，不生成盈利结果。
  const boundary = JSON.parse(before[0].toString());
  const m = boundary.research.hours[1].cumulative.metrics;
  m.actual_reserved_pusd = { Known: { numerator: "0", denominator: "1" } };
  m.maker_fee_pusd = "Unknown";
  m.taker_fee_pusd = "Unavailable";
  delete m.taker_principal_pusd;
  m.maker_principal_pusd = {
    Known: { numerator: "900719925474099301", denominator: "100" },
  };
  m.locked_remainder_rebates_pusd = null;
  await writeFile(path.join(temp, "states.json"), JSON.stringify(boundary));
  runs.push({
    label: "Display boundary test fixture",
    report: path.join(temp, "states.json"),
  });
  await writeFile(path.join(temp, "sources.json"), JSON.stringify({ runs }));
  const binary = path.join(
    process.env.CARGO_TARGET_DIR ?? path.join(workspace, "target"),
    "debug/visualization",
  );
  const logs = [];
  server = spawn(
    binary,
    [
      path.join(temp, "sources.json"),
      "127.0.0.1:4328",
      path.join(workspace, "visualization/ui/dist"),
    ],
    { cwd: workspace, stdio: ["ignore", "pipe", "pipe"] },
  );
  server.stdout.on("data", (d) => logs.push(d.toString()));
  server.stderr.on("data", (d) => logs.push(d.toString()));
  let ready = false;
  for (let i = 0; i < 100; i++) {
    try {
      const r = await fetch("http://127.0.0.1:4328/api/runs");
      if (r.ok) {
        ready = true;
        break;
      }
    } catch {}
    await new Promise((r) => setTimeout(r, 100));
  }
  assert(ready, "snapshot server did not start: " + logs.join(""));
  browser = await chromium.launch({
    executablePath: process.env.CHROME_EXECUTABLE ?? "/usr/bin/google-chrome",
    headless: true,
    args: ["--no-sandbox"],
  });
  const page = await browser.newPage({
    viewport: { width: 1280, height: 720 },
  });
  page.on("pageerror", (e) => errors.push(String(e)));
  page.on("response", async (r) => {
    if (r.url().includes("/snapshot")) {
      const b = await r.body();
      responses.push({ url: r.url(), bytes: b.length });
    }
  });
  await page.addInitScript(() =>
    localStorage.setItem("visualization.language", "en"),
  );
  await page.goto("http://127.0.0.1:4328");
  const root = page.locator(".hourly-snapshot"),
    panel = page.locator(".evidence-panel");
  const field = (key) =>
    root.locator(".fact-field").filter({
      has: page.locator(`small`, { hasText: new RegExp(key + "$") }),
    });
  const value = async (key, expected) =>
    assert.equal(await field(key).locator("strong").innerText(), expected, key);
  const hour = page.getByRole("combobox", {
    name: "Saved snapshot",
    exact: true,
  });
  const source = page.getByRole("combobox", {
    name: "Run selection",
    exact: true,
  });
  const clear = async () =>
    assert.equal(
      await panel.locator("pre").count(),
      0,
      "previous selected fact leaked",
    );
  const navPath = "/metrics/conservative_nav_pusd/Known";
  async function shot(name) {
    if (output) {
      await page.screenshot({ path: path.join(output, name), fullPage: false });
      screenshotNames.push(name);
    }
  }
  async function noOverflow() {
    assert(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
      "horizontal overflow",
    );
  }
  await root.waitFor();
  assert.equal(await hour.inputValue(), "1");
  await hour.selectOption("0");
  await clear();
  await value(navPath, "3021.46");
  await value("/metrics/confirmed_net_pnl_pusd/Known", "1041.31");
  assert((await root.innerText()).includes("1021.46"));
  await field(navPath).click();
  assert((await panel.innerText()).includes("/research/hours/0/cumulative"));
  assert.deepEqual(JSON.parse(await panel.locator("pre").innerText()), {
    denominator: "10000",
    numerator: "30214593",
  });
  await shot("center-1h-1280.png");
  checks.push(
    "center 1h NAV3021.4593 / NAV change1021.4593 / confirmed1041.3093 independent",
  );
  await hour.selectOption("1");
  await clear();
  await value(navPath, "3519.03");
  await value("/metrics/actual_available_pusd/Known", "2683.27");
  await value("/day_economics/maximum_nav_drawdown_fraction/Known", "4.16");
  assert((await root.innerText()).includes("75.95"));
  assert(!(await root.innerText()).includes("3664.871"));
  await field(navPath).click();
  assert((await panel.innerText()).includes("/research/hours/1/cumulative"));
  assert.deepEqual(JSON.parse(await panel.locator("pre").innerText()), {
    denominator: "10000",
    numerator: "35190333",
  });
  await source.selectOption("1");
  await page
    .getByRole("heading", { name: "Variant 1 (no-innovation)", exact: true })
    .waitFor();
  await clear();
  assert.equal(await hour.inputValue(), "1");
  await value(navPath, "3762.68");
  await shot("no-innovation-2h-1280.png");
  await source.selectOption("0");
  await page
    .getByRole("heading", { name: "Reference (center)", exact: true })
    .waitFor();
  await clear();
  await value(navPath, "3519.03");
  assert(
    (await root.innerText()).includes(
      "2026-09-12 00:00:00.000 UTC → 2026-09-12 02:00:00.000 UTC",
    ),
  );
  assert((await root.innerText()).includes("2026-09-12 01:59:59.995 UTC"));
  assert((await root.innerText()).includes("BudgetStop"));
  assert((await root.innerText()).includes("recording_drained: false"));
  await noOverflow();
  await shot("reference-2h-overview-1280.png");
  await field(navPath).click();
  await shot("cash-nav-original-1280.png");
  await page.setViewportSize({ width: 1920, height: 1080 });
  await noOverflow();
  await shot("reference-2h-overview-1920.png");
  await field("/day_economics/maximum_nav_drawdown_fraction/Known").click();
  assert.deepEqual(JSON.parse(await panel.locator("pre").innerText()), {
    denominator: "4490809",
    numerator: "187038",
  });
  await panel
    .getByText("Full source paths and mapping", { exact: true })
    .click();
  assert(
    (await panel.innerText()).includes(
      "/days/0/candidates/0/report/research/hours/1/cumulative/day_economics/maximum_nav_drawdown_fraction/Known",
    ),
  );
  await panel
    .getByText("Full source paths and mapping", { exact: true })
    .click();
  await shot("drawdown-original-1920.png");
  await page.setViewportSize({ width: 1280, height: 720 });
  await field("/day_economics/maximum_nav_drawdown_fraction/Known").click();
  assert(await panel.locator("pre").isVisible());
  const rawBox = await panel.locator("pre").boundingBox();
  assert(
    rawBox.y + rawBox.height <= 720,
    "drawdown fraction below recording viewport",
  );
  await shot("drawdown-original-1280.png");
  await page.setViewportSize({ width: 1920, height: 1080 });
  const clicksStart = performance.now();
  await page
    .getByRole("button", {
      name: "Intents and execution feedback",
      exact: true,
    })
    .click();
  await value("/counters/maker_success_trade_count", "1930");
  await value("/counters/maker_economic_trade_count", "1930");
  await value("/counters/open_order_count", "0");
  await value("/counters/pending_noe_records", "0");
  await value("/counters/unclosed_commitments", "0");
  await field("/counters/maker_success_trade_count").click();
  await shot("execution-1920.png");
  const executionDomMs = performance.now() - clicksStart;
  await page
    .getByRole("button", {
      name: "Account economics and lifecycle",
      exact: true,
    })
    .click();
  assert(
    !/[\u4e00-\u9fff]/.test(await root.innerText()),
    "untranslated Chinese text in English snapshot",
  );
  await value("/metrics/paired_shares/Known", "835.76");
  await value("/day_economics/average_committed_capital_pusd/Known", "676.69");
  await value("/day_economics/maximum_committed_capital_pusd/Known", "1901.99");
  await value("/metrics/accrued_unmatured_rebates_pusd/Known", "82.74");
  await value("/metrics/paid_rebates_pusd/Known", "0.00");
  await value("/metrics/confirmed_unpaid_rebates_pusd/Known", "0.00");
  await value("/day_economics/measured_duration_ms", "7200000");
  await field("/metrics/accrued_unmatured_rebates_pusd/Known").click();
  await shot("capital-rebates-1920.png");
  await page.setViewportSize({ width: 1280, height: 720 });
  await field("/metrics/accrued_unmatured_rebates_pusd/Known").click();
  await noOverflow();
  await shot("capital-rebates-1280.png");
  await page
    .getByRole("button", {
      name: "Intents and execution feedback",
      exact: true,
    })
    .click();
  await field("/counters/maker_success_trade_count").click();
  await noOverflow();
  await shot("execution-1280.png");
  await field("/pending_calculation").click();
  assert.equal(
    JSON.parse(await panel.locator("pre").innerText()).actual_sent_at_utc_ms,
    null,
  );
  await page
    .getByRole("button", { name: "Multi-clock timeline", exact: true })
    .click();
  assert((await root.innerText()).includes("Unavailable"));
  assert.equal(await page.locator(".echarts").count(), 0);
  await page
    .getByRole("button", { name: "Evidence replay", exact: true })
    .click();
  assert.equal(
    await root
      .locator(".tree")
      .first()
      .locator("summary")
      .first()
      .innerText()
      .then((s) => s.includes("/research/hours/1/cumulative")),
    true,
  );
  await source.selectOption("7");
  await page
    .getByRole("heading", {
      name: "Display boundary test fixture",
      exact: true,
    })
    .waitFor();
  await clear();
  await page
    .getByRole("button", {
      name: "Account economics and lifecycle",
      exact: true,
    })
    .click();
  await value("/metrics/actual_reserved_pusd/Known", "0.00");
  await value("/metrics/maker_fee_pusd", "Unknown");
  await value("/metrics/taker_fee_pusd", "Unavailable");
  await value("/metrics/taker_principal_pusd", "Not provided");
  await value("/metrics/locked_remainder_rebates_pusd", "Null (no value)");
  await value("/metrics/maker_principal_pusd/Known", "9007199254740993.01");
  await field("/metrics/maker_principal_pusd/Known").click();
  assert.equal(
    JSON.parse(await panel.locator("pre").innerText()).numerator,
    "900719925474099301",
  );
  assert((await root.innerText()).includes("Not provided"));
  checks.push(
    "Known zero / Unknown / Unavailable / missing / null / large integer fraction remain distinct",
  );
  await source.selectOption("0");
  await page
    .getByRole("heading", { name: "Reference (center)", exact: true })
    .waitFor();
  await clear();
  // 仅在浏览器内存变形响应：缺失字段无原值，显式 null 有真实原值。
  let cumulativeVariant = "missing";
  await page.route("**/api/runs/0/snapshot", async (route) => {
    const response = await route.fetch();
    const body = await response.json();
    if (cumulativeVariant === "missing") {
      delete body.report.exact.research.hours[1].cumulative;
    } else {
      body.report.exact.research.hours[1].cumulative = null;
    }
    await route.fulfill({ response, json: body });
  });
  for (const variant of ["missing", "null"]) {
    cumulativeVariant = variant;
    await page.reload();
    await root.waitFor();
    await page
      .getByRole("button", { name: "Evidence replay", exact: true })
      .click();
    const originalFields = root
      .getByRole("heading", {
        name: "Selected cumulative snapshot · Original fields",
        exact: true,
      })
      .locator("..");
    if (variant === "missing") {
      assert((await originalFields.innerText()).includes("Not provided"));
      assert.equal(await originalFields.getByRole("button").count(), 0);
      await clear();
    } else {
      await originalFields
        .getByRole("button", {
          name: "/research/hours/1/cumulative",
          exact: true,
        })
        .click();
      assert.equal(await panel.locator("pre").innerText(), "null");
      assert.equal(
        await panel.locator(":scope > code").innerText(),
        "/research/hours/1/cumulative",
      );
    }
  }
  await page.unroute("**/api/runs/0/snapshot");
  checks.push(
    "Missing cumulative: Not provided, no original-value button; explicit null: selectable at actual pointer",
  );
  const snap = await fetch("http://127.0.0.1:4328/api/runs/0/snapshot").then(
    (r) => r.json(),
  );
  assert.equal(snap.report.read_attempts, "1");
  assert.equal(snap.report.decode_attempts, "1");
  const snap2 = await fetch("http://127.0.0.1:4328/api/runs/0/snapshot").then(
    (r) => r.json(),
  );
  assert.deepEqual(snap2, snap);
  assert.equal(
    await page
      .evaluate(
        () =>
          getComputedStyle(document.querySelector(".snapshot-shell"))
            .fontFamily,
      )
      .then((s) => s.includes("Noto Sans")),
    true,
  );
  assert.equal(
    await page
      .evaluate(
        () =>
          getComputedStyle(document.querySelector(".fact-field strong"))
            .fontFamily,
      )
      .then((s) => s.includes("Noto Sans Mono")),
    true,
  );
  assert.equal(errors.length, 0, errors.join("\n"));
  const after = await Promise.all(
    sourceConfig.runs.flatMap((r) => [
      readFile(r.report),
      readFile(r.report_provenance),
    ]),
  );
  before.forEach((b, i) => assert(b.equals(after[i])));
  const result = {
    checks: [
      ...checks,
      "center1h → center2h → no-innovation2h → center2h with selection clearing",
      "Boundaries / original pointers / missing market history / pending calculation verified",
      "1280×720 and1920×1080 no overflow; Noto font families; pageerrors0",
    ],
    source_cost: {
      report: {
        read_attempts: snap.report.read_attempts,
        decode_attempts: snap.report.decode_attempts,
        bytes_read: snap.report.bytes_read,
        load_elapsed_us: snap.report.load_elapsed_us,
      },
      provenance: {
        read_attempts: snap.report_provenance.read_attempts,
        bytes_read: snap.report_provenance.bytes_read,
      },
      snapshot_response_bytes: responses.find((r) =>
        r.url.endsWith("/0/snapshot"),
      )?.bytes,
    },
    execution_click_to_dom_ms: executionDomMs,
    total_browser_ms: performance.now() - started,
    responses,
    screenshotNames,
    originals_unchanged: true,
  };
  if (output)
    await writeFile(
      path.join(output, "snapshot-browser-results.json"),
      JSON.stringify(result, null, 2),
    );
  console.log(JSON.stringify(result, null, 2));
} finally {
  if (browser) await browser.close();
  if (server && server.exitCode === null) {
    server.kill("SIGINT");
    await new Promise((r) => server.once("exit", r));
  }
  if (!output) await rm(temp, { recursive: true, force: true });
}
