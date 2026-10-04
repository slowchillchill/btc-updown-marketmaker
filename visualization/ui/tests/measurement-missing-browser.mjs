// 两个真实字段正反断言：缺失禁选，显式null仍可查原值。
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
const [originalConfig, output] = process.argv.slice(2);
if (!output) throw Error("measurement-missing-browser.mjs sources.json output");
await mkdir(output, { recursive: true });
const explicit = JSON.parse(await readFile(originalConfig)).runs.find(
  (r) => r.existing_source?.kind === "remeasurement_12m",
);
assert.ok(explicit);
const config = path.join(output, "sources.json");
await writeFile(config, JSON.stringify({ runs: [explicit] }));
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
const result = { errors: [], cases: [] };
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

  // 独立原字段期待：prepare.old没有独占CPU字段，research.new明确保存null。
  const source = JSON.parse(await readFile(config));
  const raw = JSON.parse(
    await readFile(source.runs[0].existing_source.paths.measurement),
  );
  assert.equal(
    Object.hasOwn(raw.execution.phases.prepare.old, "exclusive_cpu_seconds"),
    false,
  );
  assert.equal(
    Object.hasOwn(
      raw.execution.phases.two_market_research.new,
      "exclusive_cpu_seconds",
    ),
    true,
  );
  assert.equal(
    raw.execution.phases.two_market_research.new.exclusive_cpu_seconds,
    null,
  );
  await page.goto("http://127.0.0.1:4324");
  await page.getByLabel("运行选择").selectOption("0");
  await page.locator('[data-existing-kind="remeasurement_12m"]').waitFor();
  const absentPath = "/execution/phases/prepare/old/exclusive_cpu_seconds";
  const nullPath =
    "/execution/phases/two_market_research/new/exclusive_cpu_seconds";
  const absent = page.locator(`[data-original-field="${absentPath}"]`);
  const storedNull = page.locator(`[data-original-field="${nullPath}"]`);
  assert.ok(await absent.isDisabled());
  assert.equal(await absent.innerText(), "未保存");
  assert.ok(await storedNull.isEnabled());
  await storedNull.click();
  const selection = page.locator(".evidence-panel");
  await selection.waitFor();
  assert.ok(
    (await selection.innerText()).includes("/existing/measurement" + nullPath),
  );
  assert.equal(await selection.locator("pre").innerText(), "null");
  const before = await selection.innerText();
  await absent.evaluate((button) => button.click());
  assert.equal(await selection.innerText(), before);
  result.cases = [
    {
      path: absentPath,
      original_field_present: false,
      disabled: true,
      label: "未保存",
      did_not_replace_selection: true,
    },
    {
      path: nullPath,
      original_field_present: true,
      original_value: null,
      selectable: true,
      selected_exact: null,
    },
  ];
  await page.evaluate(() => scrollTo(0, 0));
  await page.screenshot({
    path: path.join(output, "measurement-undefined-null.png"),
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
