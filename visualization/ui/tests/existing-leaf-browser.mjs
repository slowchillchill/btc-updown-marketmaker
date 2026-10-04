// PR229真实原叶字段/整记录一致性；只验证原时钟，非成本负载或领域重新归约。
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
const [configPath, output] = process.argv.slice(2);
if (!output)
  throw Error(
    "existing-leaf-browser.mjs <four-source-config> <ignored-output>",
  );
await mkdir(output, { recursive: true });
const original = JSON.parse(await readFile(configPath));
const config = path.join(output, "sources.json");
await writeFile(
  config,
  JSON.stringify({ runs: [original.runs[2], original.runs[3]] }),
);
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
const result = { errors: [], cases: [], negative: [] };
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
  await page.goto("http://127.0.0.1:4324");
  await page.locator(".existing-panel").waitFor();
  const nav = async (name) =>
    page.getByRole("button", { name, exact: true }).click();
  const readPath = async (document, p) => {
    await nav("证据重放");
    await page.getByLabel("工程历史文档").selectOption(document);
    await page.getByLabel("工程历史路径").fill(p);
    const waiting = page.waitForResponse((r) => r.url().includes("/existing?"));
    await nav("读取原区段");
    const response = await waiting;
    const data = await response.json();
    assert.equal(data.path, p);
    await page.waitForFunction(
      (p) =>
        document
          .querySelector("[data-existing-loaded] h3")
          ?.textContent.includes(p + " "),
      p,
    );
    return data;
  };
  const loaded = () => page.locator("[data-existing-loaded]");
  const choose = async (field, whole) => {
    if (whole) {
      await loaded()
        .locator("details.tree")
        .first()
        .locator("summary")
        .first()
        .click();
      await loaded().getByRole("button", { name: field, exact: true }).click();
    } else
      await loaded()
        .getByRole("button", { name: "原字段详情", exact: true })
        .click();
  };
  const dump = async (name) => {
    await nav("多时钟时间线");
    await page.getByLabel("导出范围").selectOption("selection");
    assert.ok(
      await page
        .getByRole("button", { name: "导出 Perfetto", exact: true })
        .isEnabled(),
    );
    const waiting = page.waitForEvent("download");
    await nav("导出 Perfetto");
    const download = await waiting;
    const file = path.join(output, name + ".json");
    await download.saveAs(file);
    const trace = JSON.parse(await readFile(file));
    return trace.traceEvents.filter((e) => e.ph === "i");
  };
  for (const test of [
    {
      run: "0",
      document: "calls",
      row: "/0",
      field: "now",
      value: "1789517395414",
      clock: "宿主逻辑 UTC ms",
    },
    {
      run: "1",
      document: "manifest",
      row: "/files/0",
      field: "first_recv_time_utc_ms",
      value: "1788912002229",
      clock: "原 UTC ms",
    },
  ]) {
    await page.getByLabel("运行选择").selectOption(test.run);
    await nav("证据重放");
    await page.locator(".existing-panel").waitFor();
    const row = await readPath(test.document, test.row);
    assert.equal(row.exact[test.field], test.value);
    await choose(test.field, true);
    const whole = await dump(test.document + "-whole-field");
    assert.equal(whole.length, 1);
    const leafPath = test.row + "/" + test.field;
    const leaf = await readPath(test.document, leafPath);
    assert.equal(leaf.exact, test.value);
    await choose(test.field, false);
    const points = await dump(test.document + "-scalar-leaf");
    assert.equal(points.length, 1);
    const expectedRef = `/existing/${test.document}${leafPath}`;
    for (const point of [whole[0], points[0]]) {
      assert.equal(point.args.original_utc_ms, test.value);
      assert.equal(point.args.reference, expectedRef);
      assert.equal(point.args.clock_kind, test.clock);
      assert.equal(point.args.existing_provenance.document, test.document);
      assert.equal(point.args.existing_provenance.reference, leafPath);
      assert.equal(String(point.ts), (BigInt(test.value) * 1000n).toString());
    }
    result.cases.push({
      ...test,
      reference: expectedRef,
      whole_points: whole.length,
      leaf_points: points.length,
    });
    await page.evaluate(() => scrollTo(0, 0));
    await page.screenshot({
      path: path.join(output, test.document + "-leaf-timeline.png"),
      fullPage: true,
    });
  }
  for (const test of [
    { run: "0", document: "calls", path: "/0/cpu_ns" },
    { run: "0", document: "calls", path: "/0/actions" },
    { run: "1", document: "manifest", path: "/files/0/source_timestamp_start" },
  ]) {
    await page.getByLabel("运行选择").selectOption(test.run);
    await readPath(test.document, test.path);
    await choose("", false);
    await nav("多时钟时间线");
    await page.getByLabel("导出范围").selectOption("selection");
    assert.ok(
      await page
        .getByRole("button", { name: "导出 Perfetto", exact: true })
        .isDisabled(),
    );
    result.negative.push({ ...test, export_disabled: true });
  }
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
