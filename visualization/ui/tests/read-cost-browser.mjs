// 本来源两页及最近页重试的有限成本观察；没有性能阈值。
import assert from "node:assert/strict";
import { chromium } from "playwright-core";
import { spawn } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
const workspace = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const [config, output] = process.argv.slice(2);
if (!config || !output) throw new Error("用法：read-cost-browser.mjs <sources.json> <忽略产物目录>");
await mkdir(output, { recursive: true });
const server = spawn(path.join(process.env.CARGO_TARGET_DIR ?? path.join(workspace,"target"), "debug/visualization"),
  [config, "127.0.0.1:4320", path.join(workspace,"visualization/ui/dist")], {stdio:["ignore","ignore","pipe"]});
let log = "", browser;
server.stderr.on("data", b => log += b);
const started = performance.now();
const results = { initialization_until_first_http_ok_ms: null, pages: [], errors: [] };
try {
  for (;;) {
    if (server.exitCode !== null) throw new Error(log);
    try { if ((await fetch("http://127.0.0.1:4320/api/runs")).ok) break; } catch {}
    await new Promise(r => setTimeout(r, 25));
  }
  results.initialization_until_first_http_ok_ms = performance.now() - started;
  browser = await chromium.launch({executablePath: process.env.CHROME_EXECUTABLE ?? "/usr/bin/google-chrome", headless:true});
  const page = await browser.newPage({viewport:{width:1600,height:1150}});
  page.on("pageerror", e => results.errors.push(e.message));
  await page.goto("http://127.0.0.1:4320");
  await page.locator(".module-card").nth(7).waitFor();
  for (let n=0;n<2;n++) {
    await page.evaluate(() => {window.costStart = performance.now(); performance.clearResourceTimings();});
    const received = page.waitForResponse(r => r.url().includes("/api/runs/0/public") && r.status()===200);
    await page.getByRole("button", {name:n===0?"读取当前页":"下一页",exact:true}).click();
    const response = await received;
    await page.getByText(new RegExp(`页 ${n} · 本页`)).waitFor();
    assert.equal(await page.locator("table tbody tr").count(),100);
    const rendering = await page.evaluate(async () => {
      await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
      const resource = performance.getEntriesByType("resource").find(r => r.name.includes("/api/runs/0/public"));
      return {interaction_to_dom_and_two_animation_frames_ms:performance.now()-window.costStart,
        request_to_response_end_ms:resource.responseEnd-resource.requestStart,
        resource_duration_ms:resource.duration,transfer_bytes:resource.transferSize,
        encoded_body_bytes:resource.encodedBodySize,decoded_body_bytes:resource.decodedBodySize};
    });
    const bytes = await response.body();
    const body = JSON.parse(bytes.toString());
    assert.equal(body.row_start,String(n*100));
    assert.equal(body.cost.next_page_calls,String(n+1));
    assert.equal(body.cost.reader_open_count,"1");
    const rowsBytes = Buffer.byteLength(JSON.stringify(body.rows));
    assert.equal(body.cost.rows_json_bytes,String(rowsBytes));
    results.pages.push({page:n,rows:body.returned_rows,response_bytes:bytes.length,rows_json_bytes:rowsBytes,cost:body.cost,...rendering});
  }
  results.retry = await page.evaluate(async () => {
    const start=performance.now(), response=await fetch("/api/runs/0/public?page=1"), text=await response.text();
    const received=performance.now(), parsed=JSON.parse(text), end=performance.now();
    return {http_until_body_ms:received-start,browser_json_parse_ms:end-received,response_bytes:new TextEncoder().encode(text).length,next_page_calls:parsed.cost.next_page_calls,status:response.status};
  });
  assert.equal(results.retry.next_page_calls,"2");
  assert.equal(results.retry.response_bytes,results.pages[1].response_bytes);
  assert.deepEqual(results.errors,[]);
  await page.evaluate(()=>window.scrollTo(0,0));
  await page.screenshot({path:path.join(output,"read-cost-desktop.png"),fullPage:true});
  await writeFile(path.join(output,"browser.json"),JSON.stringify(results,null,2));
  console.log(JSON.stringify(results));
} finally {
  await browser?.close();
  server.kill("SIGINT");
  await new Promise(r => server.exitCode !== null ? r() : server.once("exit",r));
}
