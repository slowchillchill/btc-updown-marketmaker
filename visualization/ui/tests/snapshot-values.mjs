// 独立算术样例验证显示舍入，不调用生产 formatter 生成期望。
import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, writeFile, rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import ts from "typescript";
const ui = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cache = path.join(ui, "node_modules/.cache");
await mkdir(cache, { recursive: true });
const temp = await mkdtemp(path.join(cache, "snapshots-"));
try {
  for (const name of ["facts", "snapshotValues"]) {
    const source = await readFile(path.join(ui, "src", name + ".ts"), "utf8");
    const js = ts
      .transpileModule(source, {
        compilerOptions: {
          target: ts.ScriptTarget.ES2022,
          module: ts.ModuleKind.ES2022,
        },
      })
      .outputText.replace('from "./facts"', 'from "./facts.mjs"');
    await writeFile(path.join(temp, name + ".mjs"), js);
  }
  const { fraction, rounded, navChange, utc } = await import(
    pathToFileURL(path.join(temp, "snapshotValues.mjs"))
  );
  const known = (n, d) => ({ Known: { numerator: n, denominator: d } });
  assert.equal(rounded(fraction(known("187038", "4490809")), 2, 100n), "4.16");
  assert.equal(
    rounded(fraction(known("81202662886339", "120000000000"))),
    "676.69",
  );
  assert.equal(
    rounded(fraction(known("900719925474099301", "100"))),
    "9007199254740993.01",
  );
  assert.equal(rounded(fraction(known("0", "1"))), "0.00");
  assert.equal(rounded(fraction(known("-1005", "1000"))), "-1.01");
  for (const v of [
    undefined,
    null,
    "Unknown",
    "Unavailable",
    { Unknown: "Missing public NAV" },
  ])
    assert.equal(fraction(v), undefined);
  const change = navChange(known("30214593", "10000"), "2000");
  assert.equal(rounded(change.amount, 4), "1021.4593");
  assert.equal(rounded(change.percent), "51.07");
  assert.equal(navChange(known("30214593", "10000"), undefined), undefined);
  assert.equal(utc("1789178399995"), "2026-09-12 01:59:59.995 UTC");
  assert.equal(utc("9007199254740993"), undefined);
  console.log(
    "Snapshot exact rounding / large integers / distinct states / NAV basis: passed",
  );
} finally {
  await rm(temp, { recursive: true, force: true });
}
