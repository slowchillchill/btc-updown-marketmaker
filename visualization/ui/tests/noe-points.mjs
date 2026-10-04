// 用原核心/writer +版本化读取器产生的 DTO，检验明确路径；没有新领域schema。
import assert from "node:assert/strict";
import ts from "typescript";
import { mkdtemp, readFile, writeFile, rm, mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
const ui = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const [clockPath, browserPath] = process.argv.slice(2);
if (!browserPath)
  throw Error(
    "noe-points.mjs <noe_clock_evidence/decoded.json> <execution-browser-results.json>",
  );
const cache = path.join(ui, "node_modules/.cache");
await mkdir(cache, { recursive: true });
const temp = await mkdtemp(path.join(cache, "noe-points-"));
try {
  for (const name of ["facts", "browse", "noePoints"]) {
    const source = await readFile(path.join(ui, "src", name + ".ts"), "utf8");
    const js = ts
      .transpileModule(source, {
        compilerOptions: {
          target: ts.ScriptTarget.ES2022,
          module: ts.ModuleKind.ES2022,
        },
      })
      .outputText.replace(
        /from "\.\/(facts|browse)"/g,
        (_, n) => `from "./${n}.mjs"`,
      );
    await writeFile(path.join(temp, name + ".mjs"), js);
  }
  const { noePoints } = await import(
    pathToFileURL(path.join(temp, "noePoints.mjs"))
  );
  const { traceJson } = await import(
    pathToFileURL(path.join(temp, "browse.mjs"))
  );
  const clocks = JSON.parse(await readFile(clockPath));
  const suffixes = [
    "/effective_processing_at_utc_ms",
    "/matched_at_utc_ms",
    "/matched_at_utc_ms",
    "/economic_effective_at_utc_ms",
    "/economic_effective_at_utc_ms",
    "/resolution_source_event_at_utc_ms",
  ];
  const expected = [
    "9007199254740993",
    "-3",
    "-3",
    "1788840000000",
    "1788840000000",
    "1788840000000",
  ];
  for (let i = 0; i < clocks.length; i++) {
    const points = noePoints(clocks[i]);
    assert.equal(points.length, 4);
    const sourceKind = [
      "DirectResponse",
      "TradeLifecycle",
      "TradeLifecycle",
      "OrderLifecycle",
      "OrderLifecycle",
      "SettlementLifecycle",
    ][i];
    assert.equal(
      points.filter((p) =>
        p.path.includes("/source_reference/record/" + sourceKind + "/"),
      ).length,
      2,
    );

    const payload = points.find((p) => p.path.endsWith(suffixes[i]));
    assert.equal(payload.utcMs, expected[i]);
    assert.equal(payload.condition, "0x" + "3".repeat(64));
    assert.equal(points[0].condition, undefined);
    assert.equal(new Set(points.map((p) => p.path)).size, 4); // 同ms字段不能合并
    assert.equal(
      points.filter((p) => p.utcMs === "1788840000000").length,
      i < 3 ? 2 : 3,
    );
    assert.ok(
      points.every(
        (p) =>
          p.source === "noe" &&
          !p.references &&
          p.noe.reference &&
          p.noe.context_reference &&
          p.noe.source_watermark,
      ),
    );
    const trace = traceJson([payload], {});
    assert.ok(
      new RegExp('\"ts\":\\s*' + BigInt(expected[i]) * 1000n + "[,\\s]").test(
        trace,
      ),
    );
  }
  const responses = JSON.parse(await readFile(browserPath)).responses.map(
    (r) => r.data,
  );
  for (const [index, n] of [
    [0, 1],
    [1, 1],
    [2, 3],
    [3, 5],
    [4, 3],
    [5, 3],
    [6, 1],
    [12, 1],
    [18, 1],
  ]) {
    const loaded = responses.find(
      (r) => r.index === String(index) && r.status === "ready",
    );
    assert.ok(loaded);
    const points = noePoints(loaded);
    assert.equal(points.length, n);
    if (index === 3) {
      assert.equal(
        points.filter((p) => p.path.includes("/original_source_reference/"))
          .length,
        2,
      );
      assert.equal(
        points.filter((p) => p.path.includes("/source_reference/")).length,
        2,
      );
    }
  }
  assert.deepEqual(noePoints(null), []);
  assert.deepEqual(noePoints({ ...clocks[0], status: "read_error" }), []);
  assert.deepEqual(noePoints({ ...clocks[0], version: "v999" }), []);
  console.log(
    "NOE 原typed时钟材料6条；实际v1-v4/7变体；同ms/负/逆序/大整数/原路径/无时钟/失败/未知：通过",
  );
} finally {
  await rm(temp, { recursive: true, force: true });
}
