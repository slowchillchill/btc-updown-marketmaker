// 原公开产物中的初始化 fallback / 多时钟 / 原投影列，使用独立字段常量期待。
import assert from "node:assert/strict";
import ts from "typescript";
import { readFile, writeFile, mkdtemp, mkdir, rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
const ui = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const source = JSON.parse(await readFile(process.argv[2]));
const exact = (v) =>
  typeof v === "number"
    ? String(v)
    : Array.isArray(v)
      ? v.map(exact)
      : v && typeof v === "object"
        ? Object.fromEntries(Object.entries(v).map(([k, a]) => [k, exact(a)]))
        : v;
const cache = path.join(ui, "node_modules/.cache");
await mkdir(cache, { recursive: true });
const temp = await mkdtemp(path.join(cache, "research-points-"));
try {
  for (const name of ["facts", "browse", "researchPoints"]) {
    const text = await readFile(path.join(ui, "src", name + ".ts"), "utf8");
    const js = ts
      .transpileModule(text, {
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
  const { researchPoints } = await import(
    pathToFileURL(path.join(temp, "researchPoints.mjs"))
  );
  const { traceJson } = await import(
    pathToFileURL(path.join(temp, "browse.mjs"))
  );
  const status = {
    status: "ready",
    summary: [
      { path: "/projection", exact: exact(source.projection) },
      {
        path: "/research/normal_prefix",
        exact: exact(source.research.normal_prefix),
      },
      {
        path: "/build_identity/source_commit",
        exact: source.build_identity.source_commit,
      },
    ],
    targets: source.research.markets.map((m) => ({
      identity: exact(m.identity),
    })),
  };
  const base = "/research/global_spot/updates_original_visible_order";
  const fragment = {
    path: base,
    rows: [0, 1].map((i) => ({
      path: base + "/" + i,
      exact: exact(
        source.research.global_spot.updates_original_visible_order[i],
      ),
    })),
    omitted_arrays: [],
  };
  const points = researchPoints(status, fragment),
    fallback = points.find((p) => p.path === base + "/0/received_at_utc_ms");
  assert.match(fallback.clock, /fallback/);
  assert.match(fallback.name, /非实测/);
  const spot = points.filter((p) => p.path.startsWith(base + "/1/"));
  assert.equal(spot.length, 3);
  assert.deepEqual(
    spot.map((p) => p.utcMs).sort(),
    ["1788911999000", "1788912002228", "1788912002251"].sort(),
  );
  assert.ok(
    spot.every((p) => p.condition === undefined && p.references === undefined),
  );
  const large = researchPoints(status, {
    path: base,
    rows: [
      {
        path: base + "/999",
        exact: {
          source_event_at_utc_ms: ["9007199254740993"],
          received_at_utc_ms: "-3",
          first_visible_at_utc_ms: "-3",
          source_ids: ["original"],
        },
      },
    ],
    omitted_arrays: [],
  });
  assert.equal(large.length, 3);
  const trace = traceJson(large, {});
  assert.ok(trace.includes('"ts": 9007199254740993000'));
  assert.equal(large.filter((p) => p.utcMs === "-3").length, 2);
  const labels = researchPoints(status, {
    path: "/research/markets/11/posthoc_label_qualification/changes_original_visible_order",
    rows: source.research.markets[11].posthoc_label_qualification.changes_original_visible_order.map(
      (v, i) => ({
        path: `/research/markets/11/posthoc_label_qualification/changes_original_visible_order/${i}`,
        exact: exact(v),
      }),
    ),
    omitted_arrays: [],
  });
  assert.equal(labels.length, 1);
  assert.equal(labels[0].clock, "事后标签公开可见");
  assert.equal(
    labels[0].condition,
    source.research.markets[11].identity.condition_id,
  );
  assert.equal(researchPoints({ status: "read_error" }, fragment).length, 0);
  assert.equal(
    researchPoints(status, {
      path: base,
      rows: [{ path: base + "/0", exact: { received_at_utc_ms: null } }],
      omitted_arrays: [],
    }).length,
    0,
  );
  console.log(
    "真实初始化 fallback、三个 spot 时钟、事后 Unknown、同毫秒字段身份与精确大整数通过",
  );
} finally {
  await rm(temp, { recursive: true, force: true });
}
