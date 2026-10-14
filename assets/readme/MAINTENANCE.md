# README visual assets

`architecture-v2.json` and `architecture-en.json` maintain the Chinese and English diagrams. Their repository sources are pinned to the historical source snapshot; training status is described in [#181](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181). The diagram explains relationships and ordering, not measured latency or a running training job.

Regenerate each HTML with Archify 3.1, then run its Python renderer from the repository root. The renderers require Pillow, Noto Sans regular/bold, Noto Sans CJK SC regular/bold and Noto Sans Mono at the standard Linux font paths. Keep generated verification receipts outside the published asset set.

```bash
node <archify>/bin/archify.mjs finalize architecture assets/readme/architecture-v2.json assets/readme/architecture.html --repo-root . --quality showcase --out-dir <checks>/zh
node <archify>/bin/archify.mjs finalize architecture assets/readme/architecture-en.json assets/readme/architecture-en.html --repo-root . --quality showcase --out-dir <checks>/en
python3 assets/readme/architecture-render.py
python3 assets/readme/architecture-render-en.py
```

The PNG/GIF pair comes from each frozen HTML's compiled routes and JSON labels. Both GIFs loop through six illustrative phases in 18 seconds. Covers are generated illustrations; `cover-en-prompt.txt` records the English subtitle edit.

## PMM visual baseline

The README GIF/PNG assets use the user-confirmed PMM VI 0.4 implementation in `visualization/ui/src/style.css` and `visualization/docs/hour-snapshots.md`, referenced by [D-0337](https://github.com/slowchillchill/btc-updown-marketmaker/issues/215#issuecomment-6117032752). These implementation values are not a separate complete VI manual. The existing interactive HTML keeps its original Archify theme.

Use white `#FFFFFF`, cobalt `#0D41A2`, text `#171B20`, secondary text `#53606C`, borders `#DCE1E5`, active-node pale green `#DCEBE7`, secondary-line blue-grey `#39758A`, and notices `#A34F45`. Noto Sans serves English, Noto Sans CJK SC serves Chinese, and Noto Sans Mono serves module names. The `Prediction Market Maker` name accompanies the original logo.

The renderer reads `visualization/ui/public/pmm-cobalt.png` without changing it. It reproduces the CSS visible window (160×52.07 within a 232.66px-wide proportional image, offset −36.33/−32.13), then preserves aspect ratio and at least 0.5H clear space around the visible mark. Do not derive a crop from the noisy alpha bounding box. The expanded header moves the diagram as a whole; JSON geometry, compiled routes and the six phase descriptions remain unchanged.
