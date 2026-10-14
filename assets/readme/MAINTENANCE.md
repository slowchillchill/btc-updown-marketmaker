# README visual assets

`architecture-v2.json` and `architecture-en.json` maintain the Chinese and English diagrams. Their repository sources are pinned to the historical source snapshot; training status is described in [#181](https://github.com/slowchillchill/btc-updown-marketmaker/issues/181). The diagram explains relationships and ordering, not measured latency or a running training job.

Regenerate each HTML with Archify 3.1, then run its Python renderer from the repository root. The renderers require Pillow, Noto Sans CJK regular/bold and DejaVu Sans Mono at the standard Linux font paths. Keep generated verification receipts outside the published asset set.

```bash
node <archify>/bin/archify.mjs finalize architecture assets/readme/architecture-v2.json assets/readme/architecture.html --repo-root . --quality showcase --out-dir <checks>/zh
node <archify>/bin/archify.mjs finalize architecture assets/readme/architecture-en.json assets/readme/architecture-en.html --repo-root . --quality showcase --out-dir <checks>/en
python3 assets/readme/architecture-render.py
python3 assets/readme/architecture-render-en.py
```

The PNG/GIF pair comes from each frozen HTML's compiled routes and JSON labels. Both GIFs loop through six illustrative phases in 18 seconds. Covers are generated illustrations; `cover-en-prompt.txt` records the English subtitle edit.
