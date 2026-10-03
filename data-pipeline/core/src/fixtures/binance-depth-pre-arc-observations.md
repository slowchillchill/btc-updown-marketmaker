# 改动前完整观察夹具

`binance-depth-pre-arc-observations.json` 来自提交
`73e1e44c3b8aa54c9e0c124b4588724ef2417333` 的原公共 core，生成时公共字段与封组代码均未修改。
使用 `public_observation_depth_tests.rs::scenario` 的同一有限输入，经既有
`record` → `prepare_public_ingress_v1` → `core.push`，将每组完整 `observations` 依次
以 `serde_json::to_vec_pretty` 保存。临时生成测试已移除，普通测试只读取固定夹具。

输入为两个真实构造入口建立的市场 Snapshot、Binance Snapshot 100、连续更新 101、
确认缺口 104、另一 epoch 的 Snapshot 200。五组各保留两个市场的全部观察字段，
覆盖未观察、Fresh、数量更新、ConfirmedGap 与恢复 Fresh；并非有限身份/时钟摘要。
完整文件 SHA-256：`06779842284f8a2f2df3f1e2ef60a0c1432b4a2cff736ffb40460f4e817a87a3`。

回归测试同时读取旧 JSON、比较新实现全部观察 JSON 内容及字段形状，并用手写完整
深度期望值核对层级、顺序、数量与 update ID。旧 JSON 读取后不要求恢复组内 Arc 指针共享。
