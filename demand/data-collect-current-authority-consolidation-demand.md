# data-collect 当前权威与历史隔离整理需求

> 事项编号：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 对应计划：`plan/data-collect-current-authority-consolidation-plan.md`
>
> 对应 changelog：`changelog/data-collect-current-authority-consolidation-changelog.md`
>
> 状态：`PHASE_2_COMPLETE_PHASE_3_PENDING`
>
> 用户批准时间：2026-07-30

## 一、目标

在进入训练模块开发前，先把 `data-collect` 的当前实现、当前决策、完成状态和历史证据
分层，避免后续开发从旧计划、旧 changelog 检查点或单个检索片段误判当前 authority。

## 二、用户最新决策

1. `DC-INCIDENT-20260728-LAG-IDENTITY-REPAIR` 已由用户完成最终验收。
2. UTC day rollover 和一个完整健康 UTC 日不再是该事故事项的完成阻断门；文档不得
   把这两项写成仍需等待的当前任务，也不得反向声称未取得的运行证据已经发生。
3. paper/live 真实外部市场源 adapter 不属于本轮 `data-collect` 收尾阻断项；该工作
   延后到训练模块完成后，再以独立 demand/plan/changelog 返回实施、测试和部署。
4. 全面审查所有 `data-collect` 相关文档，以当前代码、测试、已批准最新决策和仍有效
   合同为准，修正仍冒充当前 authority 的旧状态和旧执行指令。
5. 历史事件事实不得伪造或抹除。旧的 `PENDING`、`READY`、`当前状态` 等表述如果只
   属于当时检查点，必须明确标记为历史或已被取代，不得继续驱动实现。

## 三、当前权威层要求

1. `data-collect/docs/CURRENT.md` 是所有后续 `data-collect` 任务的强制首读入口。
2. 当前层至少覆盖：
   - 模块范围与三个 binary 的真实能力；
   - 当前 schema、SDK、runtime 和部署边界；
   - 已完成、已延期和已移出本模块 completion gate 的事项；
   - 唯一当前 demand/plan/changelog 路径；
   - supersession 映射和历史读取规则；
   - 与代码或测试冲突时的停止决策门。
3. `data-collect/README.md` 只描述当前可运行接口和行为，不承载开发历史。
4. 当前文档必须能由当前 Cargo、代码、测试或明确的用户决策验证，不得依赖单个历史
   chunk 自证 authority。

## 四、历史隔离要求

1. 在移出活动工作树前，以当前 clean/pushed commit 创建带说明的 Git tag，冻结完整
   历史文档。
2. `main` 保留短小 archive manifest，记录 tag、commit、原路径和 blob hash。
3. 历史文档从活动搜索面移出；需要历史证据时，只能通过 manifest 指定的 tag/path
   显式读取。
4. 历史片段只能作为 evidence，不能单独生成当前实现或执行结论。读取历史后必须重新
   核对 `CURRENT.md`、当前代码和测试。

## 五、非范围

1. 本事项不修改 `data-collect` Rust 运行时代码、schema 或交易逻辑；允许同步仅验证
   Markdown 内容的文档契约测试，使其拒绝已失效的历史执行入口。
2. 本事项不部署、重启或操作任何远端服务，不删除远端数据。
3. 本事项不提前实现 paper/live 真实外部市场源 adapter。
4. 本事项不把带日期的历史执行事实改写成从未发生，也不把用户取消的验收门伪装成
   已获得运行证据。

## 六、验收标准

1. 2026-07-28 事故三件套统一为用户已验收完成；UTC rollover 和完整健康 UTC 日明确
   为由用户移除的完成阻断门。
2. 主需求、主计划、主 changelog、README、acceptance matrix 和 paper/live 接入说明
   统一记录：真实 adapter 延后到训练模块完成后，且不阻断本轮 `data-collect` 收尾。
3. 全部 `data-collect` 相关 Markdown 均进入审计清单；每份文件具有事项、最终状态、
   当前/历史 authority 和替代关系结论。
4. 所有与当前代码、测试或最新决策冲突的活动表述已修正；历史事实保留但不能继续
   冒充当前游标。
5. `CURRENT.md`、current 合同文档、archive manifest 和仓库路由规则落盘。
6. 自动检查能拒绝多个 current authority、活动三件套缺失、archive manifest
   目标缺失或当前文档把历史当作执行 authority。
7. 每个阶段均完成自审、`gpt-5.6-sol/ultra` 独立审查、commit 和 push；最终
   `origin/main` 只能通过 ordinary fast-forward 更新。
