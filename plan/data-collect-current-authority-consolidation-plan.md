# data-collect 当前权威与历史隔离整理计划

> 事项编号：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 对应需求：`demand/data-collect-current-authority-consolidation-demand.md`
>
> 对应 changelog：`changelog/data-collect-current-authority-consolidation-changelog.md`
>
> 状态：`PHASE_2_COMPLETE_PHASE_3_PENDING`

## 阶段 1：关闭旧验收门并冻结延期边界

- 任务 1.1：同步 2026-07-28 事故需求、计划和 changelog。
  - 步骤 1.1.1：把三件套顶部状态统一为用户已验收完成。
    - 验证：三份状态完全一致，不再出现 current `ROLLOVER_PENDING`。
  - 步骤 1.1.2：把 UTC rollover 和完整健康 UTC 日改为用户移除的完成阻断门。
    - 验证：不再存在等待指令；文档也不声称这两项运行证据已经取得。
  - 步骤 1.1.3：保留原 Stage 6 检查点，并明确其 pending 状态已被最新用户决策
    supersede。
    - 验证：历史时间线仍可审计，局部读取时不会被当成当前游标。
- 任务 1.2：同步 paper/live 延期决策。
  - 步骤 1.2.1：更新主需求、主计划、主 changelog、README、acceptance matrix 和
    paper/live 接入说明。
    - 验证：所有活动入口均明确真实 adapter 延后到训练模块完成后；当前
      heartbeat/status 能力不被夸大。
  - 步骤 1.2.2：确认延期不改变 future adapter 已冻结的公共 RTDS、handoff 和本机
    runtime receive 合同。
    - 验证：只改变排期和 completion gate，不改变 future 技术语义。
- 任务 1.3：执行文本交叉检查、diff 自审和独立阶段审查。
  - 验证：需求验收 1、2 通过，Critical/Important/Minor 均无未解决问题。
- 完成门：创建并 push 阶段 1 commit 后进入阶段 2。

## 阶段 2：全量文档与当前实现一致性审计

- 任务 2.1：建立完整文档清单和事项配对表。
  - 步骤 2.1.1：枚举 `demand/data-collect*`、`plan/data-collect*`、
    `changelog/data-collect*`、`data-collect/README.md`、
    `data-collect/docs/*.md` 和其他模块中直接约束 `data-collect` 的段落。
    - 验证：文件数与 Git 跟踪清单一致，无遗漏。
  - 步骤 2.1.2：为每份文件记录 matter id、三件套互引、状态、authority、
    superseded_by 和当前用途。
    - 验证：活动事项三件套一一对应；缺失项有明确处置。
- 任务 2.2：从当前代码和测试建立事实基线。
  - 步骤 2.2.1：核对 Cargo/SDK、三个 binary、六源、schema、RTDS、PMXT-like
    hourly layout、resolution、部署和 paper/live 能力。
    - 验证：每个当前文档结论具有代码路径或测试证据。
  - 步骤 2.2.2：区分 current implementation、future contract、historical evidence
    和 deferred work。
    - 验证：任何旧计划都不能因更晚日期或更长篇幅自动取得 authority。
- 任务 2.3：修正文档冲突。
  - 步骤 2.3.1：修正顶部状态、旧 `当前状态/下一步`、错误交叉引用和已完成仍 pending
    的活动表述。
    - 验证：当前代码、当前测试、最新用户决策和活动文档可以同时成立。
  - 步骤 2.3.2：对带日期的历史段落保留原事实并标记历史/已取代，不重写过去。
    - 验证：changelog 仍保留决策原因和失败证据。
- 任务 2.4：执行文档引用检查、相关 contract tests、diff 自审和独立阶段审查。
  - 验证：需求验收 3、4 通过，审查无未解决问题。
- 完成门：创建并 push 阶段 2 commit 后进入阶段 3。

## 阶段 3：建立当前权威层并隔离历史

- 任务 3.1：冻结阶段 2 的完整历史。
  - 步骤 3.1.1：确认阶段 2 commit clean、已 push，并创建 annotated Git tag。
    - 验证：tag 精确指向该 commit；远端可读取。
  - 步骤 3.1.2：生成 archive manifest，记录每个历史文件的 tag、commit、path 和
    blob hash。
    - 验证：manifest 中每个对象均可由 `git show` 和 `git rev-parse` 复核。
- 任务 3.2：建立活动 current 文档。
  - 步骤 3.2.1：新增 `data-collect/docs/CURRENT.md` 和 `docs/current/` 下的范围、
    接口、运行、验收、限制与审计结论文档。
    - 验证：后续模块只读 current 层即可获得完整稳定合同。
  - 步骤 3.2.2：把主 demand/plan/changelog 收敛为当前模块合同与最终状态。
    - 验证：不再要求读取历史长文才能判断当前实现。
- 任务 3.3：把历史长文移出活动搜索面。
  - 步骤 3.3.1：从 `main` 活动树移除已由 tag/manifest 冻结的旧事项和阶段报告。
    - 验证：当前层需要的文件仍存在；历史可从 tag 完整恢复。
  - 步骤 3.3.2：更新 `AGENTS.md` 路由规则，要求 data-collect 任务先读
    `CURRENT.md`，默认不得使用历史作为执行 authority。
    - 验证：规则简短，不内联完整历史。
- 任务 3.4：增加轻量自动一致性检查。
  - 步骤 3.4.1：检查唯一 current authority、活动三件套、current 引用和 archive
    manifest 对象。
    - 验证：正常树通过；构造多个 current 或坏引用时检查会失败。
  - 步骤 3.4.2：运行相关文档合同测试、diff 自审和独立阶段审查。
    - 验证：需求验收 5、6 通过，审查无未解决问题。
- 完成门：创建并 push 阶段 3 commit。

## 最终关闭

- 复核需求、计划、changelog、CURRENT、代码和测试证据一致。
- 使用独立 `gpt-5.6-sol/ultra` 做最终审查。
- 确认 `origin/main` 是当前 HEAD 的祖先后，以 ordinary push fast-forward 更新
  `main`；禁止 force。
- 验证本地 HEAD、当前分支、`origin/main` 和 `origin/fix-rtds-silents` 的目标关系。
