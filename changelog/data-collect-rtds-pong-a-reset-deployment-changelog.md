# RTDS-PONG-A 原路径重置部署 Changelog

> 生命周期：`HISTORICAL_EVIDENCE_ONLY`
>
> 最终快照：`FROZEN_AT_20260730_CONSOLIDATION`
>
> 当前执行 authority：`none`
>
> superseded_by：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 读取规则：本文仅用于审计历史事实；内部“当前、下一步、待执行、命令”均属于
> 当时检查点，不得驱动当前实现、测试、部署或远端操作。
>
> 2026-07-30：本文件只保留 2026-07-19 部署事实，已被后续部署闭环取代，不得作为
> 当前远端操作指南。
>
> 事项编号：`DC-DEPLOY-20260719-RTDS-PONG-A-RESET`
>
> 对应需求：`demand/data-collect-rtds-pong-a-reset-deployment-demand.md`
>
> 对应计划：`plan/data-collect-rtds-pong-a-reset-deployment-plan.md`

## 2026-07-19：用户批准原路径立即重置部署

- 用户先提出 UTC 11:01 同日切换；审查证明旧/current binary identity 不能共享同一 UTC 日 manifest，新程序会 exit 76。用户随后明确批准停止旧程序并删除旧数据产物，立即启动新程序，不等待 11:01，也不修改输出路径或增加 `ver2`。
- 删除范围按最小充分原则收窄为远端旧 build 独占的 current-day `utc_day=2026-07-19`；保留 2026-07-15 与 2026-07-18 closed 日及本地副本，不删除整个 `training`/`typed_raw` 根。
- 本地 `/home/chillchill/new-datasets/2026-07-18/typed_raw/utc_day=2026-07-18` 为 `closed + utc_day_barrier`，约 2.4GB，manifest 记录 71 个 finalized 文件；逐文件 SHA-256 实测 71/71 匹配。分析报告位于同一备份根的 `analysis-report-2026-07-18.md`。
- 部署前远端旧 service 为 enabled/active/running，MainPID `1128932`、NRestarts `0`，自 `2026-07-18T10:14:08Z` 运行；正式 binary SHA 为 `469c75af7e682bf59ce0106a8ee6d729f1f5184da16fb24ad3eae9f0b7479f78`。2026-07-19 manifest 为旧 commit `c6b8bac3c5d3ce7ea2fa689bd431cf1a8b7070de`、state open/coverage active；2026-07-15 与 2026-07-18 均 closed。
- RTDS-PONG-A HEAD/upstream 为 `1d95823614ceed78288ce34cb95fbf23e58ef64d`且工作树 clean；本轮复跑 patch contract 17/17 与生产 no-PONG 单 socket测试通过。先前阶段9完整门禁与香港线路15分钟SDK复测继续作为实现证据，不能冒充本次生产部署结果。

## 2026-07-19：production release 构建与远端 staging 通过

- 从 clean detached worktree `/tmp/data-collect-rtds-pong-a-build.WQmrJk/src` 的固定 commit `1d95823614ceed78288ce34cb95fbf23e58ef64d` 构建；显式设置同值 `DATA_COLLECT_GIT_COMMIT`，独立 target 为 `/tmp/data-collect-rtds-pong-a-build.WQmrJk/target`，locked release build 用时 `53.41s`并退出0，构建前后 detached worktree clean且HEAD未变化。
- production binary SHA-256 为 `d3377da9657d235e0f6ccfc7b096418e93d4c4b187efba35fe8f63a2825b804f`，Build ID 为 `f4d2f8c057b54a250645b6f031607346a1d125b9`，x86-64 PIE，interpreter `/lib64/ld-linux-x86-64.so.2`，最高 requirement `GLIBC_2.38`，`--help`与`--version`退出0。
- binary 经带完整SHA的本地staging原子发布到仓库固定`target/release`路径，再上传到远端正式binary同目录的唯一staging；远端 SHA、file、loader、`ldd`、GLIBC、`--help`、`--version`全部通过。远端 unit SHA 保持 `7e847363f9164aa63fd4bb8c27307b24fa2e096cfc806a8e5c739d3d75f08795`。

## 2026-07-19：协调停服与精确 current-day 删除完成

- `systemctl --user stop`于`2026-07-19T10:10:51Z`完成；旧 service 为 inactive/dead、MainPID 0、NRestarts 0、Result success、ExecMainStatus 0。旧 current-day coverage由runtime可靠关闭为`ended_known`，ended时间`1784455851066`，没有由operator手工改写manifest。
- 删除前目标 `/home/ubuntu/btc-updown-marketmaker/datasets/training/typed_raw/utc_day=2026-07-19` 为旧 commit `c6b8bac3c5d3ce7ea2fa689bd431cf1a8b7070de`、旧 binary SHA `469c75af7e682bf59ce0106a8ee6d729f1f5184da16fb24ad3eae9f0b7479f78`独占的open日；manifest SHA为`16314c17815f21d3e54a394caf732e52a46bbc96166b6281fb64081e4dc0effc`。
- 精确目标为`1,813,667,588` bytes、57个文件，其中manifest记录56个finalized文件；`.running=0`、holder=0、dataset lease可取得、symlink=0、special=0、非ubuntu owner=0、目标/descendant mount=0，逐级父路径均为预期真实目录。
- 只删除上述current-day目录；2026-07-15与2026-07-18 closed目录删除后仍存在。该current-day旧数据已从远端永久移除，不可从本次本地昨日副本恢复；这是用户为立即同日重置明确接受的范围。

## 2026-07-19：binary 原子替换与立即启动完成

- 第一次替换命令的旧SHA precondition常量末尾误少字符，命令在`sync/mv`前安全退出；只读复核证明正式binary仍为旧SHA且新staging完整。更正为完整旧SHA后，staging与destination同device，sync后原子替换并同步目录成功；正式binary owner/group=`ubuntu:ubuntu`、mode=`0755`、单硬链接，SHA为新值，staging消失。
- 原 unit 于`2026-07-19T10:13:17Z`立即启动，MainPID=`1152581`、NRestarts=`0`、enabled/active/running；运行中`/proc/1152581/exe`与正式binary SHA都为`d3377da9657d235e0f6ccfc7b096418e93d4c4b187efba35fe8f63a2825b804f`。
- startup recovery只读扫描保留的历史Parquet后，于`1784456022026`毫秒激活新current-day manifest。新manifest为`data_collect_daily_manifest_v3 + open`，build commit与binary SHA为新值，config digest保持`028d5ecdea7094ba3467b00f97f6d61624693248447ae81d50e681b4c012732b`，host=`ip-172-26-10-0`、region=`eu-west-1`，coverage=`active`且`first=last`、`reactivated_within_day=false`；没有混入旧小时文件或旧identity。

## 2026-07-19：即时运行初步验收通过，durable RTDS 门待自然封口

- RTDS专属`polymarket_rtds_chainlink_btc_usd_10.parquet.running`于`10:13:43.338Z`出现且由MainPID持有一个writer FD。生产writer只在收到event并建立active segment后创建该文件，进程继续运行证明首批RTDS event已通过writer调用；零字节只是Parquet row group尚未finalize，不能冒充finalized durable文件。
- `10:15:26Z`至`10:16:11Z`每5秒共10次采样跨过三次旧15秒timeout：MainPID始终`1152581`、NRestarts始终0，全部生产TCP socket集合指纹始终为`03d11f216d8cb998ef92721cc60097da0e3b58e56b699eadd8b3436a943a3d4d`。这直接排除了旧程序约16秒no-PONG重连循环在该窗口内继续发生，但不冒充无限期稳定证明。open manifest在active `.running`阶段不实时合并source counter，因此没有把重复读取的静态`reconnect_events=0`用作动态采样证据。
- `10:16:54Z` Polymarket market running文件已增长到`5,481,425` bytes；aggTrade、depth snapshot/update、RTDS和metadata均已建立各自running文件。journal只有systemd Started记录，无panic、manifest identity、writer、lease或queue terminal。
- 同时采样RSS=`66,064 KiB`、线程=9、FD=20，数据盘可用`54,636,318,720` bytes；启动恢复阶段的高CPU随后降至约26.9%，没有restart storm或资源失控迹象。
- 本轮立即启动与短时稳定性门判定`PASS`，但需求中的RTDS真实update落盘门尚未完成。当前零字节`.running`只证明事件触发writer并被ArrowWriter缓冲，不能证明durable row；必须等待自然UTC 11:00边界，以finalized Parquet可读行数、文件SHA和manifest记录完成该门，完成前不得宣告本事项全部验收通过。

## 2026-07-19：UTC 11:00 durable RTDS 门通过

- 用户级service在`10:30:07Z`至`11:01:09Z`只读守护期间始终为active，同一MainPID=`1152581`、NRestarts=`0`；守护没有执行restart、rotate、flush或任何远端写操作。独立审查另在`10:28:27.368Z`至`10:29:13.783Z`每秒采样46秒，四个ESTABLISHED TCP tuple集合始终不变，进一步排除旧约16秒no-PONG重连循环在该窗口内发生。
- 自然UTC小时边界形成`polymarket_rtds_chainlink_btc_usd_10.parquet` finalized文件，其mtime为`11:00:00.439303350Z`、大小为`132,173` bytes；manifest hour entry为`utc_hour=2026-07-19T10`、`rollover_reason=utc_hour_boundary`、`rows=2750`、`event_type_counts.update=2750`、schema v4，接收范围`10:13:43.339Z`至`10:59:59.057Z`。
- manifest source counter为`files=1`、`rows=written_rows=2750`、`reconnect_events=0`、`dropped_rows=0`、`lag_events=0`、`backfill_rows=0`、`structural_dedupe_dropped_events=0`。该counter是在finalized文件进入manifest后读取，不再把active阶段的静态0误作动态采样证据。
- manifest file entry记录的Parquet SHA、远端文件实际SHA及复制到本地临时验证目录后的SHA三方均为`e37670deac250b6021654c4f9c28b8d9ce552dbe7bb269f63c32a54b47b70d47`。本地PyArrow `23.0.1`独立读取通过：1个row group、2750行，`message_type`全部为`update`，schema、host和region逐行一致且value非空。
- 同一hour 10自然封口还包含aggTrade `11,435`行、depth snapshot `1`行、depth update `27,768`行、Polymarket market `2,319,622`行及metadata `29`行；六个source的实际文件SHA逐一与manifest匹配。六路finalized source counter均为`files=1`、`rows=written_rows`，且`dropped_rows=0`、`lag_events=0`、`reconnect_events=0`，证明本轮不只RTDS而是全部生产source均形成可校验小时产物。
- `11:01:31Z`服务仍为enabled/active/running、MainPID=`1152581`、NRestarts=`0`，正式binary与`/proc/1152581/exe`仍为新SHA；用户级journal从启动至该时刻无warning/error。新小时`11`的RTDS `.running`已自然创建，未发生人为重启。
- 独立资源复核在`10:30:47Z`测得进程RSS约`100,264 KiB`；systemd cgroup约`1,224,667,136` bytes中file cache约`1,130,479,616` bytes、anon约`86,081,536` bytes，OOM/PSI事件均为0。因此cgroup较大值主要是可回收历史Parquet page cache，不是进程anon失控。

## 2026-07-19：阶段5文档门通过

- 在durable与六源证据写入最新diff后复跑`cargo test -p data-collect --locked --test deploy_readme_contract -- --nocapture`，5/5通过；`cargo fmt --all -- --check`与`git diff --check`均退出0。
- 工作树范围为本事项3个新需求/计划/changelog文件及8个既有authority/部署README指针，共11个文档文件；没有代码改动。

## 2026-07-19：最终独立终审通过

- 独立审查先识别并推动修正四类证据表达问题：不得在RTDS durable row形成前提前宣告COMPLETE；不得把active open manifest的静态counter当作动态重连证据；不得混淆unit start与current-day activation；不得保留已过期的主计划authority指针。随后又修正mtime与publish时刻、manifest文件摘要与file entry Parquet SHA两处术语歧义。
- durable证据与修订后的最终独立终审结论为`PASS`，Critical=`0`、Important=`0`、Minor=`0`。审查者独立复跑deploy README合同5/5、fmt与diff检查均通过，并确认用户批准、精确删除边界、原路径替换、六源hour 10数据、no-PONG修复证据、资源状态和11文件文档范围彼此一致。
- 本事项3个新需求/计划/changelog文件及8个既有authority/README指针由同一收口commit提交并push到`origin/after-data-plan`；实际commit SHA与push结果在执行回执中记录，避免在commit正文中制造自引用哈希。
