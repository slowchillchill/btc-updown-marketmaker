# #176 实际跳过与定向覆盖

默认调用中的 ignored/skip 不计为通过；定向运行只覆盖下表明确列出的版本和行为。筛选单个测试时的其他 filtered 项不是额外 ignored，也没有计入该次通过数。

## 固定工作区回归

首轮及审查补证后的固定回归均为1190通过、18 ignored；另显式跳过依赖实际UTC时钟的 `four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second`。下列18项本轮未另行运行，保持其既有外部数据/长时现场原因。补证后的逐目标和跳过名单见 [review/results.json](evidence/validation/review/results.json)。

| 实际名称 | ignored 原因 |
| --- | --- |
| `training_runtime::tests::rtds_source::rtds_hong_kong_vpn_eight_hour_probe` | requires an eight-hour public RTDS run through the local Hong Kong route |
| `dpt_observation_real_tests::ms_real_001_publication_reads_missing_components` | 需要本地 MS-REAL-001 原始输入；普通回归不运行外部数据场景 |
| `real_2026_07_30_daily_manifest_header_is_validated` | requires the local finalized 2026-07-30 dataset |
| `real_daily_manifest_quality_facts_keep_closed_separate_from_gaps_and_reactivation` | requires the local finalized 2026-07-30 and 2026-07-31 datasets |
| `real_four_day_daily_manifests_pass_complete_admission` | requires the local finalized 2026-07-30 through 2026-08-02 datasets |
| `real_snapshot_pending_updates_stitch_deterministically_across_read_batches` | 需要完整准入本地已最终化的 2026-08-04 六类 typed_raw 数据 |
| `real_scoped_six_source_run_uses_one_deterministic_production_path` | requires complete admission of the local finalized 2026-08-04 dataset |
| `frozen_gate_a_v2_final_opens_with_current_reader_and_both_replays` | requires the frozen published Gate A V2 final at its approved absolute path |
| `real_complete_single_market_gate_a_candidate_is_closed_and_replayable` | requires complete admission of the local finalized 2026-08-03 dataset |
| `real_gamma_terminal_is_visible_but_has_no_event_time_projection` | requires complete admission of the local finalized 2026-08-04 dataset |
| `real_same_source_millisecond_orderbook_messages_preserve_captured_order` | requires complete admission of the local finalized 2026-08-04 dataset |
| `real_same_source_millisecond_reducer_is_invariant_to_read_batching` | requires complete admission of the local finalized 2026-08-04 dataset |
| `real_six_current_schemas_decode_through_the_validated_parquet_boundary` | requires complete admission of the local finalized 2026-08-04 dataset |
| `real_waiting_exhaustion_summary_remains_audit_only` | requires complete admission of the local finalized 2026-08-05 dataset |
| `real_ws_terminal_metadata_pair_merges_once` | requires complete admission of the local finalized 2026-08-03 and 2026-08-04 datasets |
| `stage4_baseline::durable_replay_resource_baseline` | 阶段 4 资源与重放原始基线按需显式运行 |
| `legacy_reader::tests::reads_real_5676e37_component_golden_report` | 需要通过 SIM_SYSTEM_LEGACY_COMPONENT_GOLDEN_REPORT 显式提供真实历史报告 |
| `run_v2::tests::frozen_gate_a_representative_market_matches_golden_dual_time_execution_vertical` | requires frozen Gate A V2 final and official CURRENT_QUERY endpoints |

## 原生模型包

首轮44个库测试及3个rounding集成测试通过、18 ignored，其中7个测试函数以本轮指定入口显式通过；原记录见 [native-ignored-coverage.json](evidence/validation/native-ignored-coverage.json)。审查补证后的默认回归仍为47通过，新增小域验收使 ignored 为19；新增入口也已显式通过，合计8个定向测试函数。11个旧归档/长矩阵函数未运行，不记为通过。最新命令与全部19个名称见 [review/results.json](evidence/validation/review/results.json)。

| 实际名称 | 默认 ignored 原因 | 本轮实际覆盖 |
| --- | --- | --- |
| `action_tests::actual_graph_pair_intent_execution_and_frozen_cancel_race` | requires original graph archive and locked project Python | 定向显式运行通过；未计入默认回归通过数；MAIN-1x256；validation/commands.json: cancel-race |
| `action_tests::buy_tests::same_process_python_and_rust_ort_share_native_actor_and_final_intent` | requires explicitly supplied exported graphs and matching Python wheel ORT library | 定向显式运行通过；未计入默认回归通过数；MAIN-1x256、MAIN-30x1024；validation/same-process.json |
| `action_tests::buy_tests::small_domain_joint_probability_entropy_and_singleton_optimizer` | requires locked project Python; finite real joint probability and singleton AdamW validation | 审查补证时显式运行通过，Rust入口1项/Python测试2项/AdamW子场景6项；未计入默认回归通过数；MAIN-1x256；validation/review/probability-and-optimizer.json |
| `action_tests::buy_tests::tick_grid_transition_support_measurement` | #176 单进程 release 首次网格/真实tick切换定向计时；非闭环性能矩阵 | 定向显式运行通过；未计入默认回归通过数；独立原生release进程，不含NN；probe/tick-support.json |
| `action_tests::same_process_cancel_actor_all_objects_and_original_identities` | requires explicitly supplied exported graphs and matching Python wheel ORT library | 定向显式运行通过；未计入默认回归通过数；MAIN-1x256、MAIN-30x1024；validation/latest-cancel-commands.json |
| `book_tests::full_book_three_runtime_and_original_execution` | 有限深盘口三端和原执行入口；需新版本八图 | 定向显式运行通过；未计入默认回归通过数；MAIN-1x256、MAIN-30x1024；validation/MAIN-1x256-full-book.json、MAIN-30x1024-full-book.json |
| `book_tests::native_shared_inputs_preserve_delayed_backward_and_detached_aliases` | 需锁定 Python 环境及 model-train 导入路径；真实共享输入与延后反传 | 定向显式运行通过；未计入默认回归通过数；MAIN-1x256；validation/ownership.json |
| `continuous_tests::continuous_actual_warmup_regression` | 原真实128帧预热回归；需原图和证据目录 | 未运行，不计作通过；原归档/70分钟完整C矩阵或既有长场景，超出本轮两个有限窗口和代表性验证；本轮旧帧、概率梯度、真实执行与三端行为由当前八图的定向检查覆盖。 |
| `continuous_tests::continuous_contribution_before_success_regression` | 原贡献先于成交终局的真实三端有界回归；需原图/录制/永久证据目录 | 未运行，不计作通过；原归档/70分钟完整C矩阵或既有长场景，超出本轮两个有限窗口和代表性验证；本轮旧帧、概率梯度、真实执行与三端行为由当前八图的定向检查覆盖。 |
| `continuous_tests::continuous_economic_scenarios` | 三变体真实经济前史诊断，尚非三端完整矩阵 | 未运行，不计作通过；原归档/70分钟完整C矩阵或既有长场景，超出本轮两个有限窗口和代表性验证；本轮旧帧、概率梯度、真实执行与三端行为由当前八图的定向检查覆盖。 |
| `continuous_tests::continuous_late_formation_regression` | 原BASELINE2失败组与下一真实形成反馈；需原录制/图 | 未运行，不计作通过；原归档/70分钟完整C矩阵或既有长场景，超出本轮两个有限窗口和代表性验证；本轮旧帧、概率梯度、真实执行与三端行为由当前八图的定向检查覆盖。 |
| `continuous_tests::continuous_n2_handoff_schema` | 原C2前史的N=2参数交接短检查；不提交探针意图 | 未运行，不计作通过；原归档/70分钟完整C矩阵或既有长场景，超出本轮两个有限窗口和代表性验证；本轮旧帧、概率梯度、真实执行与三端行为由当前八图的定向检查覆盖。 |
| `continuous_tests::continuous_prepare_recordings` | 保留C1/C2九份实际公共输入；正式矩阵只读复用 | 未运行，不计作通过；原归档/70分钟完整C矩阵或既有长场景，超出本轮两个有限窗口和代表性验证；本轮旧帧、概率梯度、真实执行与三端行为由当前八图的定向检查覆盖。 |
| `continuous_tests::continuous_public_seventy_minutes` | 完整70分钟真实公开输入结构探针；单独显式运行 | 未运行，不计作通过；原归档/70分钟完整C矩阵或既有长场景，超出本轮两个有限窗口和代表性验证；本轮旧帧、概率梯度、真实执行与三端行为由当前八图的定向检查覆盖。 |
| `continuous_tests::continuous_real_population_probes` | 两MAIN的真实P3—P7人口/批次/分块/梯度；需原图 | 定向显式运行通过；未计入默认回归通过数；MAIN-1x256、MAIN-30x1024；validation/population-r2-commands.json、population-summary.json |
| `continuous_tests::continuous_source_sample_schema` | 既有经济前史上的瞬时采样schema核对；不是正式三端矩阵 | 未运行，不计作通过；原归档/70分钟完整C矩阵或既有长场景，超出本轮两个有限窗口和代表性验证；本轮旧帧、概率梯度、真实执行与三端行为由当前八图的定向检查覆盖。 |
| `continuous_tests::continuous_three_runtime_matrix` | 正式每配置/线程完整C1/C2三变体三次；需原图和证据目录 | 未运行，不计作通过；原归档/70分钟完整C矩阵或既有长场景，超出本轮两个有限窗口和代表性验证；本轮旧帧、概率梯度、真实执行与三端行为由当前八图的定向检查覆盖。 |
| `continuous_tests::continuous_three_runtime_warmup_cost` | 三端真实128帧预热成本诊断；不是70分钟验收 | 未运行，不计作通过；原归档/70分钟完整C矩阵或既有长场景，超出本轮两个有限窗口和代表性验证；本轮旧帧、概率梯度、真实执行与三端行为由当前八图的定向检查覆盖。 |
| `continuous_tests::received_economics_pending_preserves_native_frame_and_frozen_read` | 需要保留的原SIM/Ledger现场与原C1录制 | 未运行，不计作通过；原归档/70分钟完整C矩阵或既有长场景，超出本轮两个有限窗口和代表性验证；本轮旧帧、概率梯度、真实执行与三端行为由当前八图的定向检查覆盖。 |

## Python及其嵌套原生调用

完整Python回归的 `test_native_inputs.NativeInputTests.test_real_native_sequence_in_original_three_runtimes` 因 `requires original frozen graph archive` 跳过，随后在当前MAIN-1x256八图上定向通过。没有提供新BASELINE-2图，不声称该配置通过。见 [completed-checks.json](evidence/validation/completed-checks.json)。

完整Python回归内调用的Rust测试忽略 `action_tests::actual_graph_pair_intent_execution_and_frozen_cancel_race`；后续native-sequence调用还忽略 `action_tests::buy_tests::tick_grid_transition_support_measurement`。这两项在各自独立入口已显式通过，对应上表；没有重复计入默认调用的通过数。
