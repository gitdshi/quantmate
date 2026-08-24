# QuantMate 全自动交易闭环设计报告

> 目标：把 QuantMate 已有的「因子研究 → 组合回测 → Paper 交易」能力，编排成一个每天自动运行、自我调整的闭环。
>
> 时间基准：Asia/Shanghai，A 股交易日。交易日历以 `trade_date` / 交易日历服务为准（非交易日跳过交易与结算，但数据同步仍可运行）。

---

## 1. 摘要

QuantMate 已经具备了闭环所需的绝大部分**原子能力**，但当前它们彼此割裂、靠人工按顺序触发，缺少一个把它们串起来的**顶层编排器和决策引擎**。要实现"每天晚上自动挖因子回测、调整因子、部署 paper，白天实盘模拟交易，晚上分析结果再调策略"的闭环，核心不是重写这些能力，而是新增三层：

1. **自动任务编排层（Autopilot Orchestrator）**——把各阶段按依赖顺序、在固定时刻自动触发，并管理运行状态、重试与崩溃恢复。
2. **决策引擎（Decision Engine）**——用可配置的规则把"回测结果 → 因子取舍 → 策略生成 → 是否部署/加仓/回滚/停止"这些决策自动化。
3. **风控与人工护栏（Guardrails）**——自动部署前的审查、运行中的 kill-switch、以及全程审计，保证"自动"不等于"失控"。

本报告先做现状盘点，再给出目标架构、每日时序、各阶段详细设计、需要新建的组件与数据模型、风险清单和分阶段实施计划。

---

## 2. 现状盘点：已有能力 → 闭环环节映射

### 2.1 数据同步（夜间）

| 能力 | 位置 | 状态 |
|---|---|---|
| 每日定时同步 daemon（`--daemon`，`SYNC_HOUR=2`） | [app/datasync/scheduler.py](../app/datasync/scheduler.py) | ✅ 已实现 |
| 每日同步后自动触发 VNPy 同步 + data_catalog 刷新 | [run_daily_sync()](../app/datasync/scheduler.py) | ✅ 已实现 |
| 独立 backfill 兜底循环（`--backfill-loop`） | [scheduler.py](../app/datasync/scheduler.py) | ✅ 已实现 |
| staging 容器 `datasync` / `datasync-backfill` | [docker-compose.staging.yml](../docker-compose.staging.yml) | ✅ 已部署 |

### 2.2 因子挖掘

| 能力 | 位置 | 状态 |
|---|---|---|
| Alpha158 因子挖掘（QLib） | [factor_screening.py::mine_alpha158_factors](../app/domains/factors/factor_screening.py) | ✅ 已实现（含 LABEL 过滤、inf 处理、窗口自适应） |
| 挖掘→筛选→落库→推荐 流水线 | [pipeline.py::run_factor_mining_pipeline](../app/domains/factors/pipeline.py) | ✅ 已实现 |
| RD-Agent 自主挖因（HypothesisGen→Experiment→CoSTEER→Feedback） | [rdagent_service.py](../app/domains/factors/rdagent_service.py) + [rdagent_tasks.py](../app/worker/service/rdagent_tasks.py) + sidecar 容器 | ✅ 已实现 |
| RQ 后台任务封装 `enqueue_factor_pipeline`（支持 `auto_promote`） | [factor_tasks.py](../app/worker/service/factor_tasks.py) | ✅ 已实现 |

### 2.3 因子回测 / 组合回测

| 能力 | 位置 | 状态 |
|---|---|---|
| 因子 → 横截面组合回测（复用组合引擎） | [backtest_task.py::run_factor_backtest_task](../app/domains/factors/backtest_task.py) | ✅ 已实现 |
| 日频组合回测引擎（T+1、涨跌停、整手） | [composite/backtest_engine.py](../app/domains/composite/backtest_engine.py) | ✅ 已实现 |
| universe/trading/risk 三层编排 | [composite/orchestrator.py](../app/domains/composite/orchestrator.py) | ✅ 已实现 |

### 2.4 策略生成与部署

| 能力 | 位置 | 状态 |
|---|---|---|
| 多因子 → vnpy CTA 代码 / Qlib TopkDropout 配置生成 | [strategies/multi_factor_engine.py](../app/domains/strategies/multi_factor_engine.py) | ✅ 已实现 |
| 组合策略（universe/trading/risk 组件）CRUD | [composite/service.py](../app/domains/composite/service.py) | ✅ 已实现 |
| Paper 部署（strategy / composite 两类来源） | [paper_trading_service.py::deploy](../app/domains/trading/paper_trading_service.py) | ✅ 已实现 |
| Paper 运行时（session/gateway/runtime 模式） | [paper_runtime_service.py](../app/domains/trading/paper_runtime_service.py) | ✅ 已实现 |
| Paper 状态调和 daemon（Redis pub/sub + 轮询） | [paper_runtime_daemon.py](../app/domains/trading/paper_runtime_daemon.py) | ✅ 已实现 |

### 2.5 白天实盘（模拟）交易

| 能力 | 位置 | 状态 |
|---|---|---|
| 实时行情快照（腾讯/AkShare） | [market/realtime_quote_service.py](../app/domains/market/realtime_quote_service.py) | ✅ 已实现 |
| 三个执行器（CTA / portfolio / composite）轮询快照→合成 Bar→驱动策略 | [paper_strategy_executor.py](../app/domains/trading/paper_strategy_executor.py) 等 | ⚠️ 兼容桥（快照轮询，非事件驱动） |
| 撮合、成交台账、账户 | [paper_matching_worker.py](../app/domains/trading/paper_matching_worker.py)、[paper_execution_ledger.py](../app/domains/trading/paper_execution_ledger.py)、[paper_account_service.py](../app/domains/trading/paper_account_service.py) | ✅ 已实现 |

### 2.6 夜间结算与分析

| 能力 | 位置 | 状态 |
|---|---|---|
| 收盘后 mark-to-market + 净值快照 | [paper_settlement_service.py::settle_all](../app/domains/trading/paper_settlement_service.py) | ✅ 已实现（**缺定时触发**） |
| Paper 绩效分析（Sharpe/Sortino/回撤/胜率/盈亏比） | [paper_analytics_service.py](../app/domains/trading/paper_analytics_service.py) | ✅ 已实现 |
| AI 回测/绩效报告 | [ai/backtest_report_service.py](../app/domains/ai/backtest_report_service.py) | ✅ 已实现 |

### 2.7 任务与队列基础设施

| 能力 | 位置 | 状态 |
|---|---|---|
| RQ Worker（队列：backtest/optimization/default/low/rdagent） | [worker/service/run_worker.py](../app/worker/service/run_worker.py) | ✅ 已实现 |
| Redis 作为 job 元数据 / pub-sub | [worker/service/config.py](../app/worker/service/config.py) | ✅ 已实现 |

---

## 3. 结论：缺什么

现有能力齐整，但**没有一条把它们串起来的自动主线**。具体缺口：

1. **没有 Autopilot 编排器**：数据同步、挖因、回测、部署、结算、分析各自独立或靠人工触发，没有按依赖顺序自动执行的主调度器（现有 `scheduler.py` 只负责 datasync）。
2. **没有决策引擎**：
   - `_promote_factors` 只写 `factor_recommendations` 表，**不会**生成策略、不会部署到 paper（见 [pipeline.py](../app/domains/factors/pipeline.py)）。
   - 回测结果→取舍因子→组合策略→自动部署，整段链路需要新写"组装 + 决策"逻辑。
   - 交易绩效→是否加仓/减仓/回滚/停止，没有自动规则。
3. **结算没有定时触发**：`PaperSettlementService.settle_all()` 已实现，但未接入任何调度器。
4. **风控与人工护栏缺失**：自动部署前的审查、运行中的止损/回撤熔断、kill-switch、全程审计日志尚未成型。
5. **可观测性/恢复缺失**：闭环自身的状态机、幂等、重试、崩溃恢复、监控告警还没有。

---

## 4. 目标闭环架构

```
                         ┌────────────────────────────────────────────┐
                         │        Autopilot Orchestrator (新)          │
                         │  - 每日 DAG 调度 (cron + 交易日历感知)        │
                         │  - 阶段状态机 / 依赖 / 重试 / 幂等 / 恢复      │
                         │  - 全程写 autopilot_runs / autopilot_stages  │
                         └──────┬─────────────────────────────┬────────┘
                                │ 通过 RQ 任务投递各阶段          │ 通过决策引擎推动
   ┌────────────────────────────▼───────┐            ┌────────▼──────────────────────────┐
   │  夜间研究循环 (Research Loop)        │            │  交易调整循环 (Trading Loop)        │
   │                                    │            │                                    │
   │ ① 数据同步 (已有 scheduler)          │            │ ⑦ 收盘结算 (已有, 需定时接入)        │
   │ ② Qlib 数据转换 (已有, 需定时接入)     │            │ ⑧ 交易绩效分析 (已有 analytics)      │
   │ ③ 因子挖掘 Alpha158 + RD-Agent (已有)│            │ ⑨ AI 归因报告 (已有 ai 服务)        │
   │ ④ 因子回测 (已有 backtest_task)      │            │ ⑩ 策略调整决策 (新 Decision Engine) │
   │ ⑤ 因子取舍/调参 (新 Decision Engine) │            │    → 继续 / 调仓 / 回滚 / 停止      │
   │ ⑥ 策略生成+自动部署 Paper (新桥接)    │            │    → 反馈回 ②/③ 或 ⑥             │
   └────────────────────────────────────┘            └──────────────────────────────────┘
                                │                                        ▲
                                ▼                                        │
                ┌────────────────────────────┐    白天实时行情/成交/持仓     │
                │ 白天交易循环 (Day Trading)    │────────────────────────────┘
                │ 09:30-15:00 paper runtime   │
                │ realtime quote → executor   │
                └────────────────────────────┘
```

两个循环的耦合点：
- **研究循环**在**盘前**生产"新的候选策略"，并决定是否**热替换/新增部署**到 paper。
- **交易调整循环**在**盘后**根据真实（模拟）绩效，把"表现差的因子/策略"反馈回研究循环，进行淘汰或再挖掘。

---

## 5. 每日时序（Asia/Shanghai，A 股交易日）

| 时刻 | 阶段 | 动作 | 现有/新建 |
|---|---|---|---|
| 02:00 | D1 数据同步 | `datasync --daemon`（已有），同步昨日/最新交易日数据 | 已有 |
| 02:00–05:00 | D1b 回填兜底 | `--backfill-loop`（已有） | 已有 |
| 05:30 | D2 Qlib 转换 | tushare → qlib bin 重导，刷新 `get_qlib_data_range` | 已有（需接入定时） |
| 06:00 | R1 因子挖掘 | Alpha158 + RD-Agent 自主挖因 | 已有 |
| 06:30 | R2 因子回测 | 横截面组合回测，产出 IC/净值/回撤 | 已有 |
| 07:00 | R3 因子取舍 | 决策引擎：IC/ICIR/相关性去重/换手率 → 选定因子集与权重 | 新建 |
| 07:30 | R4 策略生成+部署 | 生成组合策略 → 风控审查 → 部署到 paper（盘前生效） | 新建桥接 |
| 08:00 | R5 盘前校验 | kill-switch 检查、账户/持仓对账、告警 | 新建 |
| 09:15–15:00 | T 白天交易 | paper runtime 实时驱动策略下单、撮合、成交、持仓 | 已有 |
| 15:30 | E1 收盘结算 | `settle_all()` mark-to-market + 净值快照 | 已有（需接入定时） |
| 16:00 | E2 交易分析 | `PaperAnalyticsService` + AI 归因报告 | 已有 |
| 17:00 | E3 策略调整 | 决策引擎：绩效 vs 阈值 → 保留/调仓/回滚/停止 + 反馈回研究循环 | 新建 |
| 23:00 | (可选) 预跑 | 预热次日研究循环或处理上日失败重试 | 新建（可选） |

非交易日：跳过 R3/T/E 阶段；D1/D1b/D2/R1/R2 可继续在休息日跑（取决于数据可用性）。

---

## 6. 各阶段详细设计

### 6.1 阶段 A — 夜间数据同步（已有，加挂钩）

- 复用 [scheduler.py](../app/datasync/scheduler.py) 的 `--daemon` 与 `--backfill-loop`。
- **新增**：同步结束后发布 Redis 事件 `autopilot:data_ready`，作为 D2/Qlib 转换的依赖触发信号。
- **新增**：数据质量门槛——`data_sync_status` 中 `pending`/`error` 比例超阈值时，暂停研究循环并告警（避免用残缺数据挖因）。

### 6.2 阶段 B — Qlib 数据转换（已有，需接入定时）

- 现状：staging 已验证 QLib 数据可产出、`_clamp_qlib_window` 已能自适应数据日历范围。
- **新增**：一个 `run_qlib_ingest_task`（或复用现有 qlib 任务）进入每日 DAG，导完后调用 `get_qlib_data_range()` 校验新范围，验证通过才继续 R1。

### 6.3 阶段 C — 因子挖掘 + 回测

- `run_factor_mining_pipeline`（Alpha158）与 RD-Agent 挖掘并行/串行执行，产出候选因子。
- 对每个候选因子执行 `run_factor_backtest_task`（横截面组合回测），产出统一指标：`ic_mean / ic_ir / turnover / total_return / max_drawdown / sharpe`。
- 结果统一落库，供决策引擎读取。

### 6.4 阶段 D — 因子取舍与权重（新 Decision Engine 的"研究侧"）

输入：候选因子指标。输出：选定因子集 + 方向 + 权重。

规则（可配置，初版建议）：
1. `|ic_ir| >= ir_threshold` 且 `|ic_mean| >= ic_threshold`（复用 [pipeline.py](../app/domains/factors/pipeline.py) 的筛选口径）。
2. 相关性去重：两两 `|corr| > corr_threshold` 保留 |IC| 更大者。
3. 换手率上限：`turnover < turnover_cap`（高换手带来滑点/手续费敏感性）。
4. 方向符号：`sign(ic_mean)` 决定做多/做空方向。
5. 与上一期因子集对比：保留连续多期仍显著的因子（降低过拟合），淘汰新出现但历史不稳定因子。

### 6.5 阶段 E — 策略生成 + 自动部署（新桥接，核心缺口）

这是当前最大缺口。把"配菜"变成"上菜"：

1. 用选定因子构建 `CompositeStrategy`（universe = 因子的股票池 / `csi300`，trading = 因子排序，risk = 仓位上限/止损）。
2. 或走 [multi_factor_engine.py](../app/domains/strategies/multi_factor_engine.py) 生成组合策略代码。
3. 风控审查（见 7.3）通过后，调用 [paper_trading_service.py::deploy](../app/domains/trading/paper_trading_service.py) 部署。
4. 部署结果（deployment_id、状态、参数快照）写回 `autopilot_stages`，供次日对账和回滚。

### 6.6 阶段 F — 白天实时交易（已有，加增强）

- 复用 paper runtime daemon + 三个 executor + gateway + matching。
- **增强（可选）**：把快照轮询逐步换成更接近真实的事件驱动（见 [paper_trading_runtime_design.md](paper_trading_runtime_design.md) 中 phase 3/4/5 的 vnpy-native gateway 目标）。
- **增强（必要）**：盘中风险监测——权益回撤、单日亏损、异常下单量触发 `autopilot:guardrail` 事件，可自动降级为"只读/平仓"。

### 6.7 阶段 G — 收盘结算 + 分析（已有，加定时）

- 定时（15:30）触发 `PaperSettlementService.settle_all()`。
- 触发 `PaperAnalyticsService.get_analytics()` + AI 归因报告，产出当日绩效与归因。
- 结果作为 E3 决策引擎输入。

### 6.8 阶段 H — 交易结果驱动的策略调整（新 Decision Engine 的"交易侧"）

输入：paper 绩效 + 归因。输出：策略动作。

规则（可配置，初版建议）：
1. `max_drawdown >= dd_stop` → **停止**该部署，平仓，标记失败，禁止次日自动再部署同类策略。
2. `sharpe < sharpe_floor` 且连续 N 日 → **降级**（减仓/清仓），并把其中表现最差因子反馈回研究循环做**淘汰/再挖掘**。
3. 表现好的因子 → **加权**（提高权重），并保存为"已验证因子"进入下一期候选优先池。
4. 所有动作记录到 `autopilot_decisions`，可追溯。

---

## 7. 需要新建的组件

### 7.1 Autopilot Orchestrator（新 daemon）

```
app/domains/autopilot/
  orchestrator.py     # 每日 DAG + 阶段状态机 + 依赖调度 + 重试/幂等/恢复
  schedule.py         # cron + 交易日历感知
  state.py            # 阶段枚举、状态转换
  dao/
    autopilot_dao.py  # autopilot_runs / autopilot_stages / autopilot_decisions 读写
```

- 通过 RQ 投递各阶段任务（复用现有 worker），自己只做"调度 + 状态记账"。
- 用一个 `docker-compose` 新容器 `autopilot`（command: `python -m app.domains.autopilot.orchestrator`），`restart: unless-stopped`。
- 幂等：每阶段执行前检查上阶段状态；崩溃后重启时按状态机"续跑"而不是从头重跑（参考现有 backfill daemon "启动时 reset running→pending" 的思路）。

### 7.2 Decision Engine（新）

```
app/domains/autopilot/
  decision_engine.py  # 研究侧 (因子取舍/权重) + 交易侧 (继续/调仓/回滚/停止)
  policies.py         # 阈值与规则的可配置策略 (从 system_config 读取)
```

- 所有阈值放入 `system_config`（复用 [system_config_registry.py](../app/infrastructure/config/system_config_registry.py) 与 [SystemConfigTab](../quantmate-portal/src/components/SystemConfigTab.tsx) 前端），让用户可调且立即生效。

### 7.3 风控与人工护栏（新）

```
app/domains/autopilot/
  guardrails.py       # 部署前审查、盘中熔断、kill-switch、人工审批门
```

- **部署前审查**：单策略最大仓位、标的池合法性、回测样本外表现、是否与现有持仓冲突。
- **盘中熔断**：权益回撤/单日亏损阈值 → 自动平仓或降级。
- **人工审批门**：首日或资金规模超阈值时，策略进入 `pending_approval`，需人工确认后才真正下单（默认开启，可配置关闭）。
- **kill-switch**：全局开关，一键停掉所有自动部署与研发布局。

### 7.4 数据模型（新表，放在 `quantmate` 库）

| 表 | 用途 |
|---|---|
| `autopilot_runs` | 每次闭环运行（run_id、业务日期、market、状态、开始/结束时间） |
| `autopilot_stages` | 每阶段执行记录（run_id、stage、状态、参数、结果 JSON、错误、重试次数） |
| `autopilot_decisions` | 决策引擎产出的决策（run_id、决策类型、输入摘要、动作、原因、审批状态） |

### 7.5 结算/分析定时接入（改动量小）

- 在 Autopilot 编排器里注册 `settle_all` / `get_analytics` / AI 报告三个阶段入口即可，无需改现有实现。

---

## 8. 风险与失败模式

| 风险 | 影响 | 缓解 |
|---|---|---|
| 数据同步积压/卡死（已知 backfill daemon 会卡死） | 挖因/回测用旧数据，决策失真 | 数据质量门槛 + 监控重启 + 已有多 daemon 兜底 |
| Qlib 数据缺失/日期漂移 | 挖因空结果或退化指标 | `_clamp_qlib_window` 已自适应；D2 阶段转换后强校验范围 |
| 过拟合（每天换一批因子） | 实盘衰减 | 因子跨期稳定性要求 + 样本外验证 + 换手率约束 |
| 自动部署失控 | 大额亏损/异常下单 | 部署前审查 + 盘中熔断 + 人工审批门 + kill-switch |
| 重复执行（幂等缺失） | 重复下单/重复部署 | 阶段状态机 + 唯一业务键 + 先检查后执行 |
| 静默失败链 | 后阶段在残缺结果上运行 | 每阶段成功标志 + 依赖校验 + 告警 + 全链路审计 |

---

## 9. 分阶段实施计划

### Phase 1 — 主线打通（最小闭环）
1. 新增 `autopilot` 域与编排器骨架、三张新表。
2. 接入定时结算 + 分析（E1/E2）。
3. 新桥接：因子→组合策略→自动部署（E）。
4. 落地研究侧决策规则 v1（D）。
5. 完成「夜间挖因→回测→部署→白天跑→晚上结算分析」的最小自动闭环。

### Phase 2 — 决策闭环与风控
1. 交易侧决策规则（H）：回撤停止/减仓/加权/淘汰。
2. 部署前审查 + 盘中熔断 + kill-switch + 审计。
3. 因子跨期稳定性与样本外验证。

### Phase 3 — 增强与收敛
1. paper 执行器向事件驱动 gateway 收敛（复用 [paper_trading_runtime_design.md](paper_trading_runtime_design.md) phase 3/4/5）。
2. AI 归因自动生成更细粒度的因子淘汰建议，接入决策引擎。
3. 统一回测历史与旧的三条回测路径收敛（参考 [factor_composite_backtest_design_plan.md](factor_composite_backtest_design_plan.md)）。

---

## 10. 关键性能指标与验收标准

- **稳定性**：连续 N 个交易日闭环零人工干预跑通；任一阶段失败 ≤ 1 次且可自动重试恢复。
- **数据质量**：研究循环启动前数据门槛通过率 100%。
- **风控**：任何一笔自动部署都能追溯到决策记录；kill-switch 端到端生效。
- **绩效基准**：paper 组合在样本外区间 Sharpe/回撤不低于预设阈值（作为是否继续放大资金/转实盘的信号）。
- **可解释性**：每次调仓/部署都能给出"为什么"（因子指标 + 决策规则 + 归因）。

---

## 11. 附：关键文件索引

- 数据同步：[app/datasync/scheduler.py](../app/datasync/scheduler.py)
- 因子挖掘：[app/domains/factors/factor_screening.py](../app/domains/factors/factor_screening.py)、[app/domains/factors/pipeline.py](../app/domains/factors/pipeline.py)
- 因子回测：[app/domains/factors/backtest_task.py](../app/domains/factors/backtest_task.py)
- RD-Agent：[app/domains/factors/rdagent_service.py](../app/domains/factors/rdagent_service.py)、[app/worker/service/rdagent_tasks.py](../app/worker/service/rdagent_tasks.py)
- 组合回测：[app/domains/composite/backtest_engine.py](../app/domains/composite/backtest_engine.py)、[app/domains/composite/orchestrator.py](../app/domains/composite/orchestrator.py)
- 策略生成：[app/domains/strategies/multi_factor_engine.py](../app/domains/strategies/multi_factor_engine.py)
- Paper 部署/运行时：[app/domains/trading/paper_trading_service.py](../app/domains/trading/paper_trading_service.py)、[app/domains/trading/paper_runtime_service.py](../app/domains/trading/paper_runtime_service.py)、[app/domains/trading/paper_runtime_daemon.py](../app/domains/trading/paper_runtime_daemon.py)
- 结算/分析：[app/domains/trading/paper_settlement_service.py](../app/domains/trading/paper_settlement_service.py)、[app/domains/trading/paper_analytics_service.py](../app/domains/trading/paper_analytics_service.py)
- 队列/worker：[app/worker/service/run_worker.py](../app/worker/service/run_worker.py)
- 部署编排：[docker-compose.staging.yml](../docker-compose.staging.yml)
- 相关设计文档：[paper_trading_runtime_design.md](paper_trading_runtime_design.md)、[factor_composite_backtest_design_plan.md](factor_composite_backtest_design_plan.md)