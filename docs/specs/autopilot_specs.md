# Autopilot 全自动交易闭环 — 功能规格说明书

> 配套设计报告：[autopilot_closed_loop_design.md](../autopilot_closed_loop_design.md)
> 状态：实现中。本文件定义 `app/domains/autopilot/` 需要交付的每个功能模块的边界、接口与验收标准。

本说明书覆盖设计报告 Phase 1（主线打通）与 Phase 2（决策闭环与风控）。Phase 3（事件驱动 gateway 收敛）在 [paper_trading_runtime_design.md](../paper_trading_runtime_design.md) 中单独演进，不在本域范围内。

---

## SPEC-001 数据模型与迁移

**目标**：为闭环提供可追溯的运行时状态存储。

**表**（`quantmate` 库，迁移 `060_create_autopilot_tables.sql`）：

### autopilot_runs
每次闭环运行（一个业务日一条主记录）。

| 列 | 类型 | 说明 |
|---|---|---|
| id | BIGINT PK | 自增 |
| run_id | VARCHAR(64) UNIQUE | 业务键，格式 `ap-YYYYMMDD-<hex8>` |
| business_date | DATE | 业务日期（Asia/Shanghai） |
| market | VARCHAR(32) | 默认 `CN` |
| status | VARCHAR(20) | pending/running/success/failed/aborted |
| started_at / ended_at | TIMESTAMP NULL | 起止时间 |
| created_at / updated_at | TIMESTAMP | 审计 |

### autopilot_stages
每阶段执行记录（run_id + stage 唯一）。

| 列 | 类型 | 说明 |
|---|---|---|
| id | BIGINT PK | 自增 |
| run_id | VARCHAR(64) | 关联 runs |
| stage | VARCHAR(50) | 阶段枚举值（见 SPEC-002） |
| status | VARCHAR(20) | pending/running/success/failed/skipped |
| attempt | INT DEFAULT 0 | 重试次数 |
| params / result | JSON NULL | 入参摘要 / 产出结果 |
| error | TEXT NULL | 失败原因 |
| started_at / ended_at | TIMESTAMP NULL | 起止时间 |
| created_at / updated_at | TIMESTAMP | 审计 |

唯一键 `uq_autopilot_stage (run_id, stage)` 保证幂等。

### autopilot_decisions
决策引擎产出（可追溯审计）。

| 列 | 类型 | 说明 |
|---|---|---|
| id | BIGINT PK | 自增 |
| run_id | VARCHAR(64) | 关联 runs |
| decision_type | VARCHAR(50) | factor_selection / deploy / adjust / stop / rollback / weight / skip |
| action | VARCHAR(50) | 动作（keep/replace/deploy/continue/reduce/stop/rollback/approve/reject...） |
| subject_type / subject_id | VARCHAR | 决策对象（factor/composite_strategy/deployment） |
| input_summary | JSON NULL | 输入指标摘要 |
| reason | TEXT NULL | 决策理由（人类可读） |
| approval_status | VARCHAR(20) | auto/pending_approval/approved/rejected |
| created_at | TIMESTAMP | 审计 |

**验收**：迁移在 `schema_migrations` 登记；三表可幂等创建；`AutopilotDao` 覆盖全部读写接口。

---

## SPEC-002 阶段状态机（state.py）

**目标**：定义阶段枚举与合法状态转换，供编排器和 DAO 复用。

**Stage 枚举**（沿用报告时序）：

| stage | 别名 | 归属循环 | 说明 |
|---|---|---|---|
| `d2_qlib_ingest` | Qlib 数据转换 | 研究 | tushare → qlib 重导 + 范围校验 |
| `r1_factor_mining` | 因子挖掘 | 研究 | Alpha158 挖掘流水线 |
| `r2_factor_backtest` | 因子回测 | 研究 | 候选因子横截面组合回测 |
| `r3_factor_selection` | 因子取舍 | 研究 | 决策引擎（研究侧） |
| `r4_strategy_deploy` | 策略生成+部署 | 研究→交易 | 桥接 + 风控审查 + 部署 |
| `r5_premarket_check` | 盘前校验 | 交易 | kill-switch/账户对账 |
| `e1_settlement` | 收盘结算 | 交易 | settle_all() |
| `e2_analysis` | 交易分析 | 交易 | 绩效 + 归因 |
| `e3_strategy_adjust` | 策略调整 | 交易 | 决策引擎（交易侧） |

`D1 数据同步` 由 `datasync` daemon 负责，autopilot 不直接执行，而是在 `r1` 前执行**数据门槛校验**（SPEC-006）。

**Status 枚举**：`pending/running/success/failed/skipped`。

**合法转换**：`pending→running`、`running→success|failed`、任意→`skipped`（依赖不满足时）。失败可 `failed→pending`（重试，attempt+1）。

**验收**：`STAGE_ORDER` 常量给出依赖顺序；`is_terminal()` 等辅助函数存在；无非法状态写入。

---

## SPEC-003 AutopilotDao（dao/autopilot_dao.py）

**目标**：三张表的唯一 SQL 入口，遵循域内既有 DAO 约定（`connection("quantmate")` + `text()`）。

**接口**：
- `create_run(business_date, market="CN") -> dict`：生成 `run_id`，插入 runs。
- `get_run(run_id) -> dict | None`
- `update_run_status(run_id, status, *, ended_at=None)`：更新 runs 状态。
- `get_or_create_stage(run_id, stage) -> dict`：按 (run_id, stage) 幂等获取/创建阶段行。
- `start_stage(run_id, stage, params=None)`：置 running + attempt+1。
- `finish_stage(run_id, stage, status, result=None, error=None)`：置终态。
- `record_decision(...)`：写 decisions。
- `list_runs(limit)` / `list_stages(run_id)`：观测接口。
- `reset_running_runs_to_pending()`：daemon 启动时兜底（参考 backfill daemon 思路）。

**验收**：insert 使用参数化 SQL；slotted/serialization 使用 JSON 字符串；无裸 `__dict__` 序列化。

---

## SPEC-004 可配置策略（policies.py）

**目标**：把决策阈值与开关集中到 `system_config`（`autopilot.*` key），用户可在前端 SystemConfigTab 调整且即时生效。

**Policies dataclass 字段**（通过 `get_runtime_*` 读取，env 兜底）：

| key | env key | 默认 | 说明 |
|---|---|---|---|
| autopilot.enabled | AUTOPILOT_ENABLED | true | 总开关：false 时编排器不调度 |
| autopilot.kill_switch | AUTOPILOT_KILL_SWITCH | false | 全局 kill-switch：true 时禁止自动部署/下单 |
| autopilot.approval_required | AUTOPILOT_APPROVAL_REQUIRED | true | 首日部署是否需要人工审批 |
| autopilot.ic_threshold | AUTOPILOT_IC_THRESHOLD | 0.03 | 因子 |ic_mean| 下限 |
| autopilot.ir_threshold | AUTOPILOT_IR_THRESHOLD | 0.5 | 因子 |ic_ir| 下限 |
| autopilot.corr_threshold | AUTOPILOT_CORR_THRESHOLD | 0.7 | 两两相关去重阈值 |
| autopilot.turnover_cap | AUTOPILOT_TURNOVER_CAP | 0.5 | 换手率上限 |
| autopilot.top_n_factor | AUTOPILOT_TOP_N_FACTOR | 10 | 最终选定因子数 |
| autopilot.max_position_pct | AUTOPILOT_MAX_POSITION_PCT | 0.1 | 单标的仓位上限 |
| autopilot.max_drawdown_stop_pct | AUTOPILOT_MAX_DRAWDOWN_STOP_PCT | 20.0 | 回撤熔断阈值 % |
| autopilot.sharpe_floor | AUTOPILOT_SHARPE_FLOOR | 0.0 | Sharpe 下限 |
| autopilot.sharpe_floor_days | AUTOPILOT_SHARPE_FLOOR_DAYS | 3 | 连续低于下限天数 |
| autopilot.min_overlap_days | AUTOPILOT_MIN_OVERLAP_DAYS | 60 | 回测样本最短重叠天数 |
| autopilot.universe | AUTOPILOT_UNIVERSE | csi300 | 默认 universe |
| autopilot.user_id | AUTOPILOT_USER_ID | 0 | 目标用户（0=自动探测首个 admin） |
| autopilot.paper_account_id | AUTOPILOT_PAPER_ACCOUNT_ID | 0 | 目标 paper 账户（0=自动探测/创建） |

**验收**：`Policies.load()` 惰性读取且缓存 30s（复用 runtime 缓存）；新增 key 注册到 `system_config_registry.py`。

---

## SPEC-005 决策引擎（decision_engine.py）

**目标**：把"回测结果→因子取舍/权重"与"交易绩效→策略动作"两条规则自动化。

### 研究侧 `select_factors(candidates, policies, previous_factors=None)`
输入：候选因子指标列表（`ic_mean/ic_ir/rank_ir/turnover/name/方向`）。输出：选定因子集 + 方向 + 权重。

规则（可配置，初版）：
1. `abs(ic_ir) >= ir_threshold` 且 `abs(ic_mean) >= ic_threshold`。
2. 相关性去重：两两 `|corr| > corr_threshold` 保留 |IC| 更大者（无 corr 数据时跳过）。
3. 换手率上限：`turnover < turnover_cap`。
4. 方向：`sign(ic_mean)` 决定多空。
5. 跨期稳定性：与 `previous_factors` 对比，连续出现者加权；全新因子默认权重 1.0，稳定因子 1.5。
6. 权重归一化，取 `top_n_factor` 个。

返回 `{"factors": [{name, direction, weight, ic_mean, ic_ir}], "rejected": [...]}`。

### 交易侧 `evaluate_performance(analytics, policies, history)`
输入：`PaperAnalyticsService.get_analytics()` 返回 + 该部署连续表现历史。输出：动作 + 可解释理由。

规则（可配置，初版）：
1. `max_drawdown_pct >= max_drawdown_stop_pct` → `stop`（平仓、标记失败、禁止同类自动再部署）。
2. `sharpe_ratio < sharpe_floor` 且连续 ≥ `sharpe_floor_days` 日 → `reduce`（降级/减仓），并将最差因子反馈回研究循环淘汰。
3. 表现好（sharpe 达标）→ 保留并 `weight`（提高稳定因子权重）。
4. 其余 → `continue`。

返回 `{"action": "continue|reduce|stop|weight", "reason": "...", "feedback": [...]}`。

**验收**：纯函数、无副作用（DB 写入由编排器完成）；可单测（用合成 candidates/analytics 断言动作与原因）。

---

## SPEC-006 风控护栏（guardrails.py）

**目标**：保证"自动"不等于"失控"。

接口：
- `is_kill_switch_active(policies) -> bool`：读取 `autopilot.kill_switch`。
- `data_quality_gate(business_date, policies) -> dict`：检查 `data_sync_status` 中 `pending/error` 比例，超阈值返回 `{ok: False, reason}`，阻断研究循环。
- `pre_deploy_review(deployment_plan, policies) -> dict`：校验单标的仓位上限、标的池合法性、样本外/样本内最低重叠天数；返回 `{ok, reason, risk_summary}`。
- `request_approval(decision_id) -> "pending_approval"`：当 `approval_required` 且首日/资金超阈值时，将决策置为 `pending_approval`。
- `intraday_circuit_breaker(analytics, policies) -> dict`：盘中权益回撤/单日亏损触发 `{stop: True}`（供 runtime/后续盘中使用）。

**验收**：所有检查输出统一 `{ok: bool, reason: str}`；kill-switch 通过 `system_config` 端到端生效；`pre_deploy_review` 拒绝时不执行部署。

---

## SPEC-007 策略生成→自动部署桥接（deploy_bridge.py）

**目标**：把选定因子集变成可在 paper 跑的 `CompositeStrategy`，审查通过后部署。

接口：
- `build_composite_strategy(user_id, selected_factors, universe_symbols, policies) -> composite_strategy_id`
  - 复用 [composite/service.py](../domains/composite/service.py) 的 `create_component` + `create_composite`。
  - universe 组件：因子股票池/`csi300`。
  - trading 组件：因子排序（方向按 `sign`）。
  - risk 组件：`max_position_pct`、止损。
- `deploy(user_id, composite_strategy_id, paper_account_id, source_backtest_job_id, execution_mode="auto", require_approval=policies.approval_required) -> dict`
  1. 组装部署计划 → `guardrails.pre_deploy_review`。
  2. 通过则调 [paper_trading_service.py::deploy](../domains/trading/paper_trading_service.py)。
  3. 记录决策 + deployment_id + 参数快照写回 `autopilot_stages`。

**验收**：end-to-end 可由空因子集构建 composite 并成功 `deploy` 返回 `deployment_id`；审查拒绝时返回 error 且不部署。

---

## SPEC-008 编排器 + 调度（orchestrator.py / schedule.py）

**目标**：按依赖顺序、在固定时刻自动触发各阶段，管理运行状态、重试、幂等与崩溃恢复。

### schedule.py
- `is_trading_day(business_date) -> bool`：复用 `CalendarService`。
- `next_stage_times() -> dict[stage, (hh, mm)]`：每阶段触发时刻（复用报告时序）。
- `default_schedule()`：返回固定时刻表。

### orchestrator.py（CLI 入口）
```
python -m app.domains.autopilot.orchestrator --once          # 立即顺序执行完整 DAG（验证用）
python -m app.domains.autopilot.orchestrator --daemon        # 定时循环
python -m app.domains.autopilot.orchestrator --stage e1_settlement  # 单阶段
python -m app.domains.autopilot.orchestrator --status        # 状态查询
```

- 启动时 `reset_running_runs_to_pending()`。
- 每阶段执行：`guardrails.data_quality_gate`（r 系列）→ `start_stage` → 执行（因子挖掘/回测经 RQ 投递并轮询结果；结算/分析/决策直接调用）→ `finish_stage`。
- RQ 投递复用现有 worker 队列：因子挖掘 `enqueue_factor_pipeline`，因子回测 `queue.enqueue("app.domains.factors.backtest_task.run_factor_backtest_task", ...)`。
- 阶段间依赖：前置阶段非 `success` 则当前阶段 `skipped`。
- 崩溃恢复：重启后依据 `autopilot_stages` 状态机"续跑"。

**验收**：`--once` 模式从空状态能完整跑通研究循环到部署链路且各阶段状态落库；两次 `--once` 不产生重复部署（幂等）。

---

## SPEC-009 结算/分析定时接入 + 数据门槛

**目标**：把已实现但缺触发的 `settle_all`/`get_analytics` 接入闭环。

- `e1_settlement` 阶段调用 `PaperSettlementService.settle_all()`。
- `e2_analysis` 阶段对 target paper 账户调用 `PaperAnalyticsService.get_analytics()`，结果写入 `autopilot_stages.result`。
- `e3_strategy_adjust` 阶段读取 e2 结果，调用 `decision_engine.evaluate_performance`，产出动作并写入 `autopilot_decisions`。

**验收**：单阶段 `--stage e1_settlement` / `e2_analysis` / `e3_strategy_adjust` 可独立执行且产出落库。

---

## SPEC-010 部署编排（docker-compose）

**目标**：新增 `autopilot` 容器。

- `image`：复用 `quantmate-api`。
- `command: ["python", "-m", "app.domains.autopilot.orchestrator", "--daemon"]`
- `restart: unless-stopped`，`depends_on: redis (healthy)`。
- 环境变量：完整传递 `MYSQL_*` / `SECRET_KEY` / `TUSHARE_*` / `REDIS_*` / `QLIB_DATA_DIR` / `APP_ENV`，及 `AUTOPILOT_*` 阈值。
- `volumes`：挂载 `qlib_data`。

**验收**：容器可启动并进入调度循环（日志打印"Autopilot orchestrator starting"），不崩溃。

---

## 验收矩阵

| 编号 | 验收 | 验证方式 |
|---|---|---|
| 001 | 三表迁移成功 | 运行 `python -m app.infrastructure.db.migrate` |
| 002/003 | 状态机 + DAO 可用 | 单测/`--once` 后查表 |
| 004 | 阈值可配置 | `system_configs` 可读 autopilot.* |
| 005 | 决策规则正确 | 合成输入断言 action |
| 006 | kill-switch/审查生效 | 开启 kill-switch 后 `--once` 拒绝部署 |
| 007 | 桥接可部署 | `--once` 产出 deployment_id |
| 008 | 编排幂等 | 连续 `--once` 无重复部署 |
| 009 | 结算/分析接入 | `--stage` 单阶段可跑通 |
| 010 | 容器可启动 | staging `docker compose up -d autopilot` |