# Autopilot 持续运营加固 — 功能规格说明书

> 配套文档：[autopilot_specs.md](autopilot_specs.md)（闭环主体）、[autopilot_closed_loop_design.md](../autopilot_closed_loop_design.md)
> 状态：待实施。本文件定义"自动因子挖掘 → 策略组合 → 回测 → 模拟交易"闭环停止间歇性停转所需的全部加固项。

**问题背景**：闭环由 staging 单机上的 autopilot daemon 驱动，存在三类系统性缺陷：
(1) 停转不自愈——数据门不通过当天即放弃且不重试，FAILED 阶段无退避无限重试；
(2) 停转不可见——所有失败仅 logger.exception，无告警落库，全 SKIPPED 的 run 记为 success；
(3) 依赖脆弱——RD-Agent 僵尸 run 无回收、RQ 超时错位必杀长任务、paper 执行器静默丢单/结算失真/crash-loop、e3 决策无执行器。

---

## SPEC-OPS-001 数据门当日重试（消除"当天死刑"）

**问题**：`orchestrator._execute_stage` 中数据门（`guardrails.data_quality_gate`）不通过时直接 `_ensure_stage_skipped`，SKIPPED 为终态当天不再重试。datasync 03:00 未就绪则当天研究链路全废（orchestrator.py:165-169、guardrails.py:20-42）。

**方案**：
1. 数据门不通过时**不落 SKIPPED**，阶段保持 `pending`，记 `last_gate_reason` 到 stage 的 `error` 字段（不改变状态），等待下一 tick 重查。
2. 引入放弃截止时间 `autopilot.data_gate_deadline_hour`（默认 09:00，Asia/Shanghai）：超过截止时间数据门仍不通过才标记 SKIPPED 并发告警（SPEC-OPS-005）。
3. kill-switch 与非交易日跳过行为保持不变（这两者是主动决策，非等待条件）。

**验收**：数据门失败时 stage 状态保持 pending；数据就绪后同一 run 当日继续执行；超过截止时间才 SKIPPED 且 `alert_history` 有记录。

---

## SPEC-OPS-002 阶段重试退避与上限

**问题**：daemon `_tick` 对 FAILED 阶段每 30s 无限重跑，确定性失败（worker 掉线导致 1800s 轮询超时）形成当天死循环（orchestrator.py:139-149）。

**方案**：
1. 每阶段最大尝试次数 `autopilot.stage_max_attempts`（默认 3）：`autopilot_stages.attempt >= max` 后不再重试，保持 FAILED 终态并发告警。
2. 退避：FAILED 后下一 tick 不立即重跑，按 `min(300 * attempt, 1800)` 秒退避（依据 `ended_at` 计算）。
3. 手动 `--stage`/`--once` 调用不受退避限制（人工触发即明确意图），但 attempt 上限仍然生效（防止 --once 循环脚本死循环）。

**验收**：FAILED 阶段重试间隔随 attempt 增长；达到上限后停止重试且产生告警；单测覆盖退避计算与上限判定。

---

## SPEC-OPS-003 Run 状态汇总可见性

**问题**：全部阶段 SKIPPED 的 run 记为 `success`（orchestrator.py:559-569），停转无法从 run 状态发现。

**方案**：
1. `RunStatus` 增加 `SKIPPED = "skipped"`（列是 VARCHAR(20)，无需迁移）。
2. `_sync_run_status` 汇总规则改为：任一 FAILED → `failed`；全部终态且存在 SUCCESS → `success`；全部终态且**无任何 SUCCESS**（全 skipped）→ `skipped`。
3. 全 skipped 的 run 汇总时发一条告警（说明当天闭环空转及原因，取各 stage error 汇总）。

**验收**：全 skipped run 的 `autopilot_runs.status='skipped'`；混合 success/skipped 仍为 success；有 failed 仍为 failed。

---

## SPEC-OPS-004 编排互斥锁

**问题**：CI/手动 `--once` 与常驻 daemon 并发执行同一 run：`reset_running_runs_to_pending` 互踩、同 stage 双执行、重复入队/重复 deploy（orchestrator.py:85、102、111）。

**方案**：
1. Redis 互斥锁 key `autopilot:orchestrator:lock`，`SET NX EX` 实现，TTL 7200s（覆盖一次完整 `--once`）。
2. `run_once`/`run_single` 启动时获取锁，拿不到则直接返回 `{"status": "busy"}` 并退出（exit code 3，CI workflow 可识别）。
3. daemon `_tick` 同样持锁执行，拿不到则跳过本轮 tick（daemon 持锁时间短，通常 <30s；挖掘/回测 stage 阻塞期间持续持有，TTL 需覆盖 _JOB_TIMEOUT——取 7200s 足够）。
4. 进程正常退出释放锁；异常退出靠 TTL 过期。

**验收**：并发两个 `--once` 只有一个执行；daemon 与 `--once` 并发时后者返回 busy 不产生重复部署。

---

## SPEC-OPS-005 停转告警

**问题**：所有失败只有 logger 记录，停转无人知晓。

**方案**：新建 `app/domains/autopilot/alerts.py`：
- `emit_autopilot_alert(message, level="error", *, user_id=None, dedupe_key=None)`：写 `quantmate.alert_history`（复用 `AlertHistoryDao.insert`，`rule_id=NULL`）+ `logger.error`。`user_id` 缺省时经 `resolve_user_id` best-effort 解析。
- `dedupe_key` 用于周期事件去重（Redis SET NX EX 3600，避免每 tick 重复告警同一问题）。

**触发点**：
| 事件 | 级别 | 位置 |
|---|---|---|
| 阶段达到重试上限仍 FAILED | error | orchestrator（SPEC-OPS-002） |
| 数据门超截止时间放弃 | error | orchestrator（SPEC-OPS-001） |
| run 全 skipped | warning | orchestrator（SPEC-OPS-003） |
| 部署软拦截（approval/review 拒绝） | warning | orchestrator r4 |
| paper 执行器 crash 停部署 | error | paper_composite_executor（SPEC-OPS-008） |
| RD-Agent 僵尸 run 回收 | error | reaper（SPEC-OPS-006） |
| e3 stop 决策执行 | warning | orchestrator e3（SPEC-OPS-009） |

**验收**：上述事件后 `alert_history` 出现对应记录（level、message 含 run_id/stage/deployment_id）；同一 dedupe_key 1 小时内不重复；alert_history 表缺失时降级为仅日志不抛异常。

---

## SPEC-OPS-006 RD-Agent 僵尸 run 回收与超时对齐

**问题**：
1. `rdagent_runs` 无 stale-running 回收——worker/容器死亡后 run 永久卡 `running`/`queued`（对比 datasync 有 `normalize_stale_running_statuses`）。
2. sidecar deadline = `max_iterations × 1800s` 可超过 RQ job timeout（固定 14400s，`worker.queue_timeout.rdagent`），iterations>8 的 run 必被 RQ 强杀且状态卡死（service.py:464 vs config.py:23、routes/rdagent.py:75-81 未传 job_timeout）。

**方案**：
1. **超时对齐**：`routes/rdagent.py` enqueue 时按 `max_iterations × RDAGENT_TIMEOUT_PER_ITERATION_SECONDS + 600` 计算 `job_timeout` 显式传入；`rdagent_tasks._call_sidecar_mining` 的 httpx 超时同公式计算（不再用固定 14400 默认值）。
2. **僵尸回收**：`rdagent_service.reap_stale_rdagent_runs()`——将 `status IN ('queued','running')` 且 `GREATEST(COALESCE(started_at, created_at))` 早于 `now - stale_threshold` 的 run 置 `failed`（error_message 说明回收原因），返回回收数量。阈值 `rdagent.stale_run_seconds`（env `RDAGENT_STALE_RUN_SECONDS`，默认 18000s=5h）。
3. **调用点**：worker 启动时（`run_worker.main`）执行一次；autopilot daemon tick 每 10 分钟执行一次（时间戳节流）。

**说明**：降级重试 patch（`_resolve_retry_chat_model` 等）已在仓库代码中，随本次镜像发布生效，staging 手工 docker cp 的 patch 作废。

**验收**：构造 started_at 超阈值的 running run 被置 failed；`max_iterations=12` 的 run 的 RQ job_timeout ≥ 12×1800+600；单测覆盖公式与回收 SQL。

---

## SPEC-OPS-007 Worker 队列隔离（rdagent 专用 worker）

**问题**：单 worker 进程消费 5 个队列，一个 4h 的 rdagent job 阻塞 autopilot 挖掘（default 队列）与回测（backtest 队列）（docker-compose.staging.yml:87；run_worker.py:48-49 还强制把 rdagent 追加回队列名单）。

**方案**：
1. `run_worker.get_default_queue_names`：仅当 `WORKER_DEFAULT_QUEUE_NAMES` 未显式配置（env/db 都没有）时才强制追加 rdagent——显式配置则完全尊重。
2. staging compose：主 worker `WORKER_DEFAULT_QUEUE_NAMES=backtest,optimization,default,low`；新增 `worker-rdagent` 服务（同镜像/资源，`command` 传 `rdagent` 队列表参）只消费 rdagent 队列。

**验收**：主 worker 日志不包含 rdagent 队列；rdagent job 由专用 worker 消费；显式配置 `WORKER_DEFAULT_QUEUE_NAMES` 后不再被强制追加 rdagent。

---

## SPEC-OPS-008 Paper 执行器加固（静默丢单/结算失真/crash-loop）

**问题**：
1. `last_price<=0` 时订单静默 skip，无记录无告警（paper_composite_executor.py:465-468）。
2. 结算时 quote 失败该持仓市值按 0 计 → 回撤虚高 → e3 错误 stop（paper_settlement_service.py:79-84；SQL 已算 avg_cost 但未使用）。
3. executor crash 只置 `status='stopped'` 不改 `desired_status`，daemon 3 秒后重启 → crash-loop（paper_composite_executor.py:271-283）。

**方案**：
1. **价格 0 订单落库**：写 rejected order（`OrderDao.create` + `update_status 'rejected'`，reason 注入 order reason），并发告警（dedupe）。
2. **结算兜底**：quote 失败或 price<=0 时用持仓 avg_cost 兜底估值，结果里统计 `fallback_valued` 数量并记 warning 日志（不再按 0 计）。
3. **crash 止损**：executor 异常退出（非 stop_event 触发）时将 deployment 置 `status='stopped', desired_status='stopped', runtime_status='error'` 并写 `runtime_error` + 发告警——crash 的部署不再自动重启，由次日 autopilot 部署（supersede）或人工恢复。正常 stop（stop_event）行为不变。

**验收**：价格 0 订单在 orders 表出现 rejected 记录；quote 失败的持仓以 avg_cost 估值；crash 后 deployment `desired_status='stopped'` 且 daemon 不再拉起。

---

## SPEC-OPS-009 e3 决策执行器接线（闭环闭合）

**问题**：e3 的 stop/reduce 决策只写 `autopilot_decisions` 表，无任何执行器；`guardrails.intraday_circuit_breaker` 定义后全仓库无调用——策略恶化后不会真正停。

**方案**（`orchestrator._run_strategy_adjust` 扩展）：
1. **盘中熔断接线**：e3 中以 e2 analytics 调用 `intraday_circuit_breaker`，触发 `stop` 则强制 action=stop。
2. **stop 执行器**：`execute_stop_decision(user_id, account_id)`——停掉该 paper 账户上所有 `desired_status='running'` 的 deployment（复用 `PaperTradingService.stop_deployment` 的 SQL 语义 + daemon pub/sub 通知），持仓保留但不再产生新订单；执行结果写入 decision 的 `input_summary.executed` 并发告警。
3. **reduce**：减仓不在盘后执行（无撮合时机），保持 feedback 回流研究循环（现状），decision 中记录 `executed=False, reason="reduce deferred to research feedback"`。
4. **continue/weight**：无盘后动作，仅记录。

**验收**：e3 stop 后该账户无 desired_status='running' 的 deployment；decision 记录包含执行结果；熔断触发时 analytics 正常也会 stop。

---

## SPEC-OPS-010 Composite 归档清理

**问题**：autopilot 每次部署新建 composite + 3 组件，无清理，表无限膨胀（deploy_bridge.py:61-122）。

**方案**：`deploy` 成功后，将同 user 名下旧的 `Autopilot Composite %`（id != 本次）`is_active=0`；对应 `AP Universe/AP Trading/AP Risk` 组件同理（仅归属 autopilot 命名前缀的）。失败/软拦截不归档（保留现场）。归档只做软删除（is_active），不物理删除。

**验收**：连续两次部署后旧的 Autopilot composite is_active=0，新部署 active；非 autopilot 命名的组件/composite 不受影响。

---

## 验收矩阵

| 编号 | 验收 | 验证方式 |
|---|---|---|
| OPS-001 | 数据门当日重试 | 单测 + staging 观察数据晚到日 run 仍完成 |
| OPS-002 | 重试退避/上限 | 单测断言退避与上限 |
| OPS-003 | 全 skipped run 可见 | run 状态为 skipped + 告警 |
| OPS-004 | 并发互斥 | 并发 --once 只一个执行 |
| OPS-005 | 告警落库 | alert_history 出现各事件记录 |
| OPS-006 | rdagent 自愈 | 僵尸 run 被 reaper 置 failed；长任务不被 RQ 杀 |
| OPS-007 | 队列隔离 | 主 worker 不消费 rdagent；专用 worker 消费 |
| OPS-008 | paper 健壮性 | rejected 订单可见；结算兜底；crash 不 loop |
| OPS-009 | 闭环闭合 | e3 stop 真实停部署 |
| OPS-010 | 归档清理 | 旧 Autopilot composite 停用 |

staging 验证统一在任务 #14 执行：单测 + ruff 通过 → 提交 → CI 构建镜像 → staging 更新 → 触发 autopilot run 验证四表 + alert_history。
