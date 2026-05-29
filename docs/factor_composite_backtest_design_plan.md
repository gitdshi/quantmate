# Factor 与组合策略回测设计及开发计划

## 摘要

QuantMate 当前已经存在三条彼此分离的回测/评估路径：

- 策略回测：`/backtest` + `BacktestService`，基于 vn.py 单标的 CTA 回测模型。
- 组合策略回测：`/composite-backtests` + `CompositeBacktestEngine`，基于日频组合编排引擎。
- 因子评估：`FactorService.run_evaluation()`，当前只输出 IC、ICIR、long-short return 等评估指标，没有完整净值回放入口。

这三条路径分别可用，但对象模型、任务调度、结果结构、历史记录、前端入口都不一致，导致当前“回测评估”页面和能力无法自然覆盖因子与组合策略。

本方案建议不再为因子或组合策略继续扩展独立回测体系，而是把当前回测评估升级为统一的回测编排层：

- 保留现有 vn.py CTA 回测能力。
- 复用现有 `CompositeBacktestEngine` 作为组合与因子组合回测的日频组合引擎。
- 将“因子评估”和“因子回测”明确拆分为两种能力。
- 通过统一的任务、存储、结果模型和前端渲染，让当前回测评估支持 `strategy`、`factor`、`composite` 三类对象。

推荐结论：

1. 当前回测评估应扩展为统一回测中心，而不是继续堆叠独立入口。
2. 因子回测不建议直接复用单标的 vn.py CTA 逻辑，而应基于现有组合回测引擎实现“横截面组合回测”。
3. 组合策略回测应保留现有引擎，但统一接入当前回测评估、队列、历史和结果模型。

## 背景与现状

### 当前策略回测能力

当前主回测链路的核心特征：

- 请求模型要求 `vt_symbol`，天然面向单标的回测。
- 结果模型以 `symbol`、`stock_price_curve`、`symbol_name` 为核心字段，天然偏单标的展示。
- 核心引擎是 vn.py `BacktestingEngine`，适合 CTA 类、单品种、事件驱动策略。
- 主回测结果会进入当前回测历史、队列结果、AI 报告等链路。

这条链路适合当前策略模板、内置策略和用户代码策略，但不适合直接承载横截面因子组合与组合策略回测。

### 当前组合策略回测能力

当前组合策略已经具备独立回测能力：

- 已有独立路由：`/composite-backtests`。
- 已有独立引擎：`CompositeBacktestEngine`。
- 已有独立表：`composite_backtests`。
- 已有前端页面：`CompositeStrategies` 页面内可提交、查看组合回测结果。

但这条链路与当前回测评估体系是分叉的，主要问题是：

- 不进入统一回测历史。
- 不复用统一结果模型。
- 与主回测页、报表页、AI 报告链路无法天然共用。
- 结果字段和指标口径与主回测未完全对齐。

### 当前因子能力

当前因子侧已经具备：

- 因子定义管理。
- 因子表达式计算。
- 因子评估：IC、ICIR、turnover、long/short/long-short return。
- 多因子组合策略代码生成。

但当前缺失完整的“因子回测”能力：

- `FactorLab` 虽然已有 Backtest 页签，但目前还是空状态。
- 因子评估只衡量信号质量，不等于组合净值回测。
- 用户不能直接从单因子或多因子表达式出发，生成横截面持仓并回放资金曲线。

### 当前 Qlib 路径

当前还存在一条 Qlib 回测路径：

- `queue/backtest` 已支持 `engine_type = qlib`。
- Qlib 回测由独立 worker 任务执行。
- 结果落在 `qlib` 库的 `qlib_backtest_results`，并未并入当前统一回测历史。

这说明系统已经存在“多引擎回测”的现实需求，但尚未形成统一抽象。

## 核心问题

当前设计存在以下结构性问题：

1. 回测对象没有统一抽象。策略、因子、组合策略分别有不同入口和结果结构。
2. 单标的 CTA 模型与横截面组合模型混在“回测”这个概念下，但没有明确区分。
3. 因子评估和因子回测语义混淆。IC/ICIR 不是净值回测，不能替代组合回放。
4. 历史记录、队列状态、结果模型和前端展示未统一，无法跨对象比较。
5. 当前部分逻辑存在降级或兜底行为，不适合作为正式回测能力基础：
   - 因子评估失败时会回落为 stub metrics。
   - 组合回测在无明确标的池时会使用硬编码样本标的。
6. 当前结果模型天然偏单标的，不适合承载组合持仓、行业暴露、因子分层收益等信息。

## 目标

本次设计目标如下：

1. 让当前回测评估支持三类对象：策略、因子、组合策略。
2. 对用户暴露统一的回测入口、历史记录、状态查询和结果页。
3. 保留现有 vn.py CTA 能力，不破坏已有策略回测流程。
4. 复用现有组合回测引擎，避免重复实现第二套日频组合回测内核。
5. 明确区分“因子评估”和“因子回测”两类功能。
6. 统一指标口径，使不同回测对象可以在同一评估页比较核心绩效。
7. 通过分阶段改造，优先完成可落地、低风险方案。

## 非目标

本方案不包含以下内容：

1. 不在第一阶段重写 vn.py CTA 回测引擎。
2. 不在第一阶段引入分钟级或 Tick 级组合回测。
3. 不在第一阶段把所有 Qlib 路径彻底并表，只保留兼容与后续收敛接口。
4. 不在第一阶段实现完整行业中性、市值中性、风险模型归因等高级量化研究功能。
5. 不在第一阶段替换当前因子表达式引擎或重做 FactorLab 的研究流程。

## 方案选择

### 方案 A：继续保持独立回测入口

做法：

- 保持 `/backtest`、`/composite-backtests`、`/factors/.../evaluations` 各自独立。
- 只在前端页面上做聚合展示。

优点：

- 后端改动最小。
- 对现有接口破坏最小。

缺点：

- 继续维持三套结果模型和三套历史链路。
- 无法真正做到“当前回测评估支持因子与组合策略”。
- 报表、AI 解释、统一筛选、统一比较都会持续分叉。

### 方案 B：扩展当前回测评估为统一回测中心

做法：

- 在当前回测体系上增加统一的回测对象抽象。
- 让 CTA、因子、组合策略都进入统一任务与结果模型。
- 现有组合回测入口保留为兼容包装层。
- 因子回测复用组合回测引擎，不再单独再造一套日频组合引擎。

优点：

- 满足“当前回测评估支持因子与组合策略”的目标。
- 统一历史、报表、比较、AI 报告、权限和任务状态。
- 中长期维护成本更低。

缺点：

- 需要改造当前回测模型、DAO 和前端结果页。
- 需要处理现有组合回测表与主回测历史的兼容。

### 推荐方案

推荐采用方案 B。

原因：

- 当前系统已经客观存在多回测引擎，不缺“再加一个入口”，缺的是统一抽象。
- 组合回测已具备可复用引擎，因子回测可自然落在同一日频组合框架上。
- 若继续维持分叉，后续报表、工作台、AI 报告、比较分析都会越来越难维护。

## 目标能力范围

统一回测中心在第一阶段支持以下三类对象：

### 1. `strategy`

定义：

- 当前已有的单标的 CTA 策略回测。
- 继续使用 vn.py 回测引擎。

适用场景：

- 用户代码策略。
- 内置策略。
- 当前通过策略页、工作台触发的回测。

### 2. `factor`

定义：

- 直接以因子定义或因子表达式为核心输入。
- 通过横截面排序和组合构建规则，形成日频持仓并进行净值回放。

适用场景：

- 单因子组合回测。
- 多因子打分组合回测。
- 因子表达式研究到组合验证的闭环。

### 3. `composite`

定义：

- 当前组合策略系统中的 Universe / Trading / Risk 编排结果。
- 使用既有 `CompositeBacktestEngine` 执行回测。

适用场景：

- 策略组件编排后的日频组合策略回测。
- 与因子组合回测共享一套组合执行与净值回放能力。

## 因子评估与因子回测的边界

这是本次设计里最重要的概念边界之一。

### 因子评估

关注信号质量，典型输出：

- IC / ICIR
- turnover
- long return
- short return
- long-short return

作用：

- 判断因子是否有预测能力。
- 属于研究评估，不等于投资组合资金回放。

### 因子回测

关注组合绩效，典型输出：

- 净值曲线
- 年化收益
- 最大回撤
- Sharpe
- alpha / beta
- 换手率
- 持仓与交易记录

作用：

- 判断因子在给定组合构建规则、交易成本、调仓频率下能否形成可执行策略。

### 设计要求

统一结果页必须同时支持两类信息：

- 因子质量指标：IC、ICIR、long-short return。
- 组合回测指标：净值、回撤、Sharpe、换手等。

也就是说，因子回测结果需要附带因子评估视角，但两者不能混为一个概念。

## 与现有 Multi-Factor CTA 生成能力的关系

当前系统已经支持从多因子生成 vn.py CTA 代码，这条能力应继续保留，但要明确边界：

- 多因子 CTA 生成：适合把多个因子映射到单标的时序信号。
- 因子组合回测：适合横截面选股、调仓、组合净值回放。

两者不是一回事，建议并行保留：

1. 因子研究阶段：先做因子评估与因子组合回测。
2. 策略实现阶段：若需要，把稳定的因子组合逻辑再转成策略代码。

因此，本方案不建议把“因子回测”强行塞进当前单标的 CTA 引擎，而是把它设计为组合回测的一个特化场景。

## 目标架构

### 总体思路

引入统一回测编排层 `Backtest Hub`，核心职责：

1. 接收统一回测请求。
2. 根据回测对象选择对应的引擎适配器。
3. 统一排队、状态跟踪、结果持久化。
4. 将不同引擎结果标准化为统一结果结构。
5. 供当前回测评估页、FactorLab、CompositeStrategies、报表页复用。

### 统一对象模型

建议新增以下核心枚举：

- `BacktestSubjectType = strategy | factor | composite`
- `BacktestEngineType = vnpy | portfolio_daily | composite`
- `BacktestScopeType = single_symbol | cross_sectional_portfolio`

### 统一请求模型

建议新增统一请求模型 `BacktestRunRequest`，公共字段如下：

```json
{
  "subject_type": "factor",
  "subject_id": 123,
  "name": "ROE_Momentum_Top20",
  "start_date": "2023-01-01",
  "end_date": "2024-12-31",
  "benchmark": "000300.SH",
  "initial_capital": 1000000,
  "costs": {
    "commission_rate": 0.0003,
    "slippage": 0.0005
  },
  "profile": {}
}
```

其中 `profile` 为不同对象的特化配置。

#### `strategy` profile

```json
{
  "vt_symbol": "000001.SZSE",
  "parameters": {
    "fast_window": 10,
    "slow_window": 20
  },
  "period": "daily"
}
```

#### `factor` profile

```json
{
  "expression": "close / delay(close, 20) - 1",
  "universe": {
    "preset": "csi300",
    "symbols": []
  },
  "rebalance": {
    "frequency": "weekly",
    "offset": 0
  },
  "portfolio": {
    "mode": "long_only_top_n",
    "top_n": 20,
    "weighting": "equal",
    "close_on_rank_exit": true,
    "max_position_pct": 0.1,
    "max_total_positions": 20
  },
  "evaluation": {
    "attach_factor_metrics": true,
    "forward_periods": 1
  }
}
```

#### `composite` profile

```json
{
  "composite_strategy_id": 18,
  "override_constraints": {},
  "override_bindings": []
}
```

## 引擎适配器设计

建议在统一回测服务下引入适配器模式。

### 1. `VnpyStrategyBacktestAdapter`

职责：

- 复用当前 `BacktestService.run_single_backtest()`。
- 将现有 CTA 回测结果包装为统一结果结构。
- 保持旧接口和旧页面兼容。

### 2. `CompositeStrategyBacktestAdapter`

职责：

- 复用当前 `CompositeStrategyService` 的回测数据装载与 `CompositeBacktestEngine`。
- 将组合策略结果标准化。
- 保留 layer attribution、组合持仓快照、交易记录。

### 3. `FactorPortfolioBacktestAdapter`

职责：

- 新增因子组合回测适配器。
- 基于因子表达式计算横截面分数。
- 按组合构建规则生成买卖目标。
- 底层复用 `CompositeBacktestEngine`，而不是重写组合执行逻辑。

这是本次设计的关键：

- 因子回测不再走单标的 CTA。
- 因子回测也不新造独立引擎。
- 因子回测本质上是“特化的组合策略回测”。

## 因子回测详细设计

### 设计原则

因子回测采用“因子打分 + 横截面选股 + 组合执行”的流程。

核心流程：

1. 解析因子来源：`factor_id` 或 `expression`。
2. 解析标的池：预设指数成分、显式 symbols、watchlist 或后续自定义 universe provider。
3. 逐日计算标的因子分数。
4. 在调仓日按分数排序构建目标持仓。
5. 通过组合回测引擎执行交易、更新组合净值。
6. 同时附带因子评估指标，形成完整研究结果。

### 为什么复用组合回测引擎

原因如下：

- 因子回测本质上是多标的、日频、组合级别的问题。
- 当前 `CompositeStrategyOrchestrator` 已经具备基于 `factor_expression` 做 universe 排序与 trading 信号生成的能力。
- 当前 `CompositeBacktestEngine` 已经具备：
  - 组合资金曲线维护
  - 交易日志记录
  - 组合持仓快照
  - benchmark alpha / beta 计算

因此，因子回测的最低风险做法不是重写，而是把“因子组合”映射为一种临时组合策略模板。

### 因子回测的最小可用配置

第一阶段建议支持：

- `long_only_top_n`
- `long_short_top_bottom_n`
- `equal_weight`
- `score_weight`
- `daily | weekly | monthly` 调仓
- 显式 symbols 或指数预设标的池
- 手续费、滑点、持仓上限、单标的权重上限

第一阶段暂不支持：

- 行业中性
- 市值中性
- 风险模型优化权重
- 复杂成交撮合

### 因子回测执行实现

建议新增一个因子组合构建器，将因子请求转换为临时组合策略定义：

- Universe layer：
  - 读取预设 universe 或显式 symbols。
  - 支持按 `factor_expression` 排序前先做基础过滤。
- Trading layer：
  - 根据因子分数排序。
  - 按 `top_n` 或 `top/bottom_n` 生成买卖信号。
  - 控制调仓频率和 `close_on_rank_exit`。
- Risk layer：
  - 控制仓位上限、总持仓数、止损、lot size、成本参数。

然后直接调用 `CompositeBacktestEngine.run()` 执行。

### 因子评估指标如何挂载

建议提供两种模式：

1. 复用已有评估结果：
   - 若同窗口已有因子评估结果，则直接挂到回测结果的 `extensions.factor.evaluation`。
2. 同步计算评估结果：
   - 若请求要求 `attach_factor_metrics = true`，则在回测前对同时间窗执行一次评估计算。

这样用户可以在一个结果页里同时看到：

- 因子本身是否有效。
- 该因子按当前组合构建规则能否形成可执行收益。

## 组合策略回测收敛设计

### 收敛目标

组合策略回测不重写引擎，而是做以下收敛：

1. 接入统一回测提交入口。
2. 接入统一任务状态与历史记录。
3. 接入统一结果结构。
4. 保留现有 `CompositeStrategies` 页面入口，作为兼容视图。

### 当前链路保留项

以下能力继续复用：

- `CompositeStrategyService` 对组合和绑定关系的加载。
- `CompositeStrategyOrchestrator` 的 Universe / Trading / Risk 编排。
- `CompositeBacktestEngine` 的组合净值回放。

### 需要补强的点

组合回测当前还需要补强以下问题：

1. 去掉“无标的池时使用硬编码样本标的”的兜底逻辑。
2. 支持统一的 benchmark 字段和结果指标口径。
3. 将当前独立的 `composite_backtests` 历史并入统一回测历史。
4. 支持统一结果页中的 layer attribution 渲染。

### 兼容策略

建议保留 `/composite-backtests` 接口一个过渡周期，但内部实现改为：

- 兼容接口只负责参数转换。
- 实际调用统一 `Backtest Hub`。
- 返回结果结构保持兼容。

## 统一结果模型设计

### 设计目标

结果模型必须同时满足：

- CTA 单标的回测展示。
- 因子组合回测展示。
- 组合策略回测展示。
- 报表页、对比页、工作台的复用。

### 推荐结构

建议新增 `BacktestResultV2` 作为统一结果模型：

```json
{
  "job_id": "bt_xxx",
  "subject_type": "factor",
  "subject_id": 123,
  "subject_name": "ROE_20D_Momentum",
  "engine_type": "portfolio_daily",
  "scope_type": "cross_sectional_portfolio",
  "status": "completed",
  "request": {},
  "summary": {},
  "series": {},
  "artifacts": {},
  "diagnostics": {},
  "extensions": {}
}
```

### `summary` 统一字段

建议所有对象都尽量输出以下统一字段：

- `total_return`
- `annual_return`
- `max_drawdown`
- `max_drawdown_pct`
- `sharpe_ratio`
- `sortino_ratio`
- `alpha`
- `beta`
- `benchmark_return`
- `turnover`
- `total_trades`
- `win_rate`
- `profit_factor`

不是所有引擎都能一次性提供全部字段，但统一结构要先定义出来。

### `series` 统一字段

- `equity_curve`
- `benchmark_curve`
- `daily_returns`
- `position_count_curve`

### `artifacts` 统一字段

- `trade_log`
- `position_snapshots`
- `holdings`
- `attribution`

### `extensions.factor`

建议包含：

- `ic_mean`
- `ic_std`
- `ic_ir`
- `long_ret`
- `short_ret`
- `long_short_ret`
- `rank_ic_by_period`
- `top_bottom_bucket_returns`

### `extensions.composite`

建议包含：

- `universe_selection_stats`
- `trading_signal_stats`
- `risk_filter_stats`
- `component_level_attribution`

### 兼容策略

为避免一次性打断现有前后端：

- 保留当前 `BacktestResult` 用于旧策略回测接口。
- 新增 `BacktestResultV2` 给统一回测接口使用。
- 旧策略接口可从 V2 结果投影出 V1 响应。

## 存储与数据模型设计

### 建议方向

建议把当前 `backtest_history` 扩展为统一回测运行表，而不是继续新建更多平行表。

原因：

- 当前主回测历史已经被页面、报表、工作台等多处复用。
- 在已有主链路上扩展，比继续维护 `composite_backtests`、`qlib_backtest_results`、未来 `factor_backtests` 三套平行表更可控。

### 建议对 `backtest_history` 增加字段

- `subject_type` ENUM(`strategy`, `factor`, `composite`)
- `subject_id` INT NULL
- `subject_name` VARCHAR(255)
- `engine_type` VARCHAR(32)
- `scope_type` VARCHAR(32)
- `request_payload` JSON
- `summary_json` JSON
- `artifacts_json` JSON
- `diagnostics_json` JSON
- `result_schema_version` INT DEFAULT 2

### 兼容现有字段

保留以下旧字段以兼容当前策略回测：

- `strategy_id`
- `strategy_class`
- `vt_symbol`
- `start_date`
- `end_date`
- `parameters`
- `result`

处理方式：

- 第一阶段保留旧字段。
- 新字段作为统一入口的主存储。
- 老查询逻辑继续可用。

### `composite_backtests` 的处理建议

建议采用“双写 + 过渡淘汰”策略：

1. 第一阶段：
   - 组合回测仍写 `composite_backtests`。
   - 同时写统一 `backtest_history`。
2. 第二阶段：
   - 新页面与统一接口只读 `backtest_history`。
3. 第三阶段：
   - 评估是否保留 `composite_backtests` 作为兼容表，或做历史迁移后只保留只读兼容层。

### 不建议的做法

不建议第一阶段直接删除 `composite_backtests` 或强行迁移所有历史数据，风险较高。

## API 设计

### 新增统一接口

建议新增统一回测接口：

- `POST /backtest/runs`
- `GET /backtest/runs`
- `GET /backtest/runs/{job_id}`
- `DELETE /backtest/runs/{job_id}`

查询条件支持：

- `subject_type`
- `subject_id`
- `engine_type`
- `status`
- `created_at`

### 保留兼容接口

现有接口继续保留，并在内部转发到统一接口：

- `/backtest`
- `/composite-backtests`
- `POST /factors/{factor_id}/backtests`（建议新增，作为 FactorLab 专用便捷入口）

### 因子便捷接口

建议新增：

- `POST /factors/{factor_id}/backtests`
- `GET /factors/{factor_id}/backtests`

这样 FactorLab 页面对用户更直观，但底层仍走统一回测编排。

## 任务编排与队列设计

### 统一任务入口

建议新增统一 worker 任务：

- `run_unified_backtest_task(job_id, subject_type, payload)`

流程：

1. 读取统一回测运行记录。
2. 根据 `subject_type` 选择适配器。
3. 执行回测。
4. 标准化结果。
5. 更新统一回测历史、队列状态和结果缓存。

### 进度状态建议

建议统一进度阶段：

- `validating`
- `loading_data`
- `scoring_or_generating_signals`
- `running_backtest`
- `computing_statistics`
- `persisting_result`
- `completed`

### 失败处理原则

为了保证回测可信度，统一回测任务必须遵守以下原则：

1. 对正式回测，不允许使用 stub metrics 伪装成功结果。
2. 对无可用 market data 的场景，应明确失败并返回 diagnostics。
3. 对无 universe 的因子/组合回测，应报校验错误，而不是自动落到硬编码样本。

## 前端设计

### 目标

前端要实现的是“统一回测体验”，而不是仅仅在多个页面里拼装多个接口。

### 页面改造范围

#### 1. Backtest 页面

建议升级为统一回测页，支持：

- 回测对象类型切换：策略 / 因子 / 组合策略。
- 历史结果按对象类型筛选。
- 统一绩效卡片。
- 根据 `subject_type` 渲染扩展模块。

渲染规则建议：

- `strategy`：保留单标的价格曲线和交易明细。
- `factor`：新增因子诊断、换手率、top/bottom 分层收益。
- `composite`：新增 layer attribution、持仓数、组合行为分析。

#### 2. FactorLab 页面

当前 Backtest 页签为空，建议改造成：

- 因子回测参数表单。
- 因子历史回测列表。
- 因子评估指标与回测指标联动展示。

最小可用交互：

- 选择 factor。
- 选择 universe。
- 选择调仓频率。
- 选择 long-only 或 long-short。
- 提交回测。
- 查看统一结果详情。

#### 3. CompositeStrategies 页面

建议保留当前提交体验，但改为：

- 提交时走统一回测接口。
- 结果详情使用统一结果详情组件。
- 组合专属 attribution 区块保留。

### 前端类型与 API 变更

建议更新：

- `src/types/index.ts`
- `src/lib/api.ts`
- `src/pages/Backtest.tsx`
- `src/pages/FactorLab.tsx`
- `src/pages/CompositeStrategies.tsx`

## 模块改造建议

### 后端

建议改造或新增以下模块：

- `app/api/models/backtest.py`
  - 新增统一请求与 `BacktestResultV2`
- `app/api/services/backtest_service.py`
  - 增加统一编排层或适配器注册表
- `app/api/routes/backtest.py`
  - 新增统一 `runs` 接口
- `app/api/routes/factors.py`
  - 新增因子回测便捷接口
- `app/api/routes/composite.py`
  - 兼容转发到统一接口
- `app/domains/factors/backtest_service.py`
  - 新增，承载因子回测配置解析与执行逻辑
- `app/domains/backtests/`
  - 新增统一 run DAO / result normalizer / adapter registry
- `app/domains/composite/backtest_engine.py`
  - 补齐统一口径指标输出
- `app/domains/composite/service.py`
  - 统一接入回测中心
- `app/worker/service/tasks.py` 或新增统一任务模块
  - 增加 `run_unified_backtest_task`

### 数据迁移

建议新增一个 SQL migration：

- 扩展 `backtest_history` 新字段
- 增加必要索引：
  - `idx_backtest_subject_type`
  - `idx_backtest_subject`
  - `idx_backtest_engine_type`
  - `idx_backtest_user_created`

### 前端

建议改造以下文件：

- `quantmate-portal/src/types/index.ts`
- `quantmate-portal/src/lib/api.ts`
- `quantmate-portal/src/pages/Backtest.tsx`
- `quantmate-portal/src/pages/FactorLab.tsx`
- `quantmate-portal/src/pages/CompositeStrategies.tsx`
- 对应 i18n 文案文件

## 分阶段开发计划

### Phase 1：统一模型与后端基础设施

目标：建立统一回测对象模型与任务/存储骨架。

范围：

1. 扩展 `backtest_history`。
2. 新增统一请求模型和结果模型 V2。
3. 新增统一回测提交、查询、删除接口。
4. 新增适配器注册与任务入口。
5. 先把现有 `strategy` 类型接入统一链路。

交付物：

- 统一 `BacktestRunRequest`
- 统一 `BacktestResultV2`
- 统一 DAO 与 worker 任务
- 保持旧策略回测兼容

预计工作量：4 到 6 人日。

### Phase 2：因子回测能力落地

目标：让 FactorLab 真正具备“因子回测”能力。

范围：

1. 新增 `FactorPortfolioBacktestAdapter`。
2. 新增因子组合构建器，把因子请求转成临时组合策略配置。
3. 复用 `CompositeBacktestEngine` 执行因子回测。
4. 将因子评估结果挂入回测结果的 `extensions.factor`。
5. 新增 `POST /factors/{factor_id}/backtests`。

交付物：

- 单因子与多因子组合回测最小可用版本
- 支持 long-only / long-short、top_n、调仓频率、成本参数
- FactorLab Backtest 页签可用

预计工作量：6 到 8 人日。

### Phase 3：组合策略回测收敛

目标：把现有组合策略回测纳入统一回测评估。

范围：

1. `CompositeStrategyBacktestAdapter` 接入统一回测中心。
2. `/composite-backtests` 内部改为兼容包装层。
3. 组合回测双写统一历史。
4. 补齐组合专属 attribution 到统一结果结构。
5. 去掉无 universe 时的硬编码样本回退。

交付物：

- 组合回测在统一历史中可见
- 组合回测详情可在统一结果页展示
- 当前组合策略页面仍可正常使用

预计工作量：4 到 6 人日。

### Phase 4：统一前端体验与报表接入

目标：让用户在当前 UI 中真正感知到统一回测评估。

范围：

1. Backtest 页面支持对象类型切换与差异化渲染。
2. FactorLab Backtest 页签补齐提交与结果查看。
3. CompositeStrategies 页面改为复用统一结果详情组件。
4. 报表页与工作台逐步接入统一回测结果。

交付物：

- 统一回测列表与详情页
- 因子、组合、策略三类对象的统一浏览与筛选

预计工作量：5 到 7 人日。

### Phase 5：迁移、测试与灰度上线

目标：在保证现有策略回测不回退的前提下完成上线。

范围：

1. 补齐集成测试和前端交互测试。
2. 进行组合回测双写验证。
3. 进行小范围灰度开关控制。
4. 评估是否需要回填旧 `composite_backtests` 到统一历史。

交付物：

- 可灰度发布版本
- 测试报告
- 回滚方案

预计工作量：3 到 5 人日。

### 总体工期评估

按单人串行估算：22 到 32 人日。

若后端与前端并行，可压缩到约 3 到 4 周。

## 测试计划

### 后端单元测试

覆盖以下场景：

1. 统一请求校验。
2. 适配器分发正确性。
3. 因子表达式计算与排序正确性。
4. 因子组合构建规则：top_n、long-short、调仓频率。
5. 组合策略 attribution 映射正确性。
6. 统一结果标准化正确性。

### 后端集成测试

覆盖以下场景：

1. `strategy` 回测通过统一接口执行。
2. `factor` 回测能生成净值曲线与因子扩展指标。
3. `composite` 回测能进入统一历史并可查询详情。
4. 兼容接口返回结果与旧页面预期一致。
5. 无数据、无 universe、benchmark 缺失、表达式错误时的失败语义正确。

### 前端测试

覆盖以下场景：

1. FactorLab 提交因子回测。
2. CompositeStrategies 提交组合回测。
3. Backtest 页面按对象类型筛选历史。
4. 三类对象结果详情渲染正确。

### 数据质量测试

重点验证：

1. benchmark 收益率与 alpha/beta 计算口径一致。
2. 换手率、交易成本、收益曲线口径一致。
3. 因子评估指标与回测区间一致。

## 风险与应对

### 风险 1：结果口径不统一

问题：CTA、因子组合、组合策略在交易粒度和数据来源上天然不同。

应对：

- 用统一结果模型承载公共字段。
- 明确 `scope_type` 与 `engine_type`。
- 扩展指标放入 `extensions`，不强行塞进公共字段。

### 风险 2：因子回测与组合回测逻辑重复

问题：若单独实现因子引擎，后期维护成本会急剧上升。

应对：

- 因子回测直接复用组合回测引擎。
- 通过适配器与临时配置转换实现，不新造第二套组合执行层。

### 风险 3：兼容改造影响现有策略回测

问题：当前 CTA 回测已被多页面复用，不能回退。

应对：

- 采用 V2 结果模型并保留 V1 投影。
- 旧接口保留。
- 先让 `strategy` 在统一接口中跑通，再接入 `factor` 与 `composite`。

### 风险 4：历史表迁移风险

问题：直接替换 `composite_backtests` 风险较高。

应对：

- 第一阶段双写。
- 第二阶段只切读链路。
- 确认稳定后再做历史收敛。

### 风险 5：回测可信度受兜底逻辑影响

问题：stub metrics、硬编码样本 universe 等行为会让结果失真。

应对：

- 正式回测禁止 stub metrics。
- 无 universe 直接校验失败。
- diagnostics 明确暴露数据覆盖率与降级信息。

## 验收标准

满足以下条件可视为一期目标完成：

1. 当前回测评估可以查询并展示 `strategy`、`factor`、`composite` 三类对象的回测结果。
2. FactorLab 的 Backtest 页签不再为空，用户可以直接发起因子回测。
3. 因子回测结果至少包含：净值曲线、benchmark 曲线、Sharpe、最大回撤、换手率、IC/ICIR。
4. 组合策略回测进入统一历史，并可在统一结果详情页查看。
5. 现有 CTA 策略回测流程不回退，旧接口仍可用。
6. 对无数据、无 universe、表达式错误等场景，系统返回明确失败，而不是伪成功结果。

## 推荐实施顺序

从风险最低的路径看，推荐按以下顺序推进：

1. 先做统一模型与统一存储，不碰现有组合引擎实现。
2. 先让现有 `strategy` 回测接入统一接口，验证兼容层。
3. 再实现因子回测适配器，复用组合回测引擎。
4. 最后把组合回测入口切到统一中心，完成收敛。

这样做的好处是：

- 每一阶段都可独立验证。
- 不会一上来同时改动 CTA、因子、组合三条路径。
- 可以尽快把当前空的 FactorLab Backtest 页签补上，先交付业务价值。

## 最终建议

最终建议如下：

1. 将“当前回测评估支持因子与组合策略”作为主目标，而不是新增更多平行回测入口。
2. 因子回测基于现有组合回测引擎实现，定位为横截面组合回测。
3. 组合策略回测保留现有引擎，但统一进入当前回测评估链路。
4. 通过统一结果模型、统一历史、统一队列和统一前端渲染完成架构收敛。

如果按上述方案执行，QuantMate 的回测能力将从“多入口、多结果模型、多页面分叉”收敛为“统一回测评估中心 + 多引擎适配”，这也是后续扩展 Qlib、AI 报告、报表对比和工作台闭环的正确基础。

## 可执行开发任务清单

本节将上文设计拆解为可直接进入迭代的执行项。建议按两条泳道推进：

- 后端泳道：统一模型、统一任务、因子回测适配器、组合回测收敛。
- 前端泳道：统一类型、统一结果页、FactorLab 与 CompositeStrategies 接入。

建议角色分工：

- 后端负责人 1 名：负责统一模型、DAO、接口、任务编排。
- 量化后端负责人 1 名：负责因子组合构建与组合回测口径。
- 前端负责人 1 名：负责统一页面、类型、交互与兼容改造。
- QA 或开发自测负责人 1 名：负责接口回归、数据口径验证和灰度验收。

### 迭代 0：准备与边界清理

目标：在正式开发前把会影响回测可信度的兜底行为和边界条件标记清楚。

任务清单：

1. 盘点当前正式回测链路中所有“伪成功”行为。
  - 范围：`app/domains/factors/service.py`、`app/domains/composite/tasks.py`、`app/api/services/backtest_service.py`
  - 输出：一份明确的禁止降级清单。
  - 完成标准：列出哪些行为可以保留在研究模式，哪些必须在正式回测中失败返回。
2. 统一 symbol / universe / benchmark 的输入口径。
  - 范围：CTA 回测使用 `vt_symbol`，组合与因子回测使用 tushare 风格 symbols，Qlib 使用 qlib instruments。
  - 输出：输入口径映射说明。
  - 完成标准：文档化三类引擎的输入、转换点和输出标准。
3. 确定统一回测历史的主表策略。
  - 输出：确认以 `backtest_history` 为统一主表，`composite_backtests` 进入双写过渡。
  - 完成标准：技术方案评审通过。

### 工作包 A：统一后端模型与存储

目标：建立统一回测对象模型，支撑后续三类回测对象接入。

#### A1. 扩展回测数据库模型

主要文件：

- `mysql/migrations/`
- `app/domains/backtests/dao/backtest_history_dao.py`

任务：

1. 新增 SQL migration，扩展 `backtest_history` 字段。
2. 为 `subject_type`、`subject_id`、`engine_type`、`user_id + created_at` 增加索引。
3. 确认旧字段兼容保留，不破坏现有查询。

完成标准：

- 新字段已在本地 migration 中创建。
- DAO 能读写新增字段。
- 旧回测历史查询不报错。

#### A2. 定义统一 API 模型

主要文件：

- `app/api/models/backtest.py`

任务：

1. 新增 `BacktestSubjectType`、`BacktestEngineType`、`BacktestScopeType`。
2. 新增 `BacktestRunRequest`。
3. 新增 `BacktestResultV2` 与统一列表项模型。
4. 保留现有 `BacktestRequest`、`BacktestResult` 作为兼容模型。

完成标准：

- Pydantic 模型能完整表达 strategy / factor / composite 三类请求。
- 新旧模型共存，旧接口不受影响。

#### A3. 建立统一 DAO 层

主要文件：

- `app/domains/backtests/dao/backtest_history_dao.py`
- 建议新增 `app/domains/backtests/dao/backtest_run_dao.py`

任务：

1. 抽出统一 run 的插入、更新状态、查询详情、分页列表能力。
2. 支持按 `subject_type`、`subject_id`、`engine_type`、`status` 过滤。
3. 支持存储 `request_payload`、`summary_json`、`artifacts_json`、`diagnostics_json`。

完成标准：

- 统一 run DAO 可以支撑新接口。
- 不依赖旧策略字段也能完成查询。

### 工作包 B：统一任务编排与适配器注册

目标：把三类对象的回测统一放到同一提交和执行框架下。

#### B1. 建立统一回测编排服务

主要文件：

- `app/api/services/backtest_service.py`
- 建议新增 `app/domains/backtests/service.py`

任务：

1. 增加统一提交入口 `submit_run()`。
2. 统一生成 job_id、持久化 run 记录、写队列元数据。
3. 根据 `subject_type` 选择适配器。

完成标准：

- 一个服务可以调度 `strategy`、`factor`、`composite`。
- 旧 `submit_backtest()` 仍可复用或包装该服务。

#### B2. 建立适配器接口与注册表

建议新增文件：

- `app/domains/backtests/adapters/base.py`
- `app/domains/backtests/adapters/strategy_vnpy.py`
- `app/domains/backtests/adapters/factor_portfolio.py`
- `app/domains/backtests/adapters/composite.py`

任务：

1. 定义统一适配器协议：`validate()`, `run()`, `normalize_result()`。
2. 建立注册表，按 `subject_type` 获取适配器。
3. 抽出统一结果标准化逻辑。

完成标准：

- 新增一个适配器不需要改动主编排分支逻辑。
- 适配器返回结果都能落到 `BacktestResultV2`。

#### B3. 新增统一 worker 任务

主要文件：

- `app/worker/service/tasks.py` 或新增 `app/worker/service/backtest_tasks.py`

任务：

1. 新增 `run_unified_backtest_task(job_id)`。
2. 按统一状态阶段更新 run 状态。
3. 执行完成后同步更新 job storage / history / result cache。

完成标准：

- 新统一接口提交流程可以异步执行并查询状态。
- 失败时有 diagnostics 和错误原因。

### 工作包 C：Strategy 回测接入统一中心

目标：先让现有 CTA 回测接入统一体系，验证兼容层设计正确。

主要文件：

- `app/api/services/backtest_service.py`
- `app/api/routes/backtest.py`
- `app/api/routes/queue.py`

任务：

1. 建立 `VnpyStrategyBacktestAdapter`，内部复用 `run_single_backtest()`。
2. 让统一接口支持 `subject_type = strategy`。
3. 旧 `/backtest`、`/queue/backtest` 中 vn.py 路径改为包装统一服务。
4. 增加 V2 到 V1 的结果投影层。

完成标准：

- 旧策略回测页面无感知继续可用。
- 统一接口可正确返回 strategy 类型结果。

### 工作包 D：因子回测落地

目标：让 FactorLab 的 Backtest 页签真正可用。

#### D1. 新增因子回测领域服务

建议新增文件：

- `app/domains/factors/backtest_service.py`

任务：

1. 解析 `factor_id`、`expression`、`universe`、`rebalance`、`portfolio` 配置。
2. 封装因子回测输入校验。
3. 提供从因子请求到临时组合策略配置的转换能力。

完成标准：

- 可以将因子回测请求稳定转换为组合回测执行配置。

#### D2. 实现因子组合构建器

建议新增文件：

- `app/domains/factors/factor_portfolio_builder.py`

任务：

1. 支持 `long_only_top_n`。
2. 支持 `long_short_top_bottom_n`。
3. 支持 `equal_weight` 与 `score_weight`。
4. 支持 `daily | weekly | monthly` 调仓。
5. 支持 `close_on_rank_exit`、`max_position_pct`、`max_total_positions`。

完成标准：

- 输入一份因子回测 profile，可以生成明确的组合执行规则。

#### D3. 复用组合引擎执行因子回测

主要文件：

- `app/domains/composite/backtest_engine.py`
- `app/domains/composite/orchestrator.py`
- `app/domains/composite/portfolio.py`
- `app/domains/backtests/adapters/factor_portfolio.py`

任务：

1. 实现 `FactorPortfolioBacktestAdapter`。
2. 通过临时组合配置驱动 `CompositeBacktestEngine`。
3. 补齐 turnover、position_count_curve 等组合指标。
4. 把因子评估结果挂到 `extensions.factor`。

完成标准：

- 可以从单因子或多因子组合直接产出净值曲线。
- 回测结果包含因子指标和组合指标。

#### D4. 增加因子回测接口

主要文件：

- `app/api/routes/factors.py`
- `app/api/routes/backtest.py`

任务：

1. 新增 `POST /factors/{factor_id}/backtests`。
2. 新增 `GET /factors/{factor_id}/backtests`。
3. 底层走统一回测中心。

完成标准：

- FactorLab 可以通过便捷接口直接发起因子回测。

### 工作包 E：组合策略回测收敛

目标：把现有组合策略回测纳入统一回测评估，而不是维持完全平行的结果链路。

主要文件：

- `app/domains/composite/service.py`
- `app/api/routes/composite.py`
- `app/domains/composite/tasks.py`
- `app/domains/backtests/adapters/composite.py`

任务：

1. 建立 `CompositeStrategyBacktestAdapter`。
2. 将组合回测提交改为调用统一回测提交服务。
3. 现有 `/composite-backtests` 保持兼容响应，但内部走统一链路。
4. 对 `composite_backtests` 与 `backtest_history` 做双写。
5. 移除默认硬编码 sample universe 的正式回测兜底逻辑。

完成标准：

- 组合回测结果在统一历史中可见。
- 旧组合回测页面仍然可用。
- 正式回测无 universe 时明确失败。

### 工作包 F：统一 API 与路由层

目标：对外暴露统一回测接口，同时保留兼容入口。

主要文件：

- `app/api/routes/backtest.py`
- `app/api/routes/composite.py`
- `app/api/routes/factors.py`

任务：

1. 新增 `POST /backtest/runs`。
2. 新增 `GET /backtest/runs`。
3. 新增 `GET /backtest/runs/{job_id}`。
4. 新增 `DELETE /backtest/runs/{job_id}`。
5. 兼容接口内部转调统一服务。

完成标准：

- 新旧接口都可查询到同一批回测运行记录。

### 工作包 G：前端类型与 API SDK

目标：让前端能消费统一回测对象和结果结构。

主要文件：

- `quantmate-portal/src/types/index.ts`
- `quantmate-portal/src/lib/api.ts`

任务：

1. 新增统一回测类型：请求、列表项、详情、扩展结构。
2. 保留旧策略回测与组合回测类型作为兼容类型。
3. 新增统一回测 API SDK。
4. 新增 factor backtest API SDK。

完成标准：

- 页面层可以不感知底层旧接口差异。

### 工作包 H：Backtest 页面重构

目标：把当前 Backtest 页面升级为统一回测中心。

主要文件：

- `quantmate-portal/src/pages/Backtest.tsx`

任务：

1. 增加对象类型切换：策略 / 因子 / 组合策略。
2. 增加统一历史列表查询与筛选。
3. 抽出统一结果详情区域。
4. 按 `subject_type` 渲染差异化扩展模块。

完成标准：

- 三类对象结果可在同一页面查看。
- 公共 summary 卡片复用同一组件。

### 工作包 I：FactorLab 页面落地回测页签

目标：完成当前空白 Backtest 页签的业务闭环。

主要文件：

- `quantmate-portal/src/pages/FactorLab.tsx`

任务：

1. 增加因子回测参数表单。
2. 增加历史回测列表。
3. 提交后跳转或联动展示统一结果详情。
4. 将因子评估指标与回测 summary 并排展示。

完成标准：

- 用户在 FactorLab 中可以完成“选因子 -> 跑回测 -> 看结果”。

### 工作包 J：CompositeStrategies 页面兼容接入

目标：保留当前组合策略页面体验，同时统一到底层结果链路。

主要文件：

- `quantmate-portal/src/pages/CompositeStrategies.tsx`

任务：

1. 提交回测时调用统一接口或兼容包装接口。
2. 结果详情区域改为复用统一详情组件。
3. 保留组合策略专属 attribution 展示。

完成标准：

- 用户仍在原页面操作，但底层历史和详情已统一。

### 工作包 K：统一结果详情组件

目标：减少三类页面分别维护结果渲染逻辑。

建议新增文件：

- `quantmate-portal/src/components/backtest/BacktestResultSummary.tsx`
- `quantmate-portal/src/components/backtest/BacktestResultSeries.tsx`
- `quantmate-portal/src/components/backtest/BacktestResultExtensions.tsx`

任务：

1. 拆出公共 summary 区块。
2. 拆出净值/benchmark/returns 图表区块。
3. 拆出 `factor` 与 `composite` 的扩展渲染器。

完成标准：

- Backtest、FactorLab、CompositeStrategies 三页不再重复实现结果明细。

### 工作包 L：测试与发布

目标：保证改造不引入回归，且数据口径可信。

#### L1. 后端测试

主要文件：

- `tests/unit/`
- `tests/integration/`

任务：

1. 增加统一模型与 DAO 测试。
2. 增加适配器分发测试。
3. 增加因子组合构建规则测试。
4. 增加统一接口集成测试。
5. 增加兼容接口回归测试。

完成标准：

- `strategy`、`factor`、`composite` 三类回测至少各有一条集成路径覆盖。

#### L2. 前端测试

主要文件：

- `quantmate-portal/src/pages/`
- `quantmate-portal/test/`

任务：

1. 为 Backtest 页面增加类型切换与详情渲染测试。
2. 为 FactorLab 增加提交流程测试。
3. 为 CompositeStrategies 增加兼容提交流程测试。

完成标准：

- 三类对象的关键交互有稳定测试覆盖。

#### L3. 发布与灰度

任务：

1. 增加 feature flag：是否展示统一因子回测入口。
2. 增加双写监控：`composite_backtests` 与 `backtest_history` 的记录数与关键指标比对。
3. 准备回滚方案：前端隐藏新入口，后端兼容接口继续可用。

完成标准：

- 即使统一中心出现问题，旧策略回测与旧组合回测入口仍可作为回退路径。

## 推荐排期与并行方式

建议分三轮并行推进。

### 第一轮

并行项：

- 后端：A、B、C
- 前端：G 的类型预埋

里程碑：

- 统一接口已能支持 strategy 回测。
- 旧策略回测接口保持兼容。

### 第二轮

并行项：

- 后端：D、E、F
- 前端：H、I、J、K

里程碑：

- FactorLab 回测页签可用。
- 组合回测进入统一历史。

### 第三轮

并行项：

- 后端与前端：L

里程碑：

- 三类对象在统一页面可见。
- 兼容接口回归通过。
- 可以灰度上线。

## 每个工作包的 Done 定义

每个工作包完成时，至少满足以下条件：

1. 代码已合入对应模块，不依赖人工临时脚本运行。
2. 至少有一条自动化测试覆盖该工作包核心路径。
3. 接口字段、页面类型和数据库字段命名已统一。
4. 无使用 stub metrics 或默认 sample universe 伪造正式回测结果。
5. 对失败场景有明确错误消息和 diagnostics。

## 建议先创建的开发子任务

如果直接开 issue / ticket，建议先创建以下 12 个子任务：

1. 扩展 `backtest_history` 支持统一回测元数据。
2. 定义 `BacktestRunRequest` 与 `BacktestResultV2`。
3. 实现统一回测 run DAO 与查询接口。
4. 实现统一回测任务入口与适配器注册表。
5. 将 vn.py strategy 回测接入统一中心。
6. 新增因子回测领域服务与 profile 校验。
7. 实现因子组合构建器与 `FactorPortfolioBacktestAdapter`。
8. 将组合回测接入统一中心并双写统一历史。
9. 实现统一回测 API SDK 与 TS 类型。
10. 重构 Backtest 页面为统一回测中心。
11. 落地 FactorLab Backtest 页签与 CompositeStrategies 兼容详情。
12. 补齐自动化测试、灰度开关与回滚方案。