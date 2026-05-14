# Paper Trading Runtime Design

## Summary

QuantMate's current paper trading path is an MVP scaffold:

- `paper_account_service.py` manages virtual account balances and snapshots.
- `paper_trading_service.py` persists deployments and computes simplified positions/performance.
- `paper_strategy_executor.py` runs a polling thread that converts snapshot quotes into synthetic `BarData` and feeds a CTA-like strategy loop.

That path is useful for lightweight demos, but it does not provide vn.py runtime parity. The missing pieces are the ones that matter for trustworthy CTA and portfolio paper trading:

- event-driven tick/bar delivery
- realistic order and trade callbacks
- explicit gateway/account/position event flow
- multi-symbol `PortfolioStrategy` semantics
- restart/recovery and runtime health management

This document defines the first server-side runtime slice that moves QuantMate from a thread-local executor toward a vn.py-native paper gateway architecture.

## Goals

## Implementation Status

- Phase 1: completed in code.
- Phase 2: completed in code in this revision.
- Phase 3: partially completed in code.
- Phase 4: partially completed in code.
- Phase 5: not started.

The remainder of this document reflects both the target architecture and the
current implementation status.

Phase 1 goals:

1. Introduce a dedicated paper runtime control surface in backend code.
2. Introduce a `PaperGateway` abstraction that owns simulated order and quote state.
3. Route deploy/stop actions through a single runtime service instead of directly calling the legacy executor.
4. Keep current single-symbol CTA paper deployments working via a compatibility bridge.
5. Make the next phases explicit so CTA and `PortfolioStrategy` can move to native vn.py execution later.

Non-goals for phase 1:

1. Full vn.py gateway implementation.
2. Multi-symbol portfolio execution.
3. Persistent runtime state recovery.
4. Full account/position reconciliation rewrite.
5. Replacing the existing matching worker.

## Current Gaps

### Runtime

- `paper_strategy_executor.py` polls every 5 seconds.
- CTA compatibility mode now prefers synthetic `TickData` delivery into `on_tick`, falling back to `on_bar` when needed.
- The bridge now emits strategy callbacks such as `on_order` and `on_trade`, but still executes through a simplified snapshot-driven loop.

### Data

- `RealtimeQuoteService` is snapshot oriented.
- Tencent and AkShare are appropriate for UI and fallback usage, but not for execution-grade event delivery.

### Strategy coverage

- CTA is still a compatibility bridge rather than a native vn.py app bootstrap.
- A first `PortfolioStrategy` bridge now exists, including `on_bars` batch delivery and multi-symbol deployment scope.
- Advanced portfolio semantics such as `set_target` recovery, richer order lifecycle handling, and persistent state sync are still incomplete.

### Accounting and analytics

- Positions and performance are derived from simplified order aggregation.
- Account state is not driven by a gateway-level position/account event model.

## Target Architecture

### Control plane

The API layer remains responsible for:

- validating deployment requests
- persisting deployment rows
- returning deployment metadata to the frontend

The API layer should not own strategy threads directly.

### Runtime plane

The new runtime plane introduces three core objects:

1. `PaperRuntimeService`
2. `PaperGateway`
3. `PaperRuntimeSession`

The intended control flow is:

1. Route persists a deployment via `PaperTradingService.deploy()`.
2. Route calls `PaperRuntimeService.start_deployment()`.
3. Runtime service creates a runtime session and gateway.
4. In phase 1, runtime service delegates execution to the legacy CTA executor.
5. In later phases, runtime service will bootstrap `MainEngine + EventEngine + PaperGateway + CtaStrategyApp` or `PortfolioStrategyApp` directly.

## Module Plan

### `app/domains/trading/paper_gateway.py`

Phase 1 responsibilities:

- keep in-memory paper order state
- keep latest tick payloads per symbol
- expose a snapshot for observability and tests

Future responsibilities:

- implement vn.py gateway request handlers
- emit order/trade/account/position events
- own simulated matching and position bookkeeping

### `app/domains/trading/paper_runtime_service.py`

Phase 1 responsibilities:

- single runtime entrypoint for deploy/stop
- session registry
- runtime capability metadata
- compatibility bridge to the legacy executor

Future responsibilities:

- lifecycle orchestration for native CTA and portfolio runtimes
- runtime heartbeat and health endpoints
- recovery hooks
- market data source selection and gateway bootstrap

## Session Model

Each deployment gets a `PaperRuntimeSession` with:

- deployment id
- user id
- paper account id
- strategy id and name
- symbol scope
- inferred strategy kind
- runtime mode
- gateway name
- timestamps
- warnings

This session is intentionally lightweight in phase 1. It is not yet persisted.

## Runtime Modes

### `legacy_executor_bridge`

Used in phase 1 for current single-symbol deployments.

- session and gateway are created by the new runtime service
- actual CTA strategy execution is delegated to `PaperStrategyExecutor`

### `portfolio_strategy_bridge`

Used by the current multi-symbol compatibility path.

- session and gateway are created by the runtime service
- actual portfolio strategy execution is delegated to `PaperPortfolioExecutor`
- market data remains snapshot-polled and translated into synthetic `BarData` batches

### `vnpy_paper_gateway`

Reserved target mode for later phases.

- runtime service will own `EventEngine`
- strategies will talk to a real server-side paper gateway

## Strategy Kinds

### CTA

Current deploy payload already fits a single `vt_symbol` model, so CTA remains the compatibility path.

Current state:

- synthetic tick delivery is now available for strategies that implement `on_tick`
- execution still runs through a lightweight bridge, not a native `CtaEngine` app bootstrap

### Portfolio

Portfolio support requires:

- multi-symbol deployment payloads
- session metadata for multiple instruments
- `on_bars` batch delivery
- target-position and rebalance semantics

Current state:

- deploy requests can now provide `vt_symbols`
- runtime inference can route `PortfolioStrategy` classes to a dedicated portfolio executor
- `on_bars` batch delivery is available through the compatibility bridge
- deeper target-position orchestration and recovery are still deferred

## API Contract Changes

Phase 1 keeps the existing endpoints stable and adds runtime metadata in the deploy response.

Proposed additional fields:

- `runtime.runtime_mode`
- `runtime.strategy_kind`
- `runtime.gateway_name`
- `runtime.capabilities`
- `runtime.warnings`

## Future Data Model Work

Phase 1 does not change storage, but the next migration set should introduce tables for:

1. paper runtime heartbeats
2. paper order events
3. paper position lots
4. paper runtime checkpoints
5. deployment data-source bindings

## Rollout Plan

### Phase 1

- add runtime service and gateway skeleton
- wire deploy/stop routes through runtime service
- keep legacy executor bridge

### Phase 2

- add explicit runtime daemon or worker process
- move lifecycle ownership out of the API process
- add runtime health and heartbeat tracking

Phase 2 status:

- implemented with a DB-polled `PaperRuntimeDaemon`
- deploy and stop routes now declare desired state instead of directly owning strategy lifecycle
- runtime heartbeat state is persisted in `paper_runtime_heartbeats`
- `paper_deployments` now carries desired and actual runtime status fields

### Phase 3

- implement vn.py-native CTA bootstrap using `PaperGateway`
- support event-driven order and trade callbacks
- deprecate direct route-to-executor threading

Phase 3 status:

- synthetic tick-driven CTA delivery is implemented in the compatibility bridge
- order/trade strategy callbacks are available through the paper gateway bridge
- native `CtaStrategyApp` bootstrap is still pending

### Phase 4

- add portfolio runtime bootstrap
- extend deployment payloads to multi-symbol scope
- add target-position and rebalance support

Phase 4 status:

- a first `PaperPortfolioExecutor` bridge is implemented
- runtime service can infer and dispatch `PortfolioStrategy` classes
- deploy API accepts `vt_symbols`
- native portfolio app bootstrap and richer rebalance semantics are still pending

### Phase 5

- rewrite analytics and account reconciliation around gateway-level events and snapshots

## Implementation Notes For This Commit

This commit only introduces phase 1 scaffolding:

- a new `PaperGateway` skeleton
- a new `PaperRuntimeService`
- route wiring for deploy and stop
- focused unit tests for the runtime control path

It intentionally does not change the existing order matching or account settlement implementation.

## Implementation Notes For The Current Revision

This revision adds the phase-2 handoff to a daemon-owned runtime plus the first phase-3/4 compatibility slices:

- a `PaperRuntimeDaemon` reconciliation loop
- deployment desired-state fields in `paper_deployments`
- runtime heartbeat persistence in `paper_runtime_heartbeats`
- route decoupling so the API process no longer directly starts strategy threads
- CTA compatibility execution now prefers synthetic tick delivery
- a `PaperPortfolioExecutor` bridge provides initial `PortfolioStrategy` support
- deploy requests can declare multi-symbol scope with `vt_symbols`

The actual executor is still the legacy CTA bridge. Native vn.py CTA and
portfolio runtime work remains phase 3 and phase 4.