"""Qlib background worker tasks — model training, data conversion, backtesting.

These tasks run in RQ workers alongside the existing vnpy backtest tasks.
Data source: tushare + akshare databases (NOT vnpy DB).
"""

from __future__ import annotations

import logging
import traceback
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

QlibModelService = None
convert_to_qlib_format = None


def _get_qlib_model_service():
    global QlibModelService
    if QlibModelService is None:
        from app.domains.ai.qlib_model_service import QlibModelService as service_cls

        QlibModelService = service_cls
    return QlibModelService


def _get_data_converter():
    global convert_to_qlib_format
    if convert_to_qlib_format is None:
        from app.infrastructure.qlib.data_converter import convert_to_qlib_format as converter

        convert_to_qlib_format = converter
    return convert_to_qlib_format


def run_qlib_training_task(
    user_id: int,
    model_type: str = "LightGBM",
    factor_set: str = "Alpha158",
    universe: str = "csi300",
    train_start: str = "2018-01-01",
    train_end: str = "2022-12-31",
    valid_start: str = "2023-01-01",
    valid_end: str = "2023-06-30",
    test_start: str = "2023-07-01",
    test_end: str = "2024-12-31",
    hyperparams: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Train a Qlib ML model in a background worker."""
    try:
        logger.info("[qlib-worker] Starting %s training for user %d", model_type, user_id)

        service = _get_qlib_model_service()()
        result = service.train_model(
            user_id=user_id,
            model_type=model_type,
            factor_set=factor_set,
            universe=universe,
            train_start=train_start,
            train_end=train_end,
            valid_start=valid_start,
            valid_end=valid_end,
            test_start=test_start,
            test_end=test_end,
            hyperparams=hyperparams,
        )

        logger.info("[qlib-worker] Training completed: %s", result)
        return result

    except Exception as e:
        logger.exception("[qlib-worker] Training failed: %s", e)
        return {
            "status": "failed",
            "error": str(e),
            "traceback": traceback.format_exc(),
        }


def run_data_conversion_task(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    use_akshare_supplement: bool = False,
) -> Dict[str, Any]:
    """Convert tushare/akshare data to Qlib binary format."""
    try:
        from datetime import date as date_cls

        logger.info("[qlib-worker] Starting data conversion...")

        sd = date_cls.fromisoformat(start_date) if start_date else None
        ed = date_cls.fromisoformat(end_date) if end_date else None

        result = _get_data_converter()(
            start_date=sd,
            end_date=ed,
            use_akshare_supplement=use_akshare_supplement,
        )

        logger.info("[qlib-worker] Data conversion result: %s", result)
        return result

    except Exception as e:
        logger.exception("[qlib-worker] Data conversion failed: %s", e)
        return {
            "status": "failed",
            "error": str(e),
            "traceback": traceback.format_exc(),
        }


def run_qlib_backtest_task(
    user_id: int,
    job_id: str,
    training_run_id: Optional[int] = None,
    model_type: str = "LightGBM",
    factor_set: str = "Alpha158",
    universe: str = "csi300",
    start_date: str = "2023-01-01",
    end_date: str = "2024-12-31",
    strategy_type: str = "TopkDropout",
    topk: int = 50,
    n_drop: int = 5,
    benchmark: str = "SH000300",
    hyperparams: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Run a Qlib-based backtest using AI model signals.

    This is an alternative to the vnpy backtest engine.
    Uses tushare/akshare data via Qlib's data layer.
    """
    try:
        from app.infrastructure.qlib.qlib_config import ensure_qlib_initialized, SUPPORTED_STRATEGIES

        ensure_qlib_initialized()

        logger.info("[qlib-worker] Starting Qlib backtest job %s", job_id)

        # Record in DB
        _create_qlib_backtest_record(
            user_id=user_id,
            job_id=job_id,
            training_run_id=training_run_id,
            strategy_type=strategy_type,
            topk=topk,
            n_drop=n_drop,
            universe=universe,
            start_date=start_date,
            end_date=end_date,
            benchmark=benchmark,
        )
        _update_qlib_backtest_status(job_id, "running")

        from qlib.utils import init_instance_by_config
        from qlib.contrib.evaluate import backtest_daily, risk_analysis

        # Prepare dataset + model (either from existing run or fresh)
        from app.infrastructure.qlib.qlib_config import SUPPORTED_MODELS, SUPPORTED_DATASETS

        handler_class = SUPPORTED_DATASETS[factor_set]
        handler_config = {
            "class": handler_class,
            "module_path": handler_class.rsplit(".", 1)[0],
            "kwargs": {
                "instruments": universe,
                "start_time": start_date,
                "end_time": end_date,
            },
        }

        dataset_config = {
            "class": "DatasetH",
            "module_path": "qlib.data.dataset",
            "kwargs": {
                "handler": handler_config,
                "segments": {"test": (start_date, end_date)},
            },
        }

        dataset = init_instance_by_config(dataset_config)

        # Build model and generate predictions
        model_class_path = SUPPORTED_MODELS[model_type]
        model_kwargs = hyperparams or {}
        model_config = {
            "class": model_class_path.split(".")[-1],
            "module_path": model_class_path.rsplit(".", 1)[0],
            "kwargs": model_kwargs,
        }

        model = init_instance_by_config(model_config)

        # If we have an existing training run, we should load its model
        # For now, we train a fresh model on the available data
        model.fit(dataset)
        pred = model.predict(dataset)

        # Run Qlib backtest with strategy
        strategy_class_path = SUPPORTED_STRATEGIES.get(strategy_type, SUPPORTED_STRATEGIES["TopkDropout"])
        strategy_config = {
            "class": strategy_class_path.split(".")[-1],
            "module_path": strategy_class_path.rsplit(".", 1)[0],
            "kwargs": {"signal": pred, "topk": topk, "n_drop": n_drop},
        }

        port_analysis, indicator = backtest_daily(
            pred=pred,
            strategy=strategy_config,
            benchmark=benchmark,
        )

        # Extract statistics
        risk_df = risk_analysis(port_analysis["return"])
        statistics = {}
        if risk_df is not None and not risk_df.empty:
            for col in risk_df.columns:
                for idx in risk_df.index:
                    key = f"{idx}_{col}" if len(risk_df.columns) > 1 else str(idx)
                    val = risk_df.loc[idx, col]
                    statistics[key] = float(val) if val is not None else None

        result = {
            "job_id": job_id,
            "status": "completed",
            "engine": "qlib",
            "model_type": model_type,
            "factor_set": factor_set,
            "universe": universe,
            "strategy_type": strategy_type,
            "start_date": start_date,
            "end_date": end_date,
            "benchmark": benchmark,
            "statistics": statistics,
            "completed_at": datetime.now().isoformat(),
        }

        _complete_qlib_backtest(job_id, statistics, None)
        logger.info("[qlib-worker] Qlib backtest %s completed", job_id)
        return result

    except Exception as e:
        error_msg = f"Qlib backtest failed: {str(e)}"
        logger.exception("[qlib-worker] %s", error_msg)
        _update_qlib_backtest_status(job_id, "failed", str(e))
        return {
            "job_id": job_id,
            "status": "failed",
            "engine": "qlib",
            "error": error_msg,
            "traceback": traceback.format_exc(),
        }


# ── DB helpers for qlib_backtest_results ─────────────────────────────


def _create_qlib_backtest_record(**kwargs) -> None:
    from sqlalchemy import text
    from app.infrastructure.db.connections import connection

    try:
        with connection("qlib") as conn:
            conn.execute(
                text(
                    "INSERT INTO qlib_backtest_results "
                    "(user_id, job_id, training_run_id, strategy_type, topk, n_drop, "
                    "universe, start_date, end_date, benchmark, status) "
                    "VALUES (:uid, :jid, :trid, :st, :topk, :nd, :uni, :sd, :ed, :bm, 'queued')"
                ),
                {
                    "uid": kwargs["user_id"],
                    "jid": kwargs["job_id"],
                    "trid": kwargs.get("training_run_id"),
                    "st": kwargs.get("strategy_type", "TopkDropout"),
                    "topk": kwargs.get("topk", 50),
                    "nd": kwargs.get("n_drop", 5),
                    "uni": kwargs.get("universe", "csi300"),
                    "sd": kwargs["start_date"],
                    "ed": kwargs["end_date"],
                    "bm": kwargs.get("benchmark", "SH000300"),
                },
            )
            conn.commit()
    except Exception as exc:
        logger.warning("[qlib-worker] Failed to create backtest record: %s", exc)


def _update_qlib_backtest_status(job_id: str, status: str, error: Optional[str] = None) -> None:
    from sqlalchemy import text
    from app.infrastructure.db.connections import connection

    try:
        with connection("qlib") as conn:
            if error:
                conn.execute(
                    text("UPDATE qlib_backtest_results SET status = :s, error_message = :e WHERE job_id = :jid"),
                    {"s": status, "e": error[:2000], "jid": job_id},
                )
            else:
                conn.execute(
                    text("UPDATE qlib_backtest_results SET status = :s WHERE job_id = :jid"),
                    {"s": status, "jid": job_id},
                )
            conn.commit()
    except Exception as exc:
        logger.warning("[qlib-worker] Failed to update backtest status: %s", exc)


def _complete_qlib_backtest(job_id: str, statistics: dict, portfolio_analysis: Optional[dict]) -> None:
    import json
    from sqlalchemy import text
    from app.infrastructure.db.connections import connection

    try:
        with connection("qlib") as conn:
            conn.execute(
                text(
                    "UPDATE qlib_backtest_results SET status = 'completed', "
                    "statistics = :stats, portfolio_analysis = :pa, completed_at = NOW() "
                    "WHERE job_id = :jid"
                ),
                {
                    "stats": json.dumps(statistics) if statistics else None,
                    "pa": json.dumps(portfolio_analysis) if portfolio_analysis else None,
                    "jid": job_id,
                },
            )
            conn.commit()
    except Exception as exc:
        logger.warning("[qlib-worker] Failed to complete backtest record: %s", exc)
