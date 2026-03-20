"""Qlib Model Service — train, predict, and manage ML models via Qlib.

Wraps Qlib's workflow: dataset creation → model training → prediction.
Supports models from Qlib's model zoo (LightGBM, LSTM, Transformer, etc.).
Data source: tushare/akshare via Qlib binary files (NOT vnpy DB).
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection
from app.infrastructure.qlib.qlib_config import (
    SUPPORTED_MODELS,
    SUPPORTED_DATASETS,
    SUPPORTED_STRATEGIES,
    ensure_qlib_initialized,
    is_qlib_available,
)

logger = logging.getLogger(__name__)


class QlibModelService:
    """Service for Qlib model training, prediction, and backtest."""

    def train_model(
        self,
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
        """Train a Qlib model. Returns the training run record.

        This is designed to be called from a background worker task.
        """
        if model_type not in SUPPORTED_MODELS:
            raise ValueError(f"Unsupported model type: {model_type}. Supported: {list(SUPPORTED_MODELS.keys())}")
        if factor_set not in SUPPORTED_DATASETS:
            raise ValueError(f"Unsupported factor set: {factor_set}. Supported: {list(SUPPORTED_DATASETS.keys())}")

        # Create training run record
        run_id = self._create_training_run(
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

        try:
            ensure_qlib_initialized()

            from qlib.data.dataset import DatasetH
            from qlib.data.dataset.handler import DataHandlerLP
            from qlib.utils import init_instance_by_config

            # Build dataset handler config
            handler_class = SUPPORTED_DATASETS[factor_set]
            handler_config = {
                "class": handler_class,
                "module_path": handler_class.rsplit(".", 1)[0],
                "kwargs": {
                    "instruments": universe,
                    "start_time": train_start,
                    "end_time": test_end,
                    "fit_start_time": train_start,
                    "fit_end_time": train_end,
                },
            }

            # Build dataset config
            dataset_config = {
                "class": "DatasetH",
                "module_path": "qlib.data.dataset",
                "kwargs": {
                    "handler": handler_config,
                    "segments": {
                        "train": (train_start, train_end),
                        "valid": (valid_start, valid_end),
                        "test": (test_start, test_end),
                    },
                },
            }

            dataset = init_instance_by_config(dataset_config)

            # Build model config
            model_class = SUPPORTED_MODELS[model_type]
            model_kwargs = hyperparams or {}
            model_config = {
                "class": model_class.split(".")[-1],
                "module_path": model_class.rsplit(".", 1)[0],
                "kwargs": model_kwargs,
            }

            model = init_instance_by_config(model_config)

            # Train
            self._update_training_status(run_id, "running")
            model.fit(dataset)

            # Predict on test set
            pred = model.predict(dataset)

            # Calculate metrics
            metrics = self._calculate_metrics(pred, dataset)

            # Save predictions to DB
            self._save_predictions(run_id, pred)

            # Update training run with metrics
            self._complete_training_run(run_id, metrics)

            logger.info("[qlib-model] Training run %d completed: %s", run_id, metrics)
            return {
                "training_run_id": run_id,
                "status": "completed",
                "model_type": model_type,
                "metrics": metrics,
            }

        except Exception as exc:
            self._fail_training_run(run_id, str(exc))
            logger.exception("[qlib-model] Training run %d failed", run_id)
            raise

    def get_predictions(
        self,
        training_run_id: int,
        trade_date: Optional[str] = None,
        top_n: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get model predictions for a training run, optionally filtered by date."""
        with connection("qlib") as conn:
            params: Dict[str, Any] = {"run_id": training_run_id, "limit": top_n}
            query = "SELECT instrument, trade_date, score, rank_pct FROM model_predictions WHERE training_run_id = :run_id"
            if trade_date:
                query += " AND trade_date = :td"
                params["td"] = trade_date
            query += " ORDER BY score DESC LIMIT :limit"
            rows = conn.execute(text(query), params).fetchall()
            return [dict(r._mapping) for r in rows]

    def list_training_runs(
        self,
        user_id: int,
        status: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List training runs for a user."""
        with connection("qlib") as conn:
            params: Dict[str, Any] = {"uid": user_id, "limit": limit, "offset": offset}
            query = "SELECT * FROM model_training_runs WHERE user_id = :uid"
            if status:
                query += " AND status = :status"
                params["status"] = status
            query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
            rows = conn.execute(text(query), params).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_training_run(self, run_id: int) -> Optional[Dict[str, Any]]:
        """Get a single training run by ID."""
        with connection("qlib") as conn:
            row = conn.execute(
                text("SELECT * FROM model_training_runs WHERE id = :rid"),
                {"rid": run_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def list_supported_models(self) -> List[Dict[str, str]]:
        """Return available Qlib model types."""
        return [{"name": k, "class": v} for k, v in SUPPORTED_MODELS.items()]

    def list_supported_datasets(self) -> List[Dict[str, str]]:
        """Return available factor datasets."""
        return [{"name": k, "class": v} for k, v in SUPPORTED_DATASETS.items()]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _create_training_run(self, **kwargs) -> int:
        import json

        with connection("qlib") as conn:
            result = conn.execute(
                text(
                    "INSERT INTO model_training_runs "
                    "(user_id, model_type, factor_set, universe, train_start, train_end, "
                    "valid_start, valid_end, test_start, test_end, hyperparams, status) "
                    "VALUES (:user_id, :model_type, :factor_set, :universe, :train_start, :train_end, "
                    ":valid_start, :valid_end, :test_start, :test_end, :hyperparams, 'queued')"
                ),
                {
                    "user_id": kwargs["user_id"],
                    "model_type": kwargs["model_type"],
                    "factor_set": kwargs["factor_set"],
                    "universe": kwargs["universe"],
                    "train_start": kwargs["train_start"],
                    "train_end": kwargs["train_end"],
                    "valid_start": kwargs.get("valid_start"),
                    "valid_end": kwargs.get("valid_end"),
                    "test_start": kwargs.get("test_start"),
                    "test_end": kwargs.get("test_end"),
                    "hyperparams": json.dumps(kwargs.get("hyperparams")) if kwargs.get("hyperparams") else None,
                },
            )
            conn.commit()
            return result.lastrowid  # type: ignore[return-value]

    def _update_training_status(self, run_id: int, status: str) -> None:
        with connection("qlib") as conn:
            conn.execute(
                text("UPDATE model_training_runs SET status = :status WHERE id = :rid"),
                {"status": status, "rid": run_id},
            )
            conn.commit()

    def _complete_training_run(self, run_id: int, metrics: Dict[str, Any]) -> None:
        import json

        with connection("qlib") as conn:
            conn.execute(
                text(
                    "UPDATE model_training_runs SET status = 'completed', "
                    "metrics = :metrics, completed_at = NOW() WHERE id = :rid"
                ),
                {"metrics": json.dumps(metrics), "rid": run_id},
            )
            conn.commit()

    def _fail_training_run(self, run_id: int, error: str) -> None:
        with connection("qlib") as conn:
            conn.execute(
                text(
                    "UPDATE model_training_runs SET status = 'failed', "
                    "error_message = :err, completed_at = NOW() WHERE id = :rid"
                ),
                {"err": error[:2000], "rid": run_id},
            )
            conn.commit()

    def _save_predictions(self, run_id: int, pred) -> None:
        """Save prediction DataFrame to model_predictions table."""
        try:
            import pandas as pd

            if pred is None or (hasattr(pred, "empty") and pred.empty):
                return

            # pred is typically a Series with MultiIndex (instrument, datetime)
            if isinstance(pred, pd.Series):
                pred = pred.reset_index()
                pred.columns = ["instrument", "date", "score"]
            elif isinstance(pred, pd.DataFrame):
                pred = pred.reset_index()
                # Standardize column names
                cols = pred.columns.tolist()
                if len(cols) >= 3:
                    pred = pred.rename(columns={cols[0]: "instrument", cols[1]: "date", cols[2]: "score"})

            # Calculate cross-sectional rank
            pred["rank_pct"] = pred.groupby("date")["score"].rank(pct=True)

            # Clean instrument names
            pred["instrument"] = pred["instrument"].astype(str)
            pred["date"] = pd.to_datetime(pred["date"]).dt.date

            # Batch insert
            engine = __import__("app.infrastructure.db.connections", fromlist=["get_qlib_engine"]).get_qlib_engine()
            with engine.connect() as conn:
                batch_size = 5000
                rows = pred[["instrument", "date", "score", "rank_pct"]].values.tolist()
                for i in range(0, len(rows), batch_size):
                    batch = rows[i : i + batch_size]
                    conn.execute(
                        text(
                            "INSERT INTO model_predictions (training_run_id, instrument, trade_date, score, rank_pct) "
                            "VALUES (:rid, :inst, :td, :score, :rank)"
                        ),
                        [
                            {"rid": run_id, "inst": r[0], "td": r[1], "score": float(r[2]), "rank": float(r[3]) if r[3] is not None else None}
                            for r in batch
                        ],
                    )
                conn.commit()

            logger.info("[qlib-model] Saved %d predictions for run %d", len(rows), run_id)

        except Exception as exc:
            logger.exception("[qlib-model] Failed to save predictions for run %d: %s", run_id, exc)

    @staticmethod
    def _calculate_metrics(pred, dataset) -> Dict[str, Any]:
        """Calculate IC, ICIR, and other metrics from predictions."""
        try:
            import pandas as pd
            from scipy import stats

            # Get test labels
            test_data = dataset.prepare("test", col_set=["label"])
            if test_data is None:
                return {}

            label = test_data.iloc[:, 0] if isinstance(test_data, pd.DataFrame) else test_data

            # Align predictions with labels
            common_idx = pred.index.intersection(label.index)
            if len(common_idx) == 0:
                return {}

            pred_aligned = pred.loc[common_idx]
            label_aligned = label.loc[common_idx]

            # Calculate Information Coefficient per date
            if isinstance(pred_aligned, pd.Series):
                pred_df = pred_aligned.reset_index()
            else:
                pred_df = pred_aligned.reset_index()

            if isinstance(label_aligned, pd.Series):
                label_df = label_aligned.reset_index()
            else:
                label_df = label_aligned.reset_index()

            # Simple overall IC
            ic, _ = stats.spearmanr(pred_aligned.values.flatten(), label_aligned.values.flatten())

            return {
                "ic": round(float(ic), 6) if not pd.isna(ic) else None,
                "prediction_count": len(common_idx),
            }

        except Exception as exc:
            logger.warning("[qlib-model] Metrics calculation failed: %s", exc)
            return {}
