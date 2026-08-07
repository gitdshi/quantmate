"""Qlib Model Service — train, predict, and manage ML models via Qlib.

Wraps Qlib's workflow: dataset creation → model training → prediction.
Supports models from Qlib's model zoo (LightGBM, LSTM, Transformer, etc.).
Data source: tushare/akshare via Qlib binary files (NOT vnpy DB).
"""

from __future__ import annotations

import json
import logging
import os
import pickle
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from app.infrastructure.db.connections import connection, get_qlib_engine
from app.infrastructure.qlib.qlib_config import (
    SUPPORTED_MODELS,
    SUPPORTED_DATASETS,
    ensure_qlib_initialized,
)

logger = logging.getLogger(__name__)


# Directory where trained Qlib models are persisted as pickle files.
# Mounted as a docker volume so models survive container restarts.
MODEL_DIR = Path(os.getenv("QLIB_MODEL_DIR", "/app/data/qlib_models"))


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

            # Persist trained model to disk so future backtests can reuse it
            # without re-training (TASK-005).
            model_path = self._persist_model(
                run_id,
                model=model,
                dataset_config=dataset_config,
                model_config=model_config,
            )

            # Update training run with metrics
            self._complete_training_run(run_id, metrics, model_path=model_path)

            logger.info("[qlib-model] Training run %d completed: %s", run_id, metrics)
            return {
                "training_run_id": run_id,
                "status": "completed",
                "model_type": model_type,
                "metrics": metrics,
                "model_path": model_path,
            }

        except Exception as exc:
            self._fail_training_run(run_id, str(exc))
            logger.exception("[qlib-model] Training run %d failed", run_id)
            raise

    # ------------------------------------------------------------------
    # Model persistence (TASK-005: backtest model reuse)
    # ------------------------------------------------------------------

    def _persist_model(
        self,
        run_id: int,
        model: Any,
        dataset_config: Dict[str, Any],
        model_config: Dict[str, Any],
    ) -> str:
        """Pickle the trained model + dataset config to MODEL_DIR.

        Returns the absolute path of the saved model file.
        Dataset itself is not pickled — it carries Qlib internal caches that
        don't deserialize cleanly across processes. Callers rebuild it from
        ``dataset_config`` via :meth:`load_trained_model`.
        """
        try:
            MODEL_DIR.mkdir(parents=True, exist_ok=True)
            model_path = MODEL_DIR / f"{run_id}.pkl"
            payload = {
                "model": model,
                "dataset_config": dataset_config,
                "model_config": model_config,
            }
            with open(model_path, "wb") as f:
                pickle.dump(payload, f)
            logger.info("[qlib-model] Persisted model to %s", model_path)
            return str(model_path)
        except Exception:
            logger.exception("[qlib-model] Failed to persist model for run %d", run_id)
            return ""

    def load_trained_model(self, run_id: int) -> Tuple[Any, Any]:
        """Load a trained model and rebuild its dataset.

        Raises:
            ValueError: if the training run is missing, not completed, or has
                no model_path.
            FileNotFoundError: if the model file no longer exists on disk.
        """
        run = self.get_training_run(run_id)
        if not run:
            raise ValueError(f"Training run not found: {run_id}")
        if run.get("status") != "completed":
            raise ValueError(
                f"Training run {run_id} status is {run.get('status')!r}, expected 'completed'"
            )
        model_path_str = run.get("model_path")
        if not model_path_str:
            raise ValueError(f"Training run {run_id} has no model_path")

        model_path = Path(model_path_str)
        if not model_path.exists():
            raise FileNotFoundError(f"Model file not found: {model_path}")

        with open(model_path, "rb") as f:
            payload = pickle.load(f)

        # Rebuild dataset from the stored config so model.predict(dataset) works.
        try:
            from qlib.utils import init_instance_by_config  # type: ignore

            dataset = init_instance_by_config(payload["dataset_config"])
        except Exception:
            logger.exception("[qlib-model] Failed to rebuild dataset for run %d", run_id)
            raise

        return payload["model"], dataset

    def get_predictions(
        self,
        training_run_id: int,
        trade_date: Optional[str] = None,
        top_n: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get model predictions for a training run, optionally filtered by date."""
        with connection("qlib") as conn:
            params: Dict[str, Any] = {"run_id": training_run_id, "limit": top_n}
            query = (
                "SELECT instrument, trade_date, score, rank_pct FROM model_predictions WHERE training_run_id = :run_id"
            )
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

    def _complete_training_run(
        self,
        run_id: int,
        metrics: Dict[str, Any],
        model_path: Optional[str] = None,
    ) -> None:
        with connection("qlib") as conn:
            conn.execute(
                text(
                    "UPDATE model_training_runs SET status = 'completed', "
                    "metrics = :metrics, model_path = :mp, completed_at = NOW() WHERE id = :rid"
                ),
                {
                    "metrics": json.dumps(metrics),
                    "mp": model_path or None,
                    "rid": run_id,
                },
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
                            {
                                "rid": run_id,
                                "inst": r[0],
                                "td": r[1],
                                "score": float(r[2]),
                                "rank": float(r[3]) if r[3] is not None else None,
                            }
                            for r in batch
                        ],
                    )
                conn.commit()

            logger.info("[qlib-model] Saved %d predictions for run %d", len(rows), run_id)

        except Exception as exc:
            logger.exception("[qlib-model] Failed to save predictions for run %d: %s", run_id, exc)

    @staticmethod
    def _calculate_metrics(pred, dataset) -> Dict[str, Any]:
        """Calculate IC, ICIR, and other metrics from predictions.

        Aligns pred and label on (datetime, instrument) before computing
        correlation to avoid NaN results caused by misaligned indices.
        """
        try:
            import pandas as pd
            from scipy import stats

            # Get test labels
            test_data = dataset.prepare("test", col_set=["label"])
            if test_data is None:
                return {}

            label = test_data.iloc[:, 0] if isinstance(test_data, pd.DataFrame) else test_data

            # Normalise pred to a Series
            if isinstance(pred, pd.DataFrame):
                pred_series = pred.iloc[:, 0]
            else:
                pred_series = pred

            # Reset indices and rename columns so we can merge on
            # (datetime, instrument). The Qlib MultiIndex is
            # (instrument, datetime) but we tolerate either order.
            pred_df = pred_series.reset_index()
            if len(pred_df.columns) < 3:
                logger.warning("[qlib-model] pred has unexpected shape: %s", pred_df.shape)
                return {}
            pred_df.columns = ["datetime", "instrument", "score"] if pred_df.columns[0] != "datetime" else ["instrument", "datetime", "score"]

            label_df = label.reset_index()
            if len(label_df.columns) < 3:
                logger.warning("[qlib-model] label has unexpected shape: %s", label_df.shape)
                return {}
            label_df.columns = ["datetime", "instrument", "label"] if label_df.columns[0] != "datetime" else ["instrument", "datetime", "label"]

            # Inner-join on (datetime, instrument)
            merged = pd.merge(pred_df, label_df, on=["datetime", "instrument"], how="inner")
            if merged.empty:
                logger.warning("[qlib-model] No overlapping samples between pred and label")
                return {"ic": None, "rank_ic": None, "prediction_count": 0}

            # Overall IC (Pearson) and Rank IC (Spearman)
            ic = merged["score"].corr(merged["label"])
            rank_ic, _ = stats.spearmanr(merged["score"], merged["label"])

            # Per-date IC then average (standard Qlib ICIR definition)
            daily_ic = (
                merged.groupby("datetime")
                .apply(lambda g: g["score"].corr(g["label"]) if len(g) > 1 else None)
                .dropna()
            )
            daily_rank_ic = (
                merged.groupby("datetime")
                .apply(lambda g: stats.spearmanr(g["score"], g["label"])[0] if len(g) > 1 else None)
                .dropna()
            )

            ic_mean = float(daily_ic.mean()) if not daily_ic.empty else None
            ic_std = float(daily_ic.std()) if not daily_ic.empty else None
            ir = float(ic_mean / ic_std) if ic_mean is not None and ic_std not in (None, 0) else None

            rank_ic_mean = float(daily_rank_ic.mean()) if not daily_rank_ic.empty else None
            rank_ic_std = float(daily_rank_ic.std()) if not daily_rank_ic.empty else None
            rank_ir = (
                float(rank_ic_mean / rank_ic_std)
                if rank_ic_mean is not None and rank_ic_std not in (None, 0)
                else None
            )

            def _round(v):
                if v is None or pd.isna(v):
                    return None
                return round(float(v), 6)

            return {
                "ic": _round(ic),
                "rank_ic": _round(rank_ic),
                "ic_mean": _round(ic_mean),
                "ic_std": _round(ic_std),
                "ir": _round(ir),
                "rank_ic_mean": _round(rank_ic_mean),
                "rank_ic_std": _round(rank_ic_std),
                "rank_ir": _round(rank_ir),
                "prediction_count": int(len(merged)),
            }

        except Exception as exc:
            logger.warning("[qlib-model] Metrics calculation failed: %s", exc)
            return {}
