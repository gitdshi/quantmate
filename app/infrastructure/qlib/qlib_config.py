"""Qlib initialization and configuration.

Handles Qlib library initialization with QuantMate's data directory,
and provides helpers for switching between data providers.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from app.infrastructure.config import get_runtime_str

logger = logging.getLogger(__name__)

_qlib_initialized = False

# Default data directory for Qlib binary format files
QLIB_DATA_DIR = get_runtime_str(
    env_keys="QLIB_DATA_DIR",
    default=str(Path.home() / ".qlib" / "qlib_data" / "cn_data"),
)

# Supported Qlib model types
SUPPORTED_MODELS = {
    "LightGBM": "qlib.contrib.model.gbdt.LGBModel",
    "Linear": "qlib.contrib.model.linear.LinearModel",
    "LSTM": "qlib.contrib.model.pytorch_lstm.LSTM",
    "GRU": "qlib.contrib.model.pytorch_gru.GRU",
    "Transformer": "qlib.contrib.model.pytorch_transformer.Transformer",
    "ALSTM": "qlib.contrib.model.pytorch_alstm.ALSTM",
    "TabNet": "qlib.contrib.model.pytorch_tabnet.TabNet",
    "HIST": "qlib.contrib.model.pytorch_hist.HIST",
}

# Supported factor datasets
SUPPORTED_DATASETS = {
    "Alpha158": "qlib.contrib.data.handler.Alpha158",
    "Alpha360": "qlib.contrib.data.handler.Alpha360",
}

# Supported strategy types for Qlib backtest
SUPPORTED_STRATEGIES = {
    "TopkDropout": "qlib.contrib.strategy.signal_strategy.TopkDropoutStrategy",
    "WeightedAvg": "qlib.contrib.strategy.signal_strategy.WeightedAverageStrategy",
}


def init_qlib(data_dir: Optional[str] = None, region: str = "cn") -> bool:
    """Initialize Qlib with the specified data directory.

    Returns True if initialization succeeded, False otherwise.
    This is idempotent — calling it multiple times is safe.
    """
    global _qlib_initialized
    if _qlib_initialized:
        return True

    try:
        import qlib
        from qlib.config import REG_CN, REG_US

        provider_uri = data_dir or QLIB_DATA_DIR
        region_config = REG_CN if region == "cn" else REG_US

        qlib.init(provider_uri=provider_uri, region_config=region_config)
        _qlib_initialized = True
        logger.info("[qlib] Initialized with data_dir=%s region=%s", provider_uri, region)
        return True

    except ImportError:
        logger.warning("[qlib] pyqlib not installed — Qlib features disabled")
        return False
    except Exception as exc:
        logger.exception("[qlib] Initialization failed: %s", exc)
        return False


def is_qlib_available() -> bool:
    """Check if Qlib is installed and can be imported."""
    try:
        import qlib  # noqa: F401

        return True
    except ImportError:
        return False


def ensure_qlib_initialized() -> None:
    """Raise if Qlib is not initialized."""
    if not _qlib_initialized:
        if not init_qlib():
            raise RuntimeError("Qlib is not initialized. Ensure pyqlib is installed and data is available.")
