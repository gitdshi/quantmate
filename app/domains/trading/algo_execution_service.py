"""Algorithmic order execution service.

Provides TWAP, VWAP, and iceberg order splitting algorithms
that convert a large parent order into smaller child slices.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any


@dataclass
class OrderSlice:
    """A single child order slice."""
    sequence: int
    quantity: int
    scheduled_time: datetime
    price_limit: float | None
    status: str = "pending"


class AlgoExecutionService:
    """Split parent orders into algorithmic child slices."""

    def twap(
        self,
        total_quantity: int,
        num_slices: int,
        start_time: datetime,
        end_time: datetime,
        price_limit: float | None = None,
    ) -> list[dict[str, Any]]:
        """Time-Weighted Average Price: equal-sized slices evenly spaced.

        Splits *total_quantity* into *num_slices* equally-spaced child orders
        between start_time and end_time.
        """
        if num_slices <= 0:
            raise ValueError("num_slices must be positive")
        if total_quantity <= 0:
            raise ValueError("total_quantity must be positive")

        interval = (end_time - start_time) / num_slices
        base_qty = total_quantity // num_slices
        remainder = total_quantity % num_slices

        slices: list[dict[str, Any]] = []
        for i in range(num_slices):
            qty = base_qty + (1 if i < remainder else 0)
            slices.append({
                "sequence": i + 1,
                "quantity": qty,
                "scheduled_time": (start_time + interval * i).isoformat(),
                "price_limit": price_limit,
                "status": "pending",
            })
        return slices

    def vwap(
        self,
        total_quantity: int,
        volume_profile: list[float],
        start_time: datetime,
        interval_minutes: int = 30,
        price_limit: float | None = None,
    ) -> list[dict[str, Any]]:
        """Volume-Weighted Average Price: slices proportional to volume profile.

        *volume_profile* should be a list of expected volume ratios per
        time bucket (e.g. [0.1, 0.15, 0.25, 0.25, 0.15, 0.1] for 6 buckets).
        """
        if not volume_profile:
            raise ValueError("volume_profile cannot be empty")
        total_weight = sum(volume_profile)
        if total_weight <= 0:
            raise ValueError("volume_profile weights must be positive")

        slices: list[dict[str, Any]] = []
        allocated = 0
        for i, weight in enumerate(volume_profile):
            ratio = weight / total_weight
            qty = round(total_quantity * ratio)
            # Last slice absorbs rounding difference
            if i == len(volume_profile) - 1:
                qty = total_quantity - allocated
            allocated += qty
            slices.append({
                "sequence": i + 1,
                "quantity": max(qty, 0),
                "scheduled_time": (start_time + timedelta(minutes=interval_minutes * i)).isoformat(),
                "price_limit": price_limit,
                "volume_weight": round(ratio, 4),
                "status": "pending",
            })
        return slices

    def iceberg(
        self,
        total_quantity: int,
        display_quantity: int,
        price_limit: float,
    ) -> list[dict[str, Any]]:
        """Iceberg order: shows only display_quantity at a time.

        Splits the parent order into chunks of *display_quantity*, each
        submitted only after the previous one fills.
        """
        if display_quantity <= 0:
            raise ValueError("display_quantity must be positive")
        if total_quantity <= 0:
            raise ValueError("total_quantity must be positive")

        slices: list[dict[str, Any]] = []
        remaining = total_quantity
        seq = 0
        while remaining > 0:
            seq += 1
            qty = min(display_quantity, remaining)
            remaining -= qty
            slices.append({
                "sequence": seq,
                "quantity": qty,
                "price_limit": price_limit,
                "visible": True,
                "hidden_remaining": remaining,
                "status": "pending",
            })
        return slices
