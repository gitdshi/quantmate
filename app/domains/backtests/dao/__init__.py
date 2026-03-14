"""Backtests DAOs (quantmate DB + akshare DB where appropriate).

No cross-DB joins: benchmark/index reads (akshare) are separate from history persistence (quantmate).
"""
