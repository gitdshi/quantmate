"""Factor expression AST parser (TASK-009).

Replaces the regex-stacking ``normalize_factor_expression`` + blacklist-based
``df.eval`` with a proper AST walker. Supports three syntaxes:

  1. pandas expressions: ``close / open - 1``
  2. plain math:          ``(close - open) / open``
  3. LaTeX subset:        ``\\frac{close - open}{open}``

Security guarantees:
  - Only whitelisted column names and math functions can be referenced.
  - Attribute access, imports, lambdas, comprehensions and assignments are
    rejected, so the expression cannot escape into arbitrary Python.
"""

from __future__ import annotations

import ast
import operator
import re
from typing import Any, Optional, Set

import numpy as np
import pandas as pd


# ── Allowed operators ────────────────────────────────────────────────

_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
    ast.Mod: operator.mod,
    ast.FloorDiv: operator.floordiv,
}

_UNARY_OPS = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}

# ── Allowed math / rolling functions ────────────────────────────────

def _instrument_level(index: pd.Index) -> Optional[int]:
    """Return the level index of the instrument column, or None for single-stock frames."""
    if isinstance(index, pd.MultiIndex) and index.nlevels >= 2:
        return 0
    return None


def _by_instrument(series: pd.Series, operation) -> pd.Series:
    """Apply ``operation`` per-instrument when the Series has a MultiIndex."""
    level = _instrument_level(series.index)
    if level is None:
        return operation(series)
    return series.groupby(level=level, group_keys=False).apply(operation)


def _rank(x: pd.Series) -> pd.Series:
    """Cross-sectional rank percentile (0..1)."""
    return _by_instrument(x, lambda s: s.rank(pct=True))


def _delay(x: pd.Series, n: int) -> pd.Series:
    return _by_instrument(x, lambda s: s.shift(int(n)))


def _delta(x: pd.Series, n: int) -> pd.Series:
    return _by_instrument(x, lambda s: s.diff(int(n)))


def _ma(x: pd.Series, n: int) -> pd.Series:
    return _by_instrument(x, lambda s: s.rolling(int(n), min_periods=1).mean())


def _ema(x: pd.Series, n: int) -> pd.Series:
    return _by_instrument(x, lambda s: s.ewm(span=int(n), adjust=False).mean())


def _std(x: pd.Series, n: int) -> pd.Series:
    return _by_instrument(x, lambda s: s.rolling(int(n), min_periods=1).std())


def _corr(x: pd.Series, y: pd.Series, n: int = 20) -> pd.Series:
    # Rolling correlation is naturally pairwise; group by instrument.
    level = _instrument_level(x.index)
    if level is None:
        return x.rolling(int(n), min_periods=2).corr(y)
    df = pd.concat([x.rename("x"), y.rename("y")], axis=1)
    return df.groupby(level=level, group_keys=False).apply(
        lambda g: g["x"].rolling(int(n), min_periods=2).corr(g["y"])
    )


def _cov(x: pd.Series, y: pd.Series, n: int = 20) -> pd.Series:
    level = _instrument_level(x.index)
    if level is None:
        return x.rolling(int(n), min_periods=2).cov(y)
    df = pd.concat([x.rename("x"), y.rename("y")], axis=1)
    return df.groupby(level=level, group_keys=False).apply(
        lambda g: g["x"].rolling(int(n), min_periods=2).cov(g["y"])
    )


def _max_(x: pd.Series, n: int) -> pd.Series:
    return _by_instrument(x, lambda s: s.rolling(int(n), min_periods=1).max())


def _min_(x: pd.Series, n: int) -> pd.Series:
    return _by_instrument(x, lambda s: s.rolling(int(n), min_periods=1).min())


_ALLOWED_FUNCTIONS = {
    "abs": np.abs,
    "log": np.log,
    "log10": np.log10,
    "exp": np.exp,
    "sqrt": np.sqrt,
    "sin": np.sin,
    "cos": np.cos,
    "tan": np.tan,
    "sign": np.sign,
    "max": _max_,
    "min": _min_,
    "mean": lambda x, n=20: _ma(x, n),
    "std": _std,
    "sum": lambda x, n=20: _by_instrument(x, lambda s: s.rolling(int(n), min_periods=1).sum()),
    "rank": _rank,
    "delay": _delay,
    "delta": _delta,
    "ma": _ma,
    "ema": _ema,
    "corr": _corr,
    "cov": _cov,
    # Aliases used by normalize_factor_expression output (ts_* namespace).
    "ts_mean": _ma,
    "ts_std": _std,
    "ts_max": _max_,
    "ts_min": _min_,
    "ts_sum": lambda x, n=20: _by_instrument(x, lambda s: s.rolling(int(n), min_periods=1).sum()),
    "ts_corr": _corr,
    "ts_rank": lambda x, n=20: _by_instrument(x, lambda s: s.rolling(int(n)).rank(pct=True)),
}

# Default column whitelist (callers may override).
_ALLOWED_COLUMNS: Set[str] = {
    "open", "high", "low", "close", "vol", "volume", "amount",
    "vwap", "turnover", "turnover_rate", "factor", "adj_factor",
    "pe", "pe_ttm", "pb", "ps", "ps_ttm", "pcfb",
    "total_mv", "circ_mv", "free_share", "total_share",
    "pct_chg", "change", "pre_close",
}


# ── LaTeX → Python translation ───────────────────────────────────────


def latex_to_python(expr: str) -> str:
    """Translate a LaTeX subset into a Python expression.

    Handles nested ``\\frac`` and ``\\sqrt`` via iterative substitution.
    """
    if "\\" not in expr and "^" not in expr:
        return expr

    # \frac{A}{B} → (A)/(B)  — iterative to handle nesting
    for _ in range(10):
        new_expr = re.sub(
            r"\\frac\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}",
            r"(\1)/(\2)",
            expr,
        )
        if new_expr == expr:
            break
        expr = new_expr

    # \sqrt{A} → (A)**0.5
    expr = re.sub(r"\\sqrt\{([^{}]+)\}", r"(\1)**0.5", expr)

    # \text{X} → X
    expr = re.sub(r"\\text\{([^}]+)\}", r"\1", expr)

    # \times / \cdot → *, \div → /
    expr = expr.replace(r"\times", "*").replace(r"\cdot", "*").replace(r"\div", "/")

    # ^ → ** (a^b or a^{b})
    expr = re.sub(r"\^\{([^}]+)\}", r"**(\1)", expr)
    expr = re.sub(r"\^(\w)", r"**\1", expr)

    # Strip remaining unknown LaTeX commands
    expr = re.sub(r"\\[a-zA-Z]+", "", expr)

    return expr.strip()


# ── AST evaluator ────────────────────────────────────────────────────


class ExpressionEvaluator(ast.NodeVisitor):
    """Safe AST evaluator — only whitelisted columns and functions."""

    def __init__(
        self,
        df: pd.DataFrame,
        allowed_columns: Optional[Set[str]] = None,
    ) -> None:
        self.df = df
        self.columns = allowed_columns if allowed_columns is not None else _ALLOWED_COLUMNS

    # Entry point
    def visit_Expression(self, node: ast.Expression) -> Any:
        return self.visit(node.body)

    # Literals
    def visit_Constant(self, node: ast.Constant) -> Any:
        return node.value

    def visit_Num(self, node: ast.Num) -> Any:  # Py<3.8 compat
        return node.n

    # Names — must be a whitelisted column
    def visit_Name(self, node: ast.Name) -> Any:
        name = node.id
        if name in self.columns and name in self.df.columns:
            return self.df[name]
        # Allow constants like pi/e? For now reject everything else.
        raise ValueError(f"Unknown column or variable: {name!r}")

    # Binary / unary operators
    def visit_BinOp(self, node: ast.BinOp) -> Any:
        left = self.visit(node.left)
        right = self.visit(node.right)
        op_func = _BIN_OPS.get(type(node.op))
        if op_func is None:
            raise ValueError(f"Unsupported binary operator: {type(node.op).__name__}")
        return op_func(left, right)

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
        operand = self.visit(node.operand)
        op_func = _UNARY_OPS.get(type(node.op))
        if op_func is None:
            raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")
        return op_func(operand)

    def visit_BoolOp(self, node: ast.BoolOp) -> Any:
        raise ValueError("Boolean operators (and/or) not supported in factor expressions")

    def visit_Compare(self, node: ast.Compare) -> Any:
        raise ValueError("Comparison operators not supported in factor expressions")

    # Function calls — only whitelisted functions, no kwargs
    def visit_Call(self, node: ast.Call) -> Any:
        func_name = self._get_func_name(node.func)
        if func_name not in _ALLOWED_FUNCTIONS:
            raise ValueError(f"Function not allowed: {func_name!r}")
        if node.keywords:
            raise ValueError("Keyword arguments not supported")
        func = _ALLOWED_FUNCTIONS[func_name]
        args = [self.visit(a) for a in node.args]
        return func(*args)

    def _get_func_name(self, func_node: ast.AST) -> str:
        if isinstance(func_node, ast.Name):
            return func_node.id
        raise ValueError("Only simple function calls allowed")

    # Explicitly forbidden nodes
    def visit_Attribute(self, node: ast.Attribute) -> Any:
        raise ValueError("Attribute access not allowed")

    def visit_Subscript(self, node: ast.Subscript) -> Any:
        raise ValueError("Subscript not allowed")

    def visit_Assign(self, node: ast.Assign) -> Any:
        raise ValueError("Assignment not allowed")

    def visit_AugAssign(self, node: ast.AugAssign) -> Any:
        raise ValueError("Augmented assignment not allowed")

    def visit_Lambda(self, node: ast.Lambda) -> Any:
        raise ValueError("Lambda not allowed")

    def visit_ListComp(self, node: ast.ListComp) -> Any:
        raise ValueError("List comprehension not allowed")

    def visit_SetComp(self, node: ast.SetComp) -> Any:
        raise ValueError("Set comprehension not allowed")

    def visit_DictComp(self, node: ast.DictComp) -> Any:
        raise ValueError("Dict comprehension not allowed")

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> Any:
        raise ValueError("Generator expression not allowed")

    def visit_Import(self, node: ast.Import) -> Any:
        raise ValueError("Import not allowed")

    def visit_ImportFrom(self, node: ast.ImportFrom) -> Any:
        raise ValueError("Import not allowed")

    def visit_IfExp(self, node: ast.IfExp) -> Any:
        raise ValueError("Conditional expression not allowed")


# ── Public API ───────────────────────────────────────────────────────


def safe_eval(
    expr: str,
    df: pd.DataFrame,
    allowed_columns: Optional[Set[str]] = None,
) -> pd.Series:
    """Safely evaluate a factor expression against ``df``.

    Args:
        expr: Expression string (pandas / math / LaTeX subset).
        df: DataFrame containing the columns referenced by ``expr``.
        allowed_columns: Optional override for the column whitelist.

    Returns:
        pd.Series of factor values aligned to ``df.index``.

    Raises:
        ValueError: if the expression uses disallowed syntax or columns.
    """
    if not isinstance(expr, str) or not expr.strip():
        raise ValueError("Expression must be a non-empty string")

    # Translate LaTeX subset → Python first.
    if "\\" in expr or "^" in expr:
        expr = latex_to_python(expr)

    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"Invalid expression syntax: {expr!r}") from exc

    evaluator = ExpressionEvaluator(df, allowed_columns)
    result = evaluator.visit(tree)

    if not isinstance(result, pd.Series):
        result = pd.Series(result, index=df.index)

    return result
