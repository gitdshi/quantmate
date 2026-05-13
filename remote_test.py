import traceback
import sys

try:
    from app.worker.service.rdagent_tasks import (
        _build_discovered_factor_eval_context,
        _normalize_discovered_factor_expression,
        _evaluate_discovered_factor_metrics
    )

    expressions = [
        r"r_{10d} = \frac{P_t - P_{t-10}}{P_{t-10}}",
        r"r_{20d} = \frac{P_t - P_{t-20}}{P_{t-20}}",
        r"VR_{20d} = \frac{V_t}{\text{mean}(V_{t-19:t+1})}"
    ]

    scenario_params = {
        'universe': 'csi300',
        'start_date': '2024-01-01',
        'end_date': '2024-03-31'
    }

    ctx = _build_discovered_factor_eval_context(scenario_params)

    for expr in expressions:
        print(f"RAW={expr}")
        try:
            normalized = _normalize_discovered_factor_expression(expr)
            print(f"NORMALIZED={normalized}")
            metrics = _evaluate_discovered_factor_metrics(expr, ctx)
            print(f"METRICS={metrics}")
        except Exception as e:
            print(f"ERROR_FOR_EXPR={expr}")
            traceback.print_exc()
except Exception:
    traceback.print_exc()
