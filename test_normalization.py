from app.worker.service.rdagent_tasks import _normalize_discovered_factor_expression

expressions = [
    r"V_{t}^{ratio} = \frac{Volume_t}{\frac{1}{20}\sum_{i=0}^{19}Volume_{t-i}}",
    r"\sigma_{t}^{20d} = \sqrt{\frac{1}{19}\sum_{i=0}^{19}(R_{t-i} - \bar{R})^{2}}, \text{ where } R_t = \frac{Close_t}{Close_{t-1}} - 1"
]

markers = ["\\", "^", "="]

for expr in expressions:
    normalized = _normalize_discovered_factor_expression(expr)
    found_markers = [m for m in markers if m in normalized]
    print(f"Original: {expr}")
    print(f"Normalized: {normalized}")
    print(f"Markers found: {found_markers}")
    print("-" * 20)
