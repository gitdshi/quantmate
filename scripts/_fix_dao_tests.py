"""Fix MOD-based monkeypatch calls in DAO test files."""
import os

os.chdir("/Users/mac/Workspace/QuantMate/quantmate")

files_and_mods = {
    "tests/unit/dao/test_strategy_dao.py": "app.domains.strategies.dao.strategy_dao",
    "tests/unit/dao/test_strategy_history_dao.py": "app.domains.strategies.dao.strategy_history_dao",
    "tests/unit/dao/test_strategy_component_dao.py": "app.domains.composite.dao.strategy_component_dao",
    "tests/unit/dao/test_backtest_history_dao.py": "app.domains.backtests.dao.backtest_history_dao",
    "tests/unit/dao/test_collaboration_dao.py": "app.domains.collaboration.dao.collaboration_dao",
    "tests/unit/dao/test_factor_dao.py": "app.domains.factors.dao.factor_dao",
    "tests/unit/dao/test_order_dao.py": "app.domains.trading.dao.order_dao",
    "tests/unit/dao/test_paper_account_dao.py": "app.domains.trading.dao.paper_account_dao",
    "tests/unit/dao/test_template_dao.py": "app.domains.templates.dao.template_dao",
}

for fp, mod_path in files_and_mods.items():
    with open(fp, "r") as f:
        content = f.read()

    import_line = f"import {mod_path} as _dao_mod"
    if import_line not in content:
        content = content.replace(
            f'MOD = "{mod_path}"',
            f'{import_line}\nMOD = "{mod_path}"',
        )

    content = content.replace(
        'monkeypatch.setattr(MOD, "connection"',
        'monkeypatch.setattr(_dao_mod, "connection"',
    )

    with open(fp, "w") as f:
        f.write(content)
    print(f"Fixed: {fp}")

print("All done")
