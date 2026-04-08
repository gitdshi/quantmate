"""Fix composite_dao test file MOD_CS and MOD_CB patching."""
import os

os.chdir("/Users/mac/Workspace/QuantMate/quantmate")

fp = "tests/unit/dao/test_composite_dao.py"
with open(fp, "r") as f:
    content = f.read()

# Add imports
for var, path, alias in [
    ("MOD_CS", "app.domains.composite.dao.composite_strategy_dao", "_cs_mod"),
    ("MOD_CB", "app.domains.composite.dao.composite_backtest_dao", "_cb_mod"),
]:
    import_line = f"import {path} as {alias}"
    if import_line not in content:
        content = content.replace(
            f'{var} = "{path}"',
            f'import {path} as {alias}\n{var} = "{path}"',
        )
    content = content.replace(
        f'monkeypatch.setattr({var}, "connection"',
        f'monkeypatch.setattr({alias}, "connection"',
    )

with open(fp, "w") as f:
    f.write(content)
print(f"Fixed: {fp}")
