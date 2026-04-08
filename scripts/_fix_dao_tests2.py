"""Fix string-based monkeypatch calls in remaining DAO test files."""
import os, re

os.chdir("/Users/mac/Workspace/QuantMate/quantmate")

files_and_mods = {
    "tests/unit/dao/test_kyc_dao.py": "app.domains.auth.dao.kyc_dao",
    "tests/unit/dao/test_mfa_dao.py": "app.domains.auth.dao.mfa_dao",
    "tests/unit/dao/test_api_key_dao.py": "app.domains.auth.dao.api_key_dao",
    "tests/unit/dao/test_user_profile_dao.py": "app.domains.auth.dao.user_profile_dao",
    "tests/unit/dao/test_composite_dao.py": "app.domains.composite.dao.composite_strategy_dao",
}

for fp, mod_path in files_and_mods.items():
    if not os.path.exists(fp):
        print(f"SKIP: {fp} does not exist")
        continue
    with open(fp, "r") as f:
        content = f.read()

    # Find all unique full module paths used in monkeypatch.setattr strings
    pattern = r'monkeypatch\.setattr\("([^"]+)\.connection"'
    paths_found = set(re.findall(pattern, content))
    
    for path in paths_found:
        alias = "_" + path.split(".")[-1] + "_mod"
        import_line = f"import {path} as {alias}"
        if import_line not in content:
            # Add import after the existing imports
            # Find the last import line
            lines = content.split("\n")
            import_idx = 0
            for i, line in enumerate(lines):
                if line.startswith("import ") or line.startswith("from "):
                    import_idx = i
            lines.insert(import_idx + 1, import_line)
            content = "\n".join(lines)
        
        # Replace string-based monkeypatch with module-based
        content = content.replace(
            f'monkeypatch.setattr("{path}.connection"',
            f'monkeypatch.setattr({alias}, "connection"',
        )
    
    with open(fp, "w") as f:
        f.write(content)
    print(f"Fixed: {fp} (paths: {paths_found})")

print("All done")
