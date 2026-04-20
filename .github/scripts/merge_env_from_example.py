from __future__ import annotations

import re
import sys
from pathlib import Path


ASSIGNMENT_RE = re.compile(r'^(\s*(?:export\s+)?)([A-Za-z_][A-Za-z0-9_]*)(\s*=\s*)(.*)$')


def read_assignments(path: Path) -> tuple[dict[str, str], list[str]]:
    assignments: dict[str, str] = {}
    order: list[str] = []
    if not path.exists():
        return assignments, order

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        match = ASSIGNMENT_RE.match(raw_line)
        if not match:
            continue

        key = match.group(2)
        value = match.group(4)
        if key not in assignments:
            order.append(key)
        assignments[key] = value

    return assignments, order


def merge_env(example_path: Path, current_env_path: Path) -> tuple[str, int, int]:
    existing_values, existing_order = read_assignments(current_env_path)
    example_lines = example_path.read_text(encoding="utf-8").splitlines()
    example_keys: list[str] = []
    merged_count = 0
    rendered_lines: list[str] = []

    for raw_line in example_lines:
        match = ASSIGNMENT_RE.match(raw_line)
        if not match:
            rendered_lines.append(raw_line)
            continue

        prefix, key, separator, _ = match.groups()
        example_keys.append(key)

        if key in existing_values:
            rendered_lines.append(f"{prefix}{key}{separator}{existing_values[key]}")
            merged_count += 1
        else:
            rendered_lines.append(raw_line)

    legacy_keys = [key for key in existing_order if key not in example_keys]
    if legacy_keys:
        if rendered_lines and rendered_lines[-1] != "":
            rendered_lines.append("")
        rendered_lines.append("# Legacy entries preserved from previous .env")
        for key in legacy_keys:
            rendered_lines.append(f"{key}={existing_values[key]}")

    return "\n".join(rendered_lines) + "\n", merged_count, len(legacy_keys)


def main() -> int:
    if len(sys.argv) != 4:
        print(
            "Usage: merge_env_from_example.py <example-path> <current-env-path> <output-path>",
            file=sys.stderr,
        )
        return 1

    example_path = Path(sys.argv[1])
    current_env_path = Path(sys.argv[2])
    output_path = Path(sys.argv[3])

    rendered, merged_count, legacy_count = merge_env(example_path, current_env_path)
    output_path.write_text(rendered, encoding="utf-8")
    print(
        f"Merged {merged_count} existing values into regenerated .env; "
        f"preserved {legacy_count} legacy entries"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())