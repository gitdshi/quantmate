"""Strategy code linting and parsing utilities.

Provides endpoints for validating, linting, and parsing strategy Python code
without persisting to database. Used by frontend code editors.
"""
from typing import List
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import ast
import importlib.util

from app.api.services.strategy_service import parse_strategy_file


router = APIRouter(prefix="/strategy-code", tags=["Strategy Code"])


class ParseRequest(BaseModel):
    content: str


class LintRequest(BaseModel):
    content: str


@router.post('/parse')
def parse_file(req: ParseRequest):
    """Parse provided Python content and return classes and defaults.

    This endpoint does not persist files. It is intended for the frontend file
    picker to POST a file's content and receive parsed metadata to populate
    the strategy create/edit form.
    """
    if not req.content or not isinstance(req.content, str):
        raise HTTPException(status_code=400, detail='Content is required')

    parsed = parse_strategy_file(req.content)
    return parsed


@router.post('/lint')
async def lint_strategy_code(data: LintRequest):
    """Perform basic linting/syntax checks for strategy Python code.

    Returns a list of diagnostics with line/col, severity and message.
    """
    code = data.content or ''
    diagnostics = []

    # 1) Syntax check via ast.parse / compile
    try:
        ast.parse(code)
    except SyntaxError as e:
        diagnostics.append({
            'line': getattr(e, 'lineno', 0),
            'col': getattr(e, 'offset', 0) or 0,
            'severity': 'error',
            'message': str(e)
        })
        return {'diagnostics': diagnostics}

    # 2) Import existence checks (best-effort)
    try:
        tree = ast.parse(code)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for n in node.names:
                    mod = n.name.split('.')[0]
                    if importlib.util.find_spec(mod) is None:
                        diagnostics.append({
                            'line': node.lineno,
                            'col': node.col_offset,
                            'severity': 'warning',
                            'message': f"Module '{mod}' not found in environment"
                        })
            elif isinstance(node, ast.ImportFrom):
                mod = node.module.split('.')[0] if node.module else ''
                if mod and importlib.util.find_spec(mod) is None:
                    diagnostics.append({
                        'line': node.lineno,
                        'col': node.col_offset,
                        'severity': 'warning',
                        'message': f"Module '{mod}' not found in environment"
                    })
    except Exception:
        pass

    return {'diagnostics': diagnostics}


@router.post('/lint/pyright')
async def lint_strategy_code_pyright(data: LintRequest):
    """Run Pyright on the provided strategy code and return diagnostics.

    Requires `pyright` binary to be available in PATH (install via `npm i -g pyright`).
    """
    import tempfile
    import subprocess
    import json
    import os

    code = data.content or ''
    diagnostics = []

    # write to temp file and run pyright
    try:
        with tempfile.TemporaryDirectory() as td:
            p = os.path.join(td, 'strategy.py')
            with open(p, 'w', encoding='utf-8') as f:
                f.write(code)

            try:
                proc = subprocess.run(
                    ['pyright', '--outputjson', p],
                    capture_output=True,
                    text=True,
                    timeout=15
                )
            except FileNotFoundError:
                raise HTTPException(
                    status_code=501,
                    detail='pyright not installed. Install with `npm i -g pyright`'
                )

            out = proc.stdout or proc.stderr
            try:
                j = json.loads(out)
            except Exception:
                return {'pyright': out, 'diagnostics': []}

            # try to extract diagnostics from pyright JSON
            # structure: may contain 'generalDiagnostics' and 'documents'
            for gd in j.get('generalDiagnostics', []):
                diagnostics.append({
                    'line': gd.get('range', {}).get('start', {}).get('line', None),
                    'col': gd.get('range', {}).get('start', {}).get('character', None),
                    'severity': gd.get('severity', 'information'),
                    'message': gd.get('message', '')
                })

            docs = j.get('documents', {})
            if isinstance(docs, dict):
                for doc in docs.values():
                    for d in doc.get('diagnostics', []):
                        diagnostics.append({
                            'line': d.get('range', {}).get('start', {}).get('line', None),
                            'col': d.get('range', {}).get('start', {}).get('character', None),
                            'severity': d.get('severity', 'information'),
                            'message': d.get('message', '')
                        })

            return {'pyright': j, 'diagnostics': diagnostics}
    except HTTPException:
        raise
    except Exception as e:
        return {'error': str(e), 'diagnostics': []}
