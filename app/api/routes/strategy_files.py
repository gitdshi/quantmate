"""Strategy file management routes.

Manages strategy files stored in data/tradermate/strategies folder
with synchronization to project strategies folder.
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Body
from pydantic import BaseModel, Field

from app.api.models.user import TokenData
from app.api.middleware.auth import get_current_user
from app.services.strategy_manager import get_strategy_manager
from app.api.services.db import get_db_connection
from sqlalchemy import text
import ast
import importlib.util
from pydantic import BaseModel


router = APIRouter(prefix="/strategy-files", tags=["Strategy Files"])


class LintRequest(BaseModel):
    content: str


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
    import tempfile, subprocess, json, os

    code = data.content or ''
    diagnostics = []

    # write to temp file and run pyright
    try:
        with tempfile.TemporaryDirectory() as td:
            p = os.path.join(td, 'strategy.py')
            with open(p, 'w', encoding='utf-8') as f:
                f.write(code)

            try:
                proc = subprocess.run(['pyright', '--outputjson', p], capture_output=True, text=True, timeout=15)
            except FileNotFoundError:
                raise HTTPException(status_code=501, detail='pyright not installed. Install with `npm i -g pyright`')

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



class StrategyFileInfo(BaseModel):
    """Strategy file information."""
    name: str
    filename: str
    source: str
    path: str
    size: int
    modified: float
    hash: str


class StrategyFileContent(BaseModel):
    """Strategy file content."""
    name: str
    content: str


class StrategyFileCreate(BaseModel):
    """Create strategy file request."""
    name: str = Field(..., description="Strategy name (without .py extension)")
    content: str = Field(..., description="Python code content")
    source: str = Field("data", description="Target folder: 'data' or 'project'")


class StrategyFileUpdate(BaseModel):
    """Update strategy file request."""
    content: str = Field(..., description="New Python code content")
    source: str = Field("data", description="Target folder: 'data' or 'project'")





class ComparisonResult(BaseModel):
    """Strategy comparison result."""
    name: str
    status: str
    data: Optional[StrategyFileInfo]
    project: Optional[StrategyFileInfo]


@router.get("", response_model=List[StrategyFileInfo])
async def list_strategy_files(
    source: str = "data",
    current_user: TokenData = Depends(get_current_user)
):
    """List all strategy files.
    
    Args:
        source: 'data', 'project', or 'both'
    """
    if source not in ('data', 'project', 'both'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="source must be 'data', 'project', or 'both'"
        )
    
    manager = get_strategy_manager()
    strategies = manager.list_strategies(source=source)
    return strategies


@router.get("/{name}", response_model=StrategyFileContent)
async def get_strategy_file(
    name: str,
    source: str = "data",
    current_user: TokenData = Depends(get_current_user)
):
    """Get strategy file content.
    
    Args:
        name: Strategy name (without .py extension)
        source: 'data' or 'project'
    """
    if source not in ('data', 'project'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="source must be 'data' or 'project'"
        )
    
    manager = get_strategy_manager()
    content = manager.get_strategy_content(name, source=source)
    
    if content is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Strategy '{name}' not found in {source} folder"
        )
    
    return StrategyFileContent(name=name, content=content)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_strategy_file(
    data: StrategyFileCreate,
    current_user: TokenData = Depends(get_current_user)
):
    """Create a new strategy file."""
    if data.source not in ('data', 'project'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="source must be 'data' or 'project'"
        )
    
    manager = get_strategy_manager()
    success, message = manager.create_strategy(
        data.name,
        data.content,
        source=data.source
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=message
        )
    
    return {"success": True, "message": message}


@router.put("/{name}")
async def update_strategy_file(
    name: str,
    data: StrategyFileUpdate,
    current_user: TokenData = Depends(get_current_user)
):
    """Update an existing strategy file."""
    if data.source not in ('data', 'project'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="source must be 'data' or 'project'"
        )
    
    manager = get_strategy_manager()
    success, message = manager.update_strategy(
        name,
        data.content,
        source=data.source
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND if "not found" in message else status.HTTP_400_BAD_REQUEST,
            detail=message
        )
    
    return {"success": True, "message": message}


@router.delete("/{name}")
async def delete_strategy_file(
    name: str,
    source: str = "data",
    current_user: TokenData = Depends(get_current_user)
):
    """Delete a strategy file."""
    if source not in ('data', 'project'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="source must be 'data' or 'project'"
        )
    
    manager = get_strategy_manager()
    success, message = manager.delete_strategy(name, source=source)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND if "not found" in message else status.HTTP_400_BAD_REQUEST,
            detail=message
        )
    
    return {"success": True, "message": message}





@router.get("/compare/all", response_model=List[ComparisonResult])
async def compare_strategies(
    current_user: TokenData = Depends(get_current_user)
):
    """Compare strategies between data and project folders."""
    manager = get_strategy_manager()
    results = manager.compare_strategies()
    return results


@router.get("/{name}/history")
async def list_strategy_history(
    name: str,
    source: str = "data",
    current_user: TokenData = Depends(get_current_user)
):
    """List history versions for a strategy file."""
    if source not in ('data', 'project'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="source must be 'data' or 'project'")

    manager = get_strategy_manager()
    versions = manager.list_file_versions(name, source=source)
    return versions


@router.get("/db", response_model=List[StrategyFileInfo])
async def list_strategy_files_db(
    source: str = "data",
    current_user: TokenData = Depends(get_current_user)
):
    """List strategy file metadata stored in the DB."""
    if source not in ('data', 'project', 'both'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="source must be 'data','project' or 'both'")

    conn = get_db_connection()
    try:
        q = "SELECT name, filename, source, path, size, modified, hash FROM strategy_files"
        if source in ('data', 'project'):
            q += " WHERE source = :source"
            rows = conn.execute(text(q), {"source": source}).fetchall()
        else:
            rows = conn.execute(text(q)).fetchall()

        out = []
        for r in rows:
            out.append(StrategyFileInfo(
                name=r.name,
                filename=r.filename,
                source=r.source,
                path=r.path or "",
                size=int(r.size) if r.size is not None else 0,
                modified=float(r.modified.timestamp()) if r.modified is not None else 0.0,
                hash=r.hash or ""
            ))
        return out
    finally:
        conn.close()


@router.get("/db/{name}/history")
async def list_strategy_file_history_db(
    name: str,
    source: str = "data",
    current_user: TokenData = Depends(get_current_user)
):
    """List file history entries stored in DB for a given file name."""
    if source not in ('data', 'project'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="source must be 'data' or 'project'")

    conn = get_db_connection()
    try:
        # find file record
        res = conn.execute(text("SELECT id FROM strategy_files WHERE name = :name AND source = :source"), {"name": name, "source": source}).fetchone()
        if not res:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File metadata not found in DB")
        sf_id = res.id
        rows = conn.execute(text("SELECT id, name, created_at, LENGTH(content) as size FROM strategy_file_history WHERE strategy_file_id = :sf_id ORDER BY created_at DESC"), {"sf_id": sf_id}).fetchall()
        out = []
        for r in rows:
            out.append({"id": r.id, "name": r.name, "created_at": r.created_at.isoformat() if hasattr(r.created_at, 'isoformat') else str(r.created_at), "size": int(r.size)})
        return out
    finally:
        conn.close()


@router.get("/db/{name}/history/{history_id}")
async def get_strategy_file_history_db(
    name: str,
    history_id: int,
    source: str = "data",
    current_user: TokenData = Depends(get_current_user)
):
    """Return the content of a DB-stored historical version."""
    if source not in ('data', 'project'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="source must be 'data' or 'project'")

    conn = get_db_connection()
    try:
        res = conn.execute(text("SELECT sf.id FROM strategy_files sf WHERE sf.name = :name AND sf.source = :source"), {"name": name, "source": source}).fetchone()
        if not res:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File metadata not found")
        sf_id = res.id
        row = conn.execute(text("SELECT content FROM strategy_file_history WHERE id = :hid AND strategy_file_id = :sf_id"), {"hid": history_id, "sf_id": sf_id}).fetchone()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="History entry not found")
        return {"name": f"{name}-history-{history_id}", "content": row.content}
    finally:
        conn.close()


@router.get("/{name}/history/{version_name}")
async def get_strategy_history_content(
    name: str,
    version_name: str,
    source: str = "data",
    current_user: TokenData = Depends(get_current_user)
):
    """Get the content of a historical version."""
    if source not in ('data', 'project'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="source must be 'data' or 'project'")

    manager = get_strategy_manager()
    content = manager.get_history_content(name, version_name, source=source)
    if content is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Version not found")
    return {"name": version_name, "content": content}


class RecoverRequest(BaseModel):
    version_name: str
    source: str = "data"


@router.post("/{name}/history/recover")
async def recover_strategy_history(
    name: str,
    data: RecoverRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """Recover a historical version and overwrite the current strategy file."""
    if data.source not in ('data', 'project'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="source must be 'data' or 'project'")

    manager = get_strategy_manager()
    ok = manager.recover_file_version(name, data.version_name, source=data.source)
    if not ok:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Version not found or recovery failed")
    return {"success": True}
