"""Strategy Management Service.

Provides CRUD operations for managing trading strategies stored in the
data/tradermate/strategies folder. Includes synchronization with the
project's tradermate/strategies folder.
"""
import hashlib
import shutil
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

# DB integration
try:
    from app.api.services.db import get_db_connection
except Exception:
    get_db_connection = None

# Project root
ROOT = Path(__file__).resolve().parents[3]
PROJECT_STRATEGIES = ROOT / 'tradermate' / 'strategies'
DATA_STRATEGIES = ROOT / 'data' / 'tradermate' / 'strategies'

IGNORE_NAMES = {'__pycache__', '__init__.py'}


class StrategyManager:
    """Manages trading strategy files."""

    def __init__(self, data_dir: Optional[Path] = None, project_dir: Optional[Path] = None):
        """Initialize the strategy manager.
        
        Args:
            data_dir: Path to data strategies folder (default: data/tradermate/strategies)
            project_dir: Path to project strategies folder (default: tradermate/strategies)
        """
        self.data_dir = data_dir or DATA_STRATEGIES
        self.project_dir = project_dir or PROJECT_STRATEGIES
        self._ensure_dirs()

    def _ensure_dirs(self):
        """Ensure strategy directories exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.project_dir.mkdir(parents=True, exist_ok=True)

    def _is_ignored(self, name: str) -> bool:
        """Check if a filename should be ignored."""
        return name in IGNORE_NAMES or name.startswith('.')

    def _hash_file(self, path: Path) -> Optional[str]:
        """Calculate SHA256 hash of a file."""
        if not path.exists() or not path.is_file():
            return None
        h = hashlib.sha256()
        with path.open('rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                h.update(chunk)
        return h.hexdigest()

    def list_strategies(self, source: str = 'data') -> List[Dict[str, any]]:
        """List all strategy files.
        
        Args:
            source: 'data' for data folder, 'project' for project folder, 'both' for both
            
        Returns:
            List of strategy info dicts with name, path, size, modified time
        """
        strategies = []
        
        def scan_dir(base_dir: Path, label: str):
            for path in sorted(base_dir.glob('*.py')):
                if self._is_ignored(path.name):
                    continue
                stat = path.stat()
                strategies.append({
                    'name': path.stem,
                    'filename': path.name,
                    'source': label,
                    'path': str(path),
                    'size': stat.st_size,
                    'modified': stat.st_mtime,
                    'hash': self._hash_file(path)
                })
        
        if source in ('data', 'both'):
            scan_dir(self.data_dir, 'data')
        if source in ('project', 'both'):
            scan_dir(self.project_dir, 'project')
            
        return strategies

    def find_strategy(self, name: str, source: str = 'data') -> Optional[Dict[str, any]]:
        """Find a strategy by name.
        
        Args:
            name: Strategy name (without .py extension)
            source: 'data', 'project', or 'both'
            
        Returns:
            Strategy info dict or None if not found
        """
        strategies = self.list_strategies(source=source)
        for s in strategies:
            if s['name'] == name:
                return s
        return None

    def get_strategy_content(self, name: str, source: str = 'data') -> Optional[str]:
        """Get the content of a strategy file.
        
        Args:
            name: Strategy name (without .py extension)
            source: 'data' or 'project'
            
        Returns:
            File content as string or None if not found
        """
        base_dir = self.data_dir if source == 'data' else self.project_dir
        path = base_dir / f"{name}.py"
        
        if not path.exists():
            return None
        
        return path.read_text(encoding='utf-8')

    def create_strategy(self, name: str, content: str, source: str = 'data') -> Tuple[bool, str]:
        """Create a new strategy file.
        
        Args:
            name: Strategy name (without .py extension)
            content: Python code content
            source: 'data' or 'project'
            
        Returns:
            (success, message)
        """
        if not name.endswith('.py'):
            name = f"{name}.py"
        
        base_dir = self.data_dir if source == 'data' else self.project_dir
        path = base_dir / name
        
        if path.exists():
            return False, f"Strategy '{name}' already exists"
        
        try:
            path.write_text(content, encoding='utf-8')
            return True, f"Strategy '{name}' created successfully"
        except Exception as e:
            return False, f"Failed to create strategy: {str(e)}"

    def update_strategy(self, name: str, content: str, source: str = 'data') -> Tuple[bool, str]:
        """Update an existing strategy file.
        
        Args:
            name: Strategy name (without .py extension)
            content: New Python code content
            source: 'data' or 'project'
            
        Returns:
            (success, message)
        """
        if not name.endswith('.py'):
            name = f"{name}.py"
        
        base_dir = self.data_dir if source == 'data' else self.project_dir
        path = base_dir / name
        
        if not path.exists():
            return False, f"Strategy '{name}' not found"
        
        try:
            # Save current version to history before updating
            strategy_name = name.replace('.py', '')
            self.save_file_version(strategy_name, source=source, max_versions=5)
            
            # Backup old version (legacy)
            backup_path = path.with_suffix('.py.bak')
            shutil.copy2(str(path), str(backup_path))
            
            # Write new content
            path.write_text(content, encoding='utf-8')
            # Update file metadata in DB if available
            try:
                self._upsert_file_record(strategy_name, path.name, source, str(path), path.stat().st_size, path.stat().st_mtime, self._hash_file(path))
                # Save content snapshot into DB history and rotate
                self._save_file_history_db(strategy_name, source, path.read_text(encoding='utf-8'), max_versions=5)
            except Exception:
                pass
            return True, f"Strategy '{name}' updated successfully"
        except Exception as e:
            return False, f"Failed to update strategy: {str(e)}"

    def delete_strategy(self, name: str, source: str = 'data') -> Tuple[bool, str]:
        """Delete a strategy file.
        
        Args:
            name: Strategy name (without .py extension)
            source: 'data' or 'project'
            
        Returns:
            (success, message)
        """
        if not name.endswith('.py'):
            name = f"{name}.py"
        
        base_dir = self.data_dir if source == 'data' else self.project_dir
        path = base_dir / name
        
        if not path.exists():
            return False, f"Strategy '{name}' not found"
        
        try:
            path.unlink()
            # remove DB metadata record if present
            try:
                self._delete_file_record(name.replace('.py', ''), source)
            except Exception:
                pass
            return True, f"Strategy '{name}' deleted successfully"
        except Exception as e:
            return False, f"Failed to delete strategy: {str(e)}"

    def sync_once(self, direction: str = 'bidirectional') -> Dict[str, any]:
        """Perform a one-time sync between data and project folders.
        
        Args:
            direction: 'data_to_project', 'project_to_data', or 'bidirectional'
            
        Returns:
            Sync statistics dict
        """
        stats = {
            'copied_to_data': 0,
            'copied_to_project': 0,
            'unchanged': 0,
            'errors': []
        }
        
        if direction in ('bidirectional', 'project_to_data'):
            for path in self.project_dir.glob('*.py'):
                if self._is_ignored(path.name):
                    continue
                dst = self.data_dir / path.name
                try:
                    if not dst.exists() or self._hash_file(path) != self._hash_file(dst):
                        shutil.copy2(str(path), str(dst))
                        stats['copied_to_data'] += 1
                    else:
                        stats['unchanged'] += 1
                except Exception as e:
                    stats['errors'].append(f"Error copying {path.name} to data: {str(e)}")
        
        if direction in ('bidirectional', 'data_to_project'):
            for path in self.data_dir.glob('*.py'):
                if self._is_ignored(path.name):
                    continue
                dst = self.project_dir / path.name
                try:
                    if not dst.exists() or self._hash_file(path) != self._hash_file(dst):
                        # Only copy if data is newer or project doesn't exist
                        if not dst.exists() or path.stat().st_mtime > dst.stat().st_mtime:
                            shutil.copy2(str(path), str(dst))
                            stats['copied_to_project'] += 1
                        else:
                            stats['unchanged'] += 1
                    else:
                        stats['unchanged'] += 1
                except Exception as e:
                    stats['errors'].append(f"Error copying {path.name} to project: {str(e)}")
        
        return stats

    def compare_strategies(self) -> List[Dict[str, any]]:
        """Compare strategies between data and project folders.
        
        Returns:
            List of comparison results with status for each strategy
        """
        data_strats = {s['name']: s for s in self.list_strategies('data')}
        proj_strats = {s['name']: s for s in self.list_strategies('project')}
        
        all_names = set(data_strats.keys()) | set(proj_strats.keys())
        results = []
        
        for name in sorted(all_names):
            data_s = data_strats.get(name)
            proj_s = proj_strats.get(name)
            
            if data_s and proj_s:
                if data_s['hash'] == proj_s['hash']:
                    status = 'synced'
                elif data_s['modified'] > proj_s['modified']:
                    status = 'data_newer'
                elif proj_s['modified'] > data_s['modified']:
                    status = 'project_newer'
                else:
                    status = 'different'
            elif data_s:
                status = 'data_only'
            else:
                status = 'project_only'
            
            results.append({
                'name': name,
                'status': status,
                'data': data_s,
                'project': proj_s
            })
        
        return results

    # -----------------------------
    # File history/version utilities
    # Keep a `.history/` folder next to the strategy file and manage versions
    # -----------------------------

    def _get_history_dir_for_path(self, path: Path) -> Path:
        history_dir = path.parent / '.history'
        history_dir.mkdir(parents=True, exist_ok=True)
        return history_dir

    # -----------------------------
    # DB helpers for file metadata and history
    # -----------------------------

    def _upsert_file_record(self, name: str, filename: str, source: str, path: str, size: int, modified: float, hashval: Optional[str]):
        """Upsert file record in DB. Creates strategy entry first if needed, then links file to it."""
        if get_db_connection is None:
            return
        conn = get_db_connection()
        try:
            from sqlalchemy import text
            now = datetime.utcnow()
            
            # Read file content to extract class name
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    content = f.read()
            except Exception:
                content = ""
            
            # Extract class name from content
            import re
            class_match = re.search(r'class\s+(\w+)\s*\(.*Strategy.*\):', content)
            class_name = class_match.group(1) if class_match else name.replace('.py', '')
            
            # Get or create strategy entry in strategies table
            # Using default user_id=1 (admin) for file-based strategies
            result = conn.execute(text("""
                SELECT id FROM strategies 
                WHERE class_name = :class_name AND user_id = 1
            """), {"class_name": class_name})
            strategy_row = result.fetchone()
            
            if strategy_row:
                strategy_id = strategy_row[0]
                # Update existing strategy with latest code
                conn.execute(text("""
                    UPDATE strategies 
                    SET name = :name, code = :code, updated_at = :updated_at
                    WHERE id = :id
                """), {"name": name, "code": content, "updated_at": now, "id": strategy_id})
            else:
                # Create new strategy entry
                conn.execute(text("""
                    INSERT INTO strategies (user_id, name, class_name, description, parameters, code, is_active, created_at, updated_at)
                    VALUES (:user_id, :name, :class_name, :description, :parameters, :code, :is_active, :created_at, :updated_at)
                """), {
                    "user_id": 1,  # Default admin user
                    "name": name,
                    "class_name": class_name,
                    "description": f"Strategy from {source} file",
                    "parameters": "{}",
                    "code": content,
                    "is_active": True,
                    "created_at": now,
                    "updated_at": now
                })
                # Get the created strategy ID
                result = conn.execute(text("SELECT id FROM strategies WHERE class_name = :class_name AND user_id = 1"), {"class_name": class_name})
                strategy_id = result.fetchone()[0]
            
            # Now upsert the file record with strategy_id
            result = conn.execute(text("SELECT id FROM strategy_files WHERE name = :name AND source = :source"), {"name": name, "source": source})
            file_row = result.fetchone()
            
            if file_row:
                conn.execute(text("""
                    UPDATE strategy_files SET strategy_id = :strategy_id, filename = :filename, path = :path, size = :size, modified = :modified, hash = :hash, updated_at = :updated_at
                    WHERE id = :id
                """), {"strategy_id": strategy_id, "filename": filename, "path": path, "size": size, "modified": datetime.utcfromtimestamp(modified), "hash": hashval, "updated_at": now, "id": file_row[0]})
            else:
                conn.execute(text("""
                    INSERT INTO strategy_files (strategy_id, name, filename, source, path, size, modified, hash, created_at, updated_at)
                    VALUES (:strategy_id, :name, :filename, :source, :path, :size, :modified, :hash, :created_at, :updated_at)
                """), {"strategy_id": strategy_id, "name": name, "filename": filename, "source": source, "path": path, "size": size, "modified": datetime.utcfromtimestamp(modified), "hash": hashval, "created_at": now, "updated_at": now})
            
            conn.commit()
        finally:
            conn.close()

    def _delete_file_record(self, name: str, source: str):
        if get_db_connection is None:
            return
        conn = get_db_connection()
        try:
            from sqlalchemy import text
            conn.execute(text("DELETE FROM strategy_files WHERE name = :name AND source = :source"), {"name": name, "source": source})
            conn.commit()
        finally:
            conn.close()

    def _save_file_history_db(self, name: str, source: str, content: str, max_versions: int = 5):
        if get_db_connection is None:
            return
        conn = get_db_connection()
        try:
            from sqlalchemy import text
            # find strategy_file id
            res = conn.execute(text("SELECT id FROM strategy_files WHERE name = :name AND source = :source"), {"name": name, "source": source})
            row = res.fetchone()
            if not row:
                # create record first
                now = datetime.utcnow()
                conn.execute(text("""
                    INSERT INTO strategy_files (name, filename, source, path, size, modified, hash, created_at, updated_at)
                    VALUES (:name, :filename, :source, '', 0, NULL, '', :created_at, :updated_at)
                """), {"name": name, "filename": f"{name}.py", "source": source, "created_at": now, "updated_at": now})
                conn.commit()
                res = conn.execute(text("SELECT id FROM strategy_files WHERE name = :name AND source = :source"), {"name": name, "source": source})
                row = res.fetchone()
            sf_id = row.id

            now = datetime.utcnow()
            conn.execute(text("INSERT INTO strategy_file_history (strategy_file_id, name, content, created_at) VALUES (:sf_id, :name, :content, :created_at)"), {"sf_id": sf_id, "name": f"{name}", "content": content, "created_at": now})
            conn.commit()

            # rotate down to max_versions
            rows = conn.execute(text("SELECT id FROM strategy_file_history WHERE strategy_file_id = :sf_id ORDER BY created_at DESC"), {"sf_id": sf_id}).fetchall()
            keep = [r.id for r in rows[:max_versions]]
            if len(keep) == 0:
                return
            # delete others
            conn.execute(text(f"DELETE FROM strategy_file_history WHERE strategy_file_id = :sf_id AND id NOT IN ({','.join([':k'+str(i) for i in range(len(keep))])})"), dict({"sf_id": sf_id}, **{f"k{i}": keep[i] for i in range(len(keep))}))
            conn.commit()
        finally:
            conn.close()

    def save_file_version(self, name: str, source: str = 'data', max_versions: int = 5) -> Optional[str]:
        """Save current file into .history and rotate old versions."""
        base_dir = self.data_dir if source == 'data' else self.project_dir
        path = base_dir / f"{name}.py"
        if not path.exists():
            return None

        history_dir = self._get_history_dir_for_path(path)
        import datetime, shutil, os

        timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        dest_name = f"{path.stem}-{timestamp}.py"
        dest = history_dir / dest_name

        tmp = history_dir / (dest_name + '.tmp')
        with path.open('rb') as fsrc, tmp.open('wb') as fdst:
            shutil.copyfileobj(fsrc, fdst)
        os.replace(str(tmp), str(dest))

        # rotate
        versions = sorted(history_dir.glob(f"{path.stem}-*.py"), key=os.path.getmtime, reverse=True)
        for old in versions[max_versions:]:
            try:
                old.unlink()
            except Exception:
                pass

        # Also ensure DB history rotation (if DB available)
        try:
            self._save_file_history_db(name, source, path.read_text(encoding='utf-8'), max_versions=max_versions)
        except Exception:
            pass

        return str(dest)

    def list_file_versions(self, name: str, source: str = 'data') -> List[Dict[str, str]]:
        base_dir = self.data_dir if source == 'data' else self.project_dir
        path = base_dir / f"{name}.py"
        history_dir = self._get_history_dir_for_path(path)

        out: List[Dict[str, str]] = []
        import datetime, os
        for p in sorted(history_dir.glob(f"{path.stem}-*.py"), key=os.path.getmtime, reverse=True):
            try:
                stat = p.stat()
                out.append({
                    'name': p.name,
                    'path': str(p),
                    'mtime': datetime.datetime.utcfromtimestamp(stat.st_mtime).isoformat() + 'Z',
                    'size': str(stat.st_size),
                })
            except Exception:
                continue

        return out

    def get_history_content(self, name: str, version_name: str, source: str = 'data') -> Optional[str]:
        base_dir = self.data_dir if source == 'data' else self.project_dir
        path = base_dir / f"{name}.py"
        history_dir = self._get_history_dir_for_path(path)
        candidate = history_dir / version_name
        if not candidate.exists():
            return None
        return candidate.read_text(encoding='utf-8')

    def recover_file_version(self, name: str, version_name: str, source: str = 'data', create_backup: bool = True) -> bool:
        base_dir = self.data_dir if source == 'data' else self.project_dir
        path = base_dir / f"{name}.py"
        history_dir = self._get_history_dir_for_path(path)
        candidate = history_dir / version_name
        if not candidate.exists():
            return False

        # backup current
        if create_backup and path.exists():
            try:
                self.save_file_version(name, source=source, max_versions=10)
            except Exception:
                pass

        import shutil, os
        tmp = path.parent / (path.name + '.recover.tmp')
        with candidate.open('rb') as fsrc, tmp.open('wb') as fdst:
            shutil.copyfileobj(fsrc, fdst)
        os.replace(str(tmp), str(path))
        return True


# Singleton instance
_manager = None

def get_strategy_manager() -> StrategyManager:
    """Get the singleton strategy manager instance."""
    global _manager
    if _manager is None:
        _manager = StrategyManager()
    return _manager
