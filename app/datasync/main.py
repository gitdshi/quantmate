"""Datasync service entrypoint.

Provides a small CLI wrapper so the datasync package can be run as an
independent service (e.g. `python -m app.datasync`). It delegates to the
existing `data_sync_daemon` module which contains the daemon implementation
and CLI flags.
"""
import sys
from pathlib import Path

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv()

from app.infrastructure.logging import configure_logging, get_logger  # noqa: E402
configure_logging()
logger = get_logger(__name__)

def main():
    # Delegate to the existing daemon module which implements argument parsing
    # and the --daemon / --once behaviour.
    from app.datasync.service import data_sync_daemon as daemon  # noqa: E402
    # When invoked as a module (`python -m app.datasync`) the args are left
    # intact; simply run the module's main behaviour if present.
    if hasattr(daemon, 'main'):
        return daemon.main()
    # Fallback: execute as script
    if __name__ == "__main__":
        import runpy
        runpy.run_module('app.datasync.service.data_sync_daemon', run_name='__main__')

if __name__ == '__main__':
    main()
