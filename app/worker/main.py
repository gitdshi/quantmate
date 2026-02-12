"""Worker service entrypoint.

Allows running the worker as an independent service via
`python -m app.worker` or `python -m app.worker.main`.
This delegates to the existing `run_worker` script.
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
    # Delegate to the existing run_worker script which provides CLI for queues
    from app.worker.service import run_worker as rw  # noqa: E402
    if hasattr(rw, 'main'):
        return rw.main()
    # Otherwise execute the module's __main__ behaviour
    import runpy
    runpy.run_module('app.worker.service.run_worker', run_name='__main__')

if __name__ == '__main__':
    main()
