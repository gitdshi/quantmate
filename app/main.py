import os
import sys
from pathlib import Path

# Ensure repo root is on sys.path so local adapters (vnpy_mysql) are importable
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Load simple .env file (if present) to populate env vars for vnpy
env_path = ROOT.joinpath('.env')
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            continue
        k, v = line.split('=', 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k not in os.environ:
            os.environ[k] = v

# Apply VN_DATABASE_* env to vnpy settings early so get_database can find adapter
try:
    from vnpy.trader import setting as vnsetting
    # Note: vnpy automatically prefixes 'vnpy_' to the database driver name
    # So 'mysql' becomes 'vnpy_mysql', 'sqlite' becomes 'vnpy_sqlite'
    vnsetting.SETTINGS['database.name'] = os.getenv('VN_DATABASE_NAME', os.getenv('MYSQL_DB_DRIVER', 'mysql'))
    vnsetting.SETTINGS['database.host'] = os.getenv('VN_DATABASE_HOST', os.getenv('MYSQL_HOST', '127.0.0.1'))
    vnsetting.SETTINGS['database.port'] = int(os.getenv('VN_DATABASE_PORT', os.getenv('MYSQL_PORT', '3306')))
    vnsetting.SETTINGS['database.user'] = os.getenv('VN_DATABASE_USER', os.getenv('MYSQL_USER', 'root'))
    vnsetting.SETTINGS['database.password'] = os.getenv('VN_DATABASE_PASSWORD', os.getenv('MYSQL_PASSWORD', 'password'))
    vnsetting.SETTINGS['database.database'] = os.getenv('VN_DATABASE_DB', os.getenv('MYSQL_DATABASE', 'vnpy'))
except Exception:
    pass

# Configure datafeed from env (supports VN_DATAFEED_* or TUSHARE_TOKEN)
try:
    # prefer explicit VN_DATAFEED_NAME, else use 'tushare' when TUSHARE_TOKEN present
    # Note: vnpy automatically prefixes 'vnpy_' to the datafeed name
    datafeed_name = os.getenv('VN_DATAFEED_NAME')
    if not datafeed_name:
        if os.getenv('TUSHARE_TOKEN'):
            datafeed_name = 'tushare'
        else:
            datafeed_name = ''

    vnsetting.SETTINGS['datafeed.name'] = datafeed_name
    if datafeed_name:
        vnsetting.SETTINGS['datafeed.username'] = os.getenv('VN_DATAFEED_USERNAME', os.getenv('TUSHARE_TOKEN', ''))
        vnsetting.SETTINGS['datafeed.password'] = os.getenv('VN_DATAFEED_PASSWORD', '')
except Exception:
    pass

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp
# Note: vnpy_ctp gateway is not compatible with macOS
# from vnpy_ctp import CtpGateway
from vnpy_ctastrategy import CtaStrategyApp
from vnpy_ctabacktester import CtaBacktesterApp
from vnpy_datamanager import DataManagerApp


def main():
    """Start VeighNa Trader"""
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)

    # Note: CTP gateway removed due to macOS incompatibility
    # main_engine.add_gateway(CtpGateway)
    main_engine.add_app(CtaStrategyApp)
    main_engine.add_app(CtaBacktesterApp)
    main_engine.add_app(DataManagerApp)

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()

if __name__ == "__main__":
    main()