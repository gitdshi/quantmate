#!/usr/bin/env python3
"""Setup AkShare database and tables."""
import os
import sys

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
os.chdir(project_root)

from dotenv import load_dotenv
load_dotenv(os.path.join(project_root, '.env'))

from sqlalchemy import create_engine, text

def setup_akshare_db():
    # Create akshare database first using tushare connection
    tushare_url = os.getenv('TUSHARE_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1:3306/tushare')
    base_url = tushare_url.replace('/tushare', '')
    engine = create_engine(base_url)

    with engine.connect() as conn:
        conn.execute(text('CREATE DATABASE IF NOT EXISTS akshare DEFAULT CHARACTER SET utf8mb4'))
        conn.commit()
        print('✅ Created akshare database')

    # Now create tables
    akshare_url = os.getenv('AKSHARE_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1:3306/akshare')
    ak_engine = create_engine(akshare_url)

    with ak_engine.begin() as conn:
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS ingest_audit (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                api_name VARCHAR(64) NOT NULL,
                params JSON,
                status VARCHAR(32) DEFAULT 'running',
                fetched_rows INT DEFAULT 0,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP NULL
            )
        '''))
        
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS index_daily (
                index_code VARCHAR(32) NOT NULL,
                trade_date DATE NOT NULL,
                open DECIMAL(16,4),
                high DECIMAL(16,4),
                low DECIMAL(16,4),
                close DECIMAL(16,4),
                volume BIGINT,
                amount DECIMAL(20,4),
                PRIMARY KEY (index_code, trade_date)
            )
        '''))
        
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS stock_daily (
                ts_code VARCHAR(32) NOT NULL,
                trade_date DATE NOT NULL,
                open DECIMAL(16,4),
                high DECIMAL(16,4),
                low DECIMAL(16,4),
                close DECIMAL(16,4),
                volume BIGINT,
                amount DECIMAL(20,4),
                turnover DECIMAL(12,4),
                PRIMARY KEY (ts_code, trade_date)
            )
        '''))
        
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS stock_basic (
                ts_code VARCHAR(32) PRIMARY KEY,
                symbol VARCHAR(16) NOT NULL,
                name VARCHAR(64),
                is_active BOOLEAN DEFAULT TRUE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''))
        
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS sync_log (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(64) NOT NULL,
                sync_type VARCHAR(32) NOT NULL,
                source_rows INT DEFAULT 0,
                synced_rows INT DEFAULT 0,
                status VARCHAR(32) DEFAULT 'running',
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP NULL,
                error_message TEXT
            )
        '''))
        
        print('✅ Created akshare tables')

if __name__ == '__main__':
    setup_akshare_db()
