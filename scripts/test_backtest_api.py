#!/usr/bin/env python3
"""Test backtest API submission and result retrieval."""
import os
import sys
import requests
import json
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

API_URL = 'http://localhost:8000'

def test_backtest():
    # First login to get token
    print('1. Logging in...')
    login_resp = requests.post(f'{API_URL}/api/auth/login', json={
        'username': 'admin',
        'password': 'admin123'
    })

    if login_resp.status_code != 200:
        print(f'   Login failed: {login_resp.text}')
        return False

    token = login_resp.json().get('access_token')
    print(f'   ✅ Got token: {token[:20]}...')

    headers = {'Authorization': f'Bearer {token}'}

    # Get a strategy
    print('2. Getting strategies...')
    strat_resp = requests.get(f'{API_URL}/api/strategies', headers=headers)
    strategies = strat_resp.json()
    if not strategies:
        print('   No strategies found!')
        return False

    strategy = strategies[0]
    print(f'   ✅ Using strategy: {strategy["name"]} (id={strategy["id"]})')

    # Submit backtest to queue
    print('3. Submitting backtest to queue...')
    backtest_data = {
        'strategy_id': strategy['id'],
        'symbol': '000001.SZ',
        'start_date': '2025-01-01',
        'end_date': '2025-06-30',
        'initial_capital': 100000,
        'rate': 0.0003,
        'slippage': 0.0001
    }

    bt_resp = requests.post(f'{API_URL}/api/queue/backtest', headers=headers, json=backtest_data)
    if bt_resp.status_code != 200:
        print(f'   Failed: {bt_resp.text}')
        return False

    job_id = bt_resp.json().get('job_id')
    print(f'   ✅ Job submitted: {job_id}')

    # Poll for status
    print('4. Waiting for job to complete...')
    for i in range(30):
        time.sleep(2)
        status_resp = requests.get(f'{API_URL}/api/queue/jobs/{job_id}', headers=headers)
        job_data = status_resp.json()
        status = job_data.get('status', 'unknown')
        print(f'   [{i+1}] Status: {status}')
        
        if status in ['finished', 'completed', 'failed']:
            print(f'   ✅ Job finished with status: {status}')
            if job_data.get('result'):
                result = job_data['result']
                stats = result.get('statistics', {})
                print(f'   Total Return: {stats.get("total_return", "N/A")}%')
                print(f'   Alpha: {stats.get("alpha", "N/A")}')
                print(f'   Beta: {stats.get("beta", "N/A")}')
                print(f'   Benchmark Return: {stats.get("benchmark_return", "N/A")}%')
                print(f'   Equity curve points: {len(result.get("equity_curve", []))}')
            return True
    
    print('   Timeout waiting for job')
    return False

if __name__ == '__main__':
    success = test_backtest()
    sys.exit(0 if success else 1)
