#!/usr/bin/env python3
"""Test that deleting a strategy cleans up history files."""
import requests
import os
import glob

# Login
token = requests.post('http://localhost:8000/api/auth/login', json={'username': 'admin', 'password': 'admin123'}).json()['access_token']
headers = {'Authorization': f'Bearer {token}'}

# Create a test strategy
test_code = '''from vnpy_ctastrategy import CtaTemplate
from vnpy.trader.object import BarData, TickData

class TestDeleteStrategy(CtaTemplate):
    def on_init(self):
        pass
    
    def on_start(self):
        pass
    
    def on_stop(self):
        pass
    
    def on_tick(self, tick: TickData):
        pass
    
    def on_bar(self, bar: BarData):
        pass
'''

print("Creating test strategy...")
create_resp = requests.post('http://localhost:8000/api/strategies', headers=headers, json={
    'name': 'Test Delete Strategy',
    'class_name': 'TestDeleteStrategy',
    'code': test_code
})

if create_resp.status_code != 201:
    print(f"❌ Failed to create strategy: {create_resp.status_code} - {create_resp.text}")
    exit(1)

strategy = create_resp.json()
strategy_id = strategy['id']
print(f"✅ Created strategy id={strategy_id}")

# Update it a few times to create history
import time
for i in range(3):
    time.sleep(0.5)
    updated_code = f'# Version {i+1}\n' + test_code
    requests.put(f'http://localhost:8000/api/strategies/{strategy_id}', headers=headers, json={
        'name': strategy['name'],
        'code': updated_code
    })
    print(f"  Updated version {i+1}")

time.sleep(1)

# Check files exist
file_path = '/Users/mac/Workspace/Projects/TraderMate/data/tradermate/strategies/test_delete_strategy.py'
history_pattern = '/Users/mac/Workspace/Projects/TraderMate/data/tradermate/strategies/.history/test_delete_strategy-*.py'

if os.path.exists(file_path):
    print(f"\n✅ Strategy file exists: {file_path}")
else:
    print(f"\n❌ Strategy file not found: {file_path}")

history_files = glob.glob(history_pattern)
print(f"✅ Found {len(history_files)} history files")
for hf in history_files:
    print(f"  - {os.path.basename(hf)}")

# Delete the strategy
print(f"\nDeleting strategy {strategy_id}...")
delete_resp = requests.delete(f'http://localhost:8000/api/strategies/{strategy_id}', headers=headers)

if delete_resp.status_code == 204:
    print("✅ Strategy deleted successfully")
else:
    print(f"❌ Delete failed: {delete_resp.status_code} - {delete_resp.text}")
    exit(1)

time.sleep(1)

# Verify files are deleted
if os.path.exists(file_path):
    print(f"\n❌ Strategy file still exists: {file_path}")
else:
    print(f"\n✅ Strategy file deleted")

remaining_history = glob.glob(history_pattern)
if remaining_history:
    print(f"❌ History files still exist: {len(remaining_history)}")
    for hf in remaining_history:
        print(f"  - {os.path.basename(hf)}")
else:
    print("✅ All history files deleted")
