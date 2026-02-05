#!/usr/bin/env python3
"""Test that strategy updates sync to files."""
import requests
import time

# Login
token = requests.post('http://localhost:8000/api/auth/login', json={'username': 'admin', 'password': 'admin123'}).json()['access_token']
headers = {'Authorization': f'Bearer {token}'}

# Get strategy 8
strategy = requests.get('http://localhost:8000/api/strategies/8', headers=headers).json()
original_code = strategy['code']

print(f"Original code length: {len(original_code)}")
print(f"Original first line: {original_code.split(chr(10))[0]}")

# Add a comment to test
test_comment = f"# Test edit at {time.time()}\n"
modified_code = test_comment + original_code

print(f"\nUpdating strategy with test comment...")
update_resp = requests.put('http://localhost:8000/api/strategies/8', headers=headers, json={
    'name': strategy['name'],
    'code': modified_code
})

if update_resp.status_code == 200:
    print(f"✅ Update successful")
    
    # Check if file was updated
    time.sleep(1)
    import os
    file_path = '/Users/mac/Workspace/Projects/TraderMate/data/tradermate/strategies/simple_m_a_strategy.py'
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            file_content = f.read()
        print(f"\n✅ File exists")
        print(f"File first line: {file_content.split(chr(10))[0]}")
        if file_content.startswith(test_comment):
            print("✅ File was updated correctly!")
        else:
            print("❌ File was NOT updated - still has old content")
    else:
        print(f"❌ File does not exist at {file_path}")
else:
    print(f"❌ Update failed: {update_resp.status_code} - {update_resp.text}")
