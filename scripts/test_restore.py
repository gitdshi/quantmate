#!/usr/bin/env python3
"""Test restore functionality."""
import requests

# Login
token = requests.post('http://localhost:8000/api/auth/login', json={'username': 'admin', 'password': 'admin123'}).json()['access_token']
headers = {'Authorization': f'Bearer {token}'}

# Get current strategy 8
strategy = requests.get('http://localhost:8000/api/strategies/8', headers=headers).json()
print(f"Current code (first 50 chars): {strategy['code'][:50]}...")
print(f"Current code length: {len(strategy['code'])}")

# Get history
history = requests.get('http://localhost:8000/api/strategies/8/code-history', headers=headers).json()
print(f"\nHistory versions: {len(history)}")

# Get the oldest version to restore
oldest = history[-1]
print(f"\nRestoring to history #{oldest['id']} from {oldest['created_at']}")

# Get that history version's code
hist_detail = requests.get(f"http://localhost:8000/api/strategies/8/code-history/{oldest['id']}", headers=headers).json()
print(f"History code (first 50 chars): {hist_detail['code'][:50]}...")
print(f"History code length: {len(hist_detail['code'])}")

# Restore it
restore_resp = requests.post(f"http://localhost:8000/api/strategies/8/code-history/{oldest['id']}/restore", headers=headers)
print(f"\n✅ Restore response: {restore_resp.status_code}")
if restore_resp.status_code == 200:
    result = restore_resp.json()
    print(f"   Message: {result['message']}")
    
    # Verify the restoration
    updated_strategy = requests.get('http://localhost:8000/api/strategies/8', headers=headers).json()
    print(f"\n✅ After restore:")
    print(f"   Code (first 50 chars): {updated_strategy['code'][:50]}...")
    print(f"   Code length: {len(updated_strategy['code'])}")
    print(f"   Matches history: {updated_strategy['code'] == hist_detail['code']}")
    
    # Check if a new history entry was created (backup of previous current)
    new_history = requests.get('http://localhost:8000/api/strategies/8/code-history', headers=headers).json()
    print(f"\n✅ History count after restore: {len(new_history)} (was {len(history)})")
else:
    print(f"   Error: {restore_resp.text}")
