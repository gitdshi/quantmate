#!/usr/bin/env python3
"""Sync all database strategies to files."""
import requests

# Login
token = requests.post('http://localhost:8000/api/auth/login', json={'username': 'admin', 'password': 'admin123'}).json()['access_token']
headers = {'Authorization': f'Bearer {token}'}

# Get all strategies
strategies = requests.get('http://localhost:8000/api/strategies', headers=headers).json()
print(f"Found {len(strategies)} strategies\n")

# Trigger update for each strategy to save to file
for strategy in strategies:
    strategy_id = strategy['id']
    print(f"Processing strategy {strategy_id}: {strategy['name']}")
    
    # Get full strategy details
    full_strategy = requests.get(f'http://localhost:8000/api/strategies/{strategy_id}', headers=headers).json()
    
    # Update with same data to trigger file save
    update_data = {
        'name': full_strategy['name'],
        'code': full_strategy['code']
    }
    resp = requests.put(f'http://localhost:8000/api/strategies/{strategy_id}', headers=headers, json=update_data)
    
    if resp.status_code == 200:
        print(f"  ✓ Saved to file")
    else:
        print(f"  ✗ Failed: {resp.status_code} - {resp.text}")

print("\n✅ Done!")
