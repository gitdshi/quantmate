#!/usr/bin/env python3
"""Create history versions for testing."""
import requests

# Login
token = requests.post('http://localhost:8000/api/auth/login', json={'username': 'admin', 'password': 'admin123'}).json()['access_token']
headers = {'Authorization': f'Bearer {token}'}

# Get strategy 8
strategy = requests.get('http://localhost:8000/api/strategies/8', headers=headers).json()
original_code = strategy['code']
print(f"Original code length: {len(original_code)}")

# Edit 1: Add a comment
modified_code_1 = '# Version 1 - Added comment\n' + original_code
resp1 = requests.put('http://localhost:8000/api/strategies/8', headers=headers, json={'name': strategy['name'], 'code': modified_code_1})
print(f'Edit 1: {resp1.status_code}')

# Edit 2: Add another comment
modified_code_2 = '# Version 2 - Second edit\n' + modified_code_1
resp2 = requests.put('http://localhost:8000/api/strategies/8', headers=headers, json={'name': strategy['name'], 'code': modified_code_2})
print(f'Edit 2: {resp2.status_code}')

# Check history
history = requests.get('http://localhost:8000/api/strategies/8/code-history', headers=headers).json()
print(f'\nHistory count: {len(history)}')
for h in history:
    print(f"  id={h['id']}, created_at={h['created_at']}")
