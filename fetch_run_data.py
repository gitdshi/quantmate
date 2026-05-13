import requests
import json
import sys

base_url = "https://test.quantmate.net/api/v1"
run_id = "5194ab4b-82a7-4602-aa53-a801aa17c441"

def fetch_data():
    session = requests.Session()
    
    # Login
    login_url = f"{base_url}/auth/login"
    login_payload = {"username": "admin", "password": "admin123"}
    login_resp = session.post(login_url, json=login_payload)
    
    if login_resp.status_code != 200:
        print(f"Login failed: {login_resp.status_code} {login_resp.text}", file=sys.stderr)
        return

    # Extract token
    token = login_resp.json().get("access_token")
    if not token:
         # Check if it was in set-cookie or elsewhere? 
         # But usually it's in the response body if it's not a cookie-based session.
         # Let's try to see if it's cookie based first by just continuing, 
         # but let's also try setting Authorization header if token exists.
         pass
    else:
         session.headers.update({"Authorization": f"Bearer {token}"})
    
    # Endpoints to fetch
    endpoints = [
        f"/rdagent/runs/{run_id}",
        f"/rdagent/runs/{run_id}/factors",
        f"/rdagent/runs/{run_id}/iterations"
    ]
    
    results = {}
    for endpoint in endpoints:
        url = f"{base_url}{endpoint}"
        resp = session.get(url)
        if resp.status_code != 200:
            results[endpoint] = {"error": resp.status_code, "text": resp.text}
        else:
            results[endpoint] = resp.json()
    
    print(json.dumps(results))

if __name__ == "__main__":
    fetch_data()
