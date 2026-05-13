import requests
import time
import json
import sys

BASE_URL = "https://test.quantmate.net/api/v1"

def test_flow():
    # 1. Login
    login_resp = requests.post(f"{BASE_URL}/auth/login", json={"username": "admin", "password": "admin123"})
    login_resp.raise_for_status()
    token = login_resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # 3. Create Run
    payload = {
        "scenario": "fin_factor",
        "max_iterations": 1,
        "llm_model": "minimax-m2.5-free",
        "universe": "csi300",
        "feature_columns": [],
        "start_date": "2024-01-01",
        "end_date": "2024-03-31"
    }
    create_resp = requests.post(f"{BASE_URL}/rdagent/runs", json=payload, headers=headers)
    create_resp.raise_for_status()
    run_data = create_resp.json()
    run_id = run_data.get("id") or run_data.get("run_id")
    print(f"Run started: {run_id}")

    # 5. Poll
    start_time = time.time()
    last_status = None
    while time.time() - start_time < 1200:
        get_resp = requests.get(f"{BASE_URL}/rdagent/runs/{run_id}", headers=headers)
        get_resp.raise_for_status()
        status = get_resp.json()["status"]
        if status != last_status:
            print(f"Status: {status}")
            last_status = status
        
        if status in ["completed", "failed", "cancelled"]:
            break
        time.sleep(10)
    else:
        print("Timeout reached")

    # 6. Fetch Factors
    factors_resp = requests.get(f"{BASE_URL}/rdagent/runs/{run_id}/factors", headers=headers)
    factors_resp.raise_for_status()
    factors = factors_resp.json()

    # 7. Print Summary
    print(f"\n--- Summary ---")
    print(f"Run ID: {run_id}")
    print(f"Final Status: {last_status}")
    print(f"Factor Count: {len(factors)}")
    
    any_zero = False
    for f in factors:
        name = f.get('name')
        ic_mean = f.get('ic_mean', 0)
        icir = f.get('icir', 0)
        sharpe = f.get('sharpe', 0)
        expression = f.get('expression', 'N/A')
        print(f"Factor: {name} | IC: {ic_mean:.4f} | ICIR: {icir:.4f} | Sharpe: {sharpe:.4f} | Expr: {expression}")
        if ic_mean == 0 and icir == 0 and sharpe == 0:
            any_zero = True
            
    print(f"Any factors with all zero metrics: {any_zero}")

if __name__ == "__main__":
    test_flow()
