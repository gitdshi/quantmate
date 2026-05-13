import requests
import time
import sys

BASE_URL = "https://test.quantmate.net/api/v1"

def test():
    # 1. Login
    login_resp = requests.post(f"{BASE_URL}/auth/login", json={"username": "admin", "password": "admin123"})
    if login_resp.status_code != 200:
        print(f"Login failed {login_resp.status_code}: {login_resp.text}")
        return
    
    auth_data = login_resp.json()
    token = auth_data.get("access_token")
    headers = {"Authorization": f"Bearer {token}"}
    print("Login successful")

    # 3. Create run
    run_payload = {
        "scenario": "fin_factor",
        "max_iterations": 1,
        "llm_model": "minimax-m2.5-free",
        "universe": "csi300",
        "feature_columns": [],
        "start_date": "2024-01-01",
        "end_date": "2024-03-31"
    }
    create_resp = requests.post(f"{BASE_URL}/rdagent/runs", json=run_payload, headers=headers)
    if create_resp.status_code not in [200, 201]:
        print(f"Create failed {create_resp.status_code}: {create_resp.text}")
        return
    
    run_data = create_resp.json()
    run_id = run_data.get("id") or run_data.get("run_id")
    print(f"Created run_id: {run_id}")

    # 5. Polling
    start_time = time.time()
    last_status = None
    timeout = 1200 # 20 minutes
    
    while time.time() - start_time < timeout:
        get_resp = requests.get(f"{BASE_URL}/rdagent/runs/{run_id}", headers=headers)
        if get_resp.status_code != 200:
            print(f"Polling error {get_resp.status_code}: {get_resp.text}")
            break
        
        status_data = get_resp.json()
        current_status = status_data.get("status")
        iter_count = status_data.get("current_iteration", 0)
        
        if current_status != last_status:
            print(f"Status transition: {last_status} -> {current_status} (Iteration: {iter_count})")
            last_status = current_status
            
        if current_status in ["completed", "failed", "cancelled"]:
            break
            
        time.sleep(10)
    else:
        print("Polling timed out after 20 minutes.")

    # 6. Fetch outcome
    factors_resp = requests.get(f"{BASE_URL}/rdagent/runs/{run_id}/factors", headers=headers)
    iters_resp = requests.get(f"{BASE_URL}/rdagent/runs/{run_id}/iterations", headers=headers)
    
    factors = factors_resp.json() if factors_resp.status_code == 200 else []
    iterations = iters_resp.json() if iters_resp.status_code == 200 else []
    
    # 7. Summary
    print("\n--- Summary ---")
    print(f"Final Run Status: {last_status}")
    print(f"Iteration Count: {len(iterations)}")
    print(f"Factor Count: {len(factors)}")
    
    any_nonzero = False
    for f in factors:
        name = f.get("name")
        ic_mean = float(f.get("ic_mean", 0) or 0)
        icir = float(f.get("icir", 0) or 0)
        sharpe = float(f.get("sharpe", 0) or 0)
        expr = f.get("expression")
        
        print(f"Factor: {name} | IC: {ic_mean:.4f} | ICIR: {icir:.4f} | Sharpe: {sharpe:.4f} | Expr: {expr}")
        
        if any(abs(val) > 1e-9 for val in [ic_mean, icir, sharpe]):
            any_nonzero = True
            
    print(f"All factors zero metrics: {not any_nonzero}")
    print(f"At least one factor non-zero: {any_nonzero}")

if __name__ == '__main__':
    test()
