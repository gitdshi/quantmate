import requests

BASE_URL = "https://test.quantmate.net/api/v1"
RUN_ID = "3423c8d8-3b92-4e48-b57f-e948a7403c45"

def main():
    resp = requests.post(f"{BASE_URL}/auth/login", json={"username": "admin", "password": "admin123"})
    if resp.status_code != 200:
        print(f"Login failed: {resp.status_code}")
        return
    token = resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    factors_resp = requests.get(f"{BASE_URL}/rdagent/runs/{RUN_ID}/factors", headers=headers)
    if factors_resp.status_code != 200:
        print(f"Failed to fetch factors: {factors_resp.status_code}")
        return
    
    factors = factors_resp.json()
    count = 0
    for f in factors:
        ic_mean = f.get("ic_mean", 0)
        icir = f.get("icir", 0)
        sharpe = f.get("sharpe", 0)
        # Check if all are zero (handling None as well)
        if (ic_mean == 0 or ic_mean is None) and \
           (icir == 0 or icir is None) and \
           (sharpe == 0 or sharpe is None):
            print(f"Name: {f.get('name')}")
            print(f"Expression: {f.get('expression')}")
            print("-" * 20)
            count += 1
    
    if count == 0:
        print("No factors found with all metrics zero.")

if __name__ == "__main__":
    main()
