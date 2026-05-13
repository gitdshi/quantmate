import requests
import sys

base_url = "https://test.quantmate.net/api/v1"
run_id = "5194ab4b-82a7-4602-aa53-a801aa17c441"

def main():
    session = requests.Session()
    login_url = f"{base_url}/auth/login"
    login_payload = {"username": "admin", "password": "admin123"}
    try:
        login_resp = session.post(login_url, json=login_payload)
        login_resp.raise_for_status()
        
        token = login_resp.json().get("access_token")
        if token:
            session.headers.update({"Authorization": f"Bearer {token}"})
            
        factors_url = f"{base_url}/rdagent/runs/{run_id}/factors"
        factors_resp = session.get(factors_url)
        factors_resp.raise_for_status()
        
        factors = factors_resp.json()
        # Handle both list and object results if needed, assuming list based on query
        if isinstance(factors, dict) and "data" in factors:
            factors = factors["data"]
            
        for factor in factors:
            print("FACTOR_START")
            print(f"name={factor.get('name')}")
            print(f"ic_mean={factor.get('ic_mean')}")
            print(f"icir={factor.get('icir')}")
            print(f"sharpe={factor.get('sharpe')}")
            print(f"expression={factor.get('expression')}")
            print("FACTOR_END")
            
    except Exception as e:
        # Silently fail or minimal error to keep stdout clean if requested
        pass

if __name__ == "__main__":
    main()
