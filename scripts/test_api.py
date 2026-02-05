#!/usr/bin/env python3
"""Test TraderMate API endpoints."""
import requests
import json

BASE_URL = "http://localhost:8000"

def test_endpoint(url, method="GET", data=None):
    """Test an API endpoint."""
    try:
        if method == "GET":
            response = requests.get(url, timeout=5)
        elif method == "POST":
            response = requests.post(url, json=data, timeout=5)
        
        print(f"\n{'='*60}")
        print(f"{method} {url}")
        print(f"Status: {response.status_code}")
        print(f"Response:")
        print(json.dumps(response.json(), indent=2))
        return response
    except Exception as e:
        print(f"\nERROR testing {url}: {e}")
        return None

def main():
    """Test main API endpoints."""
    print("TraderMate API Test Suite")
    print("="*60)
    
    # Test root endpoint
    test_endpoint(f"{BASE_URL}/")
    
    # Test health check
    test_endpoint(f"{BASE_URL}/health")
    
    # Test API info
    test_endpoint(f"{BASE_URL}/api")
    
    print(f"\n{'='*60}")
    print("API is running successfully!")
    print(f"Interactive docs: {BASE_URL}/docs")
    print(f"ReDoc: {BASE_URL}/redoc")

if __name__ == "__main__":
    main()
