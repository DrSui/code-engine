# client.py (only showing the new test parts — replace or merge into your current client)
import os
import requests
import datetime
import time
import sys
import json

BASE = os.environ.get("BASE_URL", "http://localhost:8000")

def wait_for(url, timeout=30, interval=1):
    print(f"Waiting for {url} (timeout {timeout}s) ...")
    start = time.time()
    while True:
        try:
            r = requests.get(url, timeout=2)
            if r.status_code < 500:
                print(f"Service available (status {r.status_code})")
                return True
        except Exception:
            pass
        if time.time() - start > timeout:
            print("Timeout waiting for service.")
            return False
        time.sleep(interval)

def test_chain_nodes_webhook():
    """
    Registers a webhook flow with nodes:
      1) double_value  (doubles incoming payload.value)
      2) add_five      (adds 5 by default)
    Calls the webhook with {"value": 3} and explains the expected final result (11).
    Watch worker logs to confirm actual execution and results.
    """
    print("\n=== Test: chained nodes via webhook (double_value -> add_five) ===")
    nodes = [
        {"id": "c1", "logic": "double_value", "params": {}},
        {"id": "c2", "logic": "add_five", "params": {"add": 5}}
    ]
    body = {
        "flow_id": "chain-test",
        "trigger": {"type": "webhook"},
        "nodes": nodes
    }

    r = requests.post(f"{BASE}/triggers", json=body)
    print("Register response:", r.status_code, r.text)
    data = r.json()
    webhook_path = data.get("webhook_url")
    if webhook_path.startswith("/"):
        webhook_full = BASE + webhook_path
    else:
        webhook_full = webhook_path

    payload = {"value": 3}
    print("Calling webhook:", webhook_full, "with payload:", payload)
    r2 = requests.post(webhook_full, json=payload)
    print("Webhook call status:", r2.status_code, r2.text)

    # Wait a short time for background worker to process and then ask you to check worker logs.
    print("Waiting 3-6s for worker to execute nodes (check trigger_worker logs)...")
    time.sleep(6)
    print("Expected chained result (for payload.value=3):")
    print("  double_value -> 6   then add_five(+5) -> 11")
    print("Check trigger_worker logs (or Redis if you've stored results) to confirm the actual outputs.")

if __name__ == "__main__":
    print("=== Test client ===")
    if not wait_for(f"{BASE}/openapi.json", timeout=30, interval=1):
        print("Web API didn't come up in time. Exiting.")
        sys.exit(1)

    # existing tests (if any)...
    # now run the chain test:
    test_chain_nodes_webhook()
    print("\nClient done.")

