# client.py
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

def test_webhook_register_and_call():
    print("\n--- Test: register webhook (nodes provided only once) ---")
    registration = {
        "flow_id": "test-flow-unique",
        "trigger": {"type": "webhook"},
        "nodes": [
            {"id": "n1", "logic": "do_something", "params": {"foo": "bar"}},
            {"id": "n2", "type": "custom", "logic": "custom", "params": {"_type": "custom", "code": "import json\nprint(json.dumps({'got_prev': prev, 'params': params}))"}}
        ]
    }
    r = requests.post(f"{BASE}/triggers", json=registration)
    print("register:", r.status_code, r.text)
    data = r.json()
    webhook_path = data.get("webhook_url")
    if webhook_path.startswith("/"):
        webhook_full = BASE + webhook_path
    else:
        webhook_full = webhook_path

    # valid payload (NO nodes included)
    print("\n-> Calling webhook with valid payload (no nodes)...")
    payload = {"event": "order.created", "order_id": 123}
    r2 = requests.post(webhook_full, json=payload)
    print("webhook call status:", r2.status_code, r2.text)

    # invalid payload (includes nodes) - should be rejected
    print("\n-> Calling webhook with INVALID payload (contains 'nodes') - expect 400...")
    bad_payload = {"event": "malformed", "nodes": [{"id":"bad"}]}
    r3 = requests.post(webhook_full, json=bad_payload)
    print("invalid webhook call status:", r3.status_code, r3.text)

def test_time_trigger_once():
    print("\n--- Test: register time trigger (once) ---")
    at_time = (datetime.datetime.utcnow() + datetime.timedelta(seconds=15)).replace(microsecond=0).isoformat() + "Z"
    body = {
        "flow_id": "test-flow-time",
        "trigger": {"type": "time", "schedule": {"mode": "once", "at": at_time}},
        "nodes": [
            {"id": "t1", "logic": "hourly_report", "params": {}},
            {"id": "t2", "type": "custom", "logic": "custom", "params": {"_type": "custom", "code": "import json\nprint(json.dumps({'prev': prev}))"}}
        ]
    }
    r = requests.post(f"{BASE}/triggers", json=body)
    print("register time-once:", r.status_code, r.text)

if __name__ == "__main__":
    print("=== Test client ===")
    if not wait_for(f"{BASE}/openapi.json", timeout=30, interval=1):
        print("Web API didn't come up in time. Exiting.")
        sys.exit(1)

    test_webhook_register_and_call()
    test_time_trigger_once()
    print("\nClient done. Watch worker/executor logs for pipeline runs.")

