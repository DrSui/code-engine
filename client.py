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


def test_pipeline_webhook():
    print("\n=== Test: webhook pipeline with pass-through and custom node ===")
    # Pipeline:
    # 1) file-based node "do_something" -> should produce some output
    # 2) custom node -> receives prev output (first param) and returns JSON
    # 3) file-based node "do_something" with NO PASS THROUGH -> should NOT receive prev

    # Build nodes
    nodes = [
        {"id": "n1", "logic": "do_something", "params": {"foo": "bar"}},
        {
            "id": "n2",
            "type": "custom",
            "logic": "custom",
            "params": {
                "_type": "custom",
                "code": (
                    "import json\n"
                    "# 'prev', 'params', and 'payload' variables are available\n"
                    "out = {'received_prev': prev, 'params': params}\n"
                    "print(json.dumps(out))\n"
                )
            }
        },
        {
            "id": "n3",
            "logic": "do_something",
            "params": {"extra": "value"},
            # disable pass-through for this node (should not get prev from n2)
            "NO PASS THROUGH": True
        }
    ]

    payload = {"initial": "payload", "x": 1}
    body = {
        "flow_id": "test-flow-runner",
        "trigger": {"type": "webhook"},
        "nodes": nodes
    }

    r = requests.post(f"{BASE}/triggers", json=body)
    print("register webhook response:", r.status_code, r.text)
    data = r.json()
    webhook_url = data.get("webhook_url")
    if webhook_url.startswith("/"):
        webhook_full = BASE + webhook_url
    else:
        webhook_full = webhook_url

    # call webhook with payload (this will schedule celery job)
    r2 = requests.post(webhook_full, json=payload)
    print("webhook call status:", r2.status_code, r2.text)


def test_pipeline_time_once():
    print("\n=== Test: time trigger once (no prev pass-through across runs) ===")
    at_time = (datetime.datetime.utcnow() + datetime.timedelta(seconds=10)).replace(microsecond=0).isoformat() + "Z"
    nodes = [
        {"id": "tn1", "logic": "hourly_report", "params": {}},
        {
            "id": "tn2",
            "type": "custom",
            "logic": "custom",
            "params": {
                "_type": "custom",
                "code": (
                    "import json\n"
                    "print(json.dumps({'prev_echo': prev}))\n"
                )
            }
        }
    ]
    body = {
        "flow_id": "test-flow-time",
        "trigger": {"type": "time", "schedule": {"mode": "once", "at": at_time}},
        "nodes": nodes
    }
    r = requests.post(f"{BASE}/triggers", json=body)
    print("register time-once response:", r.status_code, r.text)


if __name__ == "__main__":
    print("=== Test client for Trigger Pipeline API ===")
    if not wait_for(f"{BASE}/openapi.json", timeout=30, interval=1):
        print("Web API didn't come up in time. Exiting.")
        sys.exit(1)

    test_pipeline_webhook()
    test_pipeline_time_once()
    print("\nClient done. Watch worker and executor logs for execution details.")

