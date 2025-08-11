# client.py
import os
import requests
import datetime
import time
import sys

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

def register_webhook_and_call():
    print("Registering webhook trigger (with flow_id)...")
    payload = {
        "flow_id": "test-flow-1",   # <-- NEW: supply your FLOW_ID here
        "trigger": {"type": "webhook"},
        "nodes": [
            {"id": "n1", "logic": "validate_input", "params": {"required": True}},
            {"id": "n2", "logic": "do_something", "params": {"foo": "bar"}}
        ]
    }
    resp = requests.post(f"{BASE}/triggers", json=payload)
    print("Register response status:", resp.status_code)
    try:
        data = resp.json()
    except Exception:
        print("failed to decode JSON:", resp.text)
        return

    print("Register response JSON:", data)
    webhook_url = data.get("webhook_url")
    if not webhook_url:
        print("No webhook_url returned.")
        return

    if webhook_url.startswith("/"):
        webhook_full = BASE + webhook_url
    else:
        webhook_full = webhook_url

    print("Calling webhook URL:", webhook_full)
    call_payload = {"hello": "world", "ts": datetime.datetime.utcnow().isoformat()}
    call_resp = requests.post(webhook_full, json=call_payload)
    print("Webhook call status:", call_resp.status_code)
    try:
        print("Webhook call response:", call_resp.json())
    except Exception:
        print("Webhook call response (text):", call_resp.text)


def register_time_once_trigger(seconds_from_now=30):
    at_time = (datetime.datetime.utcnow() + datetime.timedelta(seconds=seconds_from_now)).replace(microsecond=0).isoformat() + "Z"
    print(f"\nRegistering time trigger (once) to run in ~{seconds_from_now} seconds at {at_time} ...")
    payload = {
        "flow_id": "test-flow-1",   # include same flow_id for scheduled runs
        "trigger": {
            "type": "time",
            "schedule": {"mode": "once", "at": at_time}
        },
        "nodes": [
            {"id": "n1", "logic": "hourly_report", "params": {}}
        ]
    }
    resp = requests.post(f"{BASE}/triggers", json=payload)
    print("Register response status:", resp.status_code)
    try:
        print("Register response JSON:", resp.json())
    except Exception:
        print("Register response text:", resp.text)


if __name__ == "__main__":
    print("=== Test client for Trigger Pipeline API ===")
    if not wait_for(f"{BASE}/openapi.json", timeout=30, interval=1):
        print("Web API didn't come up in time. Exiting.")
        sys.exit(1)

    register_webhook_and_call()
    register_time_once_trigger(30)
    print("\nDone.")

