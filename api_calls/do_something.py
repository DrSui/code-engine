def run(params, payload):
    # pretend to do something: echo params + payload
    return {
        "action": "did_something",
        "params": params,
        "payload_keys": list(payload.keys()) if isinstance(payload, dict) else None
    }

