def run(prev, params, payload):
    # prev may be a dict or anything returned by previous node
    return {"action": "did_something", "prev_type": type(prev).__name__, "params": params, "payload_keys": list(payload.keys()) if isinstance(payload, dict) else None}

