def run(params, payload):
    # very small example: ensure a required key exists in payload
    required = params.get("required", False)
    if required:
        if not isinstance(payload, dict):
            return {"ok": False, "reason": "payload must be JSON object"}
        if "important" not in payload:
            return {"ok": False, "reason": "missing 'important' key"}
    return {"ok": True, "msg": "validated"}

