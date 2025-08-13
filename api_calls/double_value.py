# double_value.py
def run(prev, params, payload):
    """
    Expect prev or payload to be a dict containing an integer/float under key "value".
    Returns {"value": doubled_value, "note": "doubled"}.
    """
    source = None
    if isinstance(prev, dict) and "value" in prev:
        source = prev
    elif isinstance(payload, dict) and "value" in payload:
        source = payload

    if not source:
        return {"error": "no 'value' found in prev or payload"}

    try:
        v = source["value"]
        v_num = float(v)
        doubled = v_num * 2
        # If it was integer-like, return int
        if isinstance(v, int) or (isinstance(v, float) and v.is_integer()):
            doubled = int(doubled)
        return {"value": doubled, "note": "doubled", "prev":prev}
    except Exception as e:
        return {"error": f"invalid numeric value: {e}"}

