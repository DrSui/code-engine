# add_five.py
def run(prev, params, payload):
    """
    Expect prev to be a dict with "value". Adds params.get("add", 5) to it.
    Returns {"value": new_value, "added": add_amount}.
    """
    if not isinstance(prev, dict) or "value" not in prev:
        return {"error": "prev must be dict containing 'value'"}

    try:
        base = float(prev["value"])
    except Exception as e:
        return {"error": f"invalid prev value: {e}"}

    add_amount = params.get("add", 5)
    try:
        add_num = float(add_amount)
    except Exception:
        # if param is not numeric, fallback to 5
        add_num = 5.0

    new_val = base + add_num
    # keep integer-like types integer
    if (isinstance(base, int) or isinstance(add_amount, int)) and float(new_val).is_integer():
        new_val = int(new_val)

    return {"value": new_val, "added": add_num, "prev":prev}

