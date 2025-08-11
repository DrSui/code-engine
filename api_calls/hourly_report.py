def run(prev, params, payload):
    import datetime
    return {"report_time": datetime.datetime.utcnow().isoformat() + "Z", "note": "sample hourly report", "prev": prev}

