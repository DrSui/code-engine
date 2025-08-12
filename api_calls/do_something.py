import requests

def run(prev, params, payload):
    # prev may be a dict or anything returned by previous node
    webhook_url = 'https://webhook.site/b380a2ea-f52e-4559-9356-ea7e7ace548e'

# Data to send in the POST request body (can be any JSON content)
    data = {
        'message': 'Hello, webhook.site!',
        'event': 'test_ping'
    }

# Send the POST request with JSON data
    response = requests.post(webhook_url, json=data)

# Print status to confirm if the request was successful
    print(f'Status code: {response.status_code}')
    if response.status_code == 200:
        print('Ping successful!')
    else:
        print('Ping failed.')
    return {"action": "did_something", "prev_type": type(prev).__name__, "params": params, "payload_keys": list(payload.keys()) if isinstance(payload, dict) else None}
