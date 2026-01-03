import requests
import time
import uuid

event = {
    "event_id": 1,
    "type": "manual_python_test",
    "payload": {"msg": "hello from python"},
    "sent_at": 1
}

r = requests.post("http://localhost:8000/event", json=event)
print(r.status_code, r.text)
