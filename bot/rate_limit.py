import time
from collections import defaultdict

_last_request = defaultdict(float)

def check_rate_limit(user_id, limit):
    now = time.time()
    if now - _last_request[user_id] < limit:
        return False
    _last_request[user_id] = now
    return True