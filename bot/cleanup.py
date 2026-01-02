import os
import time

def cleanup_tmp(path, max_age=3600):
    now = time.time()
    for f in os.listdir(path):
        p = os.path.join(path, f)
        if os.path.isfile(p) and now - os.path.getmtime(p) > max_age:
            os.remove(p)
