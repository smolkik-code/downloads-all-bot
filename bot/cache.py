import hashlib
import os

def cache_key(url, quality, audio=False):
    raw = f"{url}:{quality}:{audio}"
    return hashlib.sha256(raw.encode()).hexdigest()

def cache_path(cache_dir, key, ext):
    return os.path.join(cache_dir, f"{key}.{ext}")