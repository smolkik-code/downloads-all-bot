import hashlib


def cache_key(url: str, quality: str, audio: bool = False) -> str:
    """Генерирует ключ кэша на основе URL, качества и типа"""
    key_str = f"{url}_{quality}_{audio}"
    return hashlib.sha256(key_str.encode()).hexdigest()


def cache_path(cache_dir: str, key: str, extension: str) -> str:
    """Создает путь к файлу в кэше"""
    # Создаем вложенную структуру для лучшей организации
    subdir = key[:2]
    import os
    full_dir = os.path.join(cache_dir, subdir)
    os.makedirs(full_dir, exist_ok=True)
    return os.path.join(full_dir, f"{key}.{extension}")