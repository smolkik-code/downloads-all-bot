import os
from dotenv import load_dotenv

load_dotenv()

# Telegram
BOT_TOKEN = os.getenv("BOT_TOKEN")
LOCAL_API_URL = os.getenv("LOCAL_API_URL")

# Paths
DOWNLOAD_DIR = "/downloads"
CACHE_DIR = "/downloads/cache"
TMP_DIR = "/downloads/tmp"

# yt-dlp
COOKIES_FILE = os.getenv("COOKIES_FILE")

# Limits
MAX_DURATION_SECONDS = int(os.getenv("MAX_DURATION_SECONDS", 1800))
RATE_LIMIT_SECONDS = int(os.getenv("RATE_LIMIT_SECONDS", 20))
MAX_FILE_SIZE_MB = 2000
CACHE_MAX_AGE_DAYS = 7  # Удалять файлы старше 7 дней
CACHE_MAX_SIZE_MB = 4096  # Максимальный размер кэша 1 ГБ (0 = без ограничения)
# Private mode
ALLOWED_USERS = set(
    int(uid.strip())
    for uid in os.getenv("ALLOWED_USERS", "").split(",")
    if uid.strip()
)

# Ensure directories exist
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)
