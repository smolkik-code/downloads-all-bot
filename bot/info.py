import yt_dlp
import re


def extract_info(url: str, cookies_file: str = None) -> dict:
    """Извлекает информацию о видео"""
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "cookiefile": cookies_file,
        "extractor_args": {
            "tiktok": {"skip_impersonation": True},
            "instagram": {"skip_impersonation": True},
        },
    }
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            return ydl.extract_info(url, download=False)
        except:
            return None


def is_playlist(url: str) -> bool:
    """Проверяет, является ли ссылка плейлистом"""
    # Паттерны для плейлистов
    playlist_patterns = [
        r"youtube\.com/playlist",  # YouTube плейлист
        r"youtube\.com/watch.*list=",  # YouTube видео из плейлиста
        r"youtu\.be/.*list=",  # Сокращенная ссылка YouTube
    ]
    
    for pattern in playlist_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    
    # Используем yt-dlp для более точной проверки
    try:
        ydl_opts = {"quiet": True, "no_warnings": True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False, process=False)
            return info.get('_type') == 'playlist'
    except:
        return False


def get_platform_info(url: str) -> str:
    """Определяет платформу по ссылке"""
    patterns = {
        "youtube": r"(youtube\.com|youtu\.be)",
        "tiktok": r"tiktok\.com",
        "instagram": r"(instagram\.com|instagr\.am)",
        "twitter": r"(twitter\.com|x\.com)",
        "facebook": r"facebook\.com",
        "vk": r"vk\.com",
        "rutube": r"rutube\.ru",
        "vimeo": r"vimeo\.com",
        "twitch": r"twitch\.tv",
        "dailymotion": r"dailymotion\.com",
    }
    
    for platform, pattern in patterns.items():
        if re.search(pattern, url, re.IGNORECASE):
            return platform
    
    return "other"


def get_playlist_info(url: str) -> dict:
    """Получает информацию о плейлисте"""
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
        "playlistend": 50,  # Ограничиваем количество видео (можно изменить)
    }
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            return ydl.extract_info(url, download=False)
        except:
            return None


def get_first_video_from_playlist(url: str) -> str:
    """Получает первую ссылку на видео из плейлиста"""
    playlist_info = get_playlist_info(url)
    
    if playlist_info and playlist_info.get('entries'):
        first_video = playlist_info['entries'][0]
        return first_video.get('url')
    
    return None