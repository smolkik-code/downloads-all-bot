import yt_dlp
import threading
import os
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


class DownloadCancelled(Exception):
    pass


def _progress_hook(cancel_event, progress_cb):
    def hook(d):
        if cancel_event.is_set():
            raise DownloadCancelled("Cancelled by user")

        if d["status"] == "downloading" and progress_cb:
            progress_cb(d)
        elif d["status"] == "finished" and progress_cb:
            progress_cb(d)

    return hook


# ---------------- STANDARD VIDEO ----------------

def download_video(
    url: str,
    quality: str,
    out_path: str,
    cookies: str | None,
    cancel_event: threading.Event,
    progress_cb,
):
    ydl_opts = {
        "format": f"best[height<={quality}]/best",
        "outtmpl": out_path,
        "merge_output_format": "mp4",
        "cookiefile": cookies,
        "progress_hooks": [_progress_hook(cancel_event, progress_cb)],
        "quiet": True,
        "no_warnings": True,
        "extractor_args": {
            "tiktok": {"skip_impersonation": True},
            "instagram": {"skip_impersonation": True},
        },
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


# ---------------- ORIGINAL QUALITY (Instagram/TikTok) ----------------

def download_original_quality(
    url: str,
    out_path: str,
    cookies: str | None,
    cancel_event: threading.Event,
    progress_cb,
):
    """Скачивает видео в оригинальном качестве (без перекодировки)"""
    ydl_opts = {
        "format": "best",  # Лучшее качество доступное
        "outtmpl": out_path,
        "merge_output_format": "mp4",
        "cookiefile": cookies,
        "progress_hooks": [_progress_hook(cancel_event, progress_cb)],
        "quiet": True,
        "no_warnings": True,
        "extractor_args": {
            "tiktok": {"skip_impersonation": True},
            "instagram": {"skip_impersonation": True},
        },
        # Для Instagram и TikTok используем специфичные форматы
        "format_sort": ["quality", "res", "codec", "size"],
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


# ---------------- PLAYLIST DOWNLOAD ----------------

def download_playlist_videos(
    playlist_info: dict,
    output_dir: str,
    cookies: str | None,
    cancel_event: threading.Event,
    progress_cb,
) -> List[str]:
    """Скачивает все видео из плейлиста"""
    downloaded_files = []
    
    ydl_opts = {
        "format": "best[height<=1080]/best",
        "outtmpl": os.path.join(output_dir, "%(title)s.%(ext)s"),
        "merge_output_format": "mp4",
        "cookiefile": cookies,
        "progress_hooks": [_progress_hook(cancel_event, progress_cb)],
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,  # Игнорируем ошибки при загрузке отдельных видео
        "extract_flat": False,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Скачиваем каждое видео по отдельности
            for entry in playlist_info.get('entries', []):
                if cancel_event.is_set():
                    raise DownloadCancelled("Cancelled by user")
                
                if not entry.get('url'):
                    continue
                
                try:
                    ydl.download([entry['url']])
                    
                    # Ищем скачанный файл
                    for filename in os.listdir(output_dir):
                        if filename.endswith(('.mp4', '.mkv', '.webm')):
                            file_path = os.path.join(output_dir, filename)
                            downloaded_files.append(file_path)
                            break
                            
                except Exception as e:
                    logger.error(f"Error downloading video {entry.get('title')}: {e}")
                    continue
        
        return downloaded_files
        
    except Exception as e:
        logger.error(f"Error downloading playlist: {e}")
        raise


# ---------------- AUDIO FROM VIDEO ----------------

def download_audio(
    url: str,
    out_path: str,
    cookies: str | None,
    cancel_event: threading.Event,
    progress_cb,
):
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": out_path.replace('.mp3', ''),
        "cookiefile": cookies,
        "progress_hooks": [_progress_hook(cancel_event, progress_cb)],
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "320",
            },
            {"key": "FFmpegMetadata"},
            {"key": "EmbedThumbnail"},
        ],
        "writethumbnail": True,
        "quiet": True,
        "no_warnings": True,
        "extractor_args": {
            "tiktok": {"skip_impersonation": True},
            "instagram": {"skip_impersonation": True},
        },
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])