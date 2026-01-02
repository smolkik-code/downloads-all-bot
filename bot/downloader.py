import yt_dlp
import threading
import os
import logging

logger = logging.getLogger(__name__)


class DownloadCancelled(Exception):
    pass


def _progress_hook(cancel_event, progress_cb):
    def hook(d):
        if cancel_event.is_set():
            raise DownloadCancelled("Cancelled by user")

        if d["status"] == "downloading":
            progress_cb(d)

    return hook


# ---------------- VIDEO ----------------

def download_video(
    url: str,
    quality: str,
    out_path: str,
    cookies: str | None,
    cancel_event: threading.Event,
    progress_cb,
):
    ydl_opts = {
        "format": quality,
        "outtmpl": out_path,
        "merge_output_format": "mp4",
        "cookiefile": cookies,
        "progress_hooks": [_progress_hook(cancel_event, progress_cb)],
        "quiet": True,
        "no_warnings": True,
        # Настройки для TikTok
        "extractor_args": {
            "tiktok": {
                "skip_impersonation": True  # Пропускаем имитацию если зависимости не установлены
            }
        },
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


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
        # Настройки для TikTok
        "extractor_args": {
            "tiktok": {
                "skip_impersonation": True
            }
        },
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])