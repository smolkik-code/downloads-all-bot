import yt_dlp
import threading
import os
import logging
import subprocess
import json
import shutil
from typing import List, Optional

logger = logging.getLogger(__name__)


class DownloadCancelled(Exception):
    pass


def _progress_hook(cancel_event, progress_cb):
    def hook(d):
        if cancel_event.is_set():
            raise DownloadCancelled("Cancelled by user")

        if d["status"] in ["downloading", "finished"] and progress_cb:
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
        "concurrent_fragment_downloads": 4,
        "http_chunk_size": 10485760,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


# ---------------- ORIGINAL QUALITY ----------------

def download_original_quality(
    url: str,
    out_path: str,
    cookies: str | None,
    cancel_event: threading.Event,
    progress_cb,
):
    """Скачивает видео в оригинальном качестве"""
    ydl_opts = {
        "format": "best",
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
        "format_sort": ["quality", "res", "codec", "size"],
        "concurrent_fragment_downloads": 4,
        "http_chunk_size": 10485760,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


# ---------------- TIKTOK MUSIC ONLY ----------------

def download_tiktok_music(
    url: str,
    out_path: str,
    cookies: str | None,
    cancel_event: threading.Event,
    progress_cb,
):
    """Скачивает только звук из TikTok"""
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": out_path.replace('.mp3', ''),
        "cookiefile": cookies,
        "progress_hooks": [_progress_hook(cancel_event, progress_cb)],
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            },
        ],
        "quiet": True,
        "no_warnings": True,
        "extractor_args": {
            "tiktok": {"skip_impersonation": True},
        },
        "concurrent_fragment_downloads": 4,
        "http_chunk_size": 10485760,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


# ---------------- ADD METADATA TO AUDIO ----------------

def add_metadata_to_audio(
    input_path: str,
    output_path: str,
    metadata: dict,
):
    """Добавляет метаданные к аудио файлу"""
    try:
        # Проверяем metadata
        if metadata is None:
            metadata = {}
            
        if not input_path.lower().endswith('.mp3'):
            shutil.copy2(input_path, output_path)
            return
        
        metadata_args = []
        
        # Добавляем метаданные
        if metadata.get('title'):
            metadata_args.extend(['-metadata', f'title={metadata["title"][:100]}'])
        
        if metadata.get('artist'):
            metadata_args.extend(['-metadata', f'artist={metadata["artist"][:100]}'])
        
        if metadata.get('album'):
            metadata_args.extend(['-metadata', f'album={metadata["album"][:100]}'])
        
        cmd = [
            'ffmpeg',
            '-i', input_path,
            '-c', 'copy',
            '-id3v2_version', '3',
            '-loglevel', 'error',
            '-y',
            *metadata_args,
            output_path
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            shutil.copy2(input_path, output_path)
            
    except Exception as e:
        logger.error(f"Metadata error: {e}")
        shutil.copy2(input_path, output_path)


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
        "outtmpl": os.path.join(output_dir, "%(title)s [%(id)s].%(ext)s"),
        "merge_output_format": "mp4",
        "cookiefile": cookies,
        "progress_hooks": [_progress_hook(cancel_event, progress_cb)],
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
        "extract_flat": False,
        "nooverwrites": True,
        "concurrent_fragment_downloads": 4,
        "http_chunk_size": 10485760,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            entries = playlist_info.get('entries', [])
            total_videos = len(entries)
            
            logger.info(f"Starting playlist download with {total_videos} videos")
            
            for i, entry in enumerate(entries, 1):
                if cancel_event.is_set():
                    raise DownloadCancelled("Cancelled by user")
                
                if not entry.get('url'):
                    logger.warning(f"Entry {i} has no URL, skipping")
                    continue
                
                video_url = entry['url']
                video_title = entry.get('title', f'Video {i}')
                
                logger.info(f"Downloading video {i}/{total_videos}: {video_title}")
                
                try:
                    # Загружаем видео
                    info = ydl.extract_info(video_url, download=True)
                    
                    if not info:
                        logger.error(f"Failed to extract info for video {i}")
                        continue
                    
                    # Получаем имя фактически созданного файла
                    filename = ydl.prepare_filename(info)
                    
                    # Проверяем существование файла
                    if os.path.exists(filename):
                        downloaded_files.append(filename)
                        logger.info(f"Successfully downloaded: {os.path.basename(filename)}")
                    else:
                        # Ищем файл с другим расширением
                        base_name = filename.rsplit('.', 1)[0]
                        for ext in ['.mp4', '.mkv', '.webm', '.flv']:
                            alt_path = base_name + ext
                            if os.path.exists(alt_path):
                                downloaded_files.append(alt_path)
                                logger.info(f"Found file with extension {ext}: {os.path.basename(alt_path)}")
                                break
                        else:
                            logger.error(f"File not found for video {i}")
                            
                except Exception as e:
                    logger.error(f"Error downloading video {i} ({video_title}): {e}")
                    continue
        
        logger.info(f"Playlist download complete. Downloaded {len(downloaded_files)} files")
        return downloaded_files
        
    except Exception as e:
        logger.error(f"Error downloading playlist: {e}")
        raise


# ---------------- STANDARD AUDIO ----------------

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
                "preferredquality": "192",
            },
        ],
        "quiet": True,
        "no_warnings": True,
        "extractor_args": {
            "tiktok": {"skip_impersonation": True},
            "instagram": {"skip_impersonation": True},
        },
        "concurrent_fragment_downloads": 4,
        "http_chunk_size": 10485760,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])