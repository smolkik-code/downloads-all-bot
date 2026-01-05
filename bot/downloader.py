import yt_dlp
import threading
import os
import logging
import subprocess
import json
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
        "writedescription": True,  # Сохраняем описание
        "writeinfojson": True,  # Сохраняем информацию в JSON
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        
        # Сохраняем информацию о видео рядом с файлом
        info_file = out_path.replace('.mp4', '.info.json')
        if os.path.exists(info_file):
            with open(info_file, 'w', encoding='utf-8') as f:
                json.dump(info, f, ensure_ascii=False, indent=2)


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
        "format_sort": ["quality", "res", "codec", "size"],
        "writedescription": True,
        "writeinfojson": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        
        # Сохраняем информацию о видео
        info_file = out_path.replace('.mp4', '.info.json')
        if os.path.exists(info_file):
            with open(info_file, 'w', encoding='utf-8') as f:
                json.dump(info, f, ensure_ascii=False, indent=2)


# ---------------- TIKTOK MUSIC ONLY ----------------

def download_tiktok_music(
    url: str,
    out_path: str,
    cookies: str | None,
    cancel_event: threading.Event,
    progress_cb,
):
    """Скачивает только звук из TikTok (оригинальную музыку)"""
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
        ],
        "quiet": True,
        "no_warnings": True,
        "extractor_args": {
            "tiktok": {
                "skip_impersonation": True,
                "music": True,  # Специальный флаг для музыки TikTok
            },
        },
        "writethumbnail": True,
        "writeinfojson": True,
        # Для TikTok стараемся получить оригинальный звук
        "extract_flat": False,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        
        # Сохраняем информацию о звуке
        info_file = out_path.replace('.mp3', '.info.json')
        if os.path.exists(info_file):
            with open(info_file, 'w', encoding='utf-8') as f:
                json.dump(info, f, ensure_ascii=False, indent=2)


# ---------------- ADD METADATA TO VIDEO/AUDIO ----------------

def add_metadata_to_video(
    input_path: str,
    output_path: str,
    metadata: dict,
):
    """Добавляет метаданные к видео или аудио файлу"""
    try:
        # Определяем тип файла
        is_video = input_path.lower().endswith(('.mp4', '.mkv', '.avi', '.mov'))
        is_audio = input_path.lower().endswith(('.mp3', '.m4a', '.flac', '.wav'))
        
        if not (is_video or is_audio):
            # Если не поддерживаемый формат, просто копируем файл
            import shutil
            shutil.copy2(input_path, output_path)
            return
        
        # Подготавливаем аргументы метаданных для ffmpeg
        metadata_args = []
        
        if metadata.get('title'):
            metadata_args.extend(['-metadata', f'title={metadata["title"]}'])
        
        if metadata.get('artist') or metadata.get('uploader'):
            artist = metadata.get('artist') or metadata.get('uploader')
            metadata_args.extend(['-metadata', f'artist={artist}'])
        
        if metadata.get('description'):
            # Обрезаем описание если слишком длинное
            desc = metadata['description'][:500]
            metadata_args.extend(['-metadata', f'comment={desc}'])
        
        if metadata.get('url'):
            metadata_args.extend(['-metadata', f'copyright={metadata["url"]}'])
        
        # Для TikTok добавляем дополнительную информацию о музыке
        if metadata.get('track') and metadata.get('artist'):
            metadata_args.extend(['-metadata', f'album={metadata["track"]}'])
        
        if is_video:
            # Для видео файлов
            cmd = [
                'ffmpeg',
                '-i', input_path,
                '-c', 'copy',  # Копируем без перекодировки
                '-movflags', '+faststart',
                '-y',
                *metadata_args,
                output_path
            ]
        else:
            # Для аудио файлов
            cmd = [
                'ffmpeg',
                '-i', input_path,
                '-c', 'copy',
                '-id3v2_version', '3',
                '-y',
                *metadata_args,
                output_path
            ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode != 0:
            logger.error(f"FFmpeg metadata error: {result.stderr}")
            # Если не удалось добавить метаданные, копируем файл как есть
            import shutil
            shutil.copy2(input_path, output_path)
            
    except Exception as e:
        logger.error(f"Error adding metadata: {e}")
        import shutil
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
        "outtmpl": os.path.join(output_dir, "%(title)s.%(ext)s"),
        "merge_output_format": "mp4",
        "cookiefile": cookies,
        "progress_hooks": [_progress_hook(cancel_event, progress_cb)],
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
        "extract_flat": False,
        "writedescription": True,
        "writeinfojson": True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
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
        "writedescription": True,
        "writeinfojson": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        
        # Сохраняем информацию о аудио
        info_file = out_path.replace('.mp3', '.info.json')
        if os.path.exists(info_file):
            with open(info_file, 'w', encoding='utf-8') as f:
                json.dump(info, f, ensure_ascii=False, indent=2)