import yt_dlp
import threading
import os
import logging
import subprocess
import json
import shutil  # Импортируем shutil глобально
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
        "writedescription": True,
        "writeinfojson": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        
        # Сохраняем информацию о видео рядом с файлом
        info_file = out_path.replace('.mp4', '.info.json')
        try:
            with open(info_file, 'w', encoding='utf-8') as f:
                json.dump(info, f, ensure_ascii=False, indent=2)
        except:
            pass


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
        "writedescription": True,
        "writeinfojson": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        
        # Сохраняем информацию о видео
        info_file = out_path.replace('.mp4', '.info.json')
        try:
            with open(info_file, 'w', encoding='utf-8') as f:
                json.dump(info, f, ensure_ascii=False, indent=2)
        except:
            pass


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
                "preferredquality": "320",
            },
            {"key": "FFmpegMetadata"},
        ],
        "quiet": True,
        "no_warnings": True,
        "extractor_args": {
            "tiktok": {
                "skip_impersonation": True,
            },
        },
        "writethumbnail": True,
        "writeinfojson": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        
        # Сохраняем информацию о звуке
        info_file = out_path.replace('.mp3', '.info.json')
        try:
            with open(info_file, 'w', encoding='utf-8') as f:
                json.dump(info, f, ensure_ascii=False, indent=2)
        except:
            pass


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
                '-c', 'copy',
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
            shutil.copy2(input_path, output_path)
            
    except Exception as e:
        logger.error(f"Error adding metadata: {e}")
        shutil.copy2(input_path, output_path)


# ---------------- PLAYLIST DOWNLOAD (ИСПРАВЛЕННАЯ ВЕРСИЯ) ----------------

def download_playlist_videos(
    playlist_info: dict,
    output_dir: str,
    cookies: str | None,
    cancel_event: threading.Event,
    progress_cb,
) -> List[str]:
    """Скачивает все видео из плейлиста - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
    downloaded_files = []
    
    # Создаем yt-dlp объект для загрузки каждого видео отдельно
    ydl_opts = {
        "format": "best[height<=1080]/best",
        "outtmpl": os.path.join(output_dir, "%(title)s [%(id)s].%(ext)s"),  # Добавляем ID для уникальности
        "merge_output_format": "mp4",
        "cookiefile": cookies,
        "progress_hooks": [_progress_hook(cancel_event, progress_cb)],
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
        "extract_flat": False,
        "writedescription": True,
        "writeinfojson": True,
        "nooverwrites": True,  # Не перезаписывать существующие файлы
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
        try:
            with open(info_file, 'w', encoding='utf-8') as f:
                json.dump(info, f, ensure_ascii=False, indent=2)
        except:
            pass