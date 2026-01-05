import os
import asyncio
import threading
import logging
import subprocess
import time
import json
from datetime import datetime
from typing import Optional

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, FSInputFile, InputMediaDocument
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer

from config import (
    BOT_TOKEN,
    LOCAL_API_URL,
    CACHE_DIR,
    TMP_DIR,
    COOKIES_FILE,
    RATE_LIMIT_SECONDS,
    CACHE_MAX_AGE_DAYS,
    CACHE_MAX_SIZE_MB,
)
from keyboards import (
    quality_keyboard, 
    cancel_keyboard, 
    playlist_keyboard,
    platform_keyboard,
    tiktok_keyboard,
)
from downloader import (
    download_video, 
    download_audio, 
    download_original_quality,
    download_playlist_videos,
    download_tiktok_music,
    add_metadata_to_video,
    DownloadCancelled
)
from middleware import PrivateMiddleware
from rate_limit import check_rate_limit
from info import extract_info, is_playlist, get_platform_info
from cache import cache_key, cache_path
from cleanup import cleanup_tmp

# -------------------- init --------------------

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создаем директории
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

# Инициализация бота с локальным API (если используется)
if LOCAL_API_URL:
    api_server = TelegramAPIServer.from_base(LOCAL_API_URL)
    session = AiohttpSession(api=api_server)
    bot = Bot(token=BOT_TOKEN, session=session)
else:
    bot = Bot(token=BOT_TOKEN)

dp = Dispatcher()

# Регистрация middleware
private_middleware = PrivateMiddleware()
dp.message.middleware(private_middleware)
dp.callback_query.middleware(private_middleware)

USER_URLS: dict[int, str] = {}
USER_DATA: dict[int, dict] = {}
ACTIVE_DOWNLOADS: dict[int, dict] = {}

# -------------------- cache cleaning --------------------

def cleanup_old_cache():
    """
    Очищает старые файлы из кэша
    """
    try:
        current_time = time.time()
        deleted_count = 0
        deleted_size = 0
        
        # Удаляем файлы старше CACHE_MAX_AGE_DAYS дней
        if CACHE_MAX_AGE_DAYS > 0:
            cutoff_time = current_time - (CACHE_MAX_AGE_DAYS * 24 * 3600)
            
            for root, dirs, files in os.walk(CACHE_DIR):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        file_mtime = os.path.getmtime(file_path)
                        if file_mtime < cutoff_time:
                            file_size = os.path.getsize(file_path)
                            os.remove(file_path)
                            deleted_count += 1
                            deleted_size += file_size
                            logger.info(f"Deleted old cache file: {file}")
                    except Exception as e:
                        logger.error(f"Error deleting file {file}: {e}")
        
        # Если указан максимальный размер кэша, проверяем его
        if CACHE_MAX_SIZE_MB > 0:
            total_size_mb = get_cache_size_mb()
            if total_size_mb > CACHE_MAX_SIZE_MB:
                # Сортируем файлы по времени изменения (старые первыми)
                files_with_mtime = []
                for root, dirs, files in os.walk(CACHE_DIR):
                    for file in files:
                        file_path = os.path.join(root, file)
                        try:
                            mtime = os.path.getmtime(file_path)
                            size = os.path.getsize(file_path)
                            files_with_mtime.append((file_path, mtime, size))
                        except:
                            pass
                
                # Сортируем по времени (старые первыми)
                files_with_mtime.sort(key=lambda x: x[1])
                
                # Удаляем старые файлы пока не достигнем лимита
                target_size_mb = CACHE_MAX_SIZE_MB * 0.8  # Оставляем 80% от лимита
                
                for file_path, mtime, size in files_with_mtime:
                    if total_size_mb <= target_size_mb:
                        break
                    
                    try:
                        os.remove(file_path)
                        deleted_count += 1
                        deleted_size += size
                        total_size_mb -= size / (1024 * 1024)
                        logger.info(f"Deleted cache file to free space: {os.path.basename(file_path)}")
                    except Exception as e:
                        logger.error(f"Error deleting file {file_path}: {e}")
        
        if deleted_count > 0:
            logger.info(f"Cache cleanup: deleted {deleted_count} files, freed {deleted_size / (1024*1024):.2f} MB")
        else:
            logger.info("Cache cleanup: no files to delete")
            
    except Exception as e:
        logger.error(f"Error in cache cleanup: {e}")


def get_cache_size_mb():
    """Возвращает размер кэша в МБ"""
    total_size = 0
    for root, dirs, files in os.walk(CACHE_DIR):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                total_size += os.path.getsize(file_path)
            except:
                pass
    return total_size / (1024 * 1024)


async def scheduled_cache_cleanup():
    """Периодическая очистка кэша"""
    # Запускаем очистку сразу при старте
    logger.info("Running initial cache cleanup...")
    await asyncio.to_thread(cleanup_old_cache)
    
    last_cleanup_day = datetime.now().day
    
    while True:
        try:
            now = datetime.now()
            current_day = now.day
            
            # Запускаем очистку если наступил новый день И сейчас между 3:00 и 3:59
            if current_day != last_cleanup_day and now.hour == 3:
                logger.info("Starting scheduled cache cleanup...")
                await asyncio.to_thread(cleanup_old_cache)
                last_cleanup_day = current_day
                
            # Ждем 5 минут перед следующей проверкой
            await asyncio.sleep(300)
            
        except Exception as e:
            logger.error(f"Error in scheduled cleanup: {e}")
            await asyncio.sleep(60)


# -------------------- helpers --------------------

def render_bar(percent: float, size: int = 10) -> str:
    filled = int(size * percent / 100)
    return "█" * filled + "░" * (size - filled)


def make_progress_cb(loop, message):
    last_percent = {"value": 0}
    last_update = {"time": 0}

    async def update(d):
        try:
            downloaded = d.get("downloaded_bytes", 0)
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 1
            
            # Избегаем деления на ноль
            if total <= 0:
                return
                
            percent = min(100, downloaded * 100 / total)

            # Обновляем раз в ~2% и не чаще чем раз в 2 секунды
            current_time = time.time()
            if percent - last_percent["value"] < 2 and current_time - last_update["time"] < 2:
                return
                
            last_percent["value"] = percent
            last_update["time"] = current_time

            bar = render_bar(percent)
            eta = d.get("eta")
            
            # Безопасное форматирование ETA
            if eta is None or eta == "?":
                eta_str = "?"
            else:
                try:
                    eta_str = str(int(float(eta)))
                except (ValueError, TypeError):
                    eta_str = "?"

            text = (
                "⏬ <b>Загрузка</b>\n"
                f"<code>{bar}</code> {percent:.0f}%\n"
                f"⏱ Осталось: {eta_str} сек"
            )

            await message.edit_text(
                text,
                reply_markup=cancel_keyboard(),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Error updating progress: {e}")

    def cb(d):
        asyncio.run_coroutine_threadsafe(update(d), loop)

    return cb


def make_playlist_progress_cb(loop, message, total_videos: int):
    current_video = {"value": 0}
    last_update = {"time": 0}

    async def update(d):
        try:
            # Обновляем не чаще чем раз в 3 секунды
            current_time = time.time()
            if current_time - last_update["time"] < 3:
                return
                
            last_update["time"] = current_time

            if d.get("status") == "finished":
                current_video["value"] += 1
                
                text = (
                    f"📁 <b>Загрузка плейлиста</b>\n"
                    f"📹 Видео: {current_video['value']}/{total_videos}\n"
                    f"⏳ Продолжаем загрузку..."
                )

                await message.edit_text(
                    text,
                    parse_mode="HTML"
                )
        except Exception as e:
            logger.error(f"Error updating playlist progress: {e}")

    def cb(d):
        asyncio.run_coroutine_threadsafe(update(d), loop)

    return cb


def optimize_for_telegram(input_path: str, output_path: str, metadata: dict = None) -> bool:
    """
    Оптимизирует видео для телеграма с добавлением метаданных
    """
    try:
        import shutil  # Импортируем здесь
        
        # Проверяем размер файла
        file_size_mb = os.path.getsize(input_path) / (1024 * 1024)
        
        # Если файл больше 50 МБ, сжимаем его
        if file_size_mb > 50:
            crf = 28
        else:
            crf = 23
        
        # Подготавливаем метаданные для ffmpeg
        metadata_args = []
        if metadata:
            if metadata.get('title'):
                metadata_args.extend(['-metadata', f'title={metadata["title"]}'])
            if metadata.get('artist') or metadata.get('uploader'):
                artist = metadata.get('artist') or metadata.get('uploader')
                metadata_args.extend(['-metadata', f'artist={artist}'])
            if metadata.get('description'):
                # Обрезаем описание если слишком длинное для метаданных
                desc = metadata['description'][:1000]
                metadata_args.extend(['-metadata', f'comment={desc}'])
            if metadata.get('url'):
                metadata_args.extend(['-metadata', f'copyright={metadata["url"]}'])
            
        cmd = [
            'ffmpeg',
            '-i', input_path,
            '-c:v', 'libx264',
            '-preset', 'fast',
            '-crf', str(crf),
            '-c:a', 'aac',
            '-b:a', '128k',
            '-movflags', '+faststart',
            '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
            '-y',
            *metadata_args,
            output_path
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode != 0:
            logger.error(f"FFmpeg error: {result.stderr}")
            shutil.copy2(input_path, output_path)
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"Error optimizing video: {e}")
        import shutil
        shutil.copy2(input_path, output_path)
        return False


@dp.message(F.text.startswith("http"))
async def handle_link(message: Message):
    url = message.text.strip()
    user_id = message.from_user.id
    
    USER_URLS[user_id] = url
    
    # Определяем платформу
    platform_info = await asyncio.to_thread(get_platform_info, url)
    
    # Для TikTok показываем специальное меню
    if platform_info == "tiktok":
        USER_DATA[user_id] = {"platform": platform_info}
        await message.answer(
            f"🎵 <b>Ссылка с TikTok</b>\n\n"
            "Выберите что скачать:",
            reply_markup=tiktok_keyboard(),
            parse_mode="HTML"
        )
        return
    
    # Проверяем, является ли ссылка плейлистом (кроме TikTok)
    try:
        is_playlist_url = await asyncio.to_thread(is_playlist, url)
        if is_playlist_url:
            USER_DATA[user_id] = {"is_playlist": True}
            await message.answer(
                "📁 <b>Обнаружен плейлист!</b>\n\n"
                "Выберите действие:",
                reply_markup=playlist_keyboard(),
                parse_mode="HTML"
            )
            return
    except:
        pass
    
    # Для Instagram предлагаем оригинальное качество
    if platform_info == "instagram":
        USER_DATA[user_id] = {"platform": platform_info}
        await message.answer(
            f"📸 <b>Ссылка с Instagram</b>\n\n"
            "Выберите качество загрузки:",
            reply_markup=platform_keyboard(platform_info),
            parse_mode="HTML"
        )
    else:
        # Для других платформ обычное меню
        await message.answer(
            "🔽 <b>Выбери формат загрузки:</b>",
            reply_markup=quality_keyboard(),
            parse_mode="HTML"
        )


@dp.callback_query(F.data == "cancel")
async def cancel_download(callback: CallbackQuery):
    user_id = callback.from_user.id
    data = ACTIVE_DOWNLOADS.get(user_id)
    
    if data:
        data["cancel"].set()
        ACTIVE_DOWNLOADS.pop(user_id, None)
        await callback.answer("⛔ Загрузка отменена", show_alert=True)
    else:
        await callback.answer("❌ Нет активной загрузки", show_alert=True)


# ---------------- TIKTOK SPECIAL HANDLERS ----------------

@dp.callback_query(F.data == "tiktok_music")
async def handle_tiktok_music(callback: CallbackQuery):
    """Отдельная загрузка звука из TikTok"""
    await callback.answer()
    user_id = callback.from_user.id
    url = USER_URLS.get(user_id)
    
    if not url:
        await callback.message.answer("❌ Ссылка не найдена")
        return
    
    if not check_rate_limit(user_id, RATE_LIMIT_SECONDS):
        await callback.message.answer("⏳ Подожди немного перед следующим запросом")
        return
    
    await callback.message.edit_reply_markup(reply_markup=None)
    status = await callback.message.answer("🎵 <b>Извлекаю звук из TikTok…</b>", parse_mode="HTML")
    
    key = cache_key(url, "tiktok_music", audio=True)
    final_path = cache_path(CACHE_DIR, key, "mp3")
    tmp_path = os.path.join(TMP_DIR, f"{key}.mp3")
    
    # Создаем директории если не существуют
    os.makedirs(TMP_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    
    # Проверяем кэш
    if os.path.exists(final_path):
        await status.edit_text("📤 <b>Отправляю звук из кэша…</b>", parse_mode="HTML")
        try:
            await callback.message.answer_audio(FSInputFile(final_path))
            
            # Получаем информацию о звуке для описания
            try:
                info = await asyncio.to_thread(extract_info, url, COOKIES_FILE)
                if info:
                    metadata = get_video_description(info)
                    if metadata.get('track') or metadata.get('artist'):
                        desc = f"🎵 <b>Звук из TikTok</b>\n\n"
                        if metadata.get('track'):
                            desc += f"🎶 Трек: {metadata['track']}\n"
                        if metadata.get('artist'):
                            desc += f"👤 Исполнитель: {metadata['artist']}\n"
                        if metadata.get('url'):
                            desc += f"🔗 Оригинал: {metadata['url']}"
                        
                        await callback.message.answer(desc, parse_mode="HTML")
            except:
                pass
                
        except Exception as e:
            logger.error(f"Error sending cached audio: {e}")
            await status.edit_text("❌ Ошибка при отправке звука")
        return
    
    cancel_event = threading.Event()
    ACTIVE_DOWNLOADS[user_id] = {"cancel": cancel_event}
    loop = asyncio.get_running_loop()
    progress_cb = make_progress_cb(loop, status)
    
    try:
        # Загружаем звук из TikTok
        await asyncio.to_thread(
            download_tiktok_music,
            url,
            tmp_path,
            COOKIES_FILE,
            cancel_event,
            progress_cb,
        )
        
        # Проверяем, был ли отменен процесс
        if cancel_event.is_set():
            await status.edit_text("⛔ Загрузка отменена")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return
        
        # Проверяем, создан ли файл
        if not os.path.exists(tmp_path):
            raise Exception("Аудио файл не был создан")
            
        # Проверяем размер файла
        file_size = os.path.getsize(tmp_path)
        if file_size == 0:
            os.remove(tmp_path)
            raise Exception("Создан пустой аудио файл")
        
        # Добавляем метаданные к аудио
        try:
            info = await asyncio.to_thread(extract_info, url, COOKIES_FILE)
            if info:
                metadata = get_video_description(info)
                await asyncio.to_thread(add_metadata_to_video, tmp_path, tmp_path + "_meta.mp3", metadata)
                if os.path.exists(tmp_path + "_meta.mp3"):
                    os.remove(tmp_path)
                    os.rename(tmp_path + "_meta.mp3", tmp_path)
        except Exception as e:
            logger.error(f"Error adding metadata: {e}")
        
        # Перемещаем файл в кэш
        if os.path.exists(final_path):
            os.remove(final_path)
        os.rename(tmp_path, final_path)
        
    except DownloadCancelled:
        await status.edit_text("⛔ Загрузка отменена")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return
    except Exception as e:
        logger.error(f"Error downloading TikTok music: {str(e)}")
        await status.edit_text(f"❌ Ошибка: {str(e)[:100]}")
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass
        return
    finally:
        ACTIVE_DOWNLOADS.pop(user_id, None)
    
    await status.edit_text("📤 <b>Отправляю звук…</b>", parse_mode="HTML")
    
    try:
        await callback.message.answer_audio(FSInputFile(final_path))
        
        # Отправляем описание звука
        try:
            info = await asyncio.to_thread(extract_info, url, COOKIES_FILE)
            if info:
                metadata = get_video_description(info)
                if metadata.get('track') or metadata.get('artist'):
                    desc = f"🎵 <b>Звук из TikTok</b>\n\n"
                    if metadata.get('track'):
                        desc += f"🎶 Трек: {metadata['track']}\n"
                    if metadata.get('artist'):
                        desc += f"👤 Исполнитель: {metadata['artist']}\n"
                    if metadata.get('title'):
                        desc += f"📝 Видео: {metadata['title'][:100]}...\n"
                    if metadata.get('uploader'):
                        desc += f"👤 Автор: {metadata['uploader']}\n"
                    if metadata.get('url'):
                        desc += f"🔗 Оригинал: {metadata['url']}"
                    
                    await callback.message.answer(desc, parse_mode="HTML")
        except:
            pass
            
    except Exception as e:
        logger.error(f"Error sending audio: {e}")
        await status.edit_text("❌ Ошибка при отправке звука")
    
    cleanup_tmp(TMP_DIR)


# ---------------- ORIGINAL QUALITY HANDLERS ----------------

@dp.callback_query(F.data == "original_quality")
async def handle_original_quality(callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    url = USER_URLS.get(user_id)
    
    if not url:
        await callback.message.answer("❌ Ссылка не найдена")
        return
    
    if not check_rate_limit(user_id, RATE_LIMIT_SECONDS):
        await callback.message.answer("⏳ Подожди немного перед следующим запросом")
        return
    
    await callback.message.edit_reply_markup(reply_markup=None)
    status = await callback.message.answer("🎬 <b>Загрузка в оригинальном качестве…</b>", parse_mode="HTML")
    
    # Получаем информацию о видео для метаданных
    video_info = None
    try:
        video_info = await asyncio.to_thread(extract_info, url, COOKIES_FILE)
    except:
        pass
    
    key = cache_key(url, "original", audio=False)
    final_path = cache_path(CACHE_DIR, key, "mp4")
    tmp_path = os.path.join(TMP_DIR, f"{key}.mp4")
    
    # Проверяем кэш
    if os.path.exists(final_path):
        await status.edit_text("📤 <b>Отправляю файл из кэша…</b>", parse_mode="HTML")
        try:
            await callback.message.answer_video(FSInputFile(final_path))
            size_mb = os.path.getsize(final_path) / 1024 / 1024
            
            # Отправляем описание
            if video_info:
                metadata = get_video_description(video_info)
                desc = format_description(metadata)
                if desc:
                    await callback.message.answer(
                        f"✅ <b>Готово! (Оригинальное качество)</b>\n"
                        f"📦 Размер: {size_mb:.1f} МБ\n\n"
                        f"{desc}",
                        parse_mode="HTML"
                    )
                else:
                    await callback.message.answer(
                        f"✅ <b>Готово! (Оригинальное качество)</b>\n📦 Размер: {size_mb:.1f} МБ",
                        parse_mode="HTML"
                    )
            else:
                await callback.message.answer(
                    f"✅ <b>Готово! (Оригинальное качество)</b>\n📦 Размер: {size_mb:.1f} МБ",
                    parse_mode="HTML"
                )
        except Exception as e:
            logger.error(f"Error sending cached file: {e}")
            await status.edit_text("❌ Ошибка при отправке файла")
        return
    
    cancel_event = threading.Event()
    ACTIVE_DOWNLOADS[user_id] = {"cancel": cancel_event}
    loop = asyncio.get_running_loop()
    progress_cb = make_progress_cb(loop, status)
    
    try:
        await asyncio.to_thread(
            download_original_quality,
            url,
            tmp_path,
            COOKIES_FILE,
            cancel_event,
            progress_cb,
        )
        
        if cancel_event.is_set():
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            await status.edit_text("⛔ Загрузка отменена")
            return
            
        # Добавляем метаданные к видео
        if video_info:
            metadata = get_video_description(video_info)
            await asyncio.to_thread(add_metadata_to_video, tmp_path, tmp_path + "_meta.mp4", metadata)
            if os.path.exists(tmp_path + "_meta.mp4"):
                os.remove(tmp_path)
                os.rename(tmp_path + "_meta.mp4", tmp_path)
        
        os.rename(tmp_path, final_path)
        
    except DownloadCancelled:
        await status.edit_text("⛔ Загрузка отменена")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return
    except Exception as e:
        logger.error(f"Error downloading original quality: {e}")
        await status.edit_text(
            "❌ Не удалось скачать в оригинальном качестве\n"
            "💡 Попробуй обычное качество"
        )
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return
    finally:
        ACTIVE_DOWNLOADS.pop(user_id, None)
    
    await status.edit_text("📤 <b>Отправляю видео…</b>", parse_mode="HTML")
    
    try:
        await callback.message.answer_video(
            FSInputFile(final_path),
            supports_streaming=True
        )
        size_mb = os.path.getsize(final_path) / 1024 / 1024
        
        # Отправляем описание
        if video_info:
            metadata = get_video_description(video_info)
            desc = format_description(metadata)
            if desc:
                await callback.message.answer(
                    f"✅ <b>Готово! (Оригинальное качество)</b>\n"
                    f"📦 Размер: {size_mb:.1f} МБ\n\n"
                    f"{desc}",
                    parse_mode="HTML"
                )
            else:
                await callback.message.answer(
                    f"✅ <b>Готово! (Оригинальное качество)</b>\n📦 Размер: {size_mb:.1f} МБ",
                    parse_mode="HTML"
                )
        else:
            await callback.message.answer(
                f"✅ <b>Готово! (Оригинальное качество)</b>\n📦 Размер: {size_mb:.1f} МБ",
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Error sending video: {e}")
        try:
            await callback.message.answer_document(FSInputFile(final_path))
            size_mb = os.path.getsize(final_path) / 1024 / 1024
            
            # Отправляем описание даже если отправлено как документ
            if video_info:
                metadata = get_video_description(video_info)
                desc = format_description(metadata)
                if desc:
                    await callback.message.answer(
                        f"✅ <b>Отправлено как документ (Оригинальное качество)</b>\n"
                        f"📦 Размер: {size_mb:.1f} МБ\n\n"
                        f"{desc}",
                        parse_mode="HTML"
                    )
                else:
                    await callback.message.answer(
                        f"✅ <b>Отправлено как документ (Оригинальное качество)</b>\n📦 Размер: {size_mb:.1f} МБ",
                        parse_mode="HTML"
                    )
            else:
                await callback.message.answer(
                    f"✅ <b>Отправлено как документ (Оригинальное качество)</b>\n📦 Размер: {size_mb:.1f} МБ",
                    parse_mode="HTML"
                )
        except Exception as e2:
            logger.error(f"Error sending as document: {e2}")
            await status.edit_text("❌ Ошибка при отправке файла")
            if os.path.exists(final_path):
                os.remove(final_path)
    
    cleanup_tmp(TMP_DIR)


# ---------------- STANDARD VIDEO HANDLER ----------------

@dp.callback_query(F.data.startswith("q:"))
async def handle_video(callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    url = USER_URLS.get(user_id)
    quality = callback.data.split(":", 1)[1]

    if not url:
        await callback.message.answer("❌ Ссылка не найдена")
        return

    if not check_rate_limit(user_id, RATE_LIMIT_SECONDS):
        await callback.message.answer("⏳ Подожди немного перед следующим запросом")
        return

    await callback.message.edit_reply_markup(reply_markup=None)
    status = await callback.message.answer("🔍 <b>Анализирую ссылку…</b>", parse_mode="HTML")

    # Извлекаем информацию о видео для метаданных
    try:
        video_info = await asyncio.to_thread(extract_info, url, COOKIES_FILE)
        if not video_info:
            await status.edit_text("❌ Не удалось получить информацию о видео")
            return
    except Exception as e:
        logger.error(f"Error extracting info: {e}")
        await status.edit_text("❌ Ошибка при анализе ссылки")
        return

    key = cache_key(url, quality, audio=False)
    final_path = cache_path(CACHE_DIR, key, "mp4")
    tmp_path = os.path.join(TMP_DIR, f"{key}.mp4")
    optimized_path = os.path.join(TMP_DIR, f"{key}_optimized.mp4")

    # Проверяем кэш
    if os.path.exists(final_path):
        await status.edit_text("📤 <b>Отправляю файл из кэша…</b>", parse_mode="HTML")
        try:
            await callback.message.answer_video(FSInputFile(final_path))
            size_mb = os.path.getsize(final_path) / 1024 / 1024
            
            # Отправляем описание
            metadata = get_video_description(video_info)
            desc = format_description(metadata)
            if desc:
                await callback.message.answer(
                    f"✅ <b>Готово!</b>\n"
                    f"📦 Размер: {size_mb:.1f} МБ\n\n"
                    f"{desc}",
                    parse_mode="HTML"
                )
            else:
                await callback.message.answer(
                    f"✅ <b>Готово!</b>\n📦 Размер: {size_mb:.1f} МБ",
                    parse_mode="HTML"
                )
        except Exception as e:
            logger.error(f"Error sending cached file: {e}")
            await status.edit_text("❌ Ошибка при отправке файла")
        return

    cancel_event = threading.Event()
    ACTIVE_DOWNLOADS[user_id] = {"cancel": cancel_event}
    loop = asyncio.get_running_loop()
    progress_cb = make_progress_cb(loop, status)

    try:
        await asyncio.to_thread(
            download_video,
            url,
            quality,
            tmp_path,
            COOKIES_FILE,
            cancel_event,
            progress_cb,
        )
        
        # Проверяем, был ли отменен процесс
        if cancel_event.is_set():
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            await status.edit_text("⛔ Загрузка отменена")
            return
        
        # Получаем метаданные для видео
        metadata = get_video_description(video_info)
        
        # Оптимизируем видео для телеграма с добавлением метаданных
        if quality != "original":
            await status.edit_text("⚙️ <b>Оптимизирую видео для телеграма…</b>", parse_mode="HTML")
            await asyncio.to_thread(optimize_for_telegram, tmp_path, optimized_path, metadata)
            
            # Удаляем исходный файл и используем оптимизированный
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
                
            os.rename(optimized_path, final_path)
        else:
            # Для оригинального качества не оптимизируем, но добавляем метаданны
            await asyncio.to_thread(add_metadata_to_video, tmp_path, tmp_path + "_meta.mp4", metadata)
            if os.path.exists(tmp_path + "_meta.mp4"):
                os.remove(tmp_path)
                os.rename(tmp_path + "_meta.mp4", tmp_path)
            os.rename(tmp_path, final_path)
        
    except DownloadCancelled:
        await status.edit_text("⛔ Загрузка отменена")
        for path in [tmp_path, optimized_path]:
            if os.path.exists(path):
                os.remove(path)
        return
    except Exception as e:
        logger.error(f"Error downloading video: {e}")
        await status.edit_text(
            "❌ Не удалось скачать видео\n"
            "💡 Попробуй другое качество"
        )
        for path in [tmp_path, optimized_path]:
            if os.path.exists(path):
                os.remove(path)
        return
    finally:
        ACTIVE_DOWNLOADS.pop(user_id, None)

    await status.edit_text("📤 <b>Отправляю видео…</b>", parse_mode="HTML")

    try:
        await callback.message.answer_video(
            FSInputFile(final_path),
            supports_streaming=True
        )
        size_mb = os.path.getsize(final_path) / 1024 / 1024
        
        # Отправляем описание
        desc = format_description(metadata)
        if desc:
            await callback.message.answer(
                f"✅ <b>Готово!</b>\n"
                f"📦 Размер: {size_mb:.1f} МБ\n\n"
                f"{desc}",
                parse_mode="HTML"
            )
        else:
            await callback.message.answer(
                f"✅ <b>Готово!</b>\n📦 Размер: {size_mb:.1f} МБ",
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Error sending video: {e}")
        try:
            await callback.message.answer_document(FSInputFile(final_path))
            size_mb = os.path.getsize(final_path) / 1024 / 1024
            
            # Отправляем описание даже если отправлено как документ
            desc = format_description(metadata)
            if desc:
                await callback.message.answer(
                    f"✅ <b>Отправлено как документ</b>\n"
                    f"📦 Размер: {size_mb:.1f} МБ\n\n"
                    f"{desc}",
                    parse_mode="HTML"
                )
            else:
                await callback.message.answer(
                    f"✅ <b>Отправлено как документ</b>\n📦 Размер: {size_mb:.1f} МБ",
                    parse_mode="HTML"
                )
        except Exception as e2:
            logger.error(f"Error sending as document: {e2}")
            await status.edit_text("❌ Ошибка при отправке файла")
            if os.path.exists(final_path):
                os.remove(final_path)

    cleanup_tmp(TMP_DIR)


# ---------------- STANDARD AUDIO HANDLER ----------------

@dp.callback_query(F.data == "audio")
async def handle_audio(callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    url = USER_URLS.get(user_id)

    if not url:
        await callback.message.answer("❌ Ссылка не найдена")
        return

    if not check_rate_limit(user_id, RATE_LIMIT_SECONDS):
        await callback.message.answer("⏳ Подожди немного перед следующим запросом")
        return

    await callback.message.edit_reply_markup(reply_markup=None)
    status = await callback.message.answer("🎧 <b>Подготовка аудио…</b>", parse_mode="HTML")

    # Получаем информацию о видео для метаданных
    video_info = None
    try:
        video_info = await asyncio.to_thread(extract_info, url, COOKIES_FILE)
    except:
        pass

    key = cache_key(url, "audio", audio=True)
    final_path = cache_path(CACHE_DIR, key, "mp3")
    tmp_path = os.path.join(TMP_DIR, f"{key}.mp3")

    # Создаем директории если не существуют
    os.makedirs(TMP_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    # Проверяем кэш
    if os.path.exists(final_path):
        await status.edit_text("📤 <b>Отправляю аудио из кэша…</b>", parse_mode="HTML")
        try:
            await callback.message.answer_audio(FSInputFile(final_path))
            size_mb = os.path.getsize(final_path) / 1024 / 1024
            
            # Отправляем описание
            if video_info:
                metadata = get_video_description(video_info)
                desc = f"🎧 <b>Аудио из видео</b>\n\n"
                if metadata.get('title'):
                    desc += f"🎬 {metadata['title']}\n"
                if metadata.get('uploader'):
                    desc += f"👤 Автор: {metadata['uploader']}\n"
                if metadata.get('url'):
                    desc += f"🔗 Оригинал: {metadata['url']}"
                
                await callback.message.answer(
                    f"✅ <b>Готово!</b>\n"
                    f"📦 Размер: {size_mb:.1f} МБ\n\n"
                    f"{desc}",
                    parse_mode="HTML"
                )
            else:
                await callback.message.answer(
                    f"✅ <b>Готово!</b>\n📦 Размер: {size_mb:.1f} МБ",
                    parse_mode="HTML"
                )
        except Exception as e:
            logger.error(f"Error sending cached audio: {e}")
            await status.edit_text("❌ Ошибка при отправке аудио")
        return

    cancel_event = threading.Event()
    ACTIVE_DOWNLOADS[user_id] = {"cancel": cancel_event}
    loop = asyncio.get_running_loop()
    progress_cb = make_progress_cb(loop, status)

    try:
        await asyncio.to_thread(
            download_audio,
            url,
            tmp_path,
            COOKIES_FILE,
            cancel_event,
            progress_cb,
        )
        
        # Проверяем, был ли отменен процесс
        if cancel_event.is_set():
            await status.edit_text("⛔ Загрузка отменена")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return
        
        # Проверяем, создан ли файл
        if not os.path.exists(tmp_path):
            raise Exception("Аудио файл не был создан")
            
        # Проверяем размер файла
        file_size = os.path.getsize(tmp_path)
        if file_size == 0:
            os.remove(tmp_path)
            raise Exception("Создан пустой аудио файл")
        
        # Добавляем метаданные к аудио
        if video_info:
            metadata = get_video_description(video_info)
            await asyncio.to_thread(add_metadata_to_video, tmp_path, tmp_path + "_meta.mp3", metadata)
            if os.path.exists(tmp_path + "_meta.mp3"):
                os.remove(tmp_path)
                os.rename(tmp_path + "_meta.mp3", tmp_path)
        
        # Перемещаем файл в кэш
        if os.path.exists(final_path):
            os.remove(final_path)
        os.rename(tmp_path, final_path)
        
    except DownloadCancelled:
        await status.edit_text("⛔ Загрузка отменена")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return
    except Exception as e:
        logger.error(f"Error downloading audio: {str(e)}")
        await status.edit_text(f"❌ Ошибка: {str(e)[:100]}")
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass
        return
    finally:
        ACTIVE_DOWNLOADS.pop(user_id, None)

    await status.edit_text("📤 <b>Отправляю аудио…</b>", parse_mode="HTML")

    try:
        await callback.message.answer_audio(FSInputFile(final_path))
        size_mb = os.path.getsize(final_path) / 1024 / 1024
        
        # Отправляем описание
        if video_info:
            metadata = get_video_description(video_info)
            desc = f"🎧 <b>Аудио из видео</b>\n\n"
            if metadata.get('title'):
                desc += f"🎬 {metadata['title']}\n"
            if metadata.get('uploader'):
                desc += f"👤 Автор: {metadata['uploader']}\n"
            if metadata.get('duration'):
                minutes = metadata['duration'] // 60
                seconds = metadata['duration'] % 60
                desc += f"⏱ Длительность: {minutes}:{seconds:02d}\n"
            if metadata.get('url'):
                desc += f"🔗 Оригинал: {metadata['url']}"
            
            await callback.message.answer(
                f"✅ <b>Готово!</b>\n"
                f"📦 Размер: {size_mb:.1f} МБ\n\n"
                f"{desc}",
                parse_mode="HTML"
            )
        else:
            await callback.message.answer(
                f"✅ <b>Готово!</b>\n📦 Размер: {size_mb:.1f} МБ",
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Error sending audio: {e}")
        await status.edit_text("❌ Ошибка при отправке аудио")

    cleanup_tmp(TMP_DIR)


# ---------------- PLAYLIST HANDLERS ----------------

@dp.callback_query(F.data == "playlist_all")
async def handle_playlist_all(callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    url = USER_URLS.get(user_id)
    
    if not url:
        await callback.message.answer("❌ Ссылка не найдена")
        return
    
    if not check_rate_limit(user_id, RATE_LIMIT_SECONDS * 3):
        await callback.message.answer("⏳ Подожди немного перед следующим запросом")
        return
    
    await callback.message.edit_reply_markup(reply_markup=None)
    status = await callback.message.answer("📁 <b>Анализирую плейлист…</b>", parse_mode="HTML")
    
    try:
        # Получаем информацию о плейлисте
        from info import get_playlist_info
        playlist_info = await asyncio.to_thread(get_playlist_info, url)
        
        if not playlist_info or 'entries' not in playlist_info:
            await status.edit_text("❌ Не удалось получить информацию о плейлисте")
            return
        
        video_count = len(playlist_info['entries'])
        if video_count == 0:
            await status.edit_text("❌ Плейлист пуст")
            return
        
        # Подтверждение для больших плейлистов
        if video_count > 10:
            await callback.message.answer(
                f"⚠️ <b>Внимание!</b>\n\n"
                f"Плейлист содержит <b>{video_count}</b> видео.\n"
                f"Это может занять много времени и места.\n\n"
                f"Продолжить загрузку?",
                reply_markup=playlist_keyboard(confirm=True),
                parse_mode="HTML"
            )
            USER_DATA[user_id] = {"playlist_info": playlist_info, "status_message": status}
            return
        
        await download_playlist_confirm(callback, user_id, playlist_info, status)
        
    except Exception as e:
        logger.error(f"Error analyzing playlist: {e}")
        await status.edit_text("❌ Ошибка при анализе плейлиста")


@dp.callback_query(F.data == "playlist_confirm_yes")
async def handle_playlist_confirm(callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    
    data = USER_DATA.get(user_id, {})
    playlist_info = data.get("playlist_info")
    status = data.get("status_message")
    
    if not playlist_info or not status:
        await callback.message.answer("❌ Данные плейлиста не найдены")
        return
    
    await callback.message.edit_reply_markup(reply_markup=None)
    await download_playlist_confirm(callback, user_id, playlist_info, status)


async def download_playlist_confirm(callback, user_id, playlist_info, status):
    """Загружает плейлист после подтверждения"""
    try:
        import uuid
        import shutil
        
        video_count = len(playlist_info['entries'])
        playlist_title = playlist_info.get('title', 'Плейлист')
        
        await status.edit_text(
            f"📁 <b>Начинаю загрузку плейлиста</b>\n\n"
            f"🎬 Название: {playlist_title}\n"
            f"📹 Видео: {video_count}\n"
            f"⏳ Подготовка...",
            parse_mode="HTML"
        )
        
        cancel_event = threading.Event()
        ACTIVE_DOWNLOADS[user_id] = {"cancel": cancel_event}
        loop = asyncio.get_running_loop()
        progress_cb = make_playlist_progress_cb(loop, status, video_count)
        
        # Создаем временную директорию для плейлиста
        playlist_dir = os.path.join(TMP_DIR, f"playlist_{uuid.uuid4().hex[:8]}")
        os.makedirs(playlist_dir, exist_ok=True)
        
        # Загружаем плейлист
        downloaded_files = await asyncio.to_thread(
            download_playlist_videos,
            playlist_info,
            playlist_dir,
            COOKIES_FILE,
            cancel_event,
            progress_cb
        )
        
        if cancel_event.is_set():
            await status.edit_text("⛔ Загрузка плейлиста отменена")
            shutil.rmtree(playlist_dir, ignore_errors=True)
            return
        
        if not downloaded_files:
            await status.edit_text("❌ Не удалось загрузить видео из плейлиста")
            shutil.rmtree(playlist_dir, ignore_errors=True)
            return
        
        # Отправляем файлы частями
        await status.edit_text(f"📤 <b>Отправляю {len(downloaded_files)} видео…</b>", parse_mode="HTML")
        
        # Сортируем файлы по размеру (сначала маленькие)
        downloaded_files.sort(key=lambda x: os.path.getsize(x))
        
        sent_count = 0
        for i, file_path in enumerate(downloaded_files, 1):
            if cancel_event.is_set():
                break
                
            try:
                file_name = os.path.basename(file_path)
                # Убираем расширение для имени файла
                display_name = os.path.splitext(file_name)[0]
                
                # Отправляем файл с номером в подписи
                await callback.message.answer_document(
                    FSInputFile(file_path),
                    caption=f"🎬 Видео {i}/{len(downloaded_files)}\n📁 {display_name[:50]}"
                )
                sent_count += 1
                
                # Небольшая пауза между отправками
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error sending file {file_path}: {e}")
                continue
        
        total_size = sum(os.path.getsize(f) for f in downloaded_files)
        total_size_mb = total_size / (1024 * 1024)
        
        await callback.message.answer(
            f"✅ <b>Плейлист загружен!</b>\n\n"
            f"📁 Видео в плейлисте: {video_count}\n"
            f"📤 Отправлено: {sent_count}\n"
            f"💾 Общий размер: {total_size_mb:.1f} МБ\n"
            f"🎬 Название: {playlist_title}",
            parse_mode="HTML"
        )
        
        # Очищаем временные файлы
        shutil.rmtree(playlist_dir, ignore_errors=True)
        
    except DownloadCancelled:
        await status.edit_text("⛔ Загрузка плейлиста отменена")
    except Exception as e:
        logger.error(f"Error downloading playlist: {e}")
        await status.edit_text(f"❌ Ошибка: {str(e)[:100]}")
    finally:
        ACTIVE_DOWNLOADS.pop(user_id, None)
        cleanup_tmp(TMP_DIR)
# -------------------- entrypoint --------------------

async def main():
    # Очистка временных файлов при старте
    cleanup_tmp(TMP_DIR)
    
    # Запускаем задачу очистки кэша в фоне
    cleanup_task = asyncio.create_task(scheduled_cache_cleanup())
    
    try:
        await dp.start_polling(bot)
    finally:
        # Отменяем задачу очистки при выходе
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")