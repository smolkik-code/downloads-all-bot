import os
import asyncio
import threading
import logging
import subprocess
import time
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, FSInputFile
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
from keyboards import quality_keyboard, cancel_keyboard
from downloader import download_video, download_audio, DownloadCancelled
from middleware import PrivateMiddleware
from rate_limit import check_rate_limit
from info import extract_info
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


def optimize_for_telegram(input_path: str, output_path: str) -> bool:
    """
    Оптимизирует видео для телеграма
    """
    try:
        # Проверяем размер файла
        file_size_mb = os.path.getsize(input_path) / (1024 * 1024)
        
        # Если файл больше 50 МБ, сжимаем его
        if file_size_mb > 50:
            crf = 28
        else:
            crf = 23
            
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
            import shutil
            shutil.copy2(input_path, output_path)
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"Error optimizing video: {e}")
        import shutil
        shutil.copy2(input_path, output_path)
        return False


# -------------------- handlers --------------------

@dp.message(F.text == "/start")
async def start(message: Message):
    await message.answer(
        "👋 <b>Привет!</b>\n\n"
        "📥 Я скачиваю <b>видео</b> и <b>звук из видео</b> по ссылке.\n\n"
        "🔄 Кэш автоматически очищается раз в сутки\n"
        "👉 Просто отправь ссылку.",
        parse_mode="HTML"
    )


@dp.message(F.text == "/cache_stats")
async def cache_stats(message: Message):
    """Показывает статистику кэша"""
    try:
        total_size_mb = get_cache_size_mb()
        file_count = 0
        
        for root, dirs, files in os.walk(CACHE_DIR):
            file_count += len(files)
        
        # Получаем время последнего изменения самого старого файла
        oldest_time = None
        newest_time = None
        
        for root, dirs, files in os.walk(CACHE_DIR):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    mtime = os.path.getmtime(file_path)
                    if oldest_time is None or mtime < oldest_time:
                        oldest_time = mtime
                    if newest_time is None or mtime > newest_time:
                        newest_time = mtime
                except:
                    pass
        
        if oldest_time:
            oldest_str = datetime.fromtimestamp(oldest_time).strftime("%d.%m.%Y %H:%M")
            newest_str = datetime.fromtimestamp(newest_time).strftime("%d.%m.%Y %H:%M")
            age_info = f"🗓 Самый старый: {oldest_str}\n" \
                      f"🆕 Самый новый: {newest_str}"
        else:
            age_info = "🗓 Кэш пуст"
        
        await message.answer(
            f"📊 <b>Статистика кэша:</b>\n\n"
            f"📁 Файлов: {file_count}\n"
            f"💾 Размер: {total_size_mb:.2f} МБ\n"
            f"⏰ Очистка: ежедневно в 3:00\n\n"
            f"{age_info}",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Error getting cache stats: {e}")
        await message.answer("❌ Ошибка при получении статистики кэша")


@dp.message(F.text.startswith("http"))
async def handle_link(message: Message):
    url = message.text.strip()
    USER_URLS[message.from_user.id] = url
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


# ---------------- VIDEO ----------------

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

    # Извлекаем информацию о видео
    try:
        info = await asyncio.to_thread(extract_info, url, COOKIES_FILE)
        if not info:
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
            
        # Оптимизируем видео для телеграма
        await status.edit_text("⚙️ <b>Оптимизирую видео для телеграма…</b>", parse_mode="HTML")
        await asyncio.to_thread(optimize_for_telegram, tmp_path, optimized_path)
        
        # Удаляем исходный файл и используем оптимизированный
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
            
        os.rename(optimized_path, final_path)
        
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
        await callback.message.answer(
            f"✅ <b>Готово!</b>\n📦 Размер: {size_mb:.1f} МБ",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Error sending video: {e}")
        try:
            await callback.message.answer_document(FSInputFile(final_path))
            size_mb = os.path.getsize(final_path) / 1024 / 1024
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


# ---------------- AUDIO FROM VIDEO ----------------

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
        await callback.message.answer(
            f"✅ <b>Готово!</b>\n📦 Размер: {size_mb:.1f} МБ",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Error sending audio: {e}")
        await status.edit_text("❌ Ошибка при отправке аудио")

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