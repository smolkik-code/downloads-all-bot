from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def quality_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="480p", callback_data="q:best[height<=480]"),
                InlineKeyboardButton(text="720p", callback_data="q:best[height<=720]"),
            ],
            [
                InlineKeyboardButton(text="1080p", callback_data="q:best[height<=1080]"),
                InlineKeyboardButton(text="4K", callback_data="q:bestvideo+bestaudio"),
            ],
            [
                InlineKeyboardButton(text="🎧 Аудио из видео", callback_data="audio"),
            ]
        ]
    )



def cancel_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel")]
        ]
    )
