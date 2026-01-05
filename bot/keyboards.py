from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def quality_keyboard():
    """Клавиатура выбора качества"""
    keyboard = [
        [
            InlineKeyboardButton(text="📹 720p", callback_data="q:720"),
            InlineKeyboardButton(text="🎬 1080p", callback_data="q:1080"),
            InlineKeyboardButton(text="🎥 1440p", callback_data="q:1440"),
        ],
        [
            InlineKeyboardButton(text="🎧 Аудио", callback_data="audio"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def cancel_keyboard():
    """Клавиатура с кнопкой отмены"""
    keyboard = [
        [InlineKeyboardButton(text="⛔ Отменить загрузку", callback_data="cancel")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def playlist_keyboard(confirm=False):
    """Клавиатура для плейлистов"""
    if confirm:
        keyboard = [
            [
                InlineKeyboardButton(text="✅ Да, загрузить все", callback_data="playlist_confirm_yes"),
                InlineKeyboardButton(text="❌ Нет, отменить", callback_data="playlist_confirm_no"),
            ]
        ]
    else:
        keyboard = [
            [
                InlineKeyboardButton(text="📁 Загрузить все видео", callback_data="playlist_all"),
                InlineKeyboardButton(text="🎬 Только первое видео", callback_data="playlist_first"),
            ],
            [
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"),
            ]
        ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def platform_keyboard(platform):
    """Клавиатура для Instagram/TikTok"""
    if platform == "instagram":
        text = "📸 Instagram"
    elif platform == "tiktok":
        text = "🎵 TikTok"
    else:
        text = platform.capitalize()
    
    keyboard = [
        [
            InlineKeyboardButton(text=f"🎬 Оригинальное качество", callback_data="original_quality"),
        ],
        [
            InlineKeyboardButton(text="📹 720p", callback_data="q:720"),
            InlineKeyboardButton(text="🎬 1080p", callback_data="q:1080"),
        ],
        [
            InlineKeyboardButton(text="🎧 Аудио", callback_data="audio"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)