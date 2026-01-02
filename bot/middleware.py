from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery
from config import ALLOWED_USERS

class PrivateMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user_id = event.from_user.id

        if ALLOWED_USERS and user_id not in ALLOWED_USERS:
            if isinstance(event, Message):
                await event.answer("🚫 У тебя нет доступа к этому боту.")
            elif isinstance(event, CallbackQuery):
                await event.answer("🚫 Нет доступа", show_alert=True)
            return

        return await handler(event, data)
