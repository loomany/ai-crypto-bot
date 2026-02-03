from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

from texts import admin_url


def main_menu_kb(is_admin: bool = False) -> ReplyKeyboardMarkup:
    """Главное меню. Для админа добавляем диагностику."""
    keyboard = [
        [KeyboardButton(text="🤖 AI-сигналы")],
        [KeyboardButton(text="⚡ Pump/Dump")],
        [KeyboardButton(text="📊 Статистика")],
        [KeyboardButton(text="ℹ️ Статус бота")],
    ]
    if is_admin:
        keyboard.append([KeyboardButton(text="🛠 Диагностика (админ)")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def ai_signals_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔔 Включить AI-уведомления",
                    callback_data="ai_notify_on",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🚫 Отключить AI-уведомления",
                    callback_data="ai_notify_off",
                )
            ],
            [InlineKeyboardButton(text="✉️ Написать админу", url=admin_url())],
        ]
    )


def pumpdump_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔔 Включить Pump/Dump-уведомления",
                    callback_data="pumpdump_notify_on",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🚫 Отключить Pump/Dump-уведомления",
                    callback_data="pumpdump_notify_off",
                )
            ],
            [InlineKeyboardButton(text="✉️ Написать админу", url=admin_url())],
        ]
    )


def stats_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1 день", callback_data="stats:1d"),
                InlineKeyboardButton(text="3 дня", callback_data="stats:3d"),
            ],
            [
                InlineKeyboardButton(text="7 дней", callback_data="stats:7d"),
                InlineKeyboardButton(text="Все время", callback_data="stats:all"),
            ],
        ]
    )
