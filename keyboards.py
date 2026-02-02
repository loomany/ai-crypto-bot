from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

from texts import admin_url


def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="₿ BTC (intraday)"), KeyboardButton(text="🤖 AI-сигналы")],
            [KeyboardButton(text="⚡ Pump/Dump")],
        ],
        resize_keyboard=True,
    )


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


def btc_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔔 Включить BTC-уведомления",
                    callback_data="btc_notify_on",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🚫 Отключить BTC-уведомления",
                    callback_data="btc_notify_off",
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
