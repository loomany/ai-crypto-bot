from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

def build_main_menu_kb(is_admin: bool = False) -> ReplyKeyboardMarkup:
    """Главное меню."""
    keyboard = [
        [
            KeyboardButton(text="🎯 AI-сигналы"),
            KeyboardButton(text="⚡ Pump / Dump"),
        ],
        [
            KeyboardButton(text="📊 Статистика"),
            KeyboardButton(text="ℹ️ О системе"),
        ],
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def build_system_menu_kb(is_admin: bool = False) -> ReplyKeyboardMarkup:
    """Системное меню."""
    if is_admin:
        keyboard = [
            [
                KeyboardButton(text="🛰 Статус системы"),
                KeyboardButton(text="🧪 Диагностика (админ)"),
            ],
            [
                KeyboardButton(text="👥 Пользователи"),
                KeyboardButton(text="💳 Оплатить подписку"),
            ],
            [KeyboardButton(text="⬅️ Назад")],
        ]
    else:
        keyboard = [
            [
                KeyboardButton(text="🧪 Диагностика"),
                KeyboardButton(text="💳 Оплатить подписку"),
            ],
            [KeyboardButton(text="⬅️ Назад")],
        ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def build_admin_diagnostics_kb() -> ReplyKeyboardMarkup:
    keyboard = [
        [
            KeyboardButton(text="🧪 Тест AI (всем)"),
            KeyboardButton(text="🧪 Тест Pump/Dump (всем)"),
        ],
        [KeyboardButton(text="⬅️ Назад")],
    ]
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
        ]
    )


def stats_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1 день", callback_data="history:1d"),
                InlineKeyboardButton(text="7 дней", callback_data="history:7d"),
            ],
            [
                InlineKeyboardButton(text="30 дней", callback_data="history:30d"),
                InlineKeyboardButton(text="Все время", callback_data="history:all"),
            ],
        ]
    )


def build_about_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить подписку", callback_data="sub_pay")],
            [InlineKeyboardButton(text="💬 Связь с админом", callback_data="sub_contact")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="about_back")],
        ]
    )


def build_offer_inline_kb(back_callback: str = "system_back") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Принять", callback_data="sub_accept")],
            [InlineKeyboardButton(text="💬 Связь с админом", callback_data="sub_contact")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)],
        ]
    )


def build_payment_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Скопировать адрес", callback_data="sub_copy_address")],
            [InlineKeyboardButton(text="📎 Отправить чек + ID", callback_data="sub_send_receipt")],
            [InlineKeyboardButton(text="💬 Связь с админом", callback_data="sub_contact")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="sub_pay_back")],
        ]
    )
