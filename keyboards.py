from aiogram.types import KeyboardButton, ReplyKeyboardMarkup


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [
            KeyboardButton(text="₿ BTC (intraday)"),
            KeyboardButton(text="🤖 AI-сигналы"),
        ],
        [
            KeyboardButton(text="🧠 PRO-модули"),
            KeyboardButton(text="📊 Статистика"),
        ],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
