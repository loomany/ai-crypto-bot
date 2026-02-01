from aiogram.types import KeyboardButton, ReplyKeyboardMarkup


def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="₿ BTC (intraday)"), KeyboardButton(text="🤖 AI-сигналы")],
            [KeyboardButton(text="🧠 PRO-модули"), KeyboardButton(text="📊 Статистика")],
            [
                KeyboardButton(text="🔔 Включить уведомления"),
                KeyboardButton(text="🚫 Отключить уведомления"),
            ],
        ],
        resize_keyboard=True,
    )
