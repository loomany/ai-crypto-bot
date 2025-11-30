import asyncio
import os

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.filters import CommandStart
from dotenv import load_dotenv


# ===== ЗАГРУЖАЕМ НАСТРОЙКИ =====

def load_settings() -> str:
    load_dotenv()
    bot_token = os.getenv("BOT_TOKEN")

    if not bot_token:
        raise ValueError("Нет BOT_TOKEN в .env файле")

    return bot_token


# ===== КНОПКИ МЕНЮ =====

def main_menu_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [
            KeyboardButton(text="📊 Анализ монеты"),
            KeyboardButton(text="🎯 AI-сигналы"),
        ],
        [
            KeyboardButton(text="🚀 Pump Detector"),
            KeyboardButton(text="🧠 ML прогноз"),
        ],
        [
            KeyboardButton(text="⚠️ Безопасность сделки"),
            KeyboardButton(text="ℹ️ Обучение терминам"),
        ],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


# ===== СОЗДАЁМ БОТА =====

BOT_TOKEN = load_settings()
bot = Bot(BOT_TOKEN)
dp = Dispatcher()


# ===== ХЭНДЛЕРЫ =====

@dp.message(CommandStart())
async def cmd_start(message: Message):
    text = (
        "Привет! Я будущий AI-крипто бот 🚀\n\n"
        "Сейчас я в режиме разработки. Меню уже работает.\n"
        "Нажми кнопку ниже 👇"
    )
    await message.answer(text, reply_markup=main_menu_keyboard())


@dp.message(F.text == "📊 Анализ монеты")
async def analyze_coin(message: Message):
    await message.answer("Здесь будет анализ монеты (цена, тренд, RSI). Пока заглушка.")


@dp.message(F.text == "🎯 AI-сигналы")
async def ai_signals(message: Message):
    await message.answer("Здесь будут AI-сигналы (Buy/Sell, TP/SL).")


@dp.message(F.text == "🚀 Pump Detector")
async def pump_detector(message: Message):
    await message.answer("Здесь будет Pump Detector.")


@dp.message(F.text == "🧠 ML прогноз")
async def ml_forecast(message: Message):
    await message.answer("Здесь будет ML-прогноз на 1ч/4ч/сутки.")


@dp.message(F.text == "⚠️ Безопасность сделки")
async def safety(message: Message):
    await message.answer("Здесь будет риск-менеджмент и подсказки.")


@dp.message(F.text == "ℹ️ Обучение терминам")
async def education(message: Message):
    await message.answer("Здесь будет справочник: RSI, MACD, orderflow и т.д.")


@dp.message()
async def fallback(message: Message):
    await message.answer("Нажми кнопку в меню — пока я понимаю только их.")


# ===== ТОЧКА ВХОДА =====

async def main():
    print("Бот запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
