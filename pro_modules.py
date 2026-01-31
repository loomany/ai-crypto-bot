from aiogram import Router, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton

from pro_db import pro_add, pro_remove

router = Router(name="pro_modules")


# ============================================================
# Клавиатура PRO-модулей
# ============================================================


def get_pro_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="✅ Включить PRO-уведомления")],
        [KeyboardButton(text="❌ Отключить PRO-уведомления")],
        [KeyboardButton(text="⬅️ Главное меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def pro_menu_text() -> str:
    return (
        "🧠 Что входит в PRO:\n\n"
        "🚀 Pump/Dump Scanner (быстрые импульсы/сливы)\n"
        "🐳 Whale Flow Scanner (дайджест по всем USDT-M фьючам)\n"
        "🎯 PRO AI-сигналы (2–4 сильных сетапа в день по score)\n\n"
        "Выбери действие ниже 👇"
    )


# ============================================================
# Хендлеры меню PRO-модулей
# ============================================================


@router.message(F.text == "🧠 PRO-модули")
async def open_pro_menu(message: Message):
    await message.answer(pro_menu_text(), reply_markup=get_pro_keyboard())


@router.message(F.text == "✅ Включить PRO-уведомления")
async def enable_pro_notifications(message: Message):
    changed = pro_add(message.chat.id)
    await message.answer(
        "✅ PRO-уведомления включены. Теперь ты получаешь Pump/Dump, Whale Flow и PRO AI-сигналы."
        if changed
        else "✅ PRO-уведомления уже активны.",
        reply_markup=get_pro_keyboard(),
    )


@router.message(F.text == "❌ Отключить PRO-уведомления")
async def disable_pro_notifications(message: Message):
    changed = pro_remove(message.chat.id)
    await message.answer(
        "❌ PRO-уведомления отключены."
        if changed
        else "✅ PRO-уведомления уже были отключены.",
        reply_markup=get_pro_keyboard(),
    )
