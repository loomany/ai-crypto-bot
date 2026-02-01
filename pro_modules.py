from aiogram import Router, F
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)

from pro_db import pro_get_expires, pro_is
from keyboards import main_menu_keyboard
from texts import (
    PRO_MODULES_TEXT,
    PRO_BUY_TEXT,
    PRO_PAY_TEXT,
    admin_url,
)

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


def pro_modules_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Купить PRO", callback_data="pro_buy")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")],
        ]
    )


def pro_buy_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить PRO", callback_data="pro_pay")],
            [InlineKeyboardButton(text="✉️ Написать админу", url=admin_url())],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="pro_back")],
        ]
    )


def pro_pay_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✉️ Написать админу", url=admin_url())],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="pro_buy")],
        ]
    )


# ============================================================
# Хендлеры меню PRO-модулей
# ============================================================


@router.message(F.text == "🧠 PRO-модули")
async def open_pro_menu(message: Message):
    await message.answer(PRO_MODULES_TEXT, reply_markup=pro_modules_keyboard())


@router.callback_query(F.data == "pro_buy")
async def show_pro_buy(callback: CallbackQuery):
    if callback.message:
        await callback.message.edit_text(PRO_BUY_TEXT, reply_markup=pro_buy_keyboard())
    await callback.answer()


@router.callback_query(F.data == "pro_pay")
async def show_pro_pay(callback: CallbackQuery):
    if callback.message:
        await callback.message.edit_text(PRO_PAY_TEXT, reply_markup=pro_pay_keyboard())
    await callback.answer()


@router.callback_query(F.data == "pro_back")
async def back_to_pro_modules(callback: CallbackQuery):
    if callback.message:
        await callback.message.edit_text(
            PRO_MODULES_TEXT,
            reply_markup=pro_modules_keyboard(),
        )
    await callback.answer()


@router.callback_query(F.data == "back_main")
async def back_to_main(callback: CallbackQuery):
    if callback.message:
        await callback.message.answer(
            "Выберите раздел ⬇️",
            reply_markup=main_menu_keyboard(),
        )
    await callback.answer()


@router.message(F.text == "✅ Включить PRO-уведомления")
async def enable_pro_notifications(message: Message):
    if not pro_is(message.chat.id):
        await message.answer(
            "⚠️ PRO не активен. Для доступа напишите администратору.",
            reply_markup=get_pro_keyboard(),
        )
        return
    expires = pro_get_expires(message.chat.id)
    await message.answer(
        f"✅ PRO активен до {expires}. Уведомления включены автоматически.",
        reply_markup=get_pro_keyboard(),
    )


@router.message(F.text == "❌ Отключить PRO-уведомления")
async def disable_pro_notifications(message: Message):
    await message.answer(
        "⚠️ Отключение PRO возможно только через администратора.",
        reply_markup=get_pro_keyboard(),
    )
