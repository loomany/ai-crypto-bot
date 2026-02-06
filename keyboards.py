from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

import i18n

def build_main_menu_kb(lang: str, is_admin: bool = False) -> ReplyKeyboardMarkup:
    """Главное меню."""
    keyboard = [
        [
            KeyboardButton(text=i18n.t(lang, "MENU_AI")),
            KeyboardButton(text=i18n.t(lang, "MENU_PD")),
        ],
        [
            KeyboardButton(text=i18n.t(lang, "MENU_STATS")),
            KeyboardButton(text=i18n.t(lang, "MENU_SYSTEM")),
        ],
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def build_system_menu_kb(lang: str, is_admin: bool = False) -> ReplyKeyboardMarkup:
    """Системное меню."""
    if is_admin:
        keyboard = [
            [
                KeyboardButton(text=i18n.t(lang, "SYS_STATUS")),
                KeyboardButton(text=i18n.t(lang, "SYS_DIAG_ADMIN")),
            ],
            [
                KeyboardButton(text=i18n.t(lang, "SYS_USERS")),
                KeyboardButton(text=i18n.t(lang, "SYS_PAY")),
            ],
            [KeyboardButton(text=i18n.t(lang, "MENU_BACK"))],
        ]
    else:
        keyboard = [
            [
                KeyboardButton(text=i18n.t(lang, "SYS_DIAG")),
                KeyboardButton(text=i18n.t(lang, "SYS_PAY")),
            ],
            [KeyboardButton(text=i18n.t(lang, "MENU_BACK"))],
        ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def build_admin_diagnostics_kb(lang: str) -> ReplyKeyboardMarkup:
    keyboard = [
        [
            KeyboardButton(text=i18n.t(lang, "SYS_TEST_AI")),
            KeyboardButton(text=i18n.t(lang, "SYS_TEST_PD")),
        ],
        [KeyboardButton(text=i18n.t(lang, "MENU_BACK"))],
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def ai_signals_inline_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "BTN_AI_ON"),
                    callback_data="ai_notify_on",
                )
            ],
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "BTN_AI_OFF"),
                    callback_data="ai_notify_off",
                )
            ],
        ]
    )


def pumpdump_inline_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "BTN_PD_ON"),
                    callback_data="pumpdump_notify_on",
                )
            ],
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "BTN_PD_OFF"),
                    callback_data="pumpdump_notify_off",
                )
            ],
        ]
    )


def stats_inline_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=i18n.t(lang, "PERIOD_1D"), callback_data="hist:1d:0"),
                InlineKeyboardButton(text=i18n.t(lang, "PERIOD_7D"), callback_data="hist:7d:0"),
            ],
            [
                InlineKeyboardButton(text=i18n.t(lang, "PERIOD_30D"), callback_data="hist:30d:0"),
                InlineKeyboardButton(text=i18n.t(lang, "PERIOD_ALL"), callback_data="hist:all:0"),
            ],
        ]
    )


def build_about_inline_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=i18n.t(lang, "SYS_PAY"), callback_data="sub_pay")],
            [InlineKeyboardButton(text=i18n.t(lang, "BTN_CONTACT_ADMIN"), callback_data="sub_contact")],
            [InlineKeyboardButton(text=i18n.t(lang, "MENU_BACK"), callback_data="about_back")],
        ]
    )


def build_offer_inline_kb(lang: str, back_callback: str = "system_back") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=i18n.t(lang, "BTN_ACCEPT"), callback_data="sub_accept")],
            [InlineKeyboardButton(text=i18n.t(lang, "BTN_CONTACT_ADMIN"), callback_data="sub_contact")],
            [InlineKeyboardButton(text=i18n.t(lang, "MENU_BACK"), callback_data=back_callback)],
        ]
    )


def build_payment_inline_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=i18n.t(lang, "BTN_COPY_ADDRESS"), callback_data="sub_copy_address")],
            [InlineKeyboardButton(text=i18n.t(lang, "BTN_SEND_RECEIPT"), callback_data="sub_send_receipt")],
            [InlineKeyboardButton(text=i18n.t(lang, "BTN_CONTACT_ADMIN"), callback_data="sub_contact")],
            [InlineKeyboardButton(text=i18n.t(lang, "MENU_BACK"), callback_data="sub_pay_back")],
        ]
    )


def build_lang_select_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=i18n.t("ru", "LANG_RU"), callback_data="lang:ru"),
                InlineKeyboardButton(text=i18n.t("en", "LANG_EN"), callback_data="lang:en"),
            ]
        ]
    )
