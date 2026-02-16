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


def build_system_menu_kb(
    lang: str,
    is_admin: bool = False,
    inversion_enabled: bool = False,
) -> ReplyKeyboardMarkup:
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
            [
                KeyboardButton(
                    text=i18n.t(
                        lang,
                        "INVERSION_TOGGLE_BUTTON",
                        state=i18n.t(lang, "INVERSION_STATE_ON" if inversion_enabled else "INVERSION_STATE_OFF"),
                    )
                )
            ],
            [KeyboardButton(text=i18n.t(lang, "MENU_BACK"))],
        ]
    else:
        keyboard = [
            [
                KeyboardButton(text=i18n.t(lang, "SYS_DIAG")),
                KeyboardButton(text=i18n.t(lang, "SYS_PAY")),
                KeyboardButton(text=i18n.t(lang, "BTN_BOT_HOW_IT_WORKS")),
            ],
            [KeyboardButton(text=i18n.t(lang, "MENU_BACK"))],
        ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def build_about_bot_logic_back_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=i18n.t(lang, "MENU_BACK"), callback_data="about_back")],
        ]
    )


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
                InlineKeyboardButton(
                    text=i18n.t(lang, "BTN_ARCHIVE_AI"),
                    callback_data="stats_archive:ai",
                )
            ],
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "BTN_ARCHIVE_PD"),
                    callback_data="stats_archive:pd",
                )
            ],
        ]
    )


def stats_period_inline_kb(lang: str, archive_kind: str = "ai") -> InlineKeyboardMarkup:
    history_prefix = "history" if archive_kind == "ai" else "history_pd"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "PERIOD_1D"),
                    callback_data=f"{history_prefix}:1d:page=1",
                ),
                InlineKeyboardButton(
                    text=i18n.t(lang, "PERIOD_7D"),
                    callback_data=f"{history_prefix}:7d:page=1",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "PERIOD_30D"),
                    callback_data=f"{history_prefix}:30d:page=1",
                ),
                InlineKeyboardButton(
                    text=i18n.t(lang, "PERIOD_ALL"),
                    callback_data=f"{history_prefix}:all:page=1",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "MENU_BACK"),
                    callback_data="stats_root",
                )
            ],
        ]
    )


def build_about_inline_kb(
    lang: str,
    *,
    is_admin: bool = False,
    inversion_enabled: bool = False,
) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(text=i18n.t(lang, "SYS_PAY"), callback_data="sub_pay")],
        [InlineKeyboardButton(text=i18n.t(lang, "BTN_CONTACT_ADMIN"), callback_data="sub_contact")],
        [InlineKeyboardButton(text=i18n.t(lang, "BTN_BOT_HOW_IT_WORKS"), callback_data="about_bot_logic")],
    ]
    if is_admin:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=i18n.t(
                        lang,
                        "INVERSION_TOGGLE_BUTTON",
                        state=i18n.t(lang, "INVERSION_STATE_ON" if inversion_enabled else "INVERSION_STATE_OFF"),
                    ),
                    callback_data="admin_toggle_inversion",
                )
            ]
        )
    keyboard.append([InlineKeyboardButton(text=i18n.t(lang, "MENU_BACK"), callback_data="about_back")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


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
