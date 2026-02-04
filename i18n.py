from __future__ import annotations

from typing import Any


_TRANSLATIONS: dict[str, dict[str, str]] = {
    "ru": {
        "START_TEXT": (
            "Включи уведомления ниже — бот работает автоматически.\n\n"
            "Тебе доступно бесплатно:\n"
            "• 7 AI-сигналов\n"
            "• 7 Pump/Dump сигналов\n\n"
            "После исчерпания лимита потребуется оформить подписку,\n"
            "чтобы продолжить получать сигналы.\n\n"
            "После включения просто жди — сигналы придут автоматически."
        ),
        "AI_SIGNALS_TEXT": (
            "🤖 AI-сигналы — это готовые торговые сценарии по рынку (LONG/SHORT).\n\n"
            "Как бот находит сетап:\n"
            "• смотрит тренд и структуру на 1D/4H/1H\n"
            "• уточняет контекст по BTC (направление рынка)\n"
            "• ищет зоны интереса (POI) по локальным high/low (15m)\n"
            "• проверяет подтверждение на 15m и 5m (импульс/слабость)\n"
            "• фильтрует по объёму (spike/ratio) и волатильности (ATR)\n"
            "• добавляет тех.фильтры (EMA/RSI/дивергенции) и паттерны\n\n"
            "Таймфреймы анализа: 1D / 4H / 1H / 15m / 5m.\n\n"
            "Формат сигнала:\n"
            "• направление\n"
            "• зона входа (POI)\n"
            "• уровень отмены\n"
            "• цели движения\n\n"
            "🔔 Авто-сигналы включаются кнопками ниже."
        ),
        "PUMPDUMP_TEXT": (
            "⚡ Pump/Dump — это быстрые алерты о резких движениях цены и объёма.\n\n"
            "Как бот находит импульсы:\n"
            "• сканирует рынок по USDT-парам\n"
            "• ищет резкое движение цены за 1m/5m\n"
            "• проверяет всплеск объёма (volume spike)\n"
            "• отсекает слабые движения по фильтрам ликвидности\n"
            "• защищает от спама: повтор по одной монете не чаще N минут\n\n"
            "Таймфреймы анализа: 1m / 5m.\n"
            "Формат алерта:\n"
            "• монета и направление (PUMP или DUMP)\n"
            "• изменение за 1m и 5m\n"
            "• объёмный всплеск\n"
            "• ссылка/контекст (по желанию)\n\n"
            "🔔 Авто-алерты включаются кнопками ниже."
        ),
        "MENU_AI": "🎯 AI-сигналы",
        "MENU_PD": "⚡ Pump / Dump",
        "MENU_STATS": "📊 Статистика",
        "MENU_SYSTEM": "ℹ️ О системе",
        "MENU_BACK": "⬅️ Назад",
        "SYS_STATUS": "🛰 Статус системы",
        "SYS_DIAG_ADMIN": "🧪 Диагностика (админ)",
        "SYS_TEST_AI": "🧪 Тест AI (всем)",
        "SYS_TEST_PD": "🧪 Тест Pump/Dump (всем)",
        "SYS_USERS": "👥 Пользователи",
        "SYS_PAY": "💳 Оплатить подписку",
        "SYS_DIAG": "🧪 Диагностика",
        "BTN_AI_ON": "🔔 Включить AI-уведомления",
        "BTN_AI_OFF": "🚫 Отключить AI-уведомления",
        "BTN_PD_ON": "🔔 Включить Pump/Dump-уведомления",
        "BTN_PD_OFF": "🚫 Отключить Pump/Dump-уведомления",
        "PERIOD_1D": "1 день",
        "PERIOD_7D": "7 дней",
        "PERIOD_30D": "30 дней",
        "PERIOD_ALL": "Все время",
        "OFFER_TEXT": (
            "🧾 Оферта на подписку (30 дней)\n\n"
            "1) Подписка предоставляет доступ к функционалу бота и сигналам сроком на 30 (тридцать) дней с момента активации.\n"
            "2) Сигналы и аналитика не гарантируют прибыль и могут приводить к убыткам.\n"
            "3) Бот не является финансовым консультантом и не дает персональных рекомендаций “войти/выйти”. "
            "Материалы носят информационный характер.\n"
            "4) Сценарии формируются при помощи AI-аналитики, которая автоматически сканирует рынок по алгоритмам "
            "технического анализа (свечи/объёмы/волатильность/фильтры качества), близким к подходу профессионального трейдера.\n"
            "5) Вся ответственность за торговые решения и риски полностью лежит на пользователе.\n"
            "6) Оплата подписки означает согласие с условиями оферты.\n"
            "7) Оплата принимается только в TRX (сеть TRON).\n\n"
            "Нажимая «✅ Принять», вы подтверждаете согласие с условиями."
        ),
        "PAYMENT_TEXT_TRX": (
            "💳 Оплата подписки (30 дней)\n"
            "Стоимость: $39 в эквиваленте TRX (по текущему курсу на момент оплаты)\n\n"
            "✅ Оплата принимается только TRX (TRON) на адрес:\n"
            "{wallet}\n\n"
            "Ваш ID (укажите в комментарии/чеке):\n"
            "{user_id}\n\n"
            "После оплаты нажмите «📎 Отправить чек + ID»."
        ),
        "PAYWALL_AI": (
            "🔒 Доступ к AI-сигналам по подписке.\n"
            "Нажми «Купить подписку» — покажу инструкцию."
        ),
        "PAYWALL_PD": (
            "🔒 Доступ к Pump/Dump сигналам по подписке.\n"
            "Нажми «Купить подписку» — покажу инструкцию."
        ),
        "TRIAL_SUFFIX_AI": "\n\n🎁 Осталось {left}/{limit} бесплатных AI-сигналов",
        "TRIAL_SUFFIX_PD": "\n\n🎁 Осталось {left}/{limit} бесплатных Pump/Dump сигналов",
        "LANG_PICK_TEXT": "Выберите язык:",
        "LANG_RU": "🇷🇺 Русский",
        "LANG_EN": "🇬🇧 English",
        "BTN_ACCEPT": "✅ Принять",
        "BTN_CONTACT_ADMIN": "💬 Связь с админом",
        "BTN_COPY_ADDRESS": "📋 Скопировать адрес",
        "BTN_SEND_RECEIPT": "📎 Отправить чек + ID",
        "BTN_BUY_SUB": "💳 Купить подписку",
        "STATUS_LABEL": "Статус",
        "STATUS_ON": "✅ включено",
        "STATUS_OFF": "⛔ выключено",
        "STATS_PICK_TEXT": "📊 История сигналов\nВыбери период:",
        "SYSTEM_SECTION_TEXT": "ℹ️ Раздел: О системе",
        "BACK_TO_MAIN_TEXT": "Возвращаемся в главное меню.",
    },
    "en": {
        "START_TEXT": (
            "Enable notifications below — the bot runs automatically.\n\n"
            "Free access:\n"
            "• 7 AI signals\n"
            "• 7 Pump/Dump signals\n\n"
            "When the limit is reached, you’ll need a subscription to continue receiving signals.\n\n"
            "After enabling, just wait — signals will arrive automatically."
        ),
        "AI_SIGNALS_TEXT": (
            "🤖 AI signals are ready-made market scenarios (LONG/SHORT).\n\n"
            "How the bot finds setups:\n"
            "• checks trend and structure on 1D/4H/1H\n"
            "• refines the context via BTC (market direction)\n"
            "• finds points of interest (POI) via local high/low (15m)\n"
            "• verifies confirmation on 15m and 5m (impulse/weakness)\n"
            "• filters by volume (spike/ratio) and volatility (ATR)\n"
            "• adds technical filters (EMA/RSI/divergences) and patterns\n\n"
            "Timeframes: 1D / 4H / 1H / 15m / 5m.\n\n"
            "Signal format:\n"
            "• direction\n"
            "• entry zone (POI)\n"
            "• invalidation level\n"
            "• targets\n\n"
            "🔔 Auto-signals are enabled with the buttons below."
        ),
        "PUMPDUMP_TEXT": (
            "⚡ Pump/Dump are fast alerts about sharp price and volume moves.\n\n"
            "How the bot finds impulses:\n"
            "• scans the market across USDT pairs\n"
            "• detects sharp price moves in 1m/5m\n"
            "• checks volume spikes\n"
            "• filters weak moves with liquidity filters\n"
            "• anti-spam: repeat per coin not more often than N minutes\n\n"
            "Timeframes: 1m / 5m.\n"
            "Alert format:\n"
            "• coin and direction (PUMP or DUMP)\n"
            "• change over 1m and 5m\n"
            "• volume spike\n"
            "• link/context (optional)\n\n"
            "🔔 Auto-alerts are enabled with the buttons below."
        ),
        "MENU_AI": "🎯 AI signals",
        "MENU_PD": "⚡ Pump / Dump",
        "MENU_STATS": "📊 Stats",
        "MENU_SYSTEM": "ℹ️ System",
        "MENU_BACK": "⬅️ Back",
        "SYS_STATUS": "🛰 System status",
        "SYS_DIAG_ADMIN": "🧪 Diagnostics (admin)",
        "SYS_TEST_AI": "🧪 Test AI (all)",
        "SYS_TEST_PD": "🧪 Test Pump/Dump (all)",
        "SYS_USERS": "👥 Users",
        "SYS_PAY": "💳 Buy subscription",
        "SYS_DIAG": "🧪 Diagnostics",
        "BTN_AI_ON": "🔔 Enable AI notifications",
        "BTN_AI_OFF": "🚫 Disable AI notifications",
        "BTN_PD_ON": "🔔 Enable Pump/Dump notifications",
        "BTN_PD_OFF": "🚫 Disable Pump/Dump notifications",
        "PERIOD_1D": "1 day",
        "PERIOD_7D": "7 days",
        "PERIOD_30D": "30 days",
        "PERIOD_ALL": "All time",
        "OFFER_TEXT": (
            "🧾 Subscription offer (30 days)\n\n"
            "1) The subscription grants access to the bot functionality and signals for 30 days from activation.\n"
            "2) Signals and analytics do not guarantee profit and may result in losses.\n"
            "3) The bot is not a financial advisor and does not provide personalized recommendations to enter/exit. "
            "Materials are for informational purposes only.\n"
            "4) Scenarios are generated using AI analytics that automatically scan the market using technical analysis "
            "(candles/volume/volatility/quality filters) similar to a professional trader’s approach.\n"
            "5) All responsibility for trading decisions and risks lies with the user.\n"
            "6) Subscription payment means acceptance of the offer terms.\n"
            "7) Payments are accepted only in TRX (TRON network).\n\n"
            "By clicking «✅ Accept», you confirm agreement with the terms."
        ),
        "PAYMENT_TEXT_TRX": (
            "💳 Subscription payment (30 days)\n"
            "Price: $39 in TRX equivalent (at current rate at time of payment)\n\n"
            "✅ Payment accepted only in TRX (TRON) to the address:\n"
            "{wallet}\n\n"
            "Your ID (include in comment/receipt):\n"
            "{user_id}\n\n"
            "After payment tap «📎 Send receipt + ID»."
        ),
        "PAYWALL_AI": (
            "🔒 AI signals are available by subscription.\n"
            "Tap “Buy subscription” — I’ll show the instructions."
        ),
        "PAYWALL_PD": (
            "🔒 Pump/Dump alerts are available by subscription.\n"
            "Tap “Buy subscription” — I’ll show the instructions."
        ),
        "TRIAL_SUFFIX_AI": "\n\n🎁 {left}/{limit} free AI signals left",
        "TRIAL_SUFFIX_PD": "\n\n🎁 {left}/{limit} free Pump/Dump signals left",
        "LANG_PICK_TEXT": "Please choose a language:",
        "LANG_RU": "🇷🇺 Русский",
        "LANG_EN": "🇬🇧 English",
        "BTN_ACCEPT": "✅ Accept",
        "BTN_CONTACT_ADMIN": "💬 Contact admin",
        "BTN_COPY_ADDRESS": "📋 Copy address",
        "BTN_SEND_RECEIPT": "📎 Send receipt + ID",
        "BTN_BUY_SUB": "💳 Buy subscription",
        "STATUS_LABEL": "Status",
        "STATUS_ON": "✅ enabled",
        "STATUS_OFF": "⛔ disabled",
        "STATS_PICK_TEXT": "📊 Signal history\nChoose a period:",
        "SYSTEM_SECTION_TEXT": "ℹ️ Section: System",
        "BACK_TO_MAIN_TEXT": "Returning to the main menu.",
    },
}


def normalize_lang(lang: str | None) -> str:
    if not lang:
        return "ru"
    raw = lang.strip().lower()
    if raw.startswith("en"):
        return "en"
    if raw.startswith("ru"):
        return "ru"
    return "ru"


def t(lang: str | None, key: str, **fmt: Any) -> str:
    lang_code = normalize_lang(lang)
    lang_dict = _TRANSLATIONS.get(lang_code, {})
    value = lang_dict.get(key) or _TRANSLATIONS.get("ru", {}).get(key) or key
    if fmt:
        return value.format(**fmt)
    return value


def all_labels(key: str) -> list[str]:
    return [t("ru", key), t("en", key)]
