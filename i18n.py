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
        "SCORE_EXPLANATION": (
            "ℹ️ Чем выше Score — тем чаще сигнал \"отрабатывает\".\n\n"
            "TP1: {tp1}\n"
            "👉 Сигнал дал прибыль и закрылся в плюс.\n"
            "BE: {be}\n"
            "👉 Сигнал ушёл в безубыток — риск снят.\n"
            "SL: {sl}\n"
            "👉 Сигнал закрылся по стоп-лоссу.\n"
            "EXP: {exp}\n"
            "👉 Прошло 12 часов после активации — сценарий устарел.\n"
            "NF: {nf}\n"
            "👉 Прошло 12 часов, цена не дошла до входа."
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
            "• сканирует рынок по торговым парам\n"
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
        "MENU_BACK": "◀️ Назад",
        "SYS_STATUS": "🧠 Статус анализа рынка",
        "SYS_DIAG_ADMIN": "🧪 Диагностика (админ)",
        "SYS_TEST_AI": "🧪 Тест AI (всем)",
        "SYS_TEST_PD": "🧪 Тест Pump/Dump (всем)",
        "SYS_USERS": "👥 Пользователи",
        "SYS_CHANNEL_PANEL": "📣 Телеграм канал",
        "SYS_PAY": "💳 Оплатить подписку",
        "SYS_HOW_BOT_WORKS": "🧠 Как работает бот",
        "INVERSION_TOGGLE_BUTTON": "🔁 Инверсия: {state}",
        "INVERSION_STATE_ON": "ВКЛ",
        "INVERSION_STATE_OFF": "ВЫКЛ",
        "INVERSION_ENABLED_ALERT": "Инверсия включена ✅",
        "INVERSION_DISABLED_ALERT": "Инверсия выключена ❌",
        "SYS_DIAG": "🧪 Диагностика",
        "CHANNEL_PANEL_TITLE": "📣 Telegram Channel Panel",
        "CHANNEL_PANEL_ID_LINE": "• channel_id: {channel_id}",
        "CHANNEL_PANEL_ENABLED_LINE": "• public_enabled: {enabled}",
        "CHANNEL_PANEL_NOTE": "• note: bot must be admin in channel with post rights",
        "CHANNEL_TEST_ENTRY": "🧪 Тест: ENTRY",
        "CHANNEL_TEST_FIX8": "🧪 Тест: FIX +8% (30%)",
        "CHANNEL_TEST_FIX10": "🧪 Тест: FIX +10% (30%)",
        "CHANNEL_TEST_EXIT_TP": "🧪 Тест: EXIT TP",
        "CHANNEL_TEST_EXIT_SL": "🧪 Тест: EXIT SL",
        "CHANNEL_TEST_EXIT_BE": "🧪 Тест: EXIT BE",
        "CHANNEL_TEST_STATUS": "📊 Тест: STATUS",
        "CHANNEL_TEST_RESET_BALANCE": "🔄 Reset TEST balance",
        "CHANNEL_TEST_OK": "✅ Отправлено в канал",
        "CHANNEL_TEST_DISABLED": "⚠️ AI_PUBLIC_ENABLED=0 (включи)",
        "CHANNEL_TEST_NO_ID": "⚠️ TELEGRAM_CHANNEL_ID не задан",
        "ADMIN_ONLY": "⛔ Только для админа",
        "SYSTEM_STATUS_TITLE": "🧠 Статус анализа рынка",
        "SYSTEM_STATUS_BINANCE_LINE": "🔌 Связь с Binance: {status}",
        "SYSTEM_STATUS_LAST_CYCLE_LINE": "⏱ последний цикл анализа: {seconds} сек назад",
        "SYSTEM_STATUS_CONN_OK": "OK",
        "SYSTEM_STATUS_CONN_WARN": "WARN",
        "SYSTEM_STATUS_CONN_ERROR": "ERROR",
        "SYSTEM_STATUS_SECTION_MARKET": "📊 Контекст рынка:",
        "SYSTEM_STATUS_MARKET_STATE_LINE": "• состояние: {state}",
        "SYSTEM_STATUS_MARKET_PRIORITY_LINE": "• приоритет: {priority}",
        "SYSTEM_STATUS_MARKET_ACTIVITY_LINE": "• активность: {activity}",
        "SYSTEM_STATUS_MARKET_STATE_DOWN": "флет / давление вниз",
        "SYSTEM_STATUS_MARKET_STATE_UP": "восходящий",
        "SYSTEM_STATUS_MARKET_STATE_NEUTRAL": "нейтральный / флет",
        "SYSTEM_STATUS_MARKET_PRIORITY_SHORT": "SHORT",
        "SYSTEM_STATUS_MARKET_PRIORITY_LONG": "LONG",
        "SYSTEM_STATUS_MARKET_PRIORITY_SELECTIVE": "выборочные сделки",
        "SYSTEM_STATUS_MARKET_ACTIVITY_MODERATE": "умеренная",
        "SYSTEM_STATUS_MARKET_ACTIVITY_LOW": "низкая (фильтрация)",
        "SYSTEM_STATUS_SECTION_AI": "🎯 AI-анализ (реальное время):",
        "SYSTEM_STATUS_MARKET_COVERAGE_LINE": "• охват рынка: {count} монет",
        "SYSTEM_STATUS_MARKET_CYCLE_LINE": "• анализ за цикл: {count}",
        "SYSTEM_STATUS_SAFE_MODE_LINE": "• режим защиты: {mode}",
        "SYSTEM_STATUS_SAFE_MODE_ON": "SAFE (адаптивная нагрузка)",
        "SYSTEM_STATUS_SAFE_MODE_OFF": "стандартный",
        "SYSTEM_STATUS_SECTION_FILTERING": "🧪 Фильтрация сценариев (текущий цикл):",
        "SYSTEM_STATUS_PRESCORE_CHECKED_LINE": "• рассмотрено сценариев: {count}",
        "SYSTEM_STATUS_PRESCORE_PASSED_LINE": "• соответствуют условиям: {count}",
        "SYSTEM_STATUS_PRESCORE_FILTERED_LINE": "• отклонены по риску/структуре: {count}",
        "SYSTEM_STATUS_SIGNALS_SENT_LINE": "• сигналов отправлено: {count}{suffix}",
        "SYSTEM_STATUS_SIGNALS_SENT_NONE": "(нет подтверждения)",
        "SYSTEM_STATUS_SECTION_LAST_SIGNAL": "📉 Последний подтверждённый сценарий:",
        "SYSTEM_STATUS_LAST_SIGNAL_LINE": "{symbol} — {side} | {datetime}",
        "SYSTEM_STATUS_LAST_SIGNAL_NONE": "— если сигналов не было",
        "SYSTEM_STATUS_SECTION_PUMP": "⚡ Pump / Dump монитор:",
        "SYSTEM_STATUS_PUMP_STATUS_LINE": "• статус: {status}",
        "SYSTEM_STATUS_PUMP_IMPULSE_LINE": "• импульсы без подтверждения: игнорируются",
        "SYSTEM_STATUS_BINANCE_ACTIVE": "активна",
        "SYSTEM_STATUS_BINANCE_DOWN": "нет связи",
        "SYSTEM_STATUS_MARKET_RISK_OFF": "осторожный (приоритет SHORT)",
        "SYSTEM_STATUS_MARKET_NEUTRAL": "нейтральный",
        "SYSTEM_STATUS_MARKET_RISK_ON": "бычий (приоритет LONG)",
        "SYSTEM_STATUS_MARKET_AUTO": "по рынку",
        "SYSTEM_STATUS_CONTEXT_STATE_RISK_OFF": "флет / давление вниз",
        "SYSTEM_STATUS_CONTEXT_STATE_RISK_ON": "рост / импульс вверх",
        "SYSTEM_STATUS_CONTEXT_STATE_NEUTRAL": "флет / смешанный",
        "SYSTEM_STATUS_CONTEXT_STATE_AUTO": "по рынку",
        "SYSTEM_STATUS_CONTEXT_DIRECTION_RISK_OFF": "SHORT",
        "SYSTEM_STATUS_CONTEXT_DIRECTION_RISK_ON": "LONG",
        "SYSTEM_STATUS_CONTEXT_DIRECTION_NEUTRAL": "по рынку",
        "SYSTEM_STATUS_CONTEXT_DIRECTION_AUTO": "по рынку",
        "SYSTEM_STATUS_CONTEXT_ACTIVITY_RISK_OFF": "выборочная",
        "SYSTEM_STATUS_CONTEXT_ACTIVITY_RISK_ON": "активная",
        "SYSTEM_STATUS_CONTEXT_ACTIVITY_NEUTRAL": "умеренная",
        "SYSTEM_STATUS_CONTEXT_ACTIVITY_AUTO": "умеренная",
        "SYSTEM_STATUS_SAFE_MODE_ON": "активный режим защиты (SAFE)",
        "SYSTEM_STATUS_SAFE_MODE_OFF": "режим защиты: обычный",
        "SYSTEM_STATUS_PUMP_IMPULSE_MOST": "большинство",
        "SYSTEM_STATUS_PUMP_IMPULSE_SOME": "значительная часть",
        "SYSTEM_STATUS_PUMP_IMPULSE_FEW": "меньшинство",
        "SYSTEM_STATUS_PUMP_IMPULSE_UNKNOWN": "нет данных",
        "SYSTEM_STATUS_SIGNALS_PENDING": "ожидаю подтверждение",
        "SYSTEM_STATUS_SIGNALS_RUNNING": "работаю",
        "SYSTEM_STATUS_SIGNALS_PAUSED": "пауза/ошибка",
        "SYSTEM_STATUS_PUMP_ACTIVE": "активен",
        "SYSTEM_STATUS_PUMP_PAUSED": "пауза/ошибка",
        "BTN_AI_ON": "🔔 Включить AI-уведомления",
        "BTN_AI_OFF": "🚫 Отключить AI-уведомления",
        "BTN_PD_ON": "🔔 Включить Pump/Dump-уведомления",
        "BTN_PD_OFF": "🚫 Отключить Pump/Dump-уведомления",
        "PERIOD_1D": "1 день",
        "PERIOD_7D": "7 дней",
        "PERIOD_30D": "30 дней",
        "PERIOD_ALL": "Все время",
        "OFFER_TEXT": (
            "30 Дней - 39$\n"
            "Пожизненный - 299$"
        ),
        "PAYMENT_PICK_PLAN_TEXT": "Выберите план для оплаты в USDT:",
        "PAYMENT_INVOICE_TEXT": "💳 Оплата подписки Krypton AI\nПлан: {plan}\nСумма: {amount} USDT\nНажмите Pay.",
        "PAYMENT_CREATE_FAIL": "Не удалось создать счёт. Попробуйте позже.",
        "OFFER_POINT3_EXTRA": " Материалы носят информационный характер.",
        "OFFER_CUTOFF_MARKER": "3) Бот не является финансовым консультантом и не дает персональных рекомендаций “войти/выйти”.",
        "PD_ENABLED_TEXT": (
            "✅ Pump/Dump уведомления включены.\n"
            "Бот будет присылать алерты при резких движениях рынка."
        ),
        "PAYWALL_AI": (
            "🔒 Доступ к AI-сигналам по подписке.\n"
            "Нажми «Купить подписку» — покажу инструкцию."
        ),
        "PAYWALL_PD": (
            "🔒 Доступ к Pump/Dump сигналам по подписке.\n"
            "Нажми «Купить подписку» — покажу инструкцию."
        ),
        "PAYWALL_PREVIEW_LIVE_TITLE": "🔒 ПРЕВЬЮ (в реальном времени)",
        "PAYWALL_PREVIEW_PD_TITLE": "🔒 ПРЕВЬЮ Pump/Dump (в реальном времени)",
        "PAYWALL_PREVIEW_LEVELS": "Уровни доступны по подписке:",
        "PAYWALL_PREVIEW_PD_METRICS": "Метрики доступны по подписке:",
        "PAYWALL_PREVIEW_BUY_PROMPT": "👉 Купить подписку — чтобы видеть детали сразу",
        "TRIAL_SUFFIX_AI": "\n\n🎁 Осталось {left}/{limit} бесплатных AI-сигналов",
        "TRIAL_SUFFIX_PD": "\n\n🎁 Осталось {left}/{limit} бесплатных Pump/Dump сигналов",
        "LANG_PICK_TEXT": "Выберите язык:",
        "LANG_RU": "🇷🇺 Русский",
        "LANG_EN": "🇬🇧 English",
        "BTN_ACCEPT": "✅ Принять",
        "BTN_PAY_TON": "Оплатить USDT (39$)",
        "BTN_PAY_USDT": "Оплатить - USDT",
        "BTN_PLAN_30D_USDT": "30 дней — 39$ (USDT)",
        "BTN_PLAN_LIFE_USDT": "Пожизненный — 299$ (USDT)",
        "PLAN_30D": "30 дней",
        "PLAN_LIFE": "пожизненный",
        "BTN_CONTACT_ADMIN": "Связь с админом",
        "BTN_COPY_ADDRESS": "📋 Скопировать адрес",
        "BTN_SEND_RECEIPT": "📎 Отправить чек + ID",
        "BTN_BUY_SUB": "💳 Купить подписку",
        "BTN_EXPAND": "📖 Раскрыть",
        "BTN_COLLAPSE": "🔽 Скрыть",
        "STATUS_LABEL": "Статус",
        "STATUS_ON": "✅ включено",
        "STATUS_OFF": "⛔ выключено",
        "STATS_ROOT_TEXT": "📊 Раздел статистики\nВыбери архив:",
        "STATS_PICK_TEXT": "📊 Архив AI-сигналов\nВыбери период:",
        "STATS_PICK_PD_TEXT": "📊 Архив Pump/Dump\nВыбери период:",
        "BTN_ARCHIVE_AI": "🎯 Архив AI-сигналов",
        "BTN_ARCHIVE_PD": "⚡ Архив Pump/Dump",
        "stats_since_date_note": "Статистика считается с {date} (после обновления).",
        "legacy_hidden_notice": "Сигнал скрыт (до обновления).",
        "STATS_PRO_TITLE": "📊 Статистика сигналов (PRO)",
        "STATS_PRO_RECOMMENDED_HEADER": "🔥 Рекомендуемые сигналы",
        "STATS_PRO_RECOMMENDED_SUB": "(основной рабочий диапазон)",
        "STATS_PRO_SCORE_RANGE_90_100": "Score 90–100",
        "STATS_PRO_WINRATE_LINE": "• Winrate: {winrate}",
        "STATS_PRO_AVG_RR_LINE": "• Средний RR: {avg_rr}",
        "STATS_PRO_TOTAL_SIGNALS_LINE": "• Всего сигналов: {count}",
        "STATS_PRO_STATUS_PRIMARY": "• Статус: 🟢 Основной фокус",
        "STATS_PRO_RR_NOTE": "ℹ️ При RR > 2 даже 40–45% дают положительное ожидание.",
        "STATS_PRO_DIVIDER": "────────────────────",
        "STATS_PRO_HIGH_RISK_HEADER": "⚠️ Повышенный риск",
        "STATS_PRO_HIGH_RISK_SUB": "(только для опытных трейдеров)",
        "STATS_PRO_SCORE_RANGE_80_89": "Score 80–89",
        "STATS_PRO_STATUS_SELECTIVE": "• Статус: 🟡 Использовать выборочно",
        "STATS_PRO_BELOW_THRESHOLD_HEADER": "🚫 Ниже порога качества",
        "STATS_PRO_BELOW_THRESHOLD_SUB": "(не рекомендуется к торговле)",
        "STATS_PRO_BELOW_THRESHOLD_SCORE": "Score < 80",
        "STATS_PRO_BELOW_THRESHOLD_LINE1": "• В статистике не учитывается",
        "STATS_PRO_BELOW_THRESHOLD_LINE2": "• Используется только для анализа рынка",
        "STATS_PRO_SUMMARY_HEADER": "📈 Итоги по сделкам",
        "STATS_PRO_SUMMARY_SUB": "(за {period})",
        "STATS_PRO_TP_TOTAL": "🟢 Успешные (TP): {tp_total}",
        "STATS_PRO_SL_TOTAL": "🔴 По стопу (SL): {sl_total}",
        "STATS_PRO_NEUTRAL_TOTAL": "⚪ Без входа: {neutral_total}",
        "STATS_PRO_IN_PROGRESS_TOTAL": "🕒 В процессе: {in_progress_total}",
        "STATS_PRO_NEUTRAL_NOTE": "ℹ️ Neutral — сценарий не дошёл ни до TP, ни до SL",
        "STATS_PRO_NEUTRAL_NOTE_2": "(флет, отмена по времени или ручное закрытие).",
        "STATS_PRO_USAGE_HEADER": "🧠 Как использовать сигналы",
        "STATS_PRO_USAGE_PRIMARY": "• Основной фокус: Score 90–100",
        "STATS_PRO_USAGE_HIGH_RISK": "• 80–89 — повышенный риск",
        "STATS_PRO_USAGE_AVOID": "• Ниже 80 — не торговать",
        "STATS_PRO_RISK_NOTE": "⚠️ Рекомендуемый риск: 0.5–1% депозита на сделку",
        "STATS_PRO_LEVERAGE_NOTE": "ℹ️ Плечо выбирается трейдером",
        "SYS_HOW_BOT_WORKS_TEXT": (
            "🧠 Как работает Krypton AI\n\n"
            "Krypton AI — это системный алгоритм анализа рынка, который работает по многоуровневой модели оценки вероятности движения цены.\n\n"
            "Бот не угадывает рынок.\n"
            "Он оценивает структуру, импульс, волатильность и контекст.\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "📊 1) Рыночный режим (BTC-контекст)\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "Перед каждым сигналом анализируется состояние рынка по BTC:\n\n"
            "• TREND — направленный рынок\n"
            "• CHOP — пила / боковик\n"
            "• SQUEEZE — импульсное движение\n"
            "• RISK-OFF — повышенная волатильность\n\n"
            "Если рынок нестабилен — бот может блокировать LONG или SHORT, чтобы снизить вероятность стопов.\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "📈 2) Структура и тренд\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "Анализируется:\n\n"
            "• EMA 21 / EMA 50\n"
            "• Наклон движения (slope)\n"
            "• Pullback к EMA\n"
            "• Реакция на ключевую зону (POI)\n"
            "• Структура старшего таймфрейма\n\n"
            "Бот не входит “в никуда” — ему нужна логическая структура.\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "📍 3) Зона интереса (POI)\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "Сначала формируется сценарий.\n"
            "Затем цена должна войти в POI.\n"
            "После этого требуется подтверждение на 5–15m.\n\n"
            "Без подтверждения вход не активируется.\n"
            "Если подтверждения нет — сценарий закрывается без сделки.\n\n"
            "Это не убыток.\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "🎯 4) Risk / Reward\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "• Минимальный RR: ≥ 1:2\n"
            "• Стоп рассчитывается динамически через ATR\n"
            "• Слишком узкие стопы фильтруются\n\n"
            "Бот рассчитан под умеренное плечо (~x10).\n"
            "Он не знает ваше фактическое плечо — риск зависит от вашего управления капиталом.\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "⚙ 5) Score (0–100)\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "Каждый сигнал получает оценку качества.\n\n"
            "Баллы начисляются за:\n\n"
            "• Глобальный тренд\n"
            "• Локальный тренд\n"
            "• Реакцию на POI\n"
            "• Снос ликвидности\n"
            "• Объём\n"
            "• RSI\n"
            "• Волатильность (ATR)\n"
            "• EMA-согласование\n"
            "• Рыночный режим\n\n"
            "📌 Score ≥ 90 — основной фокус\n"
            "⚠ Score 80–89 — повышенный риск\n"
            "🚫 Ниже 80 — используется для анализа рынка\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "⏳ 6) Время жизни (TTL)\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "Каждый сценарий имеет ограниченное время.\n"
            "Если вход не подтверждён — он закрывается.\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "📌 Важно\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "Бот работает по вероятностной модели.\n\n"
            "Результат зависит от:\n"
            "• дисциплины\n"
            "• соблюдения SL\n"
            "• риск-менеджмента\n"
            "• адекватного плеча\n\n"
            "Krypton AI — это система.\n"
            "Система требует соблюдения дисциплины."
        ),
        "SYS_HOW_BOT_WORKS_CUTOFF_MARKER": "контекст.",
        "SYS_HOW_BOT_WORKS_BTN_EXPAND": "Раскрыть",
        "SYS_HOW_BOT_WORKS_BTN_HIDE": "Скрыть",
        "SYSTEM_SECTION_TEXT": "ℹ️ Раздел: О системе",
        "BACK_TO_MAIN_TEXT": "Возвращаемся в главное меню.",
        "ALREADY_ON": "Уже включено.",
        "ALREADY_OFF": "Уже выключено.",
        "AI_ALREADY_ON": "ℹ️ AI-уведомления уже включены.",
        "AI_ALREADY_OFF": "ℹ️ AI-уведомления уже выключены.",
        "AI_ON_OK": "✅ AI-уведомления включены.",
        "AI_OFF_OK": "🚫 Уведомления отключены.",
        "PD_ALREADY_ON": "ℹ️ Pump/Dump уведомления уже включены.",
        "PD_ALREADY_OFF": "ℹ️ Pump/Dump уведомления уже выключены.",
        "PD_ON_OK": (
            "✅ Pump/Dump уведомления включены.\n"
            "Теперь бот будет присылать алерты при резких движениях рынка."
        ),
        "PD_OFF_OK": "🚫 Pump/Dump уведомления отключены.",
        "NO_ACCESS": "⛔ Нет доступа",
        "SIGNAL_NOT_FOUND": "Сигнал не найден.",
        "CONTACT_ADMIN_BLOCK": (
            "💬 Связь с админом: {admin_contact}\n"
            "При обращении укажите ваш ID: {user_id}"
        ),
        "AI_STATS_TITLE": "📊 Статистика AI-сигналов ({period})",
        "AI_STATS_DISCLAIMER": "ℹ️ Это статистика отработки сценариев по рынку, не гарантия прибыли.",
        "AI_STATS_NO_COMPLETED": "Нет завершенных сигналов за период.",
        "AI_STATS_SIGNALS_COUNT": "Сигналов: {total}",
        "AI_STATS_SUMMARY": "TP1+: {tp1} | TP2: {tp2} | SL: {sl} | Exp: {exp}",
        "AI_STATS_WINRATE": "Winrate (TP1+): {winrate:.1f}%",
        "AI_STATS_SCORE_LABEL": "Score:",
        "AI_STATS_BUCKET_LINE": "{label}:  {total} (TP1+: {winrate:.0f}%)",
        "HISTORY_TITLE": "📊 История сигналов ({period})",
        "HISTORY_SUMMARY": (
            "✅ Прошло: {passed}\n"
            "❌ Не прошло: {failed}\n"
            "⚪ Neutral: {neutral}\n"
            "⏰ В процессе: {in_progress}"
        ),
        "HISTORY_STATS_TITLE": "📊 Статистика ({period}) — по Score",
        "HISTORY_SCORE_BUCKET_LINE": (
            "{label}: ✅ {passed} / ❌ {failed} / ⚪ {neutral} / ⏰ {in_progress}  ({percent}%)"
        ),
        "HISTORY_NO_SIGNALS": "Нет сигналов за период ({period}).",
        "HISTORY_EMPTY_PERIOD": "За выбранный период сигналов нет.",
        "HISTORY_NO_SIGNALS_BUTTON": "Нет сигналов за период ({period}).",
        "HISTORY_RESULT_LABEL": "Результат:",
        "HISTORY_ENTRY_LABEL": "Вход:",
        "HISTORY_DATE_LABEL": "Дата:",
        "HISTORY_LIST_TITLE": "📜 История сигналов — {period}",
        "HISTORY_PAGE_INFO": "Стр. {page}/{pages}",
        "history_title": "📜 История сигналов — {period}",
        "page_total": "Стр. {page}/{pages}",
        "section_recommended_title": "🔥 Рекомендуемые сигналы (Score 90–100)",
        "section_higher_risk_title": "⚠️ Повышенный риск (Score 80–89)",
        "section_score_below_title": "🚫 Score ниже 80",
        "line_winrate": "• Winrate: {value}%",
        "line_avg_rr": "• Avg RR: {value}",
        "line_trades": "• Сделок: {value}",
        "line_status": "• Статус: {value}",
        "status_main_focus": "🟢 Основной фокус",
        "status_use_selectively": "🟡 Использовать выборочно",
        "line_not_included": "• В статистике не учитывается",
        "line_market_analysis_only": "• Используется только для анализа рынка",
        "totals_title": "📈 Итоги",
        "totals_tp": "🟢 TP: {value}",
        "totals_be": "🟢 BE (+8%): {value}",
        "totals_be_avg": "🟢 BE: {value} | средний результат +{avg}% к депозиту (x{lev})",
        "totals_sl": "🔴 SL: {value}",
        "totals_exp": "⚪ EXP: {value}",
        "totals_active": "🟣 Active: {value}",
        "totals_expired_no_entry": "⚪ Без входа (истёк): {value}",
        "totals_no_confirmation": "🔵 Без подтверждения: {value}",
        "totals_in_progress": "🟣 В процессе: {value}",
        "line_winrate_strict": "• Winrate: {value}%",
        "line_winrate_tp_be": "📊 Winrate: {value}% | Формула: (TP+BE)/(TP+BE+SL)",
        "line_success_rate": "• Success rate: {value}%  (TP + BE)",
        "explanation_title": "ℹ️ Пояснение",
        "explanation_line_1": "• Score ≥ 80 — участвует в расчёте winrate и RR",
        "explanation_line_2": "• Score ниже 80 — используется только для анализа рынка",
        "explanation_line_be": "🟢 BE — цена дала минимум прибыли, прибыль защищена.",
        "explanation_line_be_2": "Считается успешной сделкой.",
        "explanation_line_exp": "⚪ EXP — сценарий устарел: вход не подтвердился за время жизни сигнала.",
        "HISTORY_WINRATE_TITLE": "📊 Winrate по Score:",
        "HISTORY_WINRATE_BUCKET_90_100": "Score 90–100:",
        "HISTORY_WINRATE_BUCKET_80_89": "Score 80–89:",
        "HISTORY_WINRATE_NO_DATA": "нет данных",
        "HISTORY_PRO_BLOCK": (
            "🔥 Рекомендуемые сигналы\n"
            "Score 90–100\n\n"
            "• Winrate: 35%\n"
            "• Средний RR: ~1 : —\n"
            "• Всего сигналов: 17\n"
            "• Статус: 🟢 Основной фокус\n\n"
            "ℹ️ При RR > 2 даже 40–45% дают положительное ожидание.\n\n"
            "————————————\n\n"
            "⚠️ Повышенный риск\n"
            "Score 80–89\n\n"
            "• Winrate: 7%\n"
            "• Всего сигналов: 14\n"
            "• Статус: 🟡 Использовать выборочно\n\n"
            "───────────\n\n"
            "🚫 Score < 80\n"
            "• В статистике не учитывается\n"
            "• Используется только для анализа рынка\n\n"
            "───────────\n\n"
            "📈 Итоги по сделкам\n\n"
            "🟢 TP: 8\n"
            "🔴 SL: 33\n"
            "⚪️ Без входа: 19\n"
            "🕒 В процессе: 2\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "ℹ️ Пояснение\n"
            "━━━━━━━━━━━━━━━━\n"
            "• Score ≥ 80 — расчёт winrate и RR\n"
            "• Score < 80 — анализ рынка"
        ),
        "HISTORY_LOAD_ERROR": "Ошибка загрузки истории. Попробуй позже.",
        "NAV_PREV_SHORT": "Назад",
        "NAV_NEXT_SHORT": "Вперед",
        "STATS_LOAD_ERROR": "Ошибка загрузки статистики. Мы уже фиксируем проблему.",
        "UNKNOWN_PERIOD": "Неизвестный период.",
        "STATUS_OPEN": "Открыт",
        "STATUS_ACTIVE_WAITING": "Активирован — ожидается результат",
        "STATUS_NO_FILL": "Нет входа",
        "STATUS_AMBIGUOUS": "Спорно",
        "ARCHIVE_DETAIL_LIFETIME": "⏱ Время жизни сценария: {hours} часов",
        "ARCHIVE_DETAIL_REASON_HEADER": "🧠 Почему выбран сигнал (Score {score}):",
        "ARCHIVE_DETAIL_HEADER_LINE": "📌 {symbol} {side} · Оценка {score}",
        "ARCHIVE_DETAIL_PREVIEW_TITLE": "🔒 ПРЕВЬЮ (в реальном времени)",
        "ARCHIVE_DETAIL_SUBSCRIPTION_LEVELS": "Уровни доступны по подписке:",
        "ARCHIVE_DETAIL_BUY_SUB_PROMPT": "👉 Купить подписку — чтобы видеть уровни сразу",
        "ARCHIVE_DETAIL_UNLOCK_DELAY": "🔓 Полный доступ откроется через {delay}",
        "NAV_PREV": "◀️ Назад",
        "NAV_NEXT": "▶️ Вперёд",
        "NAV_BACK": "◀️ Назад",
        "pagination_next_label": "▶️ Вперёд",
        "nav_back_label": "◀️ Назад",
        "nav_next_label": "▶️ Вперёд",
        "nav_prev_page": "◀️ Назад",
        "nav_next_page": "▶️ Вперёд",
        "nav_back_to_periods": "↩️ К выбору периода",
        "totals_no_entry_label": "Без входа",
        "history_status_tp": "🟢 TP",
        "history_status_sl": "🔴 SL",
        "history_status_expired_no_entry": "⚪ Без входа",
        "history_status_no_confirmation": "🔵 Без подтверждения",
        "history_status_poi_touched": "🟠 В зоне POI",
        "history_status_activated": "🟡 Активирован",
        "history_status_in_progress": "🟣 В процессе",
        "history_score_label_short": "Score {value}",
        "history_expired_helper": "ℹ️ Истёкшие сигналы означают, что сделка не состоялась, и это не убыток.",
        "history_indicator_waiting": "🟣 В процессе (ожидание)",
        "history_indicator_poi_touched": "🟠 Вошла в зону (POI)",
        "history_indicator_activated": "🟡 Активировался (вход подтверждён)",
        "PD_HISTORY_TITLE": "⚡ История Pump/Dump — {period}",
        "PD_EVENT_TITLE": "⚡ Pump/Dump событие",
        "PD_EVENT_SYMBOL": "SYMBOL: {symbol}",
        "PD_EVENT_TYPE": "Тип: {icon} {side}",
        "PD_EVENT_TIME": "Время: {time}",
        "PD_EVENT_DELTA_1M": "Δ (1m): {value}%",
        "PD_EVENT_DELTA_5M": "Δ (5m): {value}%",
        "PD_EVENT_VOLUME": "Объём (5m): {value} USDT",
        "PD_EVENT_VOL_MULT": "Volume xAvg: {value}x",
        "PD_SIDE_PUMP": "PUMP",
        "PD_SIDE_DUMP": "DUMP",
        "PD_BACK_TO_LIST": "⬅️ Назад к списку",
        "PD_EXPLAIN_TITLE": "ℹ️ Что это значит",
        "PD_EXPLAIN_LINE_1": "🚀 PUMP — резкий рост за 1–5 минут",
        "PD_EXPLAIN_LINE_2": "🔻 DUMP — резкое падение за 1–5 минут",
        "PD_EXPLAIN_LINE_3": "📌 Это НЕ вход в сделку, а сигнал волатильности.",
        "PD_EXPLAIN_LINE_4": "Используется как алерт для проверки графика.",
        "pd_locked_title": "🔒 Pump/Dump архив",
        "pd_locked_text": "Данные будут доступны через {time_left} (60 минут после события).\nДля мгновенного доступа оформите подписку.",
        "btn_upgrade": "🔐 Оформить подписку",
        "btn_back": "⬅️ Назад",
        "time_left_fmt": "{mm}:{ss}",
        "PD_LOCKED_UPDATE_TOAST": "🔄 Таймер обновлён",
        "explanation_block": (
            "━━━━━━━━━━━━━━━━\n"
            "ℹ️ Пояснение\n"
            "━━━━━━━━━━━━━━━━\n"
            "• Score ≥ 80 — участвует в расчёте winrate и RR\n"
            "• Score ниже 80 — используется только для анализа рынка\n"
            "• «Без входа» — цена не дошла до подтверждения/входа,\n"
            "  либо сценарий был отменён"
        ),
        "PAYMENT_COPY_ADDRESS": "📋 Адрес для оплаты (USDT):\n{wallet}",
        "RECEIPT_REQUEST_TEXT": (
            "📎 Отправьте сюда чек (скрин/фото) одним сообщением.\n"
            "Я автоматически прикреплю ваш ID и передам админу."
        ),
        "RECEIPT_SENT_CONFIRM": "✅ Чек отправлен админу. Ожидайте активацию.",
        "USER_LIST_EMPTY": "Пользователей пока нет.",
        "USER_LIST_HEADER": "👥 Пользователи (последние 50):",
        "USER_CARD_TITLE": "👤 Карточка пользователя",
        "USER_CARD_STATUS": "Статус: {status}",
        "USER_CARD_SUBSCRIPTION": "Подписка: {subscription}",
        "USER_CARD_AI_LEFT": "AI осталось: {left}/{limit}",
        "USER_CARD_PD_LEFT": "Pump/Dump осталось: {left}/{limit}",
        "USER_CARD_ACTIVE_UNTIL": "активна до {date}",
        "USER_CARD_SUB_NONE": "нет",
        "USER_CARD_STARTED_AT": "started_at: {date}",
        "USER_CARD_LAST_SEEN": "last_seen: {date}",
        "USER_BTN_UNLOCK": "🔓 Разблокировать",
        "USER_BTN_LOCK": "🔒 Заблокировать",
        "USER_BTN_TRIAL_48H": "🎁 Дать доступ 48ч",
        "USER_BTN_DELETE": "🗑 Удалить",
        "USER_DELETE_CONFIRM": (
            "⚠️ Удалить пользователя {user_id}?\n\n"
            "Это полностью удалит его из базы (включая лимиты/статусы)."
        ),
        "USER_DELETE_CONFIRM_YES": "✅ Да, удалить",
        "USER_DELETE_CONFIRM_NO": "❌ Отмена",
        "USER_DELETED_NOTICE": "Ваш аккаунт удалён администратором.",
        "USER_DELETED_PREFIX": "✅ Пользователь удалён: {user_id}",
        "USER_DELETED_ALERT": "✅ Пользователь удалён: {user_id}",
        "USER_TRIAL_48H_NOTICE": (
            "🎁 Вам предоставлен полный доступ на 48 часов для оценки бота и сигналов."
        ),
        "USER_TRIAL_48H_PREFIX": "✅ Выдан тестовый доступ 48ч: {user_id}",
        "USER_TRIAL_48H_ALERT": "✅ Доступ 48ч выдан пользователю {user_id}",
        "USER_LOCKED_NOTICE": (
            "⛔ Подписка приостановлена\n\n"
            "Доступ к сигналам временно отключён администратором.\n"
            "Для связи: {admin_contact}\n"
            "Ваш ID: {user_id}"
        ),
        "USER_UNLOCKED_NOTICE": "Подписка активирована на 30 дней",
        "TEST_NO_SUBSCRIBERS": (
            "⚠️ Подписчиков нет. Включи уведомления на тест-аккаунте и повтори."
        ),
        "TEST_AI_PREFIX": (
            "🧪 ТЕСТОВЫЙ AI-СИГНАЛ (для проверки системы)\n\n"
            "⚠️ Это тест. Если лимит 0 — вместо текста должен прийти paywall.\n\n"
        ),
        "TEST_AI_DONE": (
            "AI тест\n"
            "✅ Тест AI завершён: sent={sent}, locked={locked}, "
            "paywall={paywall}, errors={errors} (subscribers={subscribers})"
        ),
        "TEST_PD_PREFIX": "🧪 ТЕСТОВЫЙ PUMP/DUMP (для проверки системы)\n\n",
        "TEST_PD_WARNING": "⚠️ Это тест. Если лимит 0 — вместо текста должен прийти paywall.",
        "TEST_PD_DONE": (
            "Pump/Dump тест\n"
            "✅ Тест Pump/Dump завершён: sent={sent}, locked={locked}, "
            "paywall={paywall}, errors={errors} (subscribers={subscribers})"
        ),
        "TEST_NOTIFY_TEXT": "🧪 Тестовое уведомление: доставка работает.",
        "TEST_NOTIFY_ERROR": "❌ Ошибка: {error}",
        "PURGE_TESTS_DONE": "✅ Удалено тестовых сигналов: {removed}",
        "PURGE_SYMBOL_DONE": "✅ {symbol}: удалено signal_events={events}, signal_audit={audit}",
        "CMD_USAGE_LOCK": "Использование: /lock <id>",
        "CMD_USAGE_UNLOCK": "Использование: /unlock <id>",
        "CMD_USAGE_DELETE": "Использование: /delete <id>",
        "CMD_USAGE_PURGE": "Использование: /purge <symbol>",
        "CMD_LOCK_OK": "✅ user_locked=1 для {user_id}",
        "CMD_UNLOCK_OK": "✅ user_locked=0 для {user_id}",
        "CMD_DELETE_OK": "✅ пользователь {user_id} удалён",
        "ADMIN_STATS_TITLE": "📊 Статистика сигналов (30d)",
        "ADMIN_STATS_TOTAL": "• Всего: {total}",
        "ADMIN_STATS_CLOSED": "• Закрыто: {closed}",
        "ADMIN_STATS_FILLED_RATE": "• Filled rate: {rate:.1f}% ({filled} из {total})",
        "ADMIN_STATS_WINRATE": "• Winrate (filled): {winrate:.1f}%",
        "ADMIN_STATS_PROFIT_FACTOR": "• Profit factor: {profit_factor}",
        "ADMIN_STATS_AVG_R": "• Avg R: {avg_r:.2f}",
        "ADMIN_STATS_MEDIAN_R": "• Median R: {median_r:.2f}",
        "ADMIN_STATS_STREAK": "• Streak: {streak}",
        "ADMIN_STATS_LAST10": "Последние 10 сигналов:",
        "ADMIN_STATS_NO_DATA": "• Нет данных",
        "ADMIN_STATS_ROW": "• {symbol} {direction} → {outcome} ({pnl})",
        "STATUS_HUMAN_SECONDS": "{seconds} сек",
        "STATUS_HUMAN_MINUTES": "{minutes} мин",
        "STATUS_HUMAN_HOURS": "{hours} ч",
        "STATUS_AGO_SECONDS": "{seconds} сек назад",
        "STATUS_AGO_MINUTES": "{minutes} мин назад",
        "STATUS_AGO_HOURS": "{hours} ч назад",
        "SCENARIO_TREND_BULLISH": "бычий",
        "SCENARIO_TREND_BEARISH": "медвежий",
        "SCENARIO_TREND_FLAT": "флет",
        "SCENARIO_TREND_NEUTRAL": "нейтральный",
        "SCENARIO_RSI_OVERBOUGHT": "перекуплен",
        "SCENARIO_RSI_OVERSOLD": "перепродан",
        "SCENARIO_RSI_NEUTRAL": "нейтр",
        "SCENARIO_RSI_COMFORT": "комфортная зона",
        "SCENARIO_RSI_OVERSOLD_ZONE": "зона перепроданности",
        "SCENARIO_RSI_OVERBOUGHT_ZONE": "зона перекупленности",
        "SCENARIO_POSSIBLE_LINE": "{emoji} Возможный {scenario}",
        "SCENARIO_TIMEFRAME_LINE": "⏱ Таймфрейм сценария: {timeframe} | Вход: 5–15m",
        "SCENARIO_LIFETIME_LINE": "⏱ Время жизни сценария: {hours} часов",
        "SCENARIO_LIFETIME_MINUTES_LINE": "⏱ Время жизни сценария: ~{minutes} минут",
        "SCENARIO_POI_HEADER": "Зона интереса (POI):",
        "SCENARIO_CONDITIONS_HEADER": "Условия реализации:",
        "SCENARIO_VALID_ABOVE": "• сценарий актуален, пока цена удерживается выше зоны",
        "SCENARIO_VALID_BELOW": "• сценарий актуален, пока цена удерживается ниже зоны",
        "SCENARIO_CONFIRMATION_LINE": "• вход рассматривается только после подтверждения на 5–15m",
        "SCENARIO_CONFIRM_HEADER": "🔎 Подтверждение на 5–15m:",
        "SCENARIO_CONFIRM_CLOSE": (
            "• закрытие свечи по направлению (выше зоны для LONG / ниже для SHORT)"
        ),
        "SCENARIO_CONFIRM_HOLD": "• цена удерживается вне зоны без быстрого возврата",
        "SCENARIO_INVALIDATION_HEADER": "Отмена сценария:",
        "SCENARIO_INVALIDATION_LINE": "• если 1H свеча закроется {condition} {level}",
        "SCENARIO_SL_HEADER": "🛑 Stop Loss:",
        "SCENARIO_SL_LINE": "• SL: {price} ({pct})",
        "SCENARIO_TARGETS_HEADER": "Потенциальные цели:",
        "SCENARIO_CONTEXT_HEADER": "Краткий контекст:",
        "SCENARIO_CONTEXT_TREND": "• Тренд 1D / 4H: {trend_1d} / {trend_4h}",
        "SCENARIO_CONTEXT_RSI": "• RSI 1H: {rsi} ({zone})",
        "SCENARIO_CONTEXT_VOLUME": "• Объём: {volume}x к среднему",
        "SCENARIO_CONTEXT_RR": "• RR ≈ 1 : {rr}",
        "SIGNAL_QUALITY_RECOMMENDED": (
            "🔥 РЕКОМЕНДУЕМЫЙ СИГНАЛ\n"
            "Основной рабочий диапазон (Score 90–100)\n"
            "Используется для торговли"
        ),
        "SIGNAL_QUALITY_HIGH_RISK": (
            "🔥 АКТИВНЫЙ СЦЕНАРИЙ\n"
            "Для опытных трейдеров (Score 80–89)\n"
            "Использовать выборочно"
        ),
        "SIGNAL_QUALITY_ANALYSIS_ONLY": (
            "🚫 СИГНАЛ ДЛЯ АНАЛИЗА\n"
            "Ниже порога качества (Score < 80)\n"
            "Не рекомендуется к торговле"
        ),
        "SIGNAL_COMPACT_HIGH_RISK_HEADER": "🔥 АКТИВНЫЙ СЦЕНАРИЙ (Score 80–89)",
        "SIGNAL_COMPACT_META_LINE": "{side} · TF: {timeframe} · Entry: {entry_tf}",
        "SIGNAL_COMPACT_POI_LINE": "POI: {poi_from}–{poi_to}",
        "SIGNAL_COMPACT_SL_LINE": "SL: {sl}",
        "SIGNAL_COMPACT_TP1_LINE": "TP1: {tp1}",
        "SIGNAL_COMPACT_TP2_LINE": "TP2: {tp2}",
        "SIGNAL_COMPACT_SCORE_LINE": "Score: {score}",
        "SIGNAL_MARKET_REGIME_LINE": "🧭 Режим рынка: {regime} ({direction})",
        "SIGNAL_MARKET_REGIME_TREND": "Тренд",
        "SIGNAL_MARKET_REGIME_CHOP": "Пила / Боковик",
        "SIGNAL_MARKET_REGIME_SQUEEZE": "Импульс",
        "SIGNAL_MARKET_REGIME_RISK_OFF": "Риск-офф",
        "SIGNAL_MARKET_DIR_UP": "вверх",
        "SIGNAL_MARKET_DIR_DOWN": "вниз",
        "SIGNAL_MARKET_DIR_NEUTRAL": "нейтр",
        "SIGNAL_MARKET_TREND_LINE": "📈 Тренд: {trend}",
        "SIGNAL_TREND_YES": "да",
        "SIGNAL_TREND_NO": "нет",
        "SIGNAL_COMPACT_TTL_LINE": "⏱ TTL: ~{minutes} мин",
        "SIGNAL_BUTTON_EXPAND": "▾ Раскрыть",
        "SIGNAL_BUTTON_COLLAPSE": "▴ Скрыть",
        "btn_binance": "📈 Binance",
        "btn_binance_spot": "📈 Binance",
        "SIGNAL_SHORT_SIDE_LONG": "📈 LONG",
        "SIGNAL_SHORT_SIDE_SHORT": "📉 SHORT",
        "SIGNAL_SHORT_SYMBOL_SIDE_LINE": "{symbol} · {side}",
        "SIGNAL_SHORT_POI_LINE": "POI: {poi_from}–{poi_to}",
        "SIGNAL_SHORT_TP1_LINE": "TP1: {tp1}",
        "SIGNAL_SHORT_TP2_LINE": "TP2: {tp2}",
        "SIGNAL_SHORT_SL_LINE": "SL: {sl}",
        "SIGNAL_SHORT_80_89_SYMBOL_LINE": "{symbol} / USDT",
        "SIGNAL_SHORT_80_89_META_LINE": "{side} · TF: {timeframe} · Entry: {entry_tf}",
        "SIGNAL_SHORT_80_89_SCORE_LINE": "Score: {score}",
        "SIGNAL_SHORT_80_89_TTL_LINE": "TTL: ~{minutes} мин",
        "SIGNAL_SHORT_HIGH_RISK_WARNING": (
            "🔥 АКТИВНЫЙ СЦЕНАРИЙ\n"
            "Для опытных трейдеров\n"
            "Использовать выборочно"
        ),
        "SIGNAL_BUTTON_SOUND_ON": "🔔 Звук: Сигнал + Вход",
        "SIGNAL_BUTTON_SOUND_OFF": "🔕 Звук: Тихо (Сигнал + Вход)",
        "SIGNAL_SOUND_TOGGLE_TOAST_ON": "Звук для сигнала и входа: ВКЛ",
        "SIGNAL_SOUND_TOGGLE_TOAST_OFF": "Звук для сигнала и входа: ВЫКЛ",
        "SIGNAL_STATUS_TOGGLE_ON": "✅ Уведомления: ВКЛ",
        "SIGNAL_STATUS_TOGGLE_OFF": "⛔ Уведомления: ВЫКЛ",
        "SIGNAL_STATUS_TOGGLE_TOAST_ON": "Уведомления включены",
        "SIGNAL_STATUS_TOGGLE_TOAST_OFF": "Уведомления выключены",
        "SCENARIO_SCORE_LINE": "🧠 Score: {score} / 100",
        "SCENARIO_BREAKDOWN_HEADER": "🧩 Детали Score (сумма баллов):",
        "SCENARIO_BREAKDOWN_TOTAL": "= Итоговая оценка: {score}",
        "SCENARIO_DISCLAIMER_1": (
            "ℹ️ Score — внутренняя оценка качества сценария, основанная на рыночных факторах и условиях модели."
        ),
        "SCENARIO_DISCLAIMER_2": "ℹ️ Бот ищет сетапы, не гарантирует прибыль.",
        "SCENARIO_DISCLAIMER_3": "ℹ️ Сценарий требует подтверждения перед входом.",
        "SCENARIO_TP_NEEDS_REVIEW": "требуют уточнения",
        "SCENARIO_CONDITION_BELOW": "ниже",
        "SCENARIO_CONDITION_ABOVE": "выше",
        "CLOSE_SL_TITLE": "❌ SL — Сигнал закрыт по Stop Loss",
        "CLOSE_TP1_TITLE": "✅ TP1 — Take Profit достигнут",
        "CLOSE_TP2_TITLE": "✅ TP2 — Take Profit достигнут",
        "SIGNAL_RESULT_CLOSED_BY_TP1_LINE": "Закрыто по: TP1",
        "SIGNAL_RESULT_CLOSED_BY_TP2_LINE": "Закрыто по: TP2",
        "SIGNAL_RESULT_CLOSED_BY_SL_LINE": "Закрыто по: Stop Loss",
        "SIGNAL_RESULT_HEADER_TP1": "✅ TP1 — TP1 достигнут",
        "SIGNAL_RESULT_HEADER_TP2": "✅ TP2 — TP2 достигнут",
        "SIGNAL_RESULT_HEADER_BE": "⚪ BE — Безубыток",
        "SIGNAL_RESULT_HEADER_BE_AFTER_TP1": "⚪ Закрытие в безубыток (после TP1) — в статистике считается как TP1",
        "SIGNAL_BE_TRIGGERED_HEADER": "🟢 BE активирован (+8%)",
        "SIGNAL_BE_FINALISED_HEADER": "🟢 BE (+{level}%) закрыто",
        "SIGNAL_BE_LEVEL_LINE": "🎯 BE (+{level}%): {price}",
        "SIGNAL_BE_LEVEL_ONLY_LINE": "🎯 BE (+{level}%)",
        "SIGNAL_BE_REACHED_LINE": "🛡 Дошло до BE: {price}  (+{level}%)",
        "SIGNAL_BE_MAX_PNL_LINE": "🛡 Max PnL: {pnl}%",
        "SIGNAL_BE_CLOSED_BY_LINE": "Закрыто по: BE",
        "SIGNAL_RESULT_HEADER_SL": "❌ SL — Сигнал закрылся по стопу",
        "SIGNAL_RESULT_HEADER_NF": "⏳ NF — Вход не активирован",
        "SIGNAL_RESULT_HEADER_EXP": "⚪ EXP — Сценарий устарел",
        "SIGNAL_RESULT_HEADER_TP": "🟢 TP — Сигнал закрылся в плюс",
        "SIGNAL_ACTIVATED_HEADER": "🟡 Сигнал активировался (вход подтверждён)",
        "SIGNAL_ACTIVATED_ENTRY_LABEL": "Вход",
        "SIGNAL_ACTIVATED_SL_LABEL": "Stop Loss",
        "SIGNAL_ACTIVATED_WAITING": "Ждём результат.",
        "SIGNAL_POI_TOUCHED_HEADER": "🟠 Цена вошла в зону (POI)",
        "SIGNAL_POI_TOUCHED_ZONE_HEADER": "📍 Зона входа (POI):",
        "SIGNAL_POI_TOUCHED_WAIT": "⏳ Ожидаем подтверждение на 5–15m.",
        "SIGNAL_POI_TOUCHED_WAIT_2": "Вход будет только после закрытия свечи.",
        "SIGNAL_EXPIRED_NO_ENTRY_HEADER": "⌛ Сценарий устарел",
        "SIGNAL_EXPIRED_NO_ENTRY_LINE_1": "{symbol} · {side}",
        "SIGNAL_EXPIRED_NO_ENTRY_LINE_2": "Цена не получила подтверждение на 5–15m.",
        "SIGNAL_EXPIRED_NO_ENTRY_LINE_3": "Сценарий закрыт без входа.",
        "SIGNAL_RESULT_ENTRY_LINE": "Вход: {entry}",
        "SIGNAL_RESULT_EXIT_LINE": "✅ Выход: {price}",
        "SIGNAL_RESULT_PNL_LINE": "PnL: {pnl}%",
        "SIGNAL_RESULT_SL_LINE": "🛑 SL: {price}",
        "SIGNAL_RESULT_TP1_LINE": "🎯 TP1: {price}",
        "SIGNAL_RESULT_TP2_LINE": "🎯 TP2: {price}",
        "SIGNAL_PROGRESS_TP1_HEADER": "🟢 TP1 достигнут",
        "SIGNAL_RESULT_SCORE_LINE": "Score: {score}",
        "BREAKDOWN_GLOBAL_TREND": "Глобальный тренд (1D)",
        "BREAKDOWN_LOCAL_TREND": "Локальный тренд (1H)",
        "BREAKDOWN_NEAR_KEY_LEVEL": "Реакция на ключевую зону (POI)",
        "BREAKDOWN_LIQUIDITY_SWEEP": "Снос ликвидности",
        "BREAKDOWN_VOLUME_CLIMAX": "Объём относительно среднего",
        "BREAKDOWN_RSI_DIVERGENCE": "RSI-дивергенция",
        "BREAKDOWN_ATR_OK": "Волатильность (ATR)",
        "BREAKDOWN_BB_EXTREME": "Экстремум Bollinger",
        "BREAKDOWN_MA_TREND_OK": "EMA-согласование",
        "BREAKDOWN_ORDERFLOW": "Ордерфлоу",
        "BREAKDOWN_WHALE_ACTIVITY": "Китовая активность",
        "BREAKDOWN_AI_PATTERN": "AI-паттерны",
        "BREAKDOWN_MARKET_REGIME": "Рыночный режим",
        "BREAKDOWN_FALLBACK": "Фактор",
        "PUMP_HEADER_PUMP": "🚀 Pump/Dump Scanner: резкий импульс",
        "PUMP_HEADER_DUMP": "📉 Pump/Dump Scanner: резкий импульс",
        "PUMP_COIN_LINE": "Монета: {symbol}",
        "PUMP_PRICE_LINE": "Текущая цена: {price}",
        "PUMP_MOVE_HEADER": "Движение:",
        "PUMP_MOVE_1M": "• за 1 мин: {change}%",
        "PUMP_MOVE_5M": "• за 5 мин: {change}%",
        "PUMP_VOLUME_LINE": "• объём: {volume:.2f}× от среднего",
        "PUMP_BUTTON_EXPAND": "📖 Раскрыть",
        "PUMP_BUTTON_COLLAPSE": "🔽 Скрыть",
        "PUMP_TOGGLE_EXPANDED": "Открыто",
        "PUMP_TOGGLE_COLLAPSED": "Скрыто",
        "PUMP_TOGGLE_EXPIRED": "Состояние карточки недоступно",
        "PUMP_NOTE_1": "ℹ️ Это уведомление о резком импульсе цены и объёма.",
        "PUMP_NOTE_2": "Используется как сигнал внимания, а не готовая торговая идея.",
        "PUMP_RISK_1": "⚠️ Резкие импульсы высокорисковые.",
        "PUMP_RISK_2": "Бот не даёт точек входа и не управляет рисками.",
        "PUMP_SOURCE": "Источник: Binance",
        "ADMIN_NEW_USER": (
            "🆕 Новый пользователь\n"
            "ID: {user_id}\n"
            "Username: {username}\n"
            "Имя: {full_name}\n"
            "Язык: {language}"
        ),
        "DIAG_TITLE": "🛠 Диагностика бота (админ)",
        "DIAG_SECTION_OVERALL": "🧠 Общее состояние",
        "DIAG_SECTION_AI": "🎯 AI-сигналы (основной модуль)",
        "DIAG_SECTION_FILTERS": "🧪 Фильтрация (Pre-score)",
        "DIAG_SECTION_BINANCE": "🌐 Binance API",
        "DIAG_SECTION_PUMPDUMP": "🚀 Pump / Dump Scanner",
        "DIAG_STATUS_WORKING": "работает",
        "DIAG_STATUS_NOT_STARTED": "не запускался",
        "DIAG_STATUS_ISSUES": "есть проблемы",
        "DIAG_STATUS_ERROR": "ошибка",
        "DIAG_STATUS_OK": "OK",
        "DIAG_STATUS_MISSING": "нет файла",
        "DIAG_STATUS_PENDING": "ожидание",
        "DIAG_STATUS_NO_DATA": "нет данных",
        "DIAG_MODULE_STATUS": "• Статус: {status}",
        "DIAG_LAST_TICK": "• Последний тик: {tick}",
        "DIAG_ERRORS": "• Ошибки: {error}",
        "DIAG_ERRORS_HEADER": "• Ошибки:",
        "DIAG_ERRORS_NONE": "• Ошибки: ❌ нет",
        "DIAG_WARNINGS_HEADER": "• Предупреждения:",
        "DIAG_WARNINGS_NONE": "• Предупреждения: ✅ нет",
        "DIAG_NO_DATA_LINE": "• Нет данных",
        "DIAG_DB_TITLE": "🗄 База данных",
        "DIAG_DB_PATH": "• Путь: {path}",
        "DIAG_DB_MISSING": "• Файл не найден",
        "DIAG_DB_SIZE": "• Размер: {size} байт",
        "DIAG_DB_MODIFIED": "• Изменена: {mtime}",
        "DIAG_MODULE_LAST_CYCLE": "• Последний цикл: {tick}",
        "DIAG_MODULE_LAST_OK": "• Последний успешный запрос: {tick}",
        "DIAG_MODULE_ERROR": "• Ошибка: {error}",
        "DIAG_MODULE_WARNING": "• Предупреждение: {warning}",
        "DIAG_USERS_HEADER": "Пользователи",
        "DIAG_SUBSCRIBERS_LINE": "• Подписчиков: {count}",
        "DIAG_MARKET_SCAN_HEADER": "• Сканирование рынка:",
        "DIAG_MARKET_UNIVERSE": "• Монет в рынке: {count}",
        "DIAG_MARKET_CHUNK": "• Монет за цикл: {count}",
        "DIAG_MARKET_POSITION_TOTAL": "• Текущая позиция: {current} / {total}",
        "DIAG_MARKET_POSITION": "• Текущая позиция: {current}",
        "DIAG_MARKET_CURRENT": "• Текущая монета: {symbol}",
        "DIAG_AI_EXCLUDED": "• Исключенные монеты: {symbols}",
        "DIAG_CYCLE_TIME": "• Время цикла: ~{cycle}",
        "DIAG_AI_CONFIG_TITLE": "• AI config:",
        "DIAG_AI_CONFIG_MAX_DEEP": "  • Max deep per cycle: {value}",
        "DIAG_AI_CONFIG_STAGE_A": "  • Stage A top K: {value}",
        "DIAG_AI_CONFIG_PRESCORE_THRESHOLD": "  • Pre-score threshold: {value}",
        "DIAG_AI_CONFIG_PRESCORE_MIN": "  • Min pre-score: {value}",
        "DIAG_AI_CONFIG_FINAL_THRESHOLD": "  • Final scoring threshold: {value}",
        "DIAG_AI_CONFIG_MIN_VOLUME": "  • Min volume 5m: {value}",
        "DIAG_AI_CONFIG_PUMP_VOLUME": "  • Pump volume multiplier: {value}",
        "DIAG_AI_STRUCTURE_TITLE": "• Structure:",
        "DIAG_AI_STRUCTURE_MODE": "  • Mode: {value}",
        "DIAG_AI_STRUCTURE_PENALTY": "  • Neutral penalty: {value}",
        "DIAG_AI_STRUCTURE_HARD_FAIL": "  • Hard fail opposite: {value}",
        "DIAG_AI_STRUCTURE_WINDOW": "  • Window: {value}",
        "DIAG_AI_STRUCTURE_COUNTS": "  • Counts: penalty_neutral={neutral} fail_structure_opposite={opposite} fail_setup_structure={legacy}",
        "DIAG_AI_STRUCTURE_SAMPLE": "  • Sample: {sample}",
        "DIAG_TREND_TITLE": "• Trend mode:",
        "DIAG_TREND_MODE": "  • Enabled: {value}",
        "DIAG_TREND_DETECTED": "  • Detected: up={up} down={down} none={none}",
        "DIAG_TREND_SETUP_SUMMARY": "  • Setups: checked={checked} passed={passed} failed={failed}",
        "DIAG_TREND_FAIL_REASONS": "  • Fail reasons: {reasons}",
        "DIAG_TREND_SAMPLE": "  • Sample: {sample}",
        "DIAG_FINAL_SCORE_HEADER": "• Финальный скоринг:",
        "DIAG_FINAL_SCORE_THRESHOLD": "  • Порог: {threshold}",
        "DIAG_FINAL_SCORE_SUMMARY": "  • Проверено: {checked} прошло: {passed} отказов: {failed}",
        "DIAG_FINAL_SCORE_LAST_PASS": "  • Последний pass: {sample}",
        "DIAG_FINAL_SCORE_LAST_FAIL": "  • Последний fail: {sample}",
        "DIAG_FINAL_SCORE_ADJUSTMENTS": "    • Корректировки: {adjustments}",
        "DIAG_FINAL_SCORE_FAIL_REASON": "    • Причина отказа: {reason}",
        "DIAG_FINAL_SCORE_NEAR_MISS": "  • Near-miss: {sample}",
        "DIAG_FINAL_SCORE_NEAR_MISS_BLOCKERS": "    • Основные блокеры: {blockers}",
        "DIAG_PRESCORE_THRESHOLD": "• Порог: {threshold}",
        "DIAG_PRESCORE_SUMMARY": "• Проверено: {checked} | Прошло: {passed} | Отфильтровано: {failed} | Pass rate: {rate}",
        "DIAG_PRESCORE_FAILED": "• Примеры отказов: {samples}",
        "DIAG_PRESCORE_PASSED": "• Примеры прошедших: {samples}",
        "DIAG_PRESCORE_BLUECHIP": "• Bluechip bypasses: {count} | Примеры: {samples}",
        "DIAG_LIMITS_LINE": "• Лимиты: ema_near_pct={ema} | poi_max_distance_pct={poi} | min_rr={rr}",
        "DIAG_SETUP_STAGE_SUMMARY": "• Сетап: проверено={checked} | прошло={passed} | отказов={failed}",
        "DIAG_SETUP_FAIL_REASONS": "• Причины отказа сетапа: {reasons}",
        "DIAG_SETUP_NEAR_MISS_EXAMPLES": "• Near-miss примеры: {examples}",
        "DIAG_FINAL_STAGE_SUMMARY": "• Final stage: checked={checked} passed={passed} failed={failed}",
        "DIAG_FINAL_FAIL_REASONS": "• Final fail reasons: {reasons}",
        "DIAG_CONFIRM_RETRY_HEADER": "• Confirm retry:",
        "DIAG_CONFIRM_RETRY_STATUS": (
            "  • enabled: {enabled} | pending: {pending} | sent_after_retry: {sent} | "
            "fail_confirm_retry_exhausted: {dropped}"
        ),
        "DIAG_CONFIRM_RETRY_SAMPLES": "  • Примеры: {samples}",
        "DIAG_REQUESTS_HEADER": "Запросы к Binance",
        "DIAG_REQUESTS_MADE": "• Запросов сделано: {count}",
        "DIAG_CANDLES": "• Свечей получено: {count}",
        "DIAG_CACHE": "• Кеш свечей: hit={hits} miss={misses}",
        "DIAG_INFLIGHT": "• In-flight ожиданий свечей: {count}",
        "DIAG_TICKER_REQ": "• Ticker/24h запросов: {count}",
        "DIAG_DEEP_SCAN": "• Deep-scan за цикл: {count}",
        "DIAG_PUMP_HEADER": "Поиск пампов / дампов",
        "DIAG_PROGRESS": "• Прогресс: {progress}",
        "DIAG_CHECKED": "• Проверено: {count}",
        "DIAG_FOUND": "• Найдено сигналов: {count}",
        "DIAG_SENT": "• Отправлено сигналов: {count}",
        "DIAG_CURRENT_COIN": "• Текущая монета: {symbol}",
        "DIAG_ROTATION": "• Rotation: {flag} (N={n}){cursor}",
        "DIAG_ROTATION_SLICE": "• Rotation last slice size: {size}",
        "DIAG_UNIVERSE_LINE": (
            "• Universe size={universe} rotation_added={added} "
            "final_candidates={final} scanned={scanned}"
        ),
        "DIAG_BINANCE_LAST_SUCCESS": "• Последний успешный ответ: {ago}",
        "DIAG_BINANCE_LAST_SUCCESS_NO_DATA": "• Последний успешный ответ: нет данных",
        "DIAG_BINANCE_TIMEOUTS": "• Таймауты подряд: {count}",
        "DIAG_BINANCE_STAGE": "• Текущий этап: {stage}",
        "DIAG_STABILITY_HEADER": "Стабильность",
        "DIAG_SESSION_RESTARTS": "• Перезапусков сессии: {count}",
        "DIAG_FAILS_TOP": "Причины отказа",
        "DIAG_NEAR_MISS": "Near-miss",
        "ADMIN_RECEIPT_TEXT": (
            "🧾 Чек на подписку\n\n"
            "User ID: {user_id}\n"
            "Username: {username}\n"
            "Дата/время: {timestamp}\n\n"
            "Тариф: ${price} / {days} дней\n"
            "Оплата: TRX (TRON)\n"
            "Адрес: {wallet}"
        ),
    },
    "en": {
        "START_TEXT": (
            "Enable notifications below — the bot works automatically.\n\n"
            "You have free access to:\n"
            "• 7 AI signals\n"
            "• 7 Pump/Dump signals\n\n"
            "After the free limit is reached, a subscription is required\n"
            "to continue receiving signals.\n\n"
            "Once enabled, just wait — signals will arrive automatically."
        ),
        "SCORE_EXPLANATION": (
            "ℹ️ The higher the Score, the more often the signal is successful.\n\n"
            "TP1: {tp1}\n"
            "👉 The signal hit take profit and closed in profit.\n"
            "BE: {be}\n"
            "👉 The signal moved to breakeven — risk removed.\n"
            "SL: {sl}\n"
            "👉 The signal closed by stop-loss.\n"
            "EXP: {exp}\n"
            "👉 12 hours passed after activation — the scenario expired.\n"
            "NF: {nf}\n"
            "👉 12 hours passed, price never reached the entry zone."
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
            "• scans the market across trading pairs\n"
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
        "MENU_BACK": "◀️ Back",
        "SYS_STATUS": "🧠 Market Analysis Status",
        "SYS_DIAG_ADMIN": "🧪 Diagnostics (admin)",
        "SYS_TEST_AI": "🧪 Test AI (all)",
        "SYS_TEST_PD": "🧪 Test Pump/Dump (all)",
        "SYS_USERS": "👥 Users",
        "SYS_CHANNEL_PANEL": "📣 Telegram channel",
        "SYS_PAY": "💳 Buy subscription",
        "SYS_HOW_BOT_WORKS": "🧠 How the bot works",
        "INVERSION_TOGGLE_BUTTON": "🔁 Inversion: {state}",
        "INVERSION_STATE_ON": "ON",
        "INVERSION_STATE_OFF": "OFF",
        "INVERSION_ENABLED_ALERT": "Inversion enabled ✅",
        "INVERSION_DISABLED_ALERT": "Inversion disabled ❌",
        "SYS_DIAG": "🧪 Diagnostics",
        "CHANNEL_PANEL_TITLE": "📣 Telegram Channel Panel",
        "CHANNEL_PANEL_ID_LINE": "• channel_id: {channel_id}",
        "CHANNEL_PANEL_ENABLED_LINE": "• public_enabled: {enabled}",
        "CHANNEL_PANEL_NOTE": "• note: bot must be admin in channel with post rights",
        "CHANNEL_TEST_ENTRY": "🧪 Test: ENTRY",
        "CHANNEL_TEST_FIX8": "🧪 Test: FIX +8% (30%)",
        "CHANNEL_TEST_FIX10": "🧪 Test: FIX +10% (30%)",
        "CHANNEL_TEST_EXIT_TP": "🧪 Test: EXIT TP",
        "CHANNEL_TEST_EXIT_SL": "🧪 Test: EXIT SL",
        "CHANNEL_TEST_EXIT_BE": "🧪 Test: EXIT BE",
        "CHANNEL_TEST_STATUS": "📊 Test: STATUS",
        "CHANNEL_TEST_RESET_BALANCE": "🔄 Reset TEST balance",
        "CHANNEL_TEST_OK": "✅ Sent to channel",
        "CHANNEL_TEST_DISABLED": "⚠️ AI_PUBLIC_ENABLED=0 (enable it)",
        "CHANNEL_TEST_NO_ID": "⚠️ TELEGRAM_CHANNEL_ID is not set",
        "ADMIN_ONLY": "⛔ Admin only",
        "SYSTEM_STATUS_TITLE": "🧠 Market Analysis Status",
        "SYSTEM_STATUS_BINANCE_LINE": "🔌 Binance link: {status}",
        "SYSTEM_STATUS_LAST_CYCLE_LINE": "⏱ last analysis cycle: {seconds} sec ago",
        "SYSTEM_STATUS_CONN_OK": "OK",
        "SYSTEM_STATUS_CONN_WARN": "WARN",
        "SYSTEM_STATUS_CONN_ERROR": "ERROR",
        "SYSTEM_STATUS_SECTION_MARKET": "📊 Market context:",
        "SYSTEM_STATUS_MARKET_STATE_LINE": "• state: {state}",
        "SYSTEM_STATUS_MARKET_PRIORITY_LINE": "• priority: {priority}",
        "SYSTEM_STATUS_MARKET_ACTIVITY_LINE": "• activity: {activity}",
        "SYSTEM_STATUS_MARKET_STATE_DOWN": "range / downside pressure",
        "SYSTEM_STATUS_MARKET_STATE_UP": "uptrend",
        "SYSTEM_STATUS_MARKET_STATE_NEUTRAL": "neutral / range",
        "SYSTEM_STATUS_MARKET_PRIORITY_SHORT": "SHORT",
        "SYSTEM_STATUS_MARKET_PRIORITY_LONG": "LONG",
        "SYSTEM_STATUS_MARKET_PRIORITY_SELECTIVE": "selective trades",
        "SYSTEM_STATUS_MARKET_ACTIVITY_MODERATE": "moderate",
        "SYSTEM_STATUS_MARKET_ACTIVITY_LOW": "low (filtered)",
        "SYSTEM_STATUS_SECTION_AI": "🎯 AI analysis (real-time):",
        "SYSTEM_STATUS_MARKET_COVERAGE_LINE": "• market coverage: {count} coins",
        "SYSTEM_STATUS_MARKET_CYCLE_LINE": "• analyzed per cycle: {count}",
        "SYSTEM_STATUS_SAFE_MODE_LINE": "• protection mode: {mode}",
        "SYSTEM_STATUS_SAFE_MODE_ON": "SAFE (adaptive load)",
        "SYSTEM_STATUS_SAFE_MODE_OFF": "standard",
        "SYSTEM_STATUS_SECTION_FILTERING": "🧪 Scenario Filtering (current cycle):",
        "SYSTEM_STATUS_PRESCORE_CHECKED_LINE": "• scenarios reviewed: {count}",
        "SYSTEM_STATUS_PRESCORE_PASSED_LINE": "• meeting conditions: {count}",
        "SYSTEM_STATUS_PRESCORE_FILTERED_LINE": "• rejected by risk/structure: {count}",
        "SYSTEM_STATUS_SIGNALS_SENT_LINE": "• signals sent: {count}{suffix}",
        "SYSTEM_STATUS_SIGNALS_SENT_NONE": "(no confirmation)",
        "SYSTEM_STATUS_SECTION_LAST_SIGNAL": "📉 Last confirmed scenario:",
        "SYSTEM_STATUS_LAST_SIGNAL_LINE": "{symbol} — {side} | {datetime}",
        "SYSTEM_STATUS_LAST_SIGNAL_NONE": "— if there were no signals",
        "SYSTEM_STATUS_SECTION_PUMP": "⚡ Pump / Dump monitor:",
        "SYSTEM_STATUS_PUMP_STATUS_LINE": "• status: {status}",
        "SYSTEM_STATUS_PUMP_IMPULSE_LINE": "• impulses without confirmation: ignored",
        "BTN_AI_ON": "🔔 Enable AI notifications",
        "BTN_AI_OFF": "🚫 Disable AI notifications",
        "BTN_PD_ON": "🔔 Enable Pump/Dump notifications",
        "BTN_PD_OFF": "🚫 Disable Pump/Dump notifications",
        "PERIOD_1D": "1 day",
        "PERIOD_7D": "7 days",
        "PERIOD_30D": "30 days",
        "PERIOD_ALL": "All time",
        "OFFER_TEXT": (
            "30 Days - $39\n"
            "Lifetime - $299"
        ),
        "PAYMENT_PICK_PLAN_TEXT": "Choose a USDT payment plan:",
        "PAYMENT_INVOICE_TEXT": "💳 Krypton AI subscription payment\nPlan: {plan}\nAmount: {amount} USDT\nTap Pay.",
        "PAYMENT_CREATE_FAIL": "Could not create invoice. Please try again later.",
        "OFFER_POINT3_EXTRA": " Materials are for informational purposes only.",
        "OFFER_CUTOFF_MARKER": "3) The bot is not a financial advisor and does not provide personalized enter/exit recommendations.",
        "PD_ENABLED_TEXT": (
            "✅ Pump/Dump notifications enabled.\n"
            "The bot will now send alerts on sharp market movements."
        ),
        "SYSTEM_STATUS_BINANCE_ACTIVE": "connected",
        "SYSTEM_STATUS_BINANCE_DOWN": "no connection",
        "SYSTEM_STATUS_MARKET_RISK_OFF": "cautious (SHORT priority)",
        "SYSTEM_STATUS_MARKET_NEUTRAL": "neutral",
        "SYSTEM_STATUS_MARKET_RISK_ON": "bullish (LONG priority)",
        "SYSTEM_STATUS_MARKET_AUTO": "by market",
        "SYSTEM_STATUS_CONTEXT_STATE_RISK_OFF": "range / downside pressure",
        "SYSTEM_STATUS_CONTEXT_STATE_RISK_ON": "uptrend / upside impulse",
        "SYSTEM_STATUS_CONTEXT_STATE_NEUTRAL": "range / mixed",
        "SYSTEM_STATUS_CONTEXT_STATE_AUTO": "by market",
        "SYSTEM_STATUS_CONTEXT_DIRECTION_RISK_OFF": "SHORT",
        "SYSTEM_STATUS_CONTEXT_DIRECTION_RISK_ON": "LONG",
        "SYSTEM_STATUS_CONTEXT_DIRECTION_NEUTRAL": "by market",
        "SYSTEM_STATUS_CONTEXT_DIRECTION_AUTO": "by market",
        "SYSTEM_STATUS_CONTEXT_ACTIVITY_RISK_OFF": "selective",
        "SYSTEM_STATUS_CONTEXT_ACTIVITY_RISK_ON": "active",
        "SYSTEM_STATUS_CONTEXT_ACTIVITY_NEUTRAL": "moderate",
        "SYSTEM_STATUS_CONTEXT_ACTIVITY_AUTO": "moderate",
        "SYSTEM_STATUS_SAFE_MODE_ON": "protection mode active (SAFE)",
        "SYSTEM_STATUS_SAFE_MODE_OFF": "protection mode: normal",
        "SYSTEM_STATUS_PUMP_IMPULSE_MOST": "most",
        "SYSTEM_STATUS_PUMP_IMPULSE_SOME": "a significant share",
        "SYSTEM_STATUS_PUMP_IMPULSE_FEW": "minority",
        "SYSTEM_STATUS_PUMP_IMPULSE_UNKNOWN": "no data",
        "SYSTEM_STATUS_SIGNALS_PENDING": "awaiting confirmation",
        "SYSTEM_STATUS_SIGNALS_RUNNING": "running",
        "SYSTEM_STATUS_SIGNALS_PAUSED": "paused/error",
        "SYSTEM_STATUS_PUMP_ACTIVE": "active",
        "SYSTEM_STATUS_PUMP_PAUSED": "paused/error",
        "PAYWALL_AI": (
            "🔒 AI signals are available by subscription.\n"
            "Tap “Buy subscription” — I’ll show the instructions."
        ),
        "PAYWALL_PD": (
            "🔒 Pump/Dump alerts are available by subscription.\n"
            "Tap “Buy subscription” — I’ll show the instructions."
        ),
        "PAYWALL_PREVIEW_LIVE_TITLE": "🔒 PREVIEW (Real-time)",
        "PAYWALL_PREVIEW_PD_TITLE": "🔒 Pump/Dump preview (Real-time)",
        "PAYWALL_PREVIEW_LEVELS": "Levels are available with a subscription:",
        "PAYWALL_PREVIEW_PD_METRICS": "Metrics are available with a subscription:",
        "PAYWALL_PREVIEW_BUY_PROMPT": "👉 Buy subscription — to see full details instantly",
        "TRIAL_SUFFIX_AI": "\n\n🎁 {left}/{limit} free AI signals left",
        "TRIAL_SUFFIX_PD": "\n\n🎁 {left}/{limit} free Pump/Dump signals left",
        "LANG_PICK_TEXT": "Please choose a language:",
        "LANG_RU": "🇷🇺 Русский",
        "LANG_EN": "🇬🇧 English",
        "BTN_ACCEPT": "✅ Accept",
        "BTN_PAY_TON": "Pay USDT ($39)",
        "BTN_PAY_USDT": "Pay - USDT",
        "BTN_PLAN_30D_USDT": "30 days — $39 (USDT)",
        "BTN_PLAN_LIFE_USDT": "Lifetime — $299 (USDT)",
        "PLAN_30D": "30 days",
        "PLAN_LIFE": "lifetime",
        "BTN_CONTACT_ADMIN": "Contact admin",
        "BTN_COPY_ADDRESS": "📋 Copy address",
        "BTN_SEND_RECEIPT": "📎 Send receipt + ID",
        "BTN_BUY_SUB": "💳 Buy subscription",
        "BTN_EXPAND": "📖 Expand",
        "BTN_COLLAPSE": "🔽 Collapse",
        "STATUS_LABEL": "Status",
        "STATUS_ON": "✅ enabled",
        "STATUS_OFF": "⛔ disabled",
        "STATS_ROOT_TEXT": "📊 Statistics\nChoose an archive:",
        "STATS_PICK_TEXT": "📊 AI signals archive\nChoose a period:",
        "STATS_PICK_PD_TEXT": "📊 Pump/Dump archive\nChoose a period:",
        "BTN_ARCHIVE_AI": "🎯 AI signals archive",
        "BTN_ARCHIVE_PD": "⚡ Pump/Dump archive",
        "stats_since_date_note": "Stats are calculated since {date} (post-update).",
        "legacy_hidden_notice": "Signal hidden (pre-update).",
        "STATS_PRO_TITLE": "📊 Signal statistics (PRO)",
        "STATS_PRO_RECOMMENDED_HEADER": "🔥 Recommended signals",
        "STATS_PRO_RECOMMENDED_SUB": "(primary working range)",
        "STATS_PRO_SCORE_RANGE_90_100": "Score 90–100",
        "STATS_PRO_WINRATE_LINE": "• Winrate: {winrate}",
        "STATS_PRO_AVG_RR_LINE": "• Avg RR: {avg_rr}",
        "STATS_PRO_TOTAL_SIGNALS_LINE": "• Total signals: {count}",
        "STATS_PRO_STATUS_PRIMARY": "• Status: 🟢 Primary focus",
        "STATS_PRO_RR_NOTE": "ℹ️ With RR > 2, even a 40–45% winrate can be positive expectancy.",
        "STATS_PRO_DIVIDER": "────────────────────",
        "STATS_PRO_HIGH_RISK_HEADER": "⚠️ Higher risk",
        "STATS_PRO_HIGH_RISK_SUB": "(experienced traders only)",
        "STATS_PRO_SCORE_RANGE_80_89": "Score 80–89",
        "STATS_PRO_STATUS_SELECTIVE": "• Status: 🟡 Use selectively",
        "STATS_PRO_BELOW_THRESHOLD_HEADER": "🚫 Below quality threshold",
        "STATS_PRO_BELOW_THRESHOLD_SUB": "(not recommended for trading)",
        "STATS_PRO_BELOW_THRESHOLD_SCORE": "Score < 80",
        "STATS_PRO_BELOW_THRESHOLD_LINE1": "• Not included in statistics",
        "STATS_PRO_BELOW_THRESHOLD_LINE2": "• Used for market analysis only",
        "STATS_PRO_SUMMARY_HEADER": "📈 Trade summary",
        "STATS_PRO_SUMMARY_SUB": "(for {period})",
        "STATS_PRO_TP_TOTAL": "🟢 Successful (TP): {tp_total}",
        "STATS_PRO_SL_TOTAL": "🔴 Stopped (SL): {sl_total}",
        "STATS_PRO_NEUTRAL_TOTAL": "⚪ No entry: {neutral_total}",
        "STATS_PRO_IN_PROGRESS_TOTAL": "🕒 In progress: {in_progress_total}",
        "STATS_PRO_NEUTRAL_NOTE": "ℹ️ Neutral — scenario reached neither TP nor SL",
        "pd_locked_title": "🔒 Pump/Dump archive",
        "pd_locked_text": "This record will be available in {time_left} (60 minutes after event).\nUpgrade for instant access.",
        "btn_upgrade": "🔐 Upgrade",
        "btn_back": "⬅️ Back",
        "time_left_fmt": "{mm}:{ss}",
        "PD_LOCKED_UPDATE_TOAST": "🔄 Timer updated",
        "STATS_PRO_NEUTRAL_NOTE_2": "(range, time-based cancel, or manual close).",
        "STATS_PRO_USAGE_HEADER": "🧠 How to use signals",
        "STATS_PRO_USAGE_PRIMARY": "• Primary focus: Score 90–100",
        "STATS_PRO_USAGE_HIGH_RISK": "• 80–89 — higher risk",
        "STATS_PRO_USAGE_AVOID": "• Below 80 — do not trade",
        "STATS_PRO_RISK_NOTE": "⚠️ Recommended risk: 0.5–1% of equity per trade",
        "STATS_PRO_LEVERAGE_NOTE": "ℹ️ Leverage is chosen by the trader",
        "SYS_HOW_BOT_WORKS_TEXT": (
            "🧠 How Krypton AI works\n\n"
            "Krypton AI is a systematic market analysis algorithm that uses a multi-layer model to assess the probability of price movement.\n\n"
            "The bot does not guess the market.\n"
            "It evaluates structure, momentum, volatility, and context.\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "📊 1) Market regime (BTC context)\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "Before every signal, the bot evaluates BTC market state:\n\n"
            "• TREND — directional market\n"
            "• CHOP — range / chop\n"
            "• SQUEEZE — impulsive move\n"
            "• RISK-OFF — elevated volatility\n\n"
            "If the market is unstable, the bot may block LONG or SHORT setups to reduce stop-loss probability.\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "📈 2) Structure and trend\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "The bot analyzes:\n\n"
            "• EMA 21 / EMA 50\n"
            "• Move slope\n"
            "• Pullback to EMA\n"
            "• Reaction at key zone (POI)\n"
            "• Higher timeframe structure\n\n"
            "The bot does not enter \"in the middle of nowhere\" — it needs a valid structural setup.\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "📍 3) Point of Interest (POI)\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "A scenario is formed first.\n"
            "Then price must enter the POI.\n"
            "After that, confirmation on 5–15m is required.\n\n"
            "Without confirmation, entry is not activated.\n"
            "If confirmation does not appear, the scenario is closed without a trade.\n\n"
            "This is not a loss.\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "🎯 4) Risk / Reward\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "• Minimum RR: ≥ 1:2\n"
            "• Stop is calculated dynamically via ATR\n"
            "• Too-tight stops are filtered out\n\n"
            "The bot is designed for moderate leverage (~x10).\n"
            "It does not know your actual leverage — risk depends on your capital management.\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "⚙ 5) Score (0–100)\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "Each signal gets a quality score.\n\n"
            "Points are assigned for:\n\n"
            "• Global trend\n"
            "• Local trend\n"
            "• POI reaction\n"
            "• Liquidity sweep\n"
            "• Volume\n"
            "• RSI\n"
            "• Volatility (ATR)\n"
            "• EMA alignment\n"
            "• Market regime\n\n"
            "📌 Score ≥ 90 — primary focus\n"
            "⚠ Score 80–89 — higher risk\n"
            "🚫 Below 80 — used for market analysis\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "⏳ 6) Time to live (TTL)\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "Each scenario has a limited lifetime.\n"
            "If entry is not confirmed — it is closed.\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "📌 Important\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "The bot works on a probabilistic model.\n\n"
            "Result depends on:\n"
            "• discipline\n"
            "• respecting SL\n"
            "• risk management\n"
            "• adequate leverage\n\n"
            "Krypton AI is a system.\n"
            "A system requires discipline."
        ),
        "SYS_HOW_BOT_WORKS_CUTOFF_MARKER": "context.",
        "SYS_HOW_BOT_WORKS_BTN_EXPAND": "Expand",
        "SYS_HOW_BOT_WORKS_BTN_HIDE": "Hide",
        "SYSTEM_SECTION_TEXT": "ℹ️ Section: System",
        "BACK_TO_MAIN_TEXT": "Returning to the main menu.",
        "ALREADY_ON": "Already enabled.",
        "ALREADY_OFF": "Already disabled.",
        "AI_ALREADY_ON": "ℹ️ AI notifications are already enabled.",
        "AI_ALREADY_OFF": "ℹ️ AI notifications are already disabled.",
        "AI_ON_OK": "✅ AI notifications enabled.",
        "AI_OFF_OK": "🚫 Notifications disabled.",
        "PD_ALREADY_ON": "ℹ️ Pump/Dump notifications are already enabled.",
        "PD_ALREADY_OFF": "ℹ️ Pump/Dump notifications are already disabled.",
        "PD_ON_OK": (
            "✅ Pump/Dump notifications enabled.\n"
            "The bot will now send alerts on sharp market moves."
        ),
        "PD_OFF_OK": "🚫 Pump/Dump notifications disabled.",
        "NO_ACCESS": "⛔ Access denied",
        "SIGNAL_NOT_FOUND": "Signal not found.",
        "CONTACT_ADMIN_BLOCK": (
            "💬 Contact admin: {admin_contact}\n"
            "When messaging, include your ID: {user_id}"
        ),
        "AI_STATS_TITLE": "📊 AI signal stats ({period})",
        "AI_STATS_DISCLAIMER": "ℹ️ This is scenario performance statistics, not a profit guarantee.",
        "AI_STATS_NO_COMPLETED": "No completed signals for the period.",
        "AI_STATS_SIGNALS_COUNT": "Signals: {total}",
        "AI_STATS_SUMMARY": "TP1+: {tp1} | TP2: {tp2} | SL: {sl} | Exp: {exp}",
        "AI_STATS_WINRATE": "Winrate (TP1+): {winrate:.1f}%",
        "AI_STATS_SCORE_LABEL": "Score:",
        "AI_STATS_BUCKET_LINE": "{label}:  {total} (TP1+: {winrate:.0f}%)",
        "HISTORY_TITLE": "📊 Signal history ({period})",
        "HISTORY_SUMMARY": (
            "✅ Success: {passed}\n"
            "❌ Fail: {failed}\n"
            "⚪ Neutral: {neutral}\n"
            "⏰ In progress: {in_progress}"
        ),
        "HISTORY_STATS_TITLE": "📊 Stats ({period}) — by Score",
        "HISTORY_SCORE_BUCKET_LINE": (
            "{label}: ✅ {passed} / ❌ {failed} / ⚪ {neutral} / ⏰ {in_progress}  ({percent}%)"
        ),
        "HISTORY_NO_SIGNALS": "No signals for the period ({period}).",
        "HISTORY_EMPTY_PERIOD": "No signals for the selected period.",
        "HISTORY_NO_SIGNALS_BUTTON": "No signals for the period ({period}).",
        "HISTORY_RESULT_LABEL": "Result:",
        "HISTORY_ENTRY_LABEL": "Entry:",
        "HISTORY_DATE_LABEL": "Date:",
        "HISTORY_LIST_TITLE": "📜 Signal history — {period}",
        "HISTORY_PAGE_INFO": "Page {page}/{pages}",
        "history_title": "📜 Signal history — {period}",
        "page_total": "Page {page}/{pages}",
        "section_recommended_title": "🔥 Recommended signals (Score 90–100)",
        "section_higher_risk_title": "⚠️ Higher risk (Score 80–89)",
        "section_score_below_title": "🚫 Score below 80",
        "line_winrate": "• Winrate: {value}%",
        "line_avg_rr": "• Avg RR: {value}",
        "line_trades": "• Trades: {value}",
        "line_status": "• Status: {value}",
        "status_main_focus": "🟢 Main focus",
        "status_use_selectively": "🟡 Use selectively",
        "line_not_included": "• Not included in stats",
        "line_market_analysis_only": "• Used only for market analysis",
        "totals_title": "📈 Summary",
        "totals_tp": "🟢 TP: {value}",
        "totals_be": "🟢 BE (+8%): {value}",
        "totals_be_avg": "🟢 BE: {value} | avg result +{avg}% to deposit (x{lev})",
        "totals_sl": "🔴 SL: {value}",
        "totals_exp": "⚪ EXP: {value}",
        "totals_active": "🟣 Active: {value}",
        "totals_expired_no_entry": "⚪ Expired (no entry): {value}",
        "totals_no_confirmation": "🔵 No confirmation: {value}",
        "totals_in_progress": "🟣 In progress: {value}",
        "line_winrate_strict": "• Winrate: {value}%",
        "line_winrate_tp_be": "📊 Winrate: {value}% | Formula: (TP+BE)/(TP+BE+SL)",
        "line_success_rate": "• Success rate: {value}%  (TP + BE)",
        "explanation_title": "ℹ️ Explanation",
        "explanation_line_1": "• Score 80+ — included in winrate and RR calculations",
        "explanation_line_2": "• Score below 80 — used only for market analysis",
        "explanation_line_be": "🟢 BE (+8%) — price reached at least +8% profit, profit protected.",
        "explanation_line_be_2": "Counted as successful trade.",
        "explanation_line_exp": "⚪ EXP — scenario expired: entry was not confirmed within the signal lifetime.",
        "HISTORY_WINRATE_TITLE": "📊 Winrate by Score:",
        "HISTORY_WINRATE_BUCKET_90_100": "Score 90–100:",
        "HISTORY_WINRATE_BUCKET_80_89": "Score 80–89:",
        "HISTORY_WINRATE_NO_DATA": "no data",
        "HISTORY_PRO_BLOCK": (
            "🔥 Recommended signals\n"
            "Score 90–100\n\n"
            "• Winrate: {winrate_90_100}%\n"
            "• Avg RR: ~1 : {avg_rr_90_100}\n"
            "• Total signals: {closed_90_100}\n"
            "• Status: 🟢 Primary focus\n\n"
            "ℹ️ With RR > 2, even 40–45% can deliver positive expectancy.\n\n"
            "————————————\n\n"
            "⚠️ Higher risk\n"
            "Score 80–89\n\n"
            "• Winrate: {winrate_80_89}%\n"
            "• Total signals: {closed_80_89}\n"
            "• Status: 🟡 Use selectively\n\n"
            "───────────\n\n"
            "🚫 Score < 80\n"
            "• Not included in statistics\n"
            "• Used for market analysis only\n\n"
            "───────────\n\n"
            "📈 Trade summary\n\n"
            "🟢 TP: {tp_total}\n"
            "🔴 SL: {sl_total}\n"
            "⚪ Neutral: {neutral_total}\n"
            "🕒 In progress: {in_progress_total}"
        ),
        "HISTORY_LOAD_ERROR": "History loading error. Try later.",
        "NAV_PREV_SHORT": "Back",
        "NAV_NEXT_SHORT": "Forward",
        "STATS_LOAD_ERROR": "Stats loading error. We're fixing it.",
        "UNKNOWN_PERIOD": "Unknown period.",
        "STATUS_OPEN": "Open",
        "STATUS_ACTIVE_WAITING": "Active — waiting for outcome",
        "STATUS_NO_FILL": "No entry",
        "STATUS_AMBIGUOUS": "Ambiguous",
        "ARCHIVE_DETAIL_LIFETIME": "⏱ Scenario lifetime: {hours} hours",
        "ARCHIVE_DETAIL_REASON_HEADER": "🧠 Why this signal was chosen (Score {score}):",
        "ARCHIVE_DETAIL_HEADER_LINE": "📌 {symbol} {side} · Score {score}",
        "ARCHIVE_DETAIL_PREVIEW_TITLE": "🔒 PREVIEW (Real-time)",
        "ARCHIVE_DETAIL_SUBSCRIPTION_LEVELS": "Levels are available with subscription:",
        "ARCHIVE_DETAIL_BUY_SUB_PROMPT": "👉 Buy subscription — to unlock levels instantly",
        "ARCHIVE_DETAIL_UNLOCK_DELAY": "🔓 Full access unlocks in {delay}",
        "NAV_PREV": "◀️ Back",
        "NAV_NEXT": "▶️ Next",
        "NAV_BACK": "◀️ Back",
        "pagination_next_label": "▶️ Next",
        "nav_back_label": "◀️ Back",
        "nav_next_label": "▶️ Next",
        "nav_prev_page": "◀️ Back",
        "nav_next_page": "▶️ Next",
        "nav_back_to_periods": "↩️ Back to periods",
        "totals_no_entry_label": "No entry",
        "history_status_tp": "🟢 TP",
        "history_status_sl": "🔴 SL",
        "history_status_expired_no_entry": "⚪ Expired (no entry)",
        "history_status_no_confirmation": "🔵 No confirmation",
        "history_status_poi_touched": "🟠 POI touched",
        "history_status_activated": "🟡 Activated",
        "history_status_in_progress": "🟣 In progress",
        "history_score_label_short": "Score {value}",
        "history_expired_helper": "ℹ️ Expired signals mean no trade occurred and are not losses.",
        "history_indicator_waiting": "🟣 In progress (waiting)",
        "history_indicator_poi_touched": "🟠 POI touched",
        "history_indicator_activated": "🟡 Activated (entry confirmed)",
        "PD_HISTORY_TITLE": "⚡ Pump/Dump history — {period}",
        "PD_EVENT_TITLE": "⚡ Pump/Dump event",
        "PD_EVENT_SYMBOL": "SYMBOL: {symbol}",
        "PD_EVENT_TYPE": "Type: {icon} {side}",
        "PD_EVENT_TIME": "Time: {time}",
        "PD_EVENT_DELTA_1M": "Δ (1m): {value}%",
        "PD_EVENT_DELTA_5M": "Δ (5m): {value}%",
        "PD_EVENT_VOLUME": "Volume (5m): {value} USDT",
        "PD_EVENT_VOL_MULT": "Volume xAvg: {value}x",
        "PD_SIDE_PUMP": "PUMP",
        "PD_SIDE_DUMP": "DUMP",
        "PD_BACK_TO_LIST": "⬅️ Back to list",
        "PD_EXPLAIN_TITLE": "ℹ️ What it means",
        "PD_EXPLAIN_LINE_1": "🚀 PUMP — sharp rise in 1–5m",
        "PD_EXPLAIN_LINE_2": "🔻 DUMP — sharp drop in 1–5m",
        "PD_EXPLAIN_LINE_3": "📌 Not a trade entry. Volatility alert only.",
        "PD_EXPLAIN_LINE_4": "Use it as an alert to review the chart.",
        "explanation_block": (
            "━━━━━━━━━━━━━━━━\n"
            "ℹ️ Explanation\n"
            "━━━━━━━━━━━━━━━━\n"
            "• Score ≥ 80 — included in winrate and RR calculations\n"
            "• Score below 80 — used only for market analysis\n"
            "• “No entry” — price didn’t reach confirmation/entry,\n"
            "  or the scenario was canceled/expired"
        ),
        "PAYMENT_COPY_ADDRESS": "📋 Payment address (USDT):\n{wallet}",
        "RECEIPT_REQUEST_TEXT": (
            "📎 Send the receipt here (screenshot/photo) in one message.\n"
            "I will attach your ID and forward it to the admin."
        ),
        "RECEIPT_SENT_CONFIRM": "✅ Receipt sent to admin. Please wait for activation.",
        "USER_LIST_EMPTY": "No users yet.",
        "USER_LIST_HEADER": "👥 Users (last 50):",
        "USER_CARD_TITLE": "👤 User card",
        "USER_CARD_STATUS": "Status: {status}",
        "USER_CARD_SUBSCRIPTION": "Subscription: {subscription}",
        "USER_CARD_AI_LEFT": "AI left: {left}/{limit}",
        "USER_CARD_PD_LEFT": "Pump/Dump left: {left}/{limit}",
        "USER_CARD_ACTIVE_UNTIL": "active until {date}",
        "USER_CARD_SUB_NONE": "none",
        "USER_CARD_STARTED_AT": "started_at: {date}",
        "USER_CARD_LAST_SEEN": "last_seen: {date}",
        "USER_BTN_UNLOCK": "🔓 Unblock",
        "USER_BTN_LOCK": "🔒 Block",
        "USER_BTN_TRIAL_48H": "🎁 Grant 48h access",
        "USER_BTN_DELETE": "🗑 Delete",
        "USER_DELETE_CONFIRM": (
            "⚠️ Delete user {user_id}?\n\n"
            "This will remove them from the database (including limits/statuses)."
        ),
        "USER_DELETE_CONFIRM_YES": "✅ Yes, delete",
        "USER_DELETE_CONFIRM_NO": "❌ Cancel",
        "USER_DELETED_NOTICE": "Your account was deleted by an administrator.",
        "USER_DELETED_PREFIX": "✅ User deleted: {user_id}",
        "USER_DELETED_ALERT": "✅ User deleted: {user_id}",
        "USER_TRIAL_48H_NOTICE": (
            "🎁 You have been granted full access for 48 hours to evaluate the bot and signals.\n"
            "After the period ends, signal access returns to free-tier limits."
        ),
        "USER_TRIAL_48H_PREFIX": "✅ 48h trial access granted: {user_id}",
        "USER_TRIAL_48H_ALERT": "✅ 48h access granted to user {user_id}",
        "USER_LOCKED_NOTICE": (
            "⛔ Subscription paused\n\n"
            "Access to signals has been temporarily disabled by the administrator.\n"
            "Contact: {admin_contact}\n"
            "Your ID: {user_id}"
        ),
        "USER_UNLOCKED_NOTICE": "Subscription activated for 30 days",
        "TEST_NO_SUBSCRIBERS": (
            "⚠️ No subscribers. Enable notifications on a test account and try again."
        ),
        "TEST_AI_PREFIX": (
            "🧪 TEST AI SIGNAL (system check)\n\n"
            "⚠️ This is a test. If the limit is 0, a paywall should appear instead of this text.\n\n"
        ),
        "TEST_AI_DONE": (
            "AI test\n"
            "✅ AI test done: sent={sent}, locked={locked}, "
            "paywall={paywall}, errors={errors} (subscribers={subscribers})"
        ),
        "TEST_PD_PREFIX": "🧪 TEST PUMP/DUMP (system check)\n\n",
        "TEST_PD_WARNING": "⚠️ This is a test. If the limit is 0, a paywall should appear instead.",
        "TEST_PD_DONE": (
            "Pump/Dump test\n"
            "✅ Pump/Dump test done: sent={sent}, locked={locked}, "
            "paywall={paywall}, errors={errors} (subscribers={subscribers})"
        ),
        "TEST_NOTIFY_TEXT": "🧪 Test notification: delivery works.",
        "TEST_NOTIFY_ERROR": "❌ Error: {error}",
        "PURGE_TESTS_DONE": "✅ Test signals removed: {removed}",
        "PURGE_SYMBOL_DONE": "✅ {symbol}: deleted signal_events={events}, signal_audit={audit}",
        "CMD_USAGE_LOCK": "Usage: /lock <id>",
        "CMD_USAGE_UNLOCK": "Usage: /unlock <id>",
        "CMD_USAGE_DELETE": "Usage: /delete <id>",
        "CMD_USAGE_PURGE": "Usage: /purge <symbol>",
        "CMD_LOCK_OK": "✅ user_locked=1 for {user_id}",
        "CMD_UNLOCK_OK": "✅ user_locked=0 for {user_id}",
        "CMD_DELETE_OK": "✅ user {user_id} deleted",
        "ADMIN_STATS_TITLE": "📊 Signal stats (30d)",
        "ADMIN_STATS_TOTAL": "• Total: {total}",
        "ADMIN_STATS_CLOSED": "• Closed: {closed}",
        "ADMIN_STATS_FILLED_RATE": "• Filled rate: {rate:.1f}% ({filled} of {total})",
        "ADMIN_STATS_WINRATE": "• Winrate (filled): {winrate:.1f}%",
        "ADMIN_STATS_PROFIT_FACTOR": "• Profit factor: {profit_factor}",
        "ADMIN_STATS_AVG_R": "• Avg R: {avg_r:.2f}",
        "ADMIN_STATS_MEDIAN_R": "• Median R: {median_r:.2f}",
        "ADMIN_STATS_STREAK": "• Streak: {streak}",
        "ADMIN_STATS_LAST10": "Last 10 signals:",
        "ADMIN_STATS_NO_DATA": "• No data",
        "ADMIN_STATS_ROW": "• {symbol} {direction} → {outcome} ({pnl})",
        "STATUS_HUMAN_SECONDS": "{seconds} sec",
        "STATUS_HUMAN_MINUTES": "{minutes} min",
        "STATUS_HUMAN_HOURS": "{hours} h",
        "STATUS_AGO_SECONDS": "{seconds} sec ago",
        "STATUS_AGO_MINUTES": "{minutes} min ago",
        "STATUS_AGO_HOURS": "{hours} h ago",
        "SCENARIO_TREND_BULLISH": "bullish",
        "SCENARIO_TREND_BEARISH": "bearish",
        "SCENARIO_TREND_FLAT": "flat",
        "SCENARIO_TREND_NEUTRAL": "neutral",
        "SCENARIO_RSI_OVERBOUGHT": "overbought",
        "SCENARIO_RSI_OVERSOLD": "oversold",
        "SCENARIO_RSI_NEUTRAL": "neutral",
        "SCENARIO_RSI_COMFORT": "comfort zone",
        "SCENARIO_RSI_OVERSOLD_ZONE": "oversold zone",
        "SCENARIO_RSI_OVERBOUGHT_ZONE": "overbought zone",
        "SCENARIO_POSSIBLE_LINE": "{emoji} Potential {scenario}",
        "SCENARIO_TIMEFRAME_LINE": "⏱ Scenario timeframe: {timeframe} | Entry: 5–15m",
        "SCENARIO_LIFETIME_LINE": "⏱ Scenario lifetime: {hours} hours",
        "SCENARIO_LIFETIME_MINUTES_LINE": "⏱ Scenario lifetime: ~{minutes} minutes",
        "SCENARIO_POI_HEADER": "Point of interest (POI):",
        "SCENARIO_CONDITIONS_HEADER": "Execution conditions:",
        "SCENARIO_VALID_ABOVE": "• the scenario is valid while price holds above the zone",
        "SCENARIO_VALID_BELOW": "• the scenario is valid while price holds below the zone",
        "SCENARIO_CONFIRMATION_LINE": "• entry is considered only after confirmation on 5–15m",
        "SCENARIO_CONFIRM_HEADER": "🔎 Confirmation on 5–15m:",
        "SCENARIO_CONFIRM_CLOSE": (
            "• candle closes in direction (above the zone for LONG / below for SHORT)"
        ),
        "SCENARIO_CONFIRM_HOLD": "• price holds outside the zone without a quick return",
        "SCENARIO_INVALIDATION_HEADER": "Scenario invalidation:",
        "SCENARIO_INVALIDATION_LINE": "• if a 1H candle closes {condition} {level}",
        "SCENARIO_SL_HEADER": "🛑 Stop Loss:",
        "SCENARIO_SL_LINE": "• SL: {price} ({pct})",
        "SCENARIO_TARGETS_HEADER": "Potential targets:",
        "SCENARIO_CONTEXT_HEADER": "Brief context:",
        "SCENARIO_CONTEXT_TREND": "• Trend 1D / 4H: {trend_1d} / {trend_4h}",
        "SCENARIO_CONTEXT_RSI": "• RSI 1H: {rsi} ({zone})",
        "SCENARIO_CONTEXT_VOLUME": "• Volume: {volume}x vs average",
        "SCENARIO_CONTEXT_RR": "• RR ≈ 1 : {rr}",
        "SIGNAL_QUALITY_RECOMMENDED": (
            "🔥 RECOMMENDED SIGNAL\n"
            "Primary trading range (Score 90–100)\n"
            "Used for trading"
        ),
        "SIGNAL_QUALITY_HIGH_RISK": (
            "🔥 ACTIVE SCENARIO\n"
            "For experienced traders (Score 80–89)\n"
            "Use selectively"
        ),
        "SIGNAL_QUALITY_ANALYSIS_ONLY": (
            "🚫 ANALYSIS ONLY\n"
            "Below quality threshold (Score < 80)\n"
            "Not recommended for trading"
        ),
        "SIGNAL_COMPACT_HIGH_RISK_HEADER": "🔥 ACTIVE SCENARIO (Score 80–89)",
        "SIGNAL_COMPACT_META_LINE": "{side} · TF: {timeframe} · Entry: {entry_tf}",
        "SIGNAL_COMPACT_POI_LINE": "POI: {poi_from}–{poi_to}",
        "SIGNAL_COMPACT_SL_LINE": "SL: {sl}",
        "SIGNAL_COMPACT_TP1_LINE": "TP1: {tp1}",
        "SIGNAL_COMPACT_TP2_LINE": "TP2: {tp2}",
        "SIGNAL_COMPACT_SCORE_LINE": "Score: {score}",
        "SIGNAL_MARKET_REGIME_LINE": "🧭 Market regime: {regime} ({direction})",
        "SIGNAL_MARKET_REGIME_TREND": "Trend",
        "SIGNAL_MARKET_REGIME_CHOP": "Chop / Range",
        "SIGNAL_MARKET_REGIME_SQUEEZE": "Impulse",
        "SIGNAL_MARKET_REGIME_RISK_OFF": "Risk-off",
        "SIGNAL_MARKET_DIR_UP": "up",
        "SIGNAL_MARKET_DIR_DOWN": "down",
        "SIGNAL_MARKET_DIR_NEUTRAL": "neutral",
        "SIGNAL_MARKET_TREND_LINE": "📈 Trend: {trend}",
        "SIGNAL_TREND_YES": "yes",
        "SIGNAL_TREND_NO": "no",
        "SIGNAL_COMPACT_TTL_LINE": "⏱ TTL: ~{minutes} min",
        "SIGNAL_BUTTON_EXPAND": "▾ Expand",
        "SIGNAL_BUTTON_COLLAPSE": "▴ Collapse",
        "btn_binance": "📈 Binance",
        "btn_binance_spot": "📈 Binance",
        "SIGNAL_SHORT_SIDE_LONG": "📈 LONG",
        "SIGNAL_SHORT_SIDE_SHORT": "📉 SHORT",
        "SIGNAL_SHORT_SYMBOL_SIDE_LINE": "{symbol} · {side}",
        "SIGNAL_SHORT_POI_LINE": "POI: {poi_from}–{poi_to}",
        "SIGNAL_SHORT_TP1_LINE": "TP1: {tp1}",
        "SIGNAL_SHORT_TP2_LINE": "TP2: {tp2}",
        "SIGNAL_SHORT_SL_LINE": "SL: {sl}",
        "SIGNAL_SHORT_80_89_SYMBOL_LINE": "{symbol} / USDT",
        "SIGNAL_SHORT_80_89_META_LINE": "{side} · TF: {timeframe} · Entry: {entry_tf}",
        "SIGNAL_SHORT_80_89_SCORE_LINE": "Score: {score}",
        "SIGNAL_SHORT_80_89_TTL_LINE": "TTL: ~{minutes} min",
        "SIGNAL_SHORT_HIGH_RISK_WARNING": (
            "🔥 ACTIVE SCENARIO\n"
            "For experienced traders\n"
            "Use selectively"
        ),
        "SIGNAL_BUTTON_SOUND_ON": "🔔 Sound: Signal + Entry",
        "SIGNAL_BUTTON_SOUND_OFF": "🔕 Sound: Silent (Signal + Entry)",
        "SIGNAL_SOUND_TOGGLE_TOAST_ON": "Signal+Entry sound: ON",
        "SIGNAL_SOUND_TOGGLE_TOAST_OFF": "Signal+Entry sound: OFF",
        "SIGNAL_STATUS_TOGGLE_ON": "✅ Alerts: ON",
        "SIGNAL_STATUS_TOGGLE_OFF": "⛔ Alerts: OFF",
        "SIGNAL_STATUS_TOGGLE_TOAST_ON": "Alerts enabled",
        "SIGNAL_STATUS_TOGGLE_TOAST_OFF": "Alerts disabled",
        "SCENARIO_SCORE_LINE": "🧠 Score: {score} / 100",
        "SCENARIO_BREAKDOWN_HEADER": "🧩 Score details (sum of points):",
        "SCENARIO_BREAKDOWN_TOTAL": "= Final score: {score}",
        "SCENARIO_DISCLAIMER_1": (
            "ℹ️ Score is an internal quality rating based on market factors and model conditions."
        ),
        "SCENARIO_DISCLAIMER_2": "ℹ️ The bot finds setups and does not guarantee profit.",
        "SCENARIO_DISCLAIMER_3": "ℹ️ The scenario requires confirmation before entry.",
        "SCENARIO_TP_NEEDS_REVIEW": "needs clarification",
        "SCENARIO_CONDITION_BELOW": "below",
        "SCENARIO_CONDITION_ABOVE": "above",
        "CLOSE_SL_TITLE": "❌ SL — Signal closed by Stop Loss",
        "CLOSE_TP1_TITLE": "✅ TP1 — Take Profit reached",
        "CLOSE_TP2_TITLE": "✅ TP2 — Take Profit reached",
        "SIGNAL_RESULT_CLOSED_BY_TP1_LINE": "Closed by: TP1",
        "SIGNAL_RESULT_CLOSED_BY_TP2_LINE": "Closed by: TP2",
        "SIGNAL_RESULT_CLOSED_BY_SL_LINE": "Closed by: Stop Loss",
        "SIGNAL_RESULT_HEADER_TP1": "✅ TP1 — TP1 hit",
        "SIGNAL_RESULT_HEADER_TP2": "✅ TP2 — TP2 hit",
        "SIGNAL_RESULT_HEADER_BE": "⚪ BE — Breakeven",
        "SIGNAL_RESULT_HEADER_BE_AFTER_TP1": "⚪ Break-even after TP1 — counted as TP1 in stats",
        "SIGNAL_BE_TRIGGERED_HEADER": "🟢 BE activated (+8%)",
        "SIGNAL_BE_FINALISED_HEADER": "🟢 BE (+{level}%) closed",
        "SIGNAL_BE_LEVEL_LINE": "🎯 BE (+{level}%): {price}",
        "SIGNAL_BE_LEVEL_ONLY_LINE": "🎯 BE (+{level}%)",
        "SIGNAL_BE_REACHED_LINE": "🛡 Reached BE: {price}  (+{level}%)",
        "SIGNAL_BE_MAX_PNL_LINE": "🛡 Max PnL: {pnl}%",
        "SIGNAL_BE_CLOSED_BY_LINE": "Closed by: BE",
        "SIGNAL_RESULT_HEADER_SL": "❌ SL — Closed by stop",
        "SIGNAL_RESULT_HEADER_NF": "⏳ NF — Entry not filled",
        "SIGNAL_RESULT_HEADER_EXP": "⚪ EXP — Scenario expired",
        "SIGNAL_RESULT_HEADER_TP": "🟢 TP — Closed in profit",
        "SIGNAL_ACTIVATED_HEADER": "🟡 Signal activated (entry confirmed)",
        "SIGNAL_ACTIVATED_ENTRY_LABEL": "Entry",
        "SIGNAL_ACTIVATED_SL_LABEL": "Stop Loss",
        "SIGNAL_ACTIVATED_WAITING": "Waiting for outcome.",
        "SIGNAL_POI_TOUCHED_HEADER": "🟠 Price entered POI zone",
        "SIGNAL_POI_TOUCHED_ZONE_HEADER": "📍 Entry zone (POI):",
        "SIGNAL_POI_TOUCHED_WAIT": "⏳ Waiting for 5–15m confirmation.",
        "SIGNAL_POI_TOUCHED_WAIT_2": "Entry will be confirmed only after candle close.",
        "SIGNAL_EXPIRED_NO_ENTRY_HEADER": "⌛ Scenario expired",
        "SIGNAL_EXPIRED_NO_ENTRY_LINE_1": "{symbol} · {side}",
        "SIGNAL_EXPIRED_NO_ENTRY_LINE_2": "Price did not get 5–15m confirmation.",
        "SIGNAL_EXPIRED_NO_ENTRY_LINE_3": "Scenario closed without entry.",
        "SIGNAL_RESULT_ENTRY_LINE": "Entry: {entry}",
        "SIGNAL_RESULT_EXIT_LINE": "✅ Exit: {price}",
        "SIGNAL_RESULT_PNL_LINE": "PnL: {pnl}%",
        "SIGNAL_RESULT_SL_LINE": "🛑 SL: {price}",
        "SIGNAL_RESULT_TP1_LINE": "🎯 TP1: {price}",
        "SIGNAL_RESULT_TP2_LINE": "🎯 TP2: {price}",
        "SIGNAL_PROGRESS_TP1_HEADER": "🟢 TP1 hit",
        "SIGNAL_RESULT_SCORE_LINE": "Score: {score}",
        "BREAKDOWN_GLOBAL_TREND": "Global trend (1D)",
        "BREAKDOWN_LOCAL_TREND": "Local trend (1H)",
        "BREAKDOWN_NEAR_KEY_LEVEL": "Reaction to key zone (POI)",
        "BREAKDOWN_LIQUIDITY_SWEEP": "Liquidity sweep",
        "BREAKDOWN_VOLUME_CLIMAX": "Volume vs average",
        "BREAKDOWN_RSI_DIVERGENCE": "RSI divergence",
        "BREAKDOWN_ATR_OK": "Volatility (ATR)",
        "BREAKDOWN_BB_EXTREME": "Bollinger extreme",
        "BREAKDOWN_MA_TREND_OK": "EMA alignment",
        "BREAKDOWN_ORDERFLOW": "Order flow",
        "BREAKDOWN_WHALE_ACTIVITY": "Whale activity",
        "BREAKDOWN_AI_PATTERN": "AI patterns",
        "BREAKDOWN_MARKET_REGIME": "Market regime",
        "BREAKDOWN_FALLBACK": "Factor",
        "PUMP_HEADER_PUMP": "🚀 Pump/Dump Scanner: sharp impulse",
        "PUMP_HEADER_DUMP": "📉 Pump/Dump Scanner: sharp impulse",
        "PUMP_COIN_LINE": "Coin: {symbol}",
        "PUMP_PRICE_LINE": "Current price: {price}",
        "PUMP_MOVE_HEADER": "Move:",
        "PUMP_MOVE_1M": "• 1m: {change}%",
        "PUMP_MOVE_5M": "• 5m: {change}%",
        "PUMP_VOLUME_LINE": "• volume: {volume:.2f}× avg",
        "PUMP_BUTTON_EXPAND": "📖 Expand",
        "PUMP_BUTTON_COLLAPSE": "🔽 Collapse",
        "PUMP_TOGGLE_EXPANDED": "Expanded",
        "PUMP_TOGGLE_COLLAPSED": "Collapsed",
        "PUMP_TOGGLE_EXPIRED": "Card state is unavailable",
        "PUMP_NOTE_1": "ℹ️ This is an alert about a sharp price/volume impulse.",
        "PUMP_NOTE_2": "It’s an attention signal, not a ready trade idea.",
        "PUMP_RISK_1": "⚠️ Sharp impulses are high-risk.",
        "PUMP_RISK_2": "The bot does not provide entries and does not manage risk.",
        "PUMP_SOURCE": "Source: Binance",
        "ADMIN_NEW_USER": (
            "🆕 New user\n"
            "ID: {user_id}\n"
            "Username: {username}\n"
            "Name: {full_name}\n"
            "Language: {language}"
        ),
        "DIAG_TITLE": "🛠 Bot diagnostics (admin)",
        "DIAG_SECTION_OVERALL": "🧠 Overall status",
        "DIAG_SECTION_AI": "🎯 AI Signals (core module)",
        "DIAG_SECTION_FILTERS": "🧪 Filters (Pre-score)",
        "DIAG_SECTION_BINANCE": "🌐 Binance API",
        "DIAG_SECTION_PUMPDUMP": "🚀 Pump / Dump Scanner",
        "DIAG_STATUS_WORKING": "running",
        "DIAG_STATUS_NOT_STARTED": "not started",
        "DIAG_STATUS_ISSUES": "issues detected",
        "DIAG_STATUS_ERROR": "error",
        "DIAG_STATUS_OK": "OK",
        "DIAG_STATUS_MISSING": "missing file",
        "DIAG_STATUS_PENDING": "pending",
        "DIAG_STATUS_NO_DATA": "no data",
        "DIAG_MODULE_STATUS": "• Status: {status}",
        "DIAG_LAST_TICK": "• Last tick: {tick}",
        "DIAG_ERRORS": "• Errors: {error}",
        "DIAG_ERRORS_HEADER": "• Errors:",
        "DIAG_ERRORS_NONE": "• Errors: ❌ none",
        "DIAG_WARNINGS_HEADER": "• Warnings:",
        "DIAG_WARNINGS_NONE": "• Warnings: ✅ none",
        "DIAG_NO_DATA_LINE": "• No data",
        "DIAG_DB_TITLE": "🗄 Database",
        "DIAG_DB_PATH": "• Path: {path}",
        "DIAG_DB_MISSING": "• File not found",
        "DIAG_DB_SIZE": "• Size: {size} bytes",
        "DIAG_DB_MODIFIED": "• Modified: {mtime}",
        "DIAG_MODULE_LAST_CYCLE": "• Last cycle: {tick}",
        "DIAG_MODULE_LAST_OK": "• Last successful request: {tick}",
        "DIAG_MODULE_ERROR": "• Error: {error}",
        "DIAG_MODULE_WARNING": "• Warning: {warning}",
        "DIAG_USERS_HEADER": "Users",
        "DIAG_SUBSCRIBERS_LINE": "• Subscribers: {count}",
        "DIAG_MARKET_SCAN_HEADER": "• Market scan:",
        "DIAG_MARKET_UNIVERSE": "• Coins in market: {count}",
        "DIAG_MARKET_CHUNK": "• Coins per cycle: {count}",
        "DIAG_MARKET_POSITION_TOTAL": "• Current position: {current} / {total}",
        "DIAG_MARKET_POSITION": "• Current position: {current}",
        "DIAG_MARKET_CURRENT": "• Current coin: {symbol}",
        "DIAG_AI_EXCLUDED": "• Excluded symbols: {symbols}",
        "DIAG_CYCLE_TIME": "• Cycle time: ~{cycle}",
        "DIAG_AI_CONFIG_TITLE": "• AI config:",
        "DIAG_AI_CONFIG_MAX_DEEP": "  • Max deep per cycle: {value}",
        "DIAG_AI_CONFIG_STAGE_A": "  • Stage A top K: {value}",
        "DIAG_AI_CONFIG_PRESCORE_THRESHOLD": "  • Pre-score threshold: {value}",
        "DIAG_AI_CONFIG_PRESCORE_MIN": "  • Min pre-score: {value}",
        "DIAG_AI_CONFIG_FINAL_THRESHOLD": "  • Final scoring threshold: {value}",
        "DIAG_AI_CONFIG_MIN_VOLUME": "  • Min volume 5m: {value}",
        "DIAG_AI_CONFIG_PUMP_VOLUME": "  • Pump volume multiplier: {value}",
        "DIAG_AI_STRUCTURE_TITLE": "• Structure:",
        "DIAG_AI_STRUCTURE_MODE": "  • Mode: {value}",
        "DIAG_AI_STRUCTURE_PENALTY": "  • Neutral penalty: {value}",
        "DIAG_AI_STRUCTURE_HARD_FAIL": "  • Hard fail opposite: {value}",
        "DIAG_AI_STRUCTURE_WINDOW": "  • Window: {value}",
        "DIAG_AI_STRUCTURE_COUNTS": "  • Counts: penalty_neutral={neutral} fail_structure_opposite={opposite} fail_setup_structure={legacy}",
        "DIAG_AI_STRUCTURE_SAMPLE": "  • Sample: {sample}",
        "DIAG_TREND_TITLE": "• Trend mode:",
        "DIAG_TREND_MODE": "  • Enabled: {value}",
        "DIAG_TREND_DETECTED": "  • Detected: up={up} down={down} none={none}",
        "DIAG_TREND_SETUP_SUMMARY": "  • Setups: checked={checked} passed={passed} failed={failed}",
        "DIAG_TREND_FAIL_REASONS": "  • Fail reasons: {reasons}",
        "DIAG_TREND_SAMPLE": "  • Sample: {sample}",
        "DIAG_FINAL_SCORE_HEADER": "• Final scoring:",
        "DIAG_FINAL_SCORE_THRESHOLD": "  • Threshold: {threshold}",
        "DIAG_FINAL_SCORE_SUMMARY": "  • Final checked: {checked} passed: {passed} failed: {failed}",
        "DIAG_FINAL_SCORE_LAST_PASS": "  • Last pass sample: {sample}",
        "DIAG_FINAL_SCORE_LAST_FAIL": "  • Last fail sample: {sample}",
        "DIAG_FINAL_SCORE_ADJUSTMENTS": "    • Adjustments: {adjustments}",
        "DIAG_FINAL_SCORE_FAIL_REASON": "    • Fail reason: {reason}",
        "DIAG_FINAL_SCORE_NEAR_MISS": "  • Near-miss sample: {sample}",
        "DIAG_FINAL_SCORE_NEAR_MISS_BLOCKERS": "    • Main blockers: {blockers}",
        "DIAG_PRESCORE_THRESHOLD": "• Threshold: {threshold}",
        "DIAG_PRESCORE_SUMMARY": "• Checked: {checked} | Passed: {passed} | Filtered: {failed} | Pass rate: {rate}",
        "DIAG_PRESCORE_FAILED": "• Failed examples: {samples}",
        "DIAG_PRESCORE_PASSED": "• Passed examples: {samples}",
        "DIAG_PRESCORE_BLUECHIP": "• Bluechip bypasses: {count} | Samples: {samples}",
        "DIAG_LIMITS_LINE": "• Limits: ema_near_pct={ema} | poi_max_distance_pct={poi} | min_rr={rr}",
        "DIAG_SETUP_STAGE_SUMMARY": "• Setup stage: checked={checked} passed={passed} failed={failed}",
        "DIAG_SETUP_FAIL_REASONS": "• Setup fail reasons: {reasons}",
        "DIAG_SETUP_NEAR_MISS_EXAMPLES": "• Near-miss examples: {examples}",
        "DIAG_FINAL_STAGE_SUMMARY": "• Final stage: checked={checked} passed={passed} failed={failed}",
        "DIAG_FINAL_FAIL_REASONS": "• Final fail reasons: {reasons}",
        "DIAG_CONFIRM_RETRY_HEADER": "• Confirm retry:",
        "DIAG_CONFIRM_RETRY_STATUS": (
            "  • enabled: {enabled} | pending: {pending} | sent_after_retry: {sent} | "
            "fail_confirm_retry_exhausted: {dropped}"
        ),
        "DIAG_CONFIRM_RETRY_SAMPLES": "  • Samples: {samples}",
        "DIAG_REQUESTS_HEADER": "Binance requests",
        "DIAG_REQUESTS_MADE": "• Requests made: {count}",
        "DIAG_CANDLES": "• Candles received: {count}",
        "DIAG_CACHE": "• Candle cache: hit={hits} miss={misses}",
        "DIAG_INFLIGHT": "• In-flight candle waits: {count}",
        "DIAG_TICKER_REQ": "• Ticker/24h requests: {count}",
        "DIAG_DEEP_SCAN": "• Deep-scan per cycle: {count}",
        "DIAG_PUMP_HEADER": "Pump/dump scan",
        "DIAG_PROGRESS": "• Progress: {progress}",
        "DIAG_CHECKED": "• Checked: {count}",
        "DIAG_FOUND": "• Signals found: {count}",
        "DIAG_SENT": "• Signals sent: {count}",
        "DIAG_CURRENT_COIN": "• Current coin: {symbol}",
        "DIAG_ROTATION": "• Rotation: {flag} (N={n}){cursor}",
        "DIAG_ROTATION_SLICE": "• Rotation last slice size: {size}",
        "DIAG_UNIVERSE_LINE": (
            "• Universe size={universe} rotation_added={added} "
            "final_candidates={final} scanned={scanned}"
        ),
        "DIAG_BINANCE_LAST_SUCCESS": "• Last successful response: {ago}",
        "DIAG_BINANCE_LAST_SUCCESS_NO_DATA": "• Last successful response: no data",
        "DIAG_BINANCE_TIMEOUTS": "• Consecutive timeouts: {count}",
        "DIAG_BINANCE_STAGE": "• Current stage: {stage}",
        "DIAG_STABILITY_HEADER": "Stability",
        "DIAG_SESSION_RESTARTS": "• Session restarts: {count}",
        "DIAG_FAILS_TOP": "Top rejection reasons",
        "DIAG_NEAR_MISS": "Near-miss",
        "ADMIN_RECEIPT_TEXT": (
            "🧾 Subscription receipt\n\n"
            "User ID: {user_id}\n"
            "Username: {username}\n"
            "Date/time: {timestamp}\n\n"
            "Plan: ${price} / {days} days\n"
            "Payment: TRX (TRON)\n"
            "Address: {wallet}"
        ),
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
