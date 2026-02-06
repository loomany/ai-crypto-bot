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
        "SYSTEM_STATUS_TEXT": (
            "📡 Статус системы\n\n"
            "{binance_line}\n\n"
            "{ai_status_line}\n"
            "{ai_last_cycle}\n"
            "{ai_scan_line}\n"
            "{ai_current_line}\n"
            "{ai_cycle_line}"
            "{ai_last_signal}\n\n"
            "{pd_status_line}\n"
            "{pd_last_cycle}\n"
            "{pd_progress_line}\n"
            "{pd_current_line}\n"
            "{pd_last_signal}"
        ),
        "SYSTEM_STATUS_BINANCE_OK": "🔌 Связь с Binance: ✅ есть ({seconds_ago})",
        "SYSTEM_STATUS_BINANCE_NO_DATA": "🔌 Связь с Binance: ⛔ нет свежих данных",
        "SYSTEM_STATUS_AI_RUNNING_LINE": "🎯 AI-сигналы: ✅ работают",
        "SYSTEM_STATUS_AI_STOPPED_LINE": "🎯 AI-сигналы: ⛔ не запущены",
        "SYSTEM_STATUS_PD_RUNNING_LINE": "⚡ Pump / Dump: ✅ работает",
        "SYSTEM_STATUS_PD_STOPPED_LINE": "⚡ Pump / Dump: ⛔ не запущен",
        "SYSTEM_STATUS_LAST_CYCLE_LINE": "• последний цикл: {seconds_ago}",
        "SYSTEM_STATUS_LAST_CYCLE_NO_DATA": "• последний цикл: нет данных",
        "SYSTEM_STATUS_SCAN_LINE": "• скан рынка: {current} / {total}",
        "SYSTEM_STATUS_SCAN_NO_DATA": "• скан рынка: нет данных",
        "SYSTEM_STATUS_CURRENT_LINE": "• сейчас проверяю: {symbol}",
        "SYSTEM_STATUS_CURRENT_NO_DATA": "• сейчас проверяю: нет данных",
        "SYSTEM_STATUS_PROGRESS_LINE": "• прогресс: {current} / {total}",
        "SYSTEM_STATUS_PROGRESS_NO_DATA": "• прогресс: нет данных",
        "SYSTEM_STATUS_LAST_SIGNAL_LINE": "• последний сигнал: {text}",
        "SYSTEM_STATUS_LAST_SIGNAL_NONE": "нет",
        "SYSTEM_STATUS_LAST_SIGNAL_NONE_PD": "—",
        "SYSTEM_STATUS_SECONDS_AGO": "{seconds} сек назад",
        "SYSTEM_STATUS_CYCLE_LINE": "• скорость: ~{seconds} сек / цикл",
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
            "Отправь TRX (TRON) на адрес:\n"
            "{wallet}\n\n"
            "Твой ID (укажи в комментарии / чеке):\n"
            "{user_id}\n\n"
            "После оплаты нажми «Отправить чек + ID»."
        ),
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
            "⏳ Neutral: {neutral}\n"
            "⏰ В процессе: {in_progress}"
        ),
        "HISTORY_STATS_TITLE": "📊 Статистика ({period}) — по Score",
        "HISTORY_SCORE_BUCKET_LINE": (
            "{label}: ✅ {passed} / ❌ {failed} / ⏳ {neutral} / ⏰ {in_progress}  ({percent}%)"
        ),
        "HISTORY_NO_SIGNALS": "Нет сигналов за период ({period}).",
        "HISTORY_NO_SIGNALS_BUTTON": "Нет сигналов за период ({period}).",
        "STATUS_OPEN": "Открыт",
        "STATUS_NO_FILL": "Нет входа",
        "STATUS_AMBIGUOUS": "Спорно",
        "ARCHIVE_DETAIL_LIFETIME": "⏱ Время жизни сценария: {hours} часов",
        "ARCHIVE_DETAIL_REASON_HEADER": "🧠 Почему выбран сигнал (Score {score}):",
        "NAV_PREV": "⬅️ Назад",
        "NAV_NEXT": "Вперёд ➡️",
        "NAV_BACK": "⬅️ Назад",
        "PAYMENT_COPY_ADDRESS": "📋 Адрес для оплаты (TRX):\n{wallet}",
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
        "PURGE_SYMBOL_DONE": (
            "✅ {symbol}: удалено signal_events={events}, watchlist={watchlist}, signal_audit={audit}"
        ),
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
        "SCENARIO_TARGETS_HEADER": "Потенциальные цели:",
        "SCENARIO_CONTEXT_HEADER": "Краткий контекст:",
        "SCENARIO_CONTEXT_TREND": "• Тренд 1D / 4H: {trend_1d} / {trend_4h}",
        "SCENARIO_CONTEXT_RSI": "• RSI 1H: {rsi} ({zone})",
        "SCENARIO_CONTEXT_VOLUME": "• Объём: {volume}x к среднему",
        "SCENARIO_CONTEXT_RR": "• RR ≈ 1 : {rr}",
        "SCENARIO_SCORE_LINE": "🧠 Score: {score} / 100",
        "SCENARIO_MARKET_MODE_LINE": (
            "🧭 Market Mode: {mode} (bias {bias}, BTC {btc_change:+.2f}%/6h, ATR1H {btc_atr:.1f}%)"
        ),
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
        "PUMP_NOTE_1": "ℹ️ Это уведомление о резком импульсе цены и объёма.",
        "PUMP_NOTE_2": "Используется как сигнал внимания, а не готовая торговая идея.",
        "PUMP_RISK_1": "⚠️ Резкие импульсы высокорисковые.",
        "PUMP_RISK_2": "Бот не даёт точек входа и не управляет рисками.",
        "PUMP_SOURCE": "Источник данных: Binance",
        "ADMIN_NEW_USER": (
            "🆕 Новый пользователь\n"
            "ID: {user_id}\n"
            "Username: {username}\n"
            "Имя: {full_name}\n"
            "Язык: {language}"
        ),
        "DIAG_TITLE": "🛠 Диагностика бота (админ)",
        "DIAG_SECTION_OVERALL": "🧠 Общее состояние",
        "DIAG_SECTION_BTC_GATE": "🪙 BTC Gate",
        "DIAG_SECTION_AI": "🎯 AI-сигналы (основной модуль)",
        "DIAG_SECTION_FILTERS": "🧪 Фильтрация (Pre-score)",
        "DIAG_SECTION_BINANCE": "🌐 Binance API",
        "DIAG_SECTION_PUMPDUMP": "🚀 Pump / Dump Scanner",
        "DIAG_MARKET_HUB_TITLE": "🔧 MarketHub (базовый модуль рынка)",
        "DIAG_STATUS_WORKING": "работает",
        "DIAG_STATUS_NOT_STARTED": "не запускался",
        "DIAG_STATUS_ISSUES": "есть проблемы",
        "DIAG_STATUS_ERROR": "ошибка",
        "DIAG_STATUS_OK": "OK",
        "DIAG_STATUS_MISSING": "нет файла",
        "DIAG_STATUS_ENABLED": "включён",
        "DIAG_STATUS_DISABLED": "выключен",
        "DIAG_STATUS_PENDING": "ожидание",
        "DIAG_STATUS_NO_DATA": "нет данных",
        "DIAG_MODULE_STATUS": "• Статус: {status}",
        "DIAG_LAST_TICK": "• Последний тик: {tick}",
        "DIAG_ERRORS": "• Ошибки: {error}",
        "DIAG_ACTIVE_SYMBOLS": "• Активных пар в MarketHub (кеш свечей): {count}",
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
        "DIAG_BTC_CONTEXT_DISABLED": "• Контекст BTC: выключен",
        "DIAG_BTC_CONTEXT_PENDING": "• Контекст BTC: {reason}",
        "DIAG_BTC_SYMBOL": "• Символ: {symbol}",
        "DIAG_BTC_MODE": "• Режим BTC: {mode}",
        "DIAG_BTC_AGE": "• Возраст: {age} сек (TTL {ttl} сек)",
        "DIAG_BTC_ALLOW_LONGS": "• allow_longs: {flag}",
        "DIAG_BTC_ALLOW_SHORTS": "• allow_shorts: {flag}",
        "DIAG_BTC_REASON": "• причина: {reason}",
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
        "DIAG_CYCLE_TIME": "• Время цикла: ~{cycle}",
        "DIAG_AI_CONFIG_TITLE": "• AI config:",
        "DIAG_AI_CONFIG_MAX_DEEP": "  • Max deep per cycle: {value}",
        "DIAG_AI_CONFIG_STAGE_A": "  • Stage A top K: {value}",
        "DIAG_AI_CONFIG_PRESCORE_THRESHOLD": "  • Pre-score threshold: {value}",
        "DIAG_AI_CONFIG_PRESCORE_MIN": "  • Min pre-score: {value}",
        "DIAG_AI_CONFIG_FINAL_THRESHOLD": "  • Final score threshold: {value}",
        "DIAG_AI_CONFIG_MIN_VOLUME": "  • Min volume 5m (USDT): {value}",
        "DIAG_AI_CONFIG_PUMP_VOLUME": "  • Pump volume multiplier: {value}",
        "DIAG_PRESCORE_THRESHOLD": "• Порог: {threshold}",
        "DIAG_PRESCORE_SUMMARY": "• Проверено: {checked} | Прошло: {passed} | Отфильтровано: {failed} | Pass rate: {rate}",
        "DIAG_PRESCORE_FAILED": "• Примеры отказов: {samples}",
        "DIAG_PRESCORE_PASSED": "• Примеры прошедших: {samples}",
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
            "Send TRX (TRON) to the address:\n"
            "{wallet}\n\n"
            "Your ID (include in comment / receipt):\n"
            "{user_id}\n\n"
            "After payment tap “Send receipt + ID”."
        ),
        "PD_ENABLED_TEXT": (
            "✅ Pump/Dump notifications enabled.\n"
            "The bot will now send alerts on sharp market movements."
        ),
        "SYSTEM_STATUS_TEXT": (
            "📡 System Status\n\n"
            "{binance_line}\n\n"
            "{ai_status_line}\n"
            "{ai_last_cycle}\n"
            "{ai_scan_line}\n"
            "{ai_current_line}\n"
            "{ai_cycle_line}"
            "{ai_last_signal}\n\n"
            "{pd_status_line}\n"
            "{pd_last_cycle}\n"
            "{pd_progress_line}\n"
            "{pd_current_line}\n"
            "{pd_last_signal}"
        ),
        "SYSTEM_STATUS_BINANCE_OK": "🔌 Binance connection: ✅ connected ({seconds_ago})",
        "SYSTEM_STATUS_BINANCE_NO_DATA": "🔌 Binance connection: ⛔ no fresh data",
        "SYSTEM_STATUS_AI_RUNNING_LINE": "🎯 AI signals: ✅ running",
        "SYSTEM_STATUS_AI_STOPPED_LINE": "🎯 AI signals: ⛔ stopped",
        "SYSTEM_STATUS_PD_RUNNING_LINE": "⚡ Pump / Dump: ✅ running",
        "SYSTEM_STATUS_PD_STOPPED_LINE": "⚡ Pump / Dump: ⛔ stopped",
        "SYSTEM_STATUS_LAST_CYCLE_LINE": "• last cycle: {seconds_ago}",
        "SYSTEM_STATUS_LAST_CYCLE_NO_DATA": "• last cycle: no data",
        "SYSTEM_STATUS_SCAN_LINE": "• market scan: {current} / {total}",
        "SYSTEM_STATUS_SCAN_NO_DATA": "• market scan: no data",
        "SYSTEM_STATUS_CURRENT_LINE": "• currently scanning: {symbol}",
        "SYSTEM_STATUS_CURRENT_NO_DATA": "• currently scanning: no data",
        "SYSTEM_STATUS_PROGRESS_LINE": "• progress: {current} / {total}",
        "SYSTEM_STATUS_PROGRESS_NO_DATA": "• progress: no data",
        "SYSTEM_STATUS_LAST_SIGNAL_LINE": "• last signal: {text}",
        "SYSTEM_STATUS_LAST_SIGNAL_NONE": "none",
        "SYSTEM_STATUS_LAST_SIGNAL_NONE_PD": "—",
        "SYSTEM_STATUS_SECONDS_AGO": "{seconds} seconds ago",
        "SYSTEM_STATUS_CYCLE_LINE": "• speed: ~{seconds} sec / cycle",
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
            "⏳ Neutral: {neutral}\n"
            "⏰ In progress: {in_progress}"
        ),
        "HISTORY_STATS_TITLE": "📊 Stats ({period}) — by Score",
        "HISTORY_SCORE_BUCKET_LINE": (
            "{label}: ✅ {passed} / ❌ {failed} / ⏳ {neutral} / ⏰ {in_progress}  ({percent}%)"
        ),
        "HISTORY_NO_SIGNALS": "No signals for the period ({period}).",
        "HISTORY_NO_SIGNALS_BUTTON": "No signals for the period ({period}).",
        "STATUS_OPEN": "Open",
        "STATUS_NO_FILL": "No entry",
        "STATUS_AMBIGUOUS": "Ambiguous",
        "ARCHIVE_DETAIL_LIFETIME": "⏱ Scenario lifetime: {hours} hours",
        "ARCHIVE_DETAIL_REASON_HEADER": "🧠 Why this signal was chosen (Score {score}):",
        "NAV_PREV": "⬅️ Prev",
        "NAV_NEXT": "Next ➡️",
        "NAV_BACK": "⬅️ Back",
        "PAYMENT_COPY_ADDRESS": "📋 Payment address (TRX):\n{wallet}",
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
        "PURGE_SYMBOL_DONE": (
            "✅ {symbol}: deleted signal_events={events}, watchlist={watchlist}, signal_audit={audit}"
        ),
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
        "SCENARIO_TARGETS_HEADER": "Potential targets:",
        "SCENARIO_CONTEXT_HEADER": "Brief context:",
        "SCENARIO_CONTEXT_TREND": "• Trend 1D / 4H: {trend_1d} / {trend_4h}",
        "SCENARIO_CONTEXT_RSI": "• RSI 1H: {rsi} ({zone})",
        "SCENARIO_CONTEXT_VOLUME": "• Volume: {volume}x vs average",
        "SCENARIO_CONTEXT_RR": "• RR ≈ 1 : {rr}",
        "SCENARIO_SCORE_LINE": "🧠 Score: {score} / 100",
        "SCENARIO_MARKET_MODE_LINE": (
            "🧭 Market Mode: {mode} (bias {bias}, BTC {btc_change:+.2f}%/6h, ATR1H {btc_atr:.1f}%)"
        ),
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
        "PUMP_VOLUME_LINE": "• volume: {volume:.2f}× vs average",
        "PUMP_NOTE_1": "ℹ️ This is an alert about a sharp price/volume impulse.",
        "PUMP_NOTE_2": "It is a heads-up signal, not a full trading idea.",
        "PUMP_RISK_1": "⚠️ Sharp impulses are high risk.",
        "PUMP_RISK_2": "The bot does not provide entries or manage risk.",
        "PUMP_SOURCE": "Data source: Binance",
        "ADMIN_NEW_USER": (
            "🆕 New user\n"
            "ID: {user_id}\n"
            "Username: {username}\n"
            "Name: {full_name}\n"
            "Language: {language}"
        ),
        "DIAG_TITLE": "🛠 Bot diagnostics (admin)",
        "DIAG_SECTION_OVERALL": "🧠 Overall status",
        "DIAG_SECTION_BTC_GATE": "🪙 BTC Gate",
        "DIAG_SECTION_AI": "🎯 AI Signals (core module)",
        "DIAG_SECTION_FILTERS": "🧪 Filters (Pre-score)",
        "DIAG_SECTION_BINANCE": "🌐 Binance API",
        "DIAG_SECTION_PUMPDUMP": "🚀 Pump / Dump Scanner",
        "DIAG_MARKET_HUB_TITLE": "🔧 MarketHub (base market module)",
        "DIAG_STATUS_WORKING": "running",
        "DIAG_STATUS_NOT_STARTED": "not started",
        "DIAG_STATUS_ISSUES": "issues detected",
        "DIAG_STATUS_ERROR": "error",
        "DIAG_STATUS_OK": "OK",
        "DIAG_STATUS_MISSING": "missing file",
        "DIAG_STATUS_ENABLED": "enabled",
        "DIAG_STATUS_DISABLED": "disabled",
        "DIAG_STATUS_PENDING": "pending",
        "DIAG_STATUS_NO_DATA": "no data",
        "DIAG_MODULE_STATUS": "• Status: {status}",
        "DIAG_LAST_TICK": "• Last tick: {tick}",
        "DIAG_ERRORS": "• Errors: {error}",
        "DIAG_ACTIVE_SYMBOLS": "• Active pairs in MarketHub (candle cache): {count}",
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
        "DIAG_BTC_CONTEXT_DISABLED": "• BTC context: disabled",
        "DIAG_BTC_CONTEXT_PENDING": "• BTC context: {reason}",
        "DIAG_BTC_SYMBOL": "• Symbol: {symbol}",
        "DIAG_BTC_MODE": "• BTC mode: {mode}",
        "DIAG_BTC_AGE": "• Age: {age} sec (TTL {ttl} sec)",
        "DIAG_BTC_ALLOW_LONGS": "• allow_longs: {flag}",
        "DIAG_BTC_ALLOW_SHORTS": "• allow_shorts: {flag}",
        "DIAG_BTC_REASON": "• reason: {reason}",
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
        "DIAG_CYCLE_TIME": "• Cycle time: ~{cycle}",
        "DIAG_AI_CONFIG_TITLE": "• AI config:",
        "DIAG_AI_CONFIG_MAX_DEEP": "  • Max deep per cycle: {value}",
        "DIAG_AI_CONFIG_STAGE_A": "  • Stage A top K: {value}",
        "DIAG_AI_CONFIG_PRESCORE_THRESHOLD": "  • Pre-score threshold: {value}",
        "DIAG_AI_CONFIG_PRESCORE_MIN": "  • Min pre-score: {value}",
        "DIAG_AI_CONFIG_FINAL_THRESHOLD": "  • Final score threshold: {value}",
        "DIAG_AI_CONFIG_MIN_VOLUME": "  • Min volume 5m (USDT): {value}",
        "DIAG_AI_CONFIG_PUMP_VOLUME": "  • Pump volume multiplier: {value}",
        "DIAG_PRESCORE_THRESHOLD": "• Threshold: {threshold}",
        "DIAG_PRESCORE_SUMMARY": "• Checked: {checked} | Passed: {passed} | Filtered: {failed} | Pass rate: {rate}",
        "DIAG_PRESCORE_FAILED": "• Failed examples: {samples}",
        "DIAG_PRESCORE_PASSED": "• Passed examples: {samples}",
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
