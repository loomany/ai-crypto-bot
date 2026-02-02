import textwrap
from typing import Dict

from binance_rest import fetch_json

COIN_INFO: Dict[str, str] = {
    "BTC": (
        "Bitcoin — первая и самая известная криптовалюта. "
        "Используется как цифровое «золото» и средство сохранения стоимости. "
        "Запущен в 2009 году анонимным разработчиком под псевдонимом Satoshi Nakamoto."
    ),
    "ETH": (
        "Ethereum — блокчейн для смарт-контрактов и децентрализованных приложений (dApp). "
        "Монета ETH используется для оплаты комиссий и как основная валюта в экосистеме DeFi и NFT."
    ),
    "LAZIO": (
        "LAZIO Fan Token — токен болельщиков футбольного клуба «Лацио». "
        "Даёт доступ к фан-голосованиям и акциям через платформу Socios и Binance. "
        "Торгуется как обычная криптовалюта на спотовом рынке."
    ),
    "BTTC": (
        "BTTC (BitTorrent Chain) — токен экосистемы BitTorrent и TRON. "
        "Используется в кроссчейн-мостах, стейкинге и оплате комиссий в сети BitTorrent Chain."
    ),
}

QUOTE_SUFFIXES = ("USDT", "BUSD", "USDC", "FDUSD", "TUSD")

_BINANCE_INFO_URL = (
    "https://www.binance.com/bapi/composite/v1/public/marketing/currency/get-basic-info"
)

_coin_cache: dict[str, str] = {}


def extract_base(symbol_pair: str) -> str:
    s = symbol_pair.upper()
    for q in QUOTE_SUFFIXES:
        if s.endswith(q):
            return s[: -len(q)]
    return s


async def _fetch_from_binance(base_symbol: str) -> str | None:
    """
    Пытается получить описание монеты с сайта Binance.
    Возвращает готовый текст или None, если не получилось.
    """

    if base_symbol in _coin_cache:
        return _coin_cache[base_symbol]

    params = {"symbol": base_symbol}
    data = await fetch_json(_BINANCE_INFO_URL, params=params, stage="coin_info")
    if not data:
        return None

    info = data.get("data") or {}
    parts: list[str] = []

    name = info.get("fullName") or info.get("name")
    if name:
        parts.append(f"{name} — токен, торгующийся на Binance. ")

    long_desc = (
        info.get("description")
        or info.get("projectIntro")
        or info.get("intro")
        or info.get("projectDetails")
    )
    if long_desc:
        long_desc = textwrap.shorten(
            long_desc.replace("\n", " "), width=380, placeholder="..."
        )
        parts.append(long_desc)

    tags = info.get("tags") or info.get("tradeTags") or []
    if isinstance(tags, list) and tags:
        tags_str = ", ".join(str(t) for t in tags[:5])
        parts.append(f"Категории: {tags_str}.")

    website = None
    links = info.get("links") or {}
    if isinstance(links, dict):
        website = links.get("website") or links.get("websiteLink")
        if isinstance(website, list):
            website = website[0] if website else None
    if website:
        parts.append(f"Официальный сайт: {website}")

    if not parts:
        return None

    text = " ".join(parts).strip()
    _coin_cache[base_symbol] = text
    return text


async def get_coin_description(symbol_pair: str) -> str:
    """
    Возвращает краткое человеческое описание монеты.
    1) сначала смотрим локальный словарь COIN_INFO;
    2) если нет — пробуем вытащить описание с Binance;
    3) если и это не удалось — отдаём аккуратный дефолт.
    """

    base = extract_base(symbol_pair)

    if base in COIN_INFO:
        return COIN_INFO[base]

    desc = await _fetch_from_binance(base)
    if desc:
        return desc

    return (
        f"{base} — торговая пара на Binance. "
        "Подробное описание монеты недоступно, "
        "но её можно анализировать по графику цены, объёмам и тех.индикаторам."
    )
