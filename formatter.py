from typing import List, Dict
from tabulate import tabulate


def format_films(rows: List[Dict]) -> str:
    """
    Форматирует список фильмов в табличный вывод (таблица GitHub).

    Args:
        rows: Список словарей с ключами film_id, title, release_year,
              length, rating (лишние ключи игнорируются).

    Returns:
        Строка с таблицей. Если список пустой — человекочитаемое сообщение.
    """
    if not rows:
        return "Нет результатов."
    headers = ["film_id", "title", "release_year", "length", "rating"]
    table = [
        [r.get("film_id"), r.get("title"), r.get("release_year"), r.get("length"), r.get("rating")]
        for r in rows
    ]
    return tabulate(table, headers=headers, tablefmt="github")


def format_list(items: List[str], title: str = "Список") -> str:
    """
    Форматирует набор строк в простой маркированный список.

    Args:
        items: Список строк.
        title: Заголовок списка.

    Returns:
        Готовая к выводу строка с заголовком и пунктами списка.
        Если список пуст — сообщение о пустом списке.
    """
    if not items:
        return f"{title}: пусто."
    return f"{title}:\n" + "\n".join(f"- {it}" for it in items)


def format_stats_popular(rows: List[Dict]) -> str:
    """
    Форматирует статистику популярных запросов по данным из MongoDB.

    Ожидается результат агрегации, где каждый элемент имеет:
    - _id: { "search_type": str, "params": dict }
    - count: int

    Args:
        rows: Список агрегированных документов.

    Returns:
        Человекочитаемая сводка "Топ популярных запросов".
        Если список пуст — сообщение об отсутствии данных.
    """
    if not rows:
        return "Популярные запросы: нет данных."
    lines = []
    for r in rows:
        key = r.get("_id", {})
        params = key.get("params", {})
        stype = key.get("search_type", "")
        count = r.get("count", 0)
        lines.append(f"- {stype} {params} — {count} раз(а)")
    return "Топ популярных запросов:\n" + "\n".join(lines)


def format_stats_latest(rows: List[Dict]) -> str:
    """
    Форматирует список последних уникальных запросов.

    Ожидается список документов с полями:
    - search_type: str
    - params: dict
    - timestamp: str (ISO-like)

    Args:
        rows: Список документов.

    Returns:
        Человекочитаемая сводка последних уникальных запросов.
        Если список пуст — сообщение об отсутствии данных.
    """
    if not rows:
        return "Последние уникальные запросы: нет данных."
    lines = []
    for r in rows:
        stype = r.get("search_type", "")
        params = r.get("params", {})
        ts = r.get("timestamp", "")
        lines.append(f"- [{ts}] {stype} {params}")
    return "Последние уникальные запросы:\n" + "\n".join(lines)