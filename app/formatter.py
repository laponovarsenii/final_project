from typing import List, Dict


def format_films_table(rows: List[Dict]) -> str:
    """
    Форматирует список фильмов (словари) в удобную для чтения табличку-строку.
    Ожидает ключи: title, release_year, rating, description (опционально), genre (опционально).
    Если список пуст — возвращает строку "Нет результатов.".

    :param rows: Список словарей с данными фильмов.
    :return: Отформатированная строка-таблица.
    """
    if not rows:
        return "Нет результатов."

    headers = ["#", "Название", "Год", "Рейтинг", "Жанр"]
    data = []
    for idx, r in enumerate(rows, start=1):
        data.append([
            str(idx),
            str(r.get("title", "")),
            str(r.get("release_year", "")),
            str(r.get("rating", "")),
            str(r.get("genre", "")),
        ])

    # compute column widths
    widths = [max(len(h), *(len(row[i]) for row in data)) for i, h in enumerate(headers)]

    def fmt_row(row_vals):
        return " | ".join(val.ljust(widths[i]) for i, val in enumerate(row_vals))

    lines = [fmt_row(headers), "-+-".join("-" * w for w in widths)]
    lines.extend(fmt_row(row) for row in data)
    return "\n".join(lines)


def format_queries_list(rows: List[Dict]) -> str:
    """
    Формирует строковое представление списка популярных или последних поисковых запросов.
    В зависимости от структуры строк использует разные шаблоны вывода.
    Если список пуст — возвращает "Нет данных.".

    :param rows: Список словарей с данными о поисковых запросах.
    :return: Отформатированная строка с запросами.
    """
    if not rows:
        return "Нет данных."

    lines = []
    for i, r in enumerate(rows, start=1):
        if "count" in r:
            lines.append(f"{i}. {r.get('search_type')} | {r.get('params')} | частота: {r.get('count')}")
        else:
            lines.append(
                f"{i}. {r.get('search_type')} | {r.get('params')} | время: {r.get('timestamp')} | результаты: {r.get('results_count')}")
    return "\n".join(lines)