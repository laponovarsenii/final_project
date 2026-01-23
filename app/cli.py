import sys
from typing import Optional

import local_settings as settings
from app import mysql_connector as db
import log_writer
import log_stats
import formatter


def input_str(prompt: str) -> str:
    """
    Запрашивает у пользователя строковый ввод с помощью указанного prompt.
    Удаляет пробельные символы по краям. Корректно обрабатывает EOF и Ctrl+C, завершает программу с сообщением.
    :param prompt: Строка-подсказка для информирования пользователя.
    :return: Введённая пользователем строка без пробелов по краям.
    """
    try:
        return input(prompt).strip()
    except (EOFError, KeyboardInterrupt):
        print("\nВыход.")
        sys.exit(0)


def input_int(prompt: str, default: Optional[int] = None) -> Optional[int]:
    """
    Запрашивает у пользователя целочисленный ввод.
    Если строка пуста и задан default, возвращает значение по умолчанию.
    Если вводится не число — выводит сообщение и возвращает None.

    :param prompt: Подсказка для пользователя.
    :param default: Значение по умолчанию при пустом вводе (может быть None).
    :return: Введённое число типа int либо default, либо None, если ошибка.
    """
    s = input_str(prompt)
    if s == "" and default is not None:
        return default
    try:
        return int(s)
    except ValueError:
        print("Введите число.")
        return None


def paginate_loop(fetch_page_func, params_for_log: dict, search_type: str):
    """
    Универсальный цикл постраничного просмотра и логирования запросов для фунций поиска по базе фильмов.
    Получает но��ую страницу результатов при сдвиге offset и логирует только первую страницу.
    :param fetch_page_func: Функция получения данных по смещению (offset).
    :param params_for_log: Параметры поиска, сохраняемые в лог.
    :param search_type: Тип поискового запроса (для лога).
    """
    page_size = settings.PAGE_SIZE
    offset = 0
    first_page_logged = False

    while True:
        page = fetch_page_func(offset)
        results = page["results"]
        total = page["total_count"]
        has_next = page["has_next"]

        print(formatter.format_films_table(results))
        print(f"\nВсего найдено: {total}. Показано: {len(results)}. Смещение: {offset}.\n")

        if not first_page_logged:
            log_writer.log_query(search_type, params_for_log, total)
            first_page_logged = True

        if not has_next:
            print("Результаты закончились.")
            break

        cmd = input_str("Команда: [n] — ещё 10, [q] — назад в меню: ").lower()
        if cmd == "n":
            offset += page_size
        else:
            break


def search_by_keyword():
    """
    Режим поиска фильмов по ключевому слову (по части названия).
    Запрашивает у пользователя ключевое слово, выводит постраничные результаты и логирует запрос.
    """
    keyword = input_str("Введите ключевое слово по названию фильма: ")
    if not keyword:
        print("Пустой запрос.")
        return

    def fetch(offset: int):
        return db.search_by_keyword(keyword, limit=settings.PAGE_SIZE, offset=offset)

    paginate_loop(fetch, {"keyword": keyword}, "keyword")


def search_by_genre_year():
    """
    Режим поиска фильмов по жанру и диапазону годов выпуска.
    Выводит список жанров и диапазон лет, запрашивает ввод пользователя, осуществляет поиск с пагинацией и логированием.
    """
    genres = db.get_all_genres()
    min_year, max_year = db.get_year_range()

    print("\nДоступные жанры:")
    print(", ".join(genres))
    print(f"\nДиапазон годов выпуска в базе: {min_year} — {max_year}\n")

    genre = input_str("Укажите жанр (точно как в списке): ")
    if genre not in genres:
        print("Такой жанр не найден.")
        return

    y_from = None
    y_to = None
    while y_from is None:
        y_from = input_int(f"Нижняя граница года (>= {min_year}): ")
        if y_from is None or y_from < min_year:
            print("Некорректный год.")
            y_from = None

    while y_to is None:
        y_to = input_int(f"Верхняя граница года (<= {max_year}, Enter — тот же год): ", default=y_from)
        if y_to is None or y_to > max_year or y_to < y_from:
            print("Некорректный диапазон.")
            y_to = None

    def fetch(offset: int):
        return db.search_by_genre_year(genre, y_from, y_to, limit=settings.PAGE_SIZE, offset=offset)

    paginate_loop(fetch, {"genre": genre, "year_from": y_from, "year_to": y_to}, "genre_year")


def show_popular():
    """
    Выводит топ-5 популярных поисковых запросов пользователей по данным MongoDB.
    """
    rows = log_stats.get_top_popular(limit=5)
    print("\nТоп 5 популярных запросов:")
    print(formatter.format_queries_list(rows))


def show_latest():
    """
    Выводит 5 последних уникальных поисковых запросов пользователей (по типу и параметрам).
    """
    rows = log_stats.get_latest_unique(limit=5)
    print("\nПоследние уникальные запросы:")
    print(formatter.format_queries_list(rows))


def main():
    """
    Главная функция консольного приложения: показывает меню, получает выбор пользователя,
    вызывает соответствующие функции поиска или отображения статистики.
    """
    print("Консольное приложение поиска фильмов (sakila) + MongoDB логирование")
    print(f"MongoDB: база '{settings.MONGODB_DATABASE}', коллекция '{settings.MONGODB_COLLECTION_NAME}'")
    print(f"MySQL: база '{settings.DATABASE}', хост '{settings.HOST}'\n")

    while True:
        print("\nМеню:")
        print("1. Поиск по ключевому слову")
        print("2. Поиск по жанру и диапазону годов")
        print("3. Показать популярные запросы (топ-5)")
        print("4. Показать последние уникальные запросы (5)")
        print("0. Выход")

        choice = input_str("Выбор: ")
        if choice == "1":
            search_by_keyword()
        elif choice == "2":
            search_by_genre_year()
        elif choice == "3":
            show_popular()
        elif choice == "4":
            show_latest()
        elif choice == "0":
            print("Пока!")
            break
        else:
            print("Неверный выбор.")


if __name__ == "__main__":
    main()