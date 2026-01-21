from typing import Optional
from mysql_connector import (
    search_by_keyword,
    count_by_keyword,
    get_all_genres,
    get_year_bounds,
    search_by_genre_and_year_range,
    count_by_genre_and_year_range,
)
from log_writer import log_search
from log_stats import top_popular, latest_unique
from formatter import format_films, format_list, format_stats_popular, format_stats_latest


def input_int(prompt: str) -> Optional[int]:
    """
    Считывает целое число из ввода пользователя.

    Пустая строка интерпретируется как None. Некорректный ввод
    сопровождается сообщением и возвращает None.

    Args:
        prompt: Текст приглашения.

    Returns:
        Целое число или None.
    """
    v = input(prompt).strip()
    if v == "":
        return None
    try:
        return int(v)
    except ValueError:
        print("Введите целое число или оставьте пусто.")
        return None


def paginate(fetch_page_fn, count_fn, page_size=10):
    """
    Универсальная пагинация: команды next/prev/exit.

    Отображает общее число результатов и постранично выводит данные,
    запрашивая у пользователя действие.

    Args:
        fetch_page_fn: Функция (offset, limit) -> List[Dict] — получает страницу.
        count_fn: Функция () -> int — общее количество результатов.
        page_size: Размер страницы.
    """
    total = count_fn()
    print(f"Всего результатов: {total}")
    if total == 0:
        return

    offset = 0
    while True:
        rows = fetch_page_fn(offset, page_size)
        print(format_films(rows))
        cmd = input("Команды: next / prev / exit > ").strip().lower()
        if cmd == "next":
            if offset + page_size < total:
                offset += page_size
            else:
                print("Это последняя страница.")
        elif cmd == "prev":
            if offset - page_size >= 0:
                offset -= page_size
            else:
                print("Это первая страница.")
        elif cmd == "exit":
            # Возврат в главное меню
            main_menu()
        else:
            print("Неизвестная команда.")


def do_search_keyword():
    """
    Обрабатывает сценарий поиска по ключевому слову с пагинацией.

    После завершения первой сессии поиска логирует запрос в MongoDB.
    """
    keyword = input("Введите ключевое слово (часть названия фильма): ").strip()
    if not keyword:
        print("Ключевое слово не должно быть пустым.")
        return

    def fetch_page(offset, limit):
        return search_by_keyword(keyword, limit=limit, offset=offset)

    def count_total():
        return count_by_keyword(keyword)

    paginate(fetch_page_fn=fetch_page, count_fn=count_total, page_size=10)
    total = count_by_keyword(keyword)
    log_search("keyword", {"keyword": keyword}, total)
    print("Запрос залогирован в MongoDB.")


def do_search_genre_year():
    """
    Обрабатывает сценарий поиска по жанру и диапазону годов с пагинацией.

    Проверяет корректность жанра, преобразует границы годов, затем
    выполняет поиск и логирование в MongoDB.
    """
    genres = get_all_genres()
    print(format_list(genres, "Доступные жанры"))
    min_year, max_year = get_year_bounds()
    print(f"Минимальный год в базе: {min_year}, максимальный: {max_year}")

    genre = input("Введите жанр из списка: ").strip()
    if genre not in genres:
        print("Жанр не найден в списке.")
        return

    year_from = input("Нижняя граница года (пусто — без нижней): ").strip()
    year_to = input("Верхняя граница года (пусто — без верхней): ").strip()
    yf = int(year_from) if year_from.isdigit() else None
    yt = int(year_to) if year_to.isdigit() else None

    def fetch_page(offset, limit):
        return search_by_genre_and_year_range(genre, yf, yt, limit=limit, offset=offset)

    def count_total():
        return count_by_genre_and_year_range(genre, yf, yt)

    paginate(fetch_page_fn=fetch_page, count_fn=count_total, page_size=10)
    total = count_by_genre_and_year_range(genre, yf, yt)
    log_search("genre_year", {"genre": genre, "year_from": yf, "year_to": yt}, total)
    print("Запрос залогирован в MongoDB.")


def show_stats():
    """
    Выводит статистику: топ популярных и последние уникальные запросы.

    Если MongoDB недоступна, функции вернут пустые списки.
    """
    print(format_stats_popular(top_popular(limit=5)))
    print()
    print(format_stats_latest(latest_unique(limit=10)))


def main_menu():
    """
    Главное меню консольного приложения.

    Предлагает пользователю выбрать один из сценариев:
    1) Поиск по ключевому слову
    2) Поиск по жанру и диапазону годов
    3) Показать популярные и последние запросы
    4) Выход
    """
    while True:
        print("\n=== Меню ===")
        print("1) Поиск по ключевому слову (название фильма)")
        print("2) Поиск по жанру и диапазону годов")
        print("3) Показать популярные и последние запросы (MongoDB)")
        print("4) Выход")
        choice = input("> ").strip()
        if choice == "1":
            do_search_keyword()
        elif choice == "2":
            do_search_genre_year()
        elif choice == "3":
            show_stats()
        elif choice == "4":
            print("До свидания!")
            break
        else:
            print("Неверный выбор, попробуйте снова.")


if __name__ == "__main__":
    # Поддержка .env (если установлен python-dotenv)
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass
    main_menu()