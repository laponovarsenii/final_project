import os
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for, flash

import local_settings as settings
import mysql_connector as db
import log_writer
import log_stats


"""
web_app.py

Flask-приложение для поиска фильмов и показа статистики по запросам.
Содержит маршруты:
- index() — главная стра��ица с формой поиска;
- search_keyword() — поиск по ключевому слову (названию);
- search_genre() — поиск по жанру и диапазону годов;
- stats() — страница статистики запросов;
- not_found() — обработчик 404.

Контекст (жанры, min/max year и настройки) добавляется в шаблоны через inject_common().
Переменные окружения и конфигурация подтягиваются из local_settings и mysql_connector.
"""

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app = Flask(
    __name__,
    template_folder=str(TEMPLATES_DIR),
    static_folder=str(STATIC_DIR),
)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev")

# Отладочные строки — временно, чтобы убедиться, что пути правильные.
print("DEBUG: web_app __file__ ->", Path(__file__).resolve())
print("DEBUG: BASE_DIR ->", BASE_DIR)
print("DEBUG: templates_dir ->", app.template_folder, "exists:", Path(app.template_folder).is_dir())
print("DEBUG: static_dir    ->", app.static_folder, "exists:", Path(app.static_folder).is_dir())
print("DEBUG: index.html exists?:", (Path(app.template_folder) / "index.html").is_file())
print("DEBUG: css exists?:", (Path(app.static_folder) / "css/style.css").is_file())

PAGE_SIZE = settings.PAGE_SIZE


@app.context_processor
def inject_common():
    """
    Внедряет общие данные в контекст всех шаблонов.

    Попытка получить:
      - genres: список доступных жанров (db.get_all_genres)
      - min_year, max_year: границы годов (db.get_year_range)
      - settings: локальные настройки (import local_settings as settings)

    Если при получении данных возникнет исключение, возвращаются безопасные значения:
      - genres = []
      - min_year = 0
      - max_year = 0

    Возвращаемое значение:
      dict с ключами 'settings', 'genres', 'min_year', 'max_year' — эти переменные будут
      доступны во всех Jinja-шаблонах.
    """
    try:
        genres = db.get_all_genres()
        min_year, max_year = db.get_year_range()
    except Exception:
        genres, min_year, max_year = [], 0, 0
    return dict(settings=settings, genres=genres, min_year=min_year, max_year=max_year)


@app.route("/")
def index():
    """
    Отображает главную страницу приложения.

    Функция:
      - Отдаёт шаблон 'index.html', где содержится форма поиска и быстрые фильтры.
      - Не принимает параметров.
      - Использует контекст, добавленный inject_common() (жанры, годы и т.д.).

    Возвращает:
      Результат render_template("index.html") — HTML для главной страницы.
    """
    return render_template("index.html")


@app.route("/search/keyword")
def search_keyword():
    """
    Обрабатывает поиск фильмов по ключевому слову (части названия).

    Логика:
      - Читает GET-параметр 'q' (строка запроса) и 'page' (номер страницы).
      - Валидирует параметр 'page' (целое, >=1).
      - Если параметр 'q' пустой — добавляет flash-сообщение и перенаправляет на главную.
      - Вычисляет offset по PAGE_SIZE и вызывает db.search_by_keyword(q, limit, offset).
      - При первой странице (page == 1) логирует запрос в log_writer.log_query с типом "keyword".
      - При ошибке DB-запроса — показывает flash с ошибкой и перенаправляет на главную.

    Ожидаемые данные от db.search_by_keyword:
      dict с ключами:
        - results: список записей фильмов для текущей страницы
        - total_count: общее количество результатов (int)
        - has_next: булево, есть ли следующая страница

    Возвращает:
      render_template("results_keyword.html", ...) — шаблон с результатами поиска.
    """
    q = (request.args.get("q") or "").strip()
    page = request.args.get("page", "1")
    try:
        page = max(1, int(page))
    except ValueError:
        page = 1

    if not q:
        flash("Введите ключевое слово для поиска.", "warning")
        return redirect(url_for("index"))

    offset = (page - 1) * PAGE_SIZE
    try:
        data = db.search_by_keyword(q, limit=PAGE_SIZE, offset=offset)
    except Exception as e:
        flash(f"Ошибка выполнения запроса: {e}", "error")
        return redirect(url_for("index"))

    if page == 1:
        try:
            log_writer.log_query("keyword", {"keyword": q}, data.get("total_count", 0))
        except Exception as e:
            app.logger.warning("Logging failed: %s", e)

    return render_template(
        "results_keyword.html",
        q=q,
        results=data.get("results", []),
        total=data.get("total_count", 0),
        page=page,
        has_next=data.get("has_next", False),
        page_size=PAGE_SIZE,
    )


@app.route("/search/genre")
def search_genre():
    """
    Обрабатывает поиск фильмов по жанру и диапазону годов.

    Логика:
      - Читает GET-параметры: 'genre', 'y_from', 'y_to', 'page'.
      - Проверяет, что указанный жанр присутствует в списке доступных жанров.
      - Валидирует годы (преобразует в int, корректность диапазона).
      - Валидирует page (целое >=1).
      - Вычисляет offset и вызывает db.search_by_genre_year(genre, y_from, y_to, limit, offset).
      - При первой странице логирует запрос через log_writer.log_query с типом "genre_year".
      - При ошибках (некорректный жанр/годы/DB) добавляет flash и перенаправляет на главную.

    Ожидаемые данные от db.search_by_genre_year:
      dict с ключами:
        - results: список записей фильмов для текущей страницы
        - total_count: общее количество результатов (int)
        - has_next: булево, есть ли следующая страница

    Возвращает:
      render_template("results_genre.html", ...) — шаблон с результатами поиска по жанру.
    """
    genre = (request.args.get("genre") or "").strip()
    y_from = request.args.get("y_from", "").strip()
    y_to = request.args.get("y_to", "").strip()
    page = request.args.get("page", "1")

    try:
        page = max(1, int(page))
    except ValueError:
        page = 1

    try:
        genres = db.get_all_genres()
        min_year, max_year = db.get_year_range()
    except Exception as e:
        flash(f"Ошибка при получении справочной информации: {e}", "error")
        return redirect(url_for("index"))

    if genre not in genres:
        flash("Укажите существующий жанр из списка.", "warning")
        return redirect(url_for("index"))

    try:
        y_from_i = int(y_from)
        y_to_i = int(y_to) if y_to else y_from_i
    except ValueError:
        flash("Годы должны быть числами.", "warning")
        return redirect(url_for("index"))

    if y_from_i < min_year or y_to_i > max_year or y_to_i < y_from_i:
        flash(f"Диапазон годов должен быть в пределах [{min_year}..{max_year}] и корректным.", "warning")
        return redirect(url_for("index"))

    offset = (page - 1) * PAGE_SIZE
    try:
        data = db.search_by_genre_year(genre, y_from_i, y_to_i, limit=PAGE_SIZE, offset=offset)
    except Exception as e:
        flash(f"Ошибка выполнения запроса: {e}", "error")
        return redirect(url_for("index"))

    if page == 1:
        try:
            log_writer.log_query("genre_year", {"genre": genre, "year_from": y_from_i, "year_to": y_to_i}, data.get("total_count", 0))
        except Exception as e:
            app.logger.warning("Logging failed: %s", e)

    return render_template(
        "results_genre.html",
        genre=genre,
        y_from=y_from_i,
        y_to=y_to_i,
        results=data.get("results", []),
        total=data.get("total_count", 0),
        page=page,
        has_next=data.get("has_next", False),
        page_size=PAGE_SIZE,
    )


@app.route("/stats")
def stats():
    """
    Отдаёт страницу статистики поисковых запросов.

    Функция:
      - Пытается получить top-популярные запросы (log_stats.get_top_popular(limit=5))
        и последние уникальные запросы (log_stats.get_latest_unique(limit=5)).
      - В случае ошибок при получении данных соответствующая переменная будет пустым списком,
        а пользователю будет показано flash-сообщение с подробностями.
      - Передаёт данные в шаблон 'stats.html' под именами 'popular' и 'latest'.

    Возвращает:
      render_template("stats.html", popular=popular, latest=latest)
    """
    try:
        popular = log_stats.get_top_popular(limit=5)
    except Exception as e:
        popular = []
        flash(f"Ошибка получения популярных запросов: {e}", "error")

    try:
        latest = log_stats.get_latest_unique(limit=5)
    except Exception as e:
        latest = []
        flash(f"Ошибка получения последних запросов: {e}", "error")

    return render_template("stats.html", popular=popular, latest=latest)


@app.errorhandler(404)
def not_found(_e):
    """
    Обработчик для ошибок 404 (страница не найдена).

    Параметры:
      - _e: объект исключения/ошибки (игнорируется в теле функции).

    Поведение:
      - Возвращает шаблон 'base.html' с простым сообщением об ошибке и кодом ответа 404.
      - Можно расширить для показа к��стомной страницы 404.

    Возвращает:
      tuple (render_template(...), 404)
    """
    return render_template("base.html", title="Не найдено", content="Страница не найдена."), 404


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=True)