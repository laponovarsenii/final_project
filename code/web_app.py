import os
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for, flash

import local_settings as settings
import mysql_connector as db
import log_writer
import log_stats

# Абсолютные пути к корню проекта, templates и static
BASE_DIR = Path(__file__).resolve().parent.parent  # .../final_project1
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app = Flask(
    __name__,
    template_folder=str(TEMPLATES_DIR),
    static_folder=str(STATIC_DIR),
)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev")

PAGE_SIZE = settings.PAGE_SIZE

@app.context_processor
def inject_common():
    """
    Внедряет в шаблоны Flask общие переменные (жанры, мин. и макс. год, настройки).
    Используется во всех рендерах шаблонов автоматически.
    :return: dict с переменными для шаблонов.
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
    Домашняя страница сайта (форма поиска по базе фильмов).
    :return: HTML-шаблон index.html
    """
    return render_template("index.html")

@app.route("/search/keyword")
def search_keyword():
    """
    Страница поиска фильмов по ключевому слову (части названия).
    Обрабатывает GET-параметры q (ключевое слово) и page (номер страницы).
    Делает поиск, логирует запрос, выводит результаты с навигацией по страницам.
    :return: HTML шаблон с результатами поиска или с сообщением об ошибке.
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
    Страница поиска фильмов по жанру и диапазону годов.
    Обрабатывает GET-параметры genre, y_from, y_to, page.
    Проверяет валидность входных данных, делает поиск, логирует запрос, выводит результаты с навигацией по страницам.
    :return: HTML шаблон с результатами поиска или ошибкой.
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
    Страница со статистикой: показывает топ-5 популярных и последние 5 уникальных запросов.
    При ошибках получения данных выводит сообщение.
    :return: HTML шаблон stats.html с таблицами статистики.
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
    Обработчик 404 для вывода html-страницы "не найдено".
    :return: HTML-шаблон base.html с сообщением об ошибке.
    """
    return render_template("base.html", title="Не найдено", content="Страница не найдена."), 404

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=True)