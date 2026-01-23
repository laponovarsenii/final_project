import os
from datetime import datetime
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for, flash

import local_settings as settings
import mysql_connector as db
import log_writer
import log_stats




BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app = Flask(
    __name__,
    template_folder=str(TEMPLATES_DIR),
    static_folder=str(STATIC_DIR),
)

app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev")



PAGE_SIZE = getattr(settings, "PAGE_SIZE", 20)


@app.context_processor
def inject_common():
    """
    Внедряет общие данные в контекст всех шаблонов:
    settings, genres, min_year, max_year.
    При ошибке возвращаем безопасные значения.
    """
    try:
        genres = db.get_all_genres()
        min_year, max_year = db.get_year_range()
        min_year = int(min_year)
        max_year = int(max_year)
    except Exception:
        genres = []
        min_year = 1900
        max_year = datetime.utcnow().year
    return dict(settings=settings, genres=genres, min_year=min_year, max_year=max_year)


@app.route("/")
def index():
    """
    Главная страница.

    Если пришли параметры y_from/y_to/genre (форма годов отправляет их сюда),
    перенаправляем на /search/genre с теми же параметрами, чтобы сразу показать результаты.
    """
    if request.args.get("y_from") or request.args.get("y_to") or request.args.get("genre"):
        params = {}
        for k in ("y_from", "y_to", "genre", "page"):
            v = request.args.get(k)
            if v:
                params[k] = v
        return redirect(url_for("search_genre", **params))

    return render_template("index.html")


@app.route("/search/keyword")
def search_keyword():
    """
    Поиск по ключевому слову.
    Защищаемся от некорректного возвращаемого значения из db.search_by_keyword.
    """
    q = (request.args.get("q") or "").strip()
    page = request.args.get("page", "1")
    try:
        page = max(1, int(page))
    except (ValueError, TypeError):
        page = 1

    if not q:
        flash("Введите ключевое слово для поиска.", "warning")
        return redirect(url_for("index"))

    offset = (page - 1) * PAGE_SIZE
    try:
        data = db.search_by_keyword(q, limit=PAGE_SIZE, offset=offset)
    except Exception as e:
        app.logger.exception("DB error in search_by_keyword")
        flash(f"Ошибка выполнения запроса: {e}", "error")
        return redirect(url_for("index"))

    if not isinstance(data, dict):
        app.logger.warning("search_by_keyword returned non-dict (%r). Falling back to empty result.", data)
        data = {"results": [], "total_count": 0, "has_next": False}

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
    Поиск фильмов по жанру и/или диапазону годов.
    Гарантируем, что передаём в DB чёткий диапазон year_from..year_to (включительно).
    Поддерживаем разные сигнатуры функций в mysql_connector (именованные/позиционные).
    """
    genre = (request.args.get("genre") or "").strip()
    raw_y_from = (request.args.get("y_from") or "").strip()
    raw_y_to = (request.args.get("y_to") or "").strip()
    page = request.args.get("page", "1")

    try:
        page = max(1, int(page))
    except (ValueError, TypeError):
        page = 1

    # Получаем справочные данные
    try:
        genres = db.get_all_genres()
        min_year, max_year = db.get_year_range()
        min_year = int(min_year)
        max_year = int(max_year)
    except Exception as e:
        app.logger.exception("Error getting reference data")
        flash(f"Ошибка при получении справочной информации: {e}", "error")
        return redirect(url_for("index"))

    # Если жанр указан, проверяем, что он есть в списке (если список доступен)
    if genre and genres and genre not in genres:
        flash("Укаж��те существующий жанр из списка.", "warning")
        return redirect(url_for("index"))

    # Парсим годы: если пусто — используем min/max
    try:
        y_from_i = int(raw_y_from) if raw_y_from != "" else min_year
    except ValueError:
        flash("Год 'От' должен быть числом.", "warning")
        return redirect(url_for("index"))

    try:
        y_to_i = int(raw_y_to) if raw_y_to != "" else max_year
    except ValueError:
        flash("Год 'До' должен быть числом.", "warning")
        return redirect(url_for("index"))

    # Кламп и нормализация диапазона
    if y_from_i < min_year:
        y_from_i = min_year
    if y_to_i > max_year:
        y_to_i = max_year
    if y_from_i > y_to_i:
        y_from_i, y_to_i = y_to_i, y_from_i

    offset = (page - 1) * PAGE_SIZE

    # Вызов mysql_connector — сначала именованный, затем позиционный (fallback).
    try:
        try:
            data = db.search_by_genre_year(genre=genre or None, year_from=y_from_i, year_to=y_to_i, limit=PAGE_SIZE, offset=offset)
        except TypeError:
            # Попытка с позиционными аргументами: genre, year_from, year_to, limit, offset
            data = db.search_by_genre_year(genre or None, y_from_i, y_to_i, PAGE_SIZE, offset)
    except Exception as e:
        app.logger.exception("DB error in search_by_genre_year")
        flash(f"Ошибка выполнения запроса: {e}", "error")
        return redirect(url_for("index"))

    if not isinstance(data, dict):
        app.logger.warning("search_by_genre_year returned non-dict (%r). Falling back to empty result.", data)
        data = {"results": [], "total_count": 0, "has_next": False}

    if page == 1:
        try:
            log_params = {"year_from": y_from_i, "year_to": y_to_i}
            if genre:
                log_params["genre"] = genre
            log_writer.log_query("genre_year", log_params, data.get("total_count", 0))
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
    Страница статистики.
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
    Обработчик 404.
    """
    return render_template("base.html", title="Не найдено", content="Страница не найдена."), 404


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=True)