from flask import Flask, render_template, request

from mysql_connector import (
    search_by_keyword,
    count_by_keyword,
    get_all_genres,
    get_year_bounds,
    search_by_genre_and_year_range,
    count_by_genre_and_year_range,
)

from log_writer import log_search
from log_stats import top_popular, latest_unique, mongo_available

app = Flask(__name__)

@app.get("/")
def index():
    """
    Главная страница: форма поиска + блоки статистики.
    """
    try:
        genres = get_all_genres()
        min_year, max_year = get_year_bounds()

        # Получение статистики (популярные и последние запросы)
        popular = top_popular(limit=5)
        latest = latest_unique(limit=10)
        mongo_down = not mongo_available()  # Проверка доступности MongoDB

        return render_template(
            "index.html",
            genres=genres,
            min_year=min_year,
            max_year=max_year,
            popular=popular,
            latest=latest,
            mongo_down=mongo_down,
        )
    except Exception as e:
        print(f"Ошибка на главной странице: {e}")
        return render_template("error.html", message="Не удалось загрузить данные страницы.")

@app.get("/search/keyword")
def search_keyword():
    """
    Страница поиска по ключевому слову.
    """
    try:
        keyword = (request.args.get("keyword") or "").strip()
        page = int(request.args.get("page", 1))
        page_size = 10

        if not keyword:
            return render_template("results.html", title="Поиск", error="Ключевое слово пустое.")

        # Получение данных из MySQL
        total = count_by_keyword(keyword)
        offset = (page - 1) * page_size
        rows = search_by_keyword(keyword, limit=page_size, offset=offset)

        mongo_status = None
        if page == 1:  # Логируем только первый поиск
            ok = log_search("keyword", {"keyword": keyword}, total)
            mongo_status = "ok" if ok else "skipped"

        return render_template(
            "results.html",
            title=f"Результаты по ключевому слову: {keyword}",
            rows=rows,
            total=total,
            page=page,
            page_size=page_size,
            query_params={"keyword": keyword},
            mode="keyword",
            mongo_status=mongo_status,
        )
    except Exception as e:
        print(f"Ошибка в поиске по ключевому слову: {e}")
        return render_template("error.html", message="Произошла ошибка при поиске.")

@app.get("/search/genre")
def search_genre():
    """
    Страница поиска по жанру и годам.
    """
    try:
        genre = (request.args.get("genre") or "").strip()
        year_from = (request.args.get("year_from") or "").strip()
        year_to = (request.args.get("year_to") or "").strip()
        page = int(request.args.get("page", 1))
        page_size = 10

        # Преобразование года в число
        yf = int(year_from) if year_from.isdigit() else None
        yt = int(year_to) if year_to.isdigit() else None

        if not genre:
            return render_template("results.html", title="Поиск", error="Жанр не выбран.")

        # Получение данных из MySQL
        total = count_by_genre_and_year_range(genre, yf, yt)
        offset = (page - 1) * page_size
        rows = search_by_genre_and_year_range(genre, yf, yt, limit=page_size, offset=offset)

        mongo_status = None
        if page == 1:  # Логируем только первый поиск
            ok = log_search("genre_year", {"genre": genre, "year_from": yf, "year_to": yt}, total)
            mongo_status = "ok" if ok else "skipped"

        return render_template(
            "results.html",
            title=f"Результаты: жанр {genre}, годы {yf or '...'} - {yt or '...'}",
            rows=rows,
            total=total,
            page=page,
            page_size=page_size,
            query_params={"genre": genre, "year_from": year_from, "year_to": year_to},
            mode="genre",
            mongo_status=mongo_status,
        )
    except Exception as e:
        print(f"Ошибка в поиске по жанру: {e}")
        return render_template("error.html", message="Произошла ошибка при поиске.")

@app.get("/stats")
def stats():
    """
    Страница статистики: популярные и последние запросы.
    """
    try:
        # Получение статистики из MongoDB
        popular = top_popular(limit=5)
        latest = latest_unique(limit=10)
        mongo_down = not mongo_available()

        return render_template("stats.html", popular=popular, latest=latest, mongo_down=mongo_down)
    except Exception as e:
        print(f"Ошибка на странице статистики: {e}")
        return render_template("error.html", message="Произошла ошибка при загрузке статистики.")


if __name__ == "__main__":
    # Загрузка переменных окружения из .env файла (если используется)
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass

    # Запуск Flask приложения
    app.run(debug=True)