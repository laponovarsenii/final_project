import mysql.connector
from typing import Tuple, List, Dict, Any, Optional
from datetime import datetime

import local_settings as settings


def _get_conn():
    cfg = getattr(settings, "MYSQL_CONFIG", None)
    if cfg:
        return mysql.connector.connect(**cfg)
    return mysql.connector.connect(
        host=getattr(settings, "HOST", "localhost"),
        user=getattr(settings, "USER", "root"),
        password=getattr(settings, "PASSWORD", ""),
        database=getattr(settings, "DATABASE", ""),
        port=getattr(settings, "PORT", 3306),
        autocommit=True,
    )


def get_all_genres() -> List[str]:
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM category ORDER BY name")
        return [r[0] for r in cur.fetchall() if r and r[0] is not None]
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def get_year_range() -> Tuple[int, int]:
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT MIN(release_year), MAX(release_year) FROM film")
        r = cur.fetchone()
        if r and r[0] is not None and r[1] is not None:
            return int(r[0]), int(r[1])
        # safe defaults when DB empty
        return 1900, datetime.utcnow().year
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def search_by_genre_year(genre: Optional[str], year_from: int, year_to: int, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
    """
    Ищет фильмы с release_year BETWEEN year_from AND year_to (включительно).
    Если genre is not None — применяет фильтр по жанру (равенство).
    Возвращает dict: {results: [...], total_count: int, has_next: bool}
    """
    yf = int(year_from)
    yt = int(year_to)
    if yf > yt:
        yf, yt = yt, yf

    conn = _get_conn()
    try:
        # используем dictionary cursor, чтобы возвращать удобные dict'ы
        cur = conn.cursor(dictionary=True)

        if genre:
            count_query = """
                SELECT COUNT(*) AS cnt
                FROM film f
                JOIN film_category fc ON fc.film_id = f.film_id
                JOIN category c ON c.category_id = fc.category_id
                WHERE c.name = %s AND f.release_year BETWEEN %s AND %s
            """
            cur.execute(count_query, (genre, yf, yt))
            cnt_row = cur.fetchone()
            total = int(cnt_row.get("cnt", 0) if cnt_row else 0)

            data_query = """
                SELECT f.title, f.release_year, f.rating, f.description, c.name AS category_name
                FROM film f
                JOIN film_category fc ON fc.film_id = f.film_id
                JOIN category c ON c.category_id = fc.category_id
                WHERE c.name = %s AND f.release_year BETWEEN %s AND %s
                ORDER BY f.title
                LIMIT %s OFFSET %s
            """
            cur.execute(data_query, (genre, yf, yt, int(limit) + 1, int(offset)))
            rows = cur.fetchall()
            results_rows = rows[:int(limit)]
        else:
            # поиск по всему каталогу (без фильтра жанра)
            count_query = "SELECT COUNT(*) AS cnt FROM film WHERE release_year BETWEEN %s AND %s"
            cur.execute(count_query, (yf, yt))
            cnt_row = cur.fetchone()
            total = int(cnt_row.get("cnt", 0) if cnt_row else 0)

            data_query = """
                SELECT f.title, f.release_year, f.rating, f.description, NULL AS category_name
                FROM film f
                WHERE f.release_year BETWEEN %s AND %s
                ORDER BY f.title
                LIMIT %s OFFSET %s
            """
            cur.execute(data_query, (yf, yt, int(limit) + 1, int(offset)))
            rows = cur.fetchall()
            results_rows = rows[:int(limit)]

        results: List[Dict[str, Any]] = []
        for r in results_rows:
            # r is a dict because of dictionary=True
            results.append({
                "title": r.get("title"),
                "release_year": r.get("release_year"),
                "rating": r.get("rating"),
                "description": r.get("description"),
                "genre": r.get("category_name"),  # may be None when searching all genres
            })

        has_next = (offset + len(results)) < total
        return {"results": results, "total_count": total, "has_next": has_next}
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def search_by_keyword(q: str, limit: int = 10, offset: int = 0,
                      year_from: Optional[int] = None, year_to: Optional[int] = None) -> Dict[str, Any]:
    """
    Поиск по части названия (title) и по описанию (description).
    Опционально применяет фильтр по годам (inclusive) если year_from/year_to заданы (не None).
    Всегда возвращает словарь с keys: results, total_count, has_next.
    """
    like = f"%{q}%"
    conn = _get_conn()
    try:
        cur = conn.cursor(dictionary=True)

        where_clauses = ["(f.title LIKE %s OR f.description LIKE %s)"]
        params: List[Any] = [like, like]

        if year_from is not None and year_to is not None:
            yf = int(year_from)
            yt = int(year_to)
            if yf > yt:
                yf, yt = yt, yf
            where_clauses.append("f.release_year BETWEEN %s AND %s")
            params.extend([yf, yt])
        elif year_from is not None:
            yf = int(year_from)
            where_clauses.append("f.release_year >= %s")
            params.append(yf)
        elif year_to is not None:
            yt = int(year_to)
            where_clauses.append("f.release_year <= %s")
            params.append(yt)

        where_sql = " AND ".join(where_clauses)

        count_sql = f"SELECT COUNT(*) AS cnt FROM film f WHERE {where_sql}"
        cur.execute(count_sql, tuple(params))
        cnt_row = cur.fetchone()
        total = int(cnt_row.get("cnt", 0) if cnt_row else 0)

        data_sql = f"""
            SELECT f.title, f.release_year, f.rating, f.description,
                   c.name AS category_name
            FROM film f
            LEFT JOIN film_category fc ON fc.film_id = f.film_id
            LEFT JOIN category c ON c.category_id = fc.category_id
            WHERE {where_sql}
            GROUP BY f.film_id
            ORDER BY f.title
            LIMIT %s OFFSET %s
        """
        params_for_data = params + [int(limit) + 1, int(offset)]
        cur.execute(data_sql, tuple(params_for_data))
        rows = cur.fetchall()
        results_rows = rows[:int(limit)]

        results: List[Dict[str, Any]] = []
        for r in results_rows:
            results.append({
                "title": r.get("title"),
                "release_year": r.get("release_year"),
                "rating": r.get("rating"),
                "description": r.get("description"),
                "genre": r.get("category_name"),
            })

        has_next = (offset + len(results)) < total
        return {"results": results, "total_count": total, "has_next": has_next}
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()