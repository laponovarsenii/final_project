import os
import mysql.connector
from typing import List, Dict, Tuple, Optional

def _get_mysql_connection():
    """
    Подключение к MySQL. Можно переопределить переменными окружения:
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
    """
    host = os.getenv("MYSQL_HOST", "ich-db.edu.itcareerhub.de")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER", "ich1")
    password = os.getenv("MYSQL_PASSWORD", "password")
    database = os.getenv("MYSQL_DB", "sakila")

    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )

def _fetch_all_dict(sql: str, params: tuple = ()) -> List[Dict]:
    conn = _get_mysql_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()

def _fetch_count(sql: str, params: tuple = ()) -> int:
    conn = _get_mysql_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return int(row[0]) if row else 0
    finally:
        conn.close()

# --------- Публичные функции, которые импортирует functions.py ---------

def search_by_keyword(keyword: str, limit: int = 10, offset: int = 0) -> List[Dict]:
    """
    Поиск по части названия фильма (film.title LIKE %keyword%)
    """
    sql = """
        SELECT f.film_id, f.title, f.description, f.release_year, f.length, f.rating
        FROM film f
        WHERE f.title LIKE %s
        ORDER BY f.title
        LIMIT %s OFFSET %s
    """
    return _fetch_all_dict(sql, (f"%{keyword}%", limit, offset))

def count_by_keyword(keyword: str) -> int:
    sql = """
        SELECT COUNT(*) AS cnt
        FROM film f
        WHERE f.title LIKE %s
    """
    return _fetch_count(sql, (f"%{keyword}%",))

def get_all_genres() -> List[str]:
    """
    Список всех жанров из таблицы category
    """
    sql = "SELECT name FROM category ORDER BY name"
    rows = _fetch_all_dict(sql)
    return [r["name"] for r in rows]

def get_year_bounds() -> Tuple[Optional[int], Optional[int]]:
    """
    Минимальный и максимальный год выпуска из film.release_year
    """
    sql = "SELECT MIN(release_year) AS min_year, MAX(release_year) AS max_year FROM film"
    rows = _fetch_all_dict(sql)
    if rows:
        return (rows[0]["min_year"], rows[0]["max_year"])
    return (None, None)

def search_by_genre_and_year_range(
    genre: str,
    year_from: Optional[int],
    year_to: Optional[int],
    limit: int = 10,
    offset: int = 0,
) -> List[Dict]:
    """
    Поиск по жанру (category.name) и диапазону годов (film.release_year),
    допускает пустые границы (NULL).
    """
    sql = """
        SELECT f.film_id, f.title, f.description, f.release_year, f.length, f.rating, c.name AS category
        FROM film f
        JOIN film_category fc ON fc.film_id = f.film_id
        JOIN category c ON c.category_id = fc.category_id
        WHERE c.name = %s
          AND (%s IS NULL OR f.release_year >= %s)
          AND (%s IS NULL OR f.release_year <= %s)
        ORDER BY f.release_year, f.title
        LIMIT %s OFFSET %s
    """
    return _fetch_all_dict(sql, (genre, year_from, year_from, year_to, year_to, limit, offset))

def count_by_genre_and_year_range(
    genre: str,
    year_from: Optional[int],
    year_to: Optional[int],
) -> int:
    sql = """
        SELECT COUNT(*) AS cnt
        FROM film f
        JOIN film_category fc ON fc.film_id = f.film_id
        JOIN category c ON c.category_id = fc.category_id
        WHERE c.name = %s
          AND (%s IS NULL OR f.release_year >= %s)
          AND (%s IS NULL OR f.release_year <= %s)
    """
    return _fetch_count(sql, (genre, year_from, year_from, year_to, year_to))