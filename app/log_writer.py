from datetime import datetime
from typing import Dict, Any

from pymongo import MongoClient
from pymongo.errors import PyMongoError
import local_settings as settings


def get_collection():
    """
    Возвращает MongoDB коллекцию для логирования поисковых запросов.

    :return: Объект коллекции pymongo Collection.
    """
    client = MongoClient(settings.MONGODB_URL)
    db = client[settings.MONGODB_DATABASE]
    return db[settings.MONGODB_COLLECTION_NAME]


def log_query(search_type: str, params: Dict[str, Any], results_count: int) -> None:
    """
    Записывает информацию о выполненном поисковом запросе в базу MongoDB.
    В случае ошибки логирования (например, отказ в доступе) выводит предупреждение в консоль.

    :param search_type: Тип поиска (например, 'keyword', 'genre_year').
    :param params: Параметры поиска (например, ключевое слово, жанр, годы).
    :param results_count: Количество найденных результатов по данному запросу.
    :return: None
    """
    doc = {
        "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
        "search_type": search_type,
        "params": params,
        "results_count": int(results_count),
    }
    try:
        coll = get_collection()
        coll.insert_one(doc)
    except PyMongoError as e:
        print(f"[LOGGING WARNING] MongoDB insert failed: {e}")