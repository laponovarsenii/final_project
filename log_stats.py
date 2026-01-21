import os
from typing import List, Dict, Any
from pymongo import MongoClient
from pymongo.errors import PyMongoError


def _get_mongo_url() -> str:
    """
    Возвращает URL подключения к MongoDB.
    Приоритет:
    - MONGO_URI
    - mongodb.local_settings.MONGODB_URL_READ (если доступно)
    - фолбэк: mongodb://localhost:27017
    """
    url = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    return url


def _mongo_collection():
    """
    Возвращает коллекцию MongoDB для работы с данными (запись и чтение логов).
    Использует переменные MONGO_DB и MONGO_COLLECTION (по умолчанию 'final_project_010825-ptm_Arsenii_Laponov'
    и 'search_logs').
    """
    uri = _get_mongo_url()
    db_name = os.getenv("MONGO_DB", "final_project_010825-ptm_Arsenii_Laponov")
    coll_name = os.getenv("MONGO_COLLECTION", "search_logs")

    client = MongoClient(uri, serverSelectionTimeoutMS=2000)
    db = client[db_name]
    return db[coll_name]


def mongo_available() -> bool:
    """
    Проверяет доступность MongoDB через команду ping.
    :return: True, если MongoDB доступна, иначе False.
    """
    try:
        coll = _mongo_collection()
        coll.database.client.admin.command("ping")
        return True
    except PyMongoError as e:
        print(f"Ошибка при подключении к MongoDB: {e}")
        return False


def top_popular(limit: int = 5) -> List[Dict[str, Any]]:
    """
    Возвращает топ популярных запросов по частоте одинаковых (search_type + params).
    В случае ошибки или недоступности MongoDB возвращает пустой список.
    """
    try:
        coll = _mongo_collection()
        pipeline = [
            {"$group": {
                "_id": {"search_type": "$search_type", "params": "$params"},
                "count": {"$sum": 1},
                "last_ts": {"$max": "$timestamp"}
            }},
            {"$sort": {"count": -1, "last_ts": -1}},
            {"$limit": limit}
        ]
        result = list(coll.aggregate(pipeline))
        print(f"Результаты топ популярных запросов: {result}")
        return result
    except PyMongoError as e:
        print(f"Ошибка MongoDB в top_popular: {e}")
        return []
    except Exception as e:
        print(f"Общая ошибка в top_popular: {e}")
        return []


def latest_unique(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Возвращает последние уникальные запросы.
    В случае ошибки или недоступности MongoDB возвращает пустой список.
    """
    try:
        coll = _mongo_collection()
        pipeline = [
            {"$sort": {"timestamp": -1}},
            {"$group": {
                "_id": {"search_type": "$search_type", "params": "$params"},
                "latest": {"$first": "$$ROOT"}
            }},
            {"$replaceWith": "$latest"},
            {"$limit": limit}
        ]
        result = list(coll.aggregate(pipeline))
        print(f"Результаты последних уникальных запросов: {result}")
        return result
    except PyMongoError as e:
        print(f"Ошибка MongoDB в latest_unique: {e}")
        return []
    except Exception as e:
        print(f"Общая ошибка в latest_unique: {e}")
        return []