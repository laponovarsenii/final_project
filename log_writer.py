import os
from datetime import datetime
from typing import Dict, Any
from pymongo import MongoClient
from pymongo.errors import PyMongoError


def _get_mongo_url() -> str:
    """
    Возвращает URL подключения к MongoDB.
    Приоритет:
    - MONGO_URI (из переменных окружения)
    - mongodb://localhost:27017 (если MONGO_URI не задан).
    """
    url = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    return url


def _mongo_collection():
    """
    Возвращает коллекцию MongoDB для записи логов.

    Использует переменные окружения MONGO_DB и MONGO_COLLECTION.
    """
    uri = _get_mongo_url()
    db_name = os.getenv("MONGO_DB", "final_project_010825-ptm_Arsenii_Laponov")
    coll_name = os.getenv("MONGO_COLLECTION", "search_logs")

    # Создаем клиента MongoDB
    client = MongoClient(uri, serverSelectionTimeoutMS=2000)
    db = client[db_name]
    return db[coll_name]


def log_search(search_type: str, params: Dict[str, Any], results_count: int) -> bool:
    """
    Логирует факт выполнения поискового запроса.

    Формат документа:
    {
        "timestamp": "YYYY-MM-DDTHH:MM:SS" (UTC),
        "search_type": "<тип_запроса>",
        "params": {...},
        "results_count": <число_результатов>
    }

    :param search_type: Тип поиска (например, "keyword", "genre_year").
    :param params: Словарь с параметрами поиска.
    :param results_count: Общее количество найденных результатов.
    :return: True, если запрос успешно записан в MongoDB, иначе False.
    """
    # Формируем документ для записи
    doc = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),  # Текущая дата и время (UTC)
        "search_type": search_type,  # Тип запроса
        "params": params,  # Параметры поиска
        "results_count": results_count,  # Количество результатов
    }

    try:
        # Получаем коллекцию MongoDB
        coll = _mongo_collection()
        # Сохраняем документ в коллекцию
        result = coll.insert_one(doc)
        print(f"Запись сохранена в MongoDB с ID: {result.inserted_id}")  # Для отладки
        return True
    except PyMongoError as e:
        print(f"Ошибка MongoDB при записи: {e}")
        return False
    except Exception as e:
        print(f"Неизвестная ошибка: {e}")
        return False