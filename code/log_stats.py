from typing import List, Dict, Any

from pymongo import MongoClient
import local_settings as settings


def _get_collection():
    """
    Возвращает MongoDB коллекцию для логирования поисковых запросов.

    :return: Объект коллекции pymongo Collection.
    """
    client = MongoClient(settings.MONGODB_URL)
    db = client[settings.MONGODB_DATABASE]
    return db[settings.MONGODB_COLLECTION_NAME]


def get_top_popular(limit: int = 5) -> List[Dict[str, Any]]:
    """
    Возвращает top-N популярных поисковых запросов, группируя по (search_type, params).
    Для каждой уникальной пары указывает частоту запросов.

    :param limit: Максимальное количество популярных записей в результате.
    :return: Список словарей с ключами: search_type, params, count.
    """
    coll = _get_collection()
    pipeline = [
        {"$group": {"_id": {"search_type": "$search_type", "params": "$params"}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": int(limit)},
        {"$project": {"_id": 0, "search_type": "$_id.search_type", "params": "$_id.params", "count": 1}},
    ]
    return list(coll.aggregate(pipeline))


def get_latest_unique(limit: int = 5) -> List[Dict[str, Any]]:
    """
    Возвращает N самых последних уникальных поисковых запросов (по search_type+params),
    отсортированных по убыванию времени запроса.

    :param limit: Максимальное число уникальных последних запросов.
    :return: Список словарей с ключами: search_type, params, timestamp, results_count.
    """
    coll = _get_collection()
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": {"search_type": "$search_type", "params": "$params"},
            "timestamp": {"$first": "$timestamp"},
            "results_count": {"$first": "$results_count"}
        }},
        {"$sort": {"timestamp": -1}},
        {"$limit": int(limit)},
        {"$project": {
            "_id": 0,
            "search_type": "$_id.search_type",
            "params": "$_id.params",
            "timestamp": 1,
            "results_count": 1
        }},
    ]
    return list(coll.aggregate(pipeline))