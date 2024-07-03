from datetime import datetime, timedelta
from typing import Dict, List, Union
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

# Получаем значения переменных окружения
MONGO_URI: str = os.getenv("MONGO_URI", "")
DB_NAME: str = os.getenv("DB_NAME", "")
COLLECTION_NAME: str = os.getenv("COLLECTION_NAME", "")

# Инициализируем клиент MongoDB
client: AsyncIOMotorClient = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]


# Функция для экранирования специальных символов в Markdown
def escape_markdown(text: str) -> str:
    escape_chars = r"\_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{char}" if char in escape_chars else char for char in text)


# Функция для агрегации данных о зарплатах
async def aggregate_salaries(
    dt_from: str, dt_upto: str, group_type: str
) -> Dict[str, Union[List[int], List[str]]]:
    collection: AsyncIOMotorCollection = db[COLLECTION_NAME]
    dt_from_dt: datetime = datetime.fromisoformat(dt_from)
    dt_upto_dt: datetime = datetime.fromisoformat(dt_upto)

    # Определяем формат группировки в зависимости от типа группировки
    if group_type == "hour":
        group_format: str = "%Y-%m-%dT%H:00:00"
    elif group_type == "day":
        group_format: str = "%Y-%m-%dT00:00:00"
    elif group_type == "month":
        group_format: str = "%Y-%m-01T00:00:00"
    else:
        raise ValueError("Invalid group_type. Must be 'hour', 'day', or 'month'.")

    # Определяем конвейер агрегации
    pipeline: List[
        Dict[
            str,
            Union[
                Dict[str, Union[str, Dict[str, str]]],
                Dict[str, Union[str, Dict[str, str]]],
            ],
        ]
    ] = [
        {"$match": {"dt": {"$gte": dt_from_dt, "$lte": dt_upto_dt}}},
        {
            "$group": {
                "_id": {"$dateToString": {"format": group_format, "date": "$dt"}},
                "total": {"$sum": "$value"},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    # Выполняем агрегацию
    cursor = collection.aggregate(pipeline)
    result: List[Dict[str, Union[str, int]]] = await cursor.to_list(length=None)

    # Формируем результат
    dataset: List[int] = [item["total"] for item in result]
    labels: List[str] = [item["_id"] for item in result]

    # Добавляем дополнительный элемент в конец массива
    dataset.append(0)
    labels.append((datetime.fromisoformat(labels[-1]) + timedelta(hours=1)).isoformat())

    return {"dataset": [dataset], "labels": [labels]}
