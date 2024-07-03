# main.py
import asyncio
import logging
import sys
from datetime import datetime
from typing import Dict, List, Union
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv
import os
import json

# Загружаем переменные окружения из .env файла
load_dotenv()

# Получаем значения переменных окружения
API_TOKEN: str = os.getenv("API_TOKEN", "")
MONGO_URI: str = os.getenv("MONGO_URI", "")
DB_NAME: str = os.getenv("DB_NAME", "")
COLLECTION_NAME: str = os.getenv("COLLECTION_NAME", "")

# Инициализируем клиент MongoDB
client: AsyncIOMotorClient = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]


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

    return {"dataset": [dataset], "labels": [labels]}


# Обработчик текстовых сообщений
async def handle_message(message: types.Message) -> None:
    try:
        # Парсим JSON данные
        data: Dict[str, str] = json.loads(message.text)
        dt_from: str = data["dt_from"]
        dt_upto: str = data["dt_upto"]
        group_type: str = data["group_type"]
        # Выполняем агрегацию
        result: Dict[str, Union[List[int], List[str]]] = await aggregate_salaries(
            dt_from, dt_upto, group_type
        )
        # Отправляем результат пользователю
        await message.reply(json.dumps(result), parse_mode=ParseMode.MARKDOWN)
    except json.JSONDecodeError:
        # Отправляем сообщение об ошибке, если JSON некорректен
        await message.reply("Error: Invalid JSON format", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        # Отправляем сообщение об ошибке пользователю
        await message.reply(f"Error: {e}", parse_mode=ParseMode.MARKDOWN)


# Основная функция
async def main() -> None:
    # Инициализируем бота с токеном и настройками по умолчанию
    bot: Bot = Bot(
        token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp: Dispatcher = Dispatcher()

    # Регистрируем обработчик текстовых сообщений
    dp.message.register(handle_message)

    # Запускаем polling
    await dp.start_polling(bot)


# Точка входа в программу
if __name__ == "__main__":
    # Настраиваем логирование
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    # Запускаем основную функцию
    asyncio.run(main())
