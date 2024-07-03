import asyncio
import logging
import sys
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv
import os

from handlers import handle_start, handle_message

# Загружаем переменные окружения из .env файла
load_dotenv()

# Получаем значения переменных окружения
API_TOKEN: str = os.getenv("API_TOKEN", "")
MONGO_URI: str = os.getenv("MONGO_URI", "")

# Проверяем, что переменные окружения не пустые
if not API_TOKEN:
    raise ValueError("API_TOKEN is not set or is empty")
if not MONGO_URI:
    raise ValueError("MONGO_URI is not set or is empty")


# Основная функция
async def main() -> None:
    # Инициализируем бота с токеном и настройками по умолчанию
    bot: Bot = Bot(
        token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp: Dispatcher = Dispatcher()

    # Регистрируем обработчик команды /start
    dp.message.register(handle_start, CommandStart())

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
