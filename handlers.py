import json
from aiogram import types
from aiogram.enums import ParseMode
from typing import Dict, Union, List

from utils import escape_markdown, aggregate_salaries


# Обработчик команды /start
async def handle_start(message: types.Message) -> None:
    first_name = escape_markdown(message.from_user.first_name)
    if message.from_user.username:
        username_link = f"[{first_name}](tg://user?id={message.from_user.id})"
    else:
        username_link = first_name
    await message.answer(f"Hi {username_link}\\!", parse_mode=ParseMode.MARKDOWN_V2)


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
        await message.answer(json.dumps(result), parse_mode=ParseMode.MARKDOWN)
    except json.JSONDecodeError:
        # Отправляем сообщение об ошибке, если JSON некорректен
        await message.reply("Error: Invalid JSON format", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        # Отправляем сообщение об ошибке пользователю
        await message.reply(f"Error: {e}", parse_mode=ParseMode.MARKDOWN)
