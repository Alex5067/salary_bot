from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message
import asyncio
import logging
import json
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aggregation import aggregate_data

API_TOKEN = "6249189353:AAGPSlM-l_CuUXzGeh6NS280pgROia5f1fE"

bot = Bot(token=API_TOKEN)
dp = Dispatcher()


# Состояния FSM
class Form(StatesGroup):
    waiting_for_message = State()

# Обработчик команды /start
@dp.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer("Привет!\nЯ бот, который агрегирует выплаты в указанном диапазоне дат.\n")
    await message.answer("Введите данные в следующем формате:\n"
                         "{\n"
                         "   \"dt_from\": \"2022-10-01T00:00:00\",\n"
                         "   \"dt_upto\": \"2022-11-30T23:59:00\",\n"
                         "   \"group_type\": \"day\"\n"
                         "}")

@dp.message(lambda message: message.text.startswith('{'))
async def aggregate_salary(message: Message):
    try:
        user_input = eval(message.text)  # Преобразуем текст в словарь
        # Проверяем, что введенные данные имеют правильный формат
        if all(key in user_input for key in ('dt_from', 'dt_upto', 'group_type')):
            # Вызываем функцию агрегации с введенными данными
            result = aggregate_data(user_input)
        # Отправляем агрегированные данные обратно пользователю
            await message.answer(f"{result}")
    except Exception as e:
        await message.answer(f"Произошла ошибка: {str(e)}")


async def main() -> None:
    print("Start", datetime.now())
    await dp.start_polling(bot)
    print("Shutdown", datetime.now())

if __name__ == '__main__':
    # Логирование
    logging.basicConfig(level=logging.DEBUG, filename="bot_log.log", filemode="a",
                        format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(main())