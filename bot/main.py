import asyncio
import os
import re
import sys
import datetime
import traceback

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, \
    BufferedInputFile
from aiogram.filters import Command
from aiohttp import ClientSession
from sqlalchemy import select, insert
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramNetworkError

from utils.ym_api import YMRequest
from utils.url_processing import urls_processing, IncorrectUrl, extract_urls_from_message, MaxCountUrlError, \
    BadRequestError
from settings import tg_token, ym_token
from database.db import async_session_maker
from database.models import User, RequestsLog
from utils.ym_api import statistic
from utils.xlsx_file_formatter import xlsx_writter
from sqlalchemy.ext.asyncio import AsyncSession
from aiohttp.client_exceptions import ClientResponseError

bot = Bot(token=tg_token)
dp = Dispatcher()


class States(StatesGroup):
    waiting_urls = State()
    waiting_two_dates = State()
    waiting_one_date = State()


class NotAccesUserError(Exception):
    def __init__(self, message='Отказано в доступе'):
        self.message = message

    def __str(self):
        return self.message


async def check_user(user_tg_id):
    async with async_session_maker() as session:
        result = await session.execute(select(User).where(User.telegram_id == user_tg_id, User.active == True))
    return result.scalar()


@dp.message(Command('start'))
async def start_handler(message: Message):
    await message.answer(
        "Привет! Я выдаю статистику посещаемости для URL-адреса(ов). URL-адреса можно вводить по одному или сразу несколько.\n" \
        "\nПри вводе нескольких URL в качестве разделителей допустимо использовать: " \
        "многострочный ввод (каждый новый URL начинается с новой строки), проблелы или запятые. " \
        "\n\n <u>Внимание!</u> При вводе нескольких URL одновременно допускается не более 20 URL в сообщении!",
        parse_mode='html'
    )


@dp.message(States.waiting_one_date)
async def get_one_date(message: Message, state: FSMContext):
    format = '%d.%m.%Y'
    try:
        date1 = datetime.datetime.strptime(message.text, format)
        date1 = date1.date()
        date2 = datetime.date.today()
        raw_processing_urls = await state.get_data()
        raw_processing_urls = raw_processing_urls.get('user_request')
        header = f'Статистика за период с {date1.strftime("%d.%m.%Y")} по {date2.strftime("%d.%m.%Y")}'
        async with ClientSession() as http_client_session:
            await request_processing(raw_processed_urls=raw_processing_urls, http_request_session=http_client_session,
                                     date1=str(date1), date2=str(date2), header=header, message=message)
        await state.clear()
    except ValueError:
        await message.answer('Некорректный формат даты')


@dp.message(States.waiting_two_dates)
async def get_two_dates(message: Message, state: FSMContext):
    format = '%d.%m.%Y'
    try:
        date1, date2 = message.text.split('-')
        date1, date2 = datetime.datetime.strptime(date1, format), datetime.datetime.strptime(date2, format)
        date1, date2 = date1.date(), date2.date()
        raw_processed_urls = await state.get_data()
        raw_processed_urls = raw_processed_urls.get('user_request')
        header = f'Статистика за период с {date1.strftime("%d.%m.%Y")} по {date2.strftime("%d.%m.%Y")}'
        async with ClientSession() as http_client_session:
            await request_processing(raw_processed_urls=raw_processed_urls, http_request_session=http_client_session,
                                     date1=str(date1), date2=str(date2), header=header, message=message)
        await state.clear()
    except ValueError:
        await message.answer('Некорректный формат даты.')


@dp.message(F.text.strip().startswith('https://'))
async def get_message(message: Message, state: FSMContext):
    try:
        user = await check_user(message.from_user.id)
        # если пользователя нет в БД, не берем его запрос в обработку
        if not bool(user):
            err_msg = f'К сожалению, у вас нет доступа к этому боту. Пожалуйста, обратитесь к администратору @antoxaSV'
            raise NotAccesUserError(err_msg)

        async with async_session_maker() as session:
            await session.execute(insert(RequestsLog).values(
                user_id=user.id, request=message.text, message_id=message.message_id))
            await session.commit()

        raw_processed_urls = await extract_urls_from_message(message.text)

        await state.update_data(user_request=raw_processed_urls)

        button_1 = InlineKeyboardButton(text='Дата начала - по сегодняшний день', callback_data='date_from-today')
        button_2 = InlineKeyboardButton(text='Дата начала - дата окончания', callback_data='date_from-date_to')
        button_3 = InlineKeyboardButton(
            text="За всё время", callback_data="all_time_statistics"
        )

        # Создаем объект инлайн-клавиатуры
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[button_1], [button_2], [button_3]])
        await message.answer('Задайте временной интервал сбора статистики:', reply_markup=keyboard)
    except IncorrectUrl as err:
        await message.answer(str(err), parse_mode='html')
    except NotAccesUserError as err:
        await message.answer(str(err))
    except MaxCountUrlError as err:
        await message.answer(str(err), parse_mode='html')
    except Exception as err:
        # тут стоит логировать ошибку в БД
        print(traceback.format_exc())
        await message.answer(f'Непредвиденная ошибка.\n\n{str(err)[:63]}')


@dp.callback_query(F.data == 'all_time_statistics')
async def stat_all_time(callback: CallbackQuery, state: FSMContext):
    raw_processed_urls = await state.get_data()
    raw_processed_urls = raw_processed_urls.get('user_request')

    header = f'Статистика на {datetime.date.today().strftime("%d.%m.%Y")}'
    async with ClientSession() as http_request_session:
        await request_processing(raw_processed_urls=raw_processed_urls,
                                 callback=callback, http_request_session=http_request_session, date1='2021-04-12',
                                 date2=datetime.date.today(),
                                 header=header)
    await state.clear()


@dp.callback_query(F.data == 'date_from-today')
async def date_from_today(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await callback.message.answer('Введите дату начала периода в формате DD.MM.YYYY')
    await state.set_state(States.waiting_one_date)


@dp.callback_query(F.data == 'date_from-date_to')
async def date_from_date_to(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await callback.message.answer("Введите дату начала и дату окончания периода в формате DD.MM.YYYY-DD.MM.YYYY")
    await state.set_state(States.waiting_two_dates)


@dp.message()
async def other_message(message: Message):
    await message.answer('Ваш запрос не является корректным URL-адресом.' \
                         ' Пожалуйста, проверьте правильность ввода URL-адреса.' \
                         '\n\nПример корректного URL: https://um.mos.ru/quizzes/kvest-kosmonavtiki/')


async def request_processing(raw_processed_urls, http_request_session: ClientSession,
                             date1, date2, header, callback: CallbackQuery = None, message: Message = None):
    try:
        if callback:
            username = callback.from_user.username
            message = callback.message
            await message.delete()
        else:
            username = message.from_user.username

        ym_request = YMRequest(ym_token)

        progress_msg = await message.answer(
            f'Получено <u><b>{len(raw_processed_urls)}</b></u> URL-адресов. Сбор статистики...', parse_mode='html')
        tasks = [ym_request.get_statistics(http_request_session, raw_url, raw_processed_urls[raw_url], date1=date1,
                                           date2=date2) for
                 raw_url in raw_processed_urls]
        result = await asyncio.gather(*tasks)

        filename = f"{username}_{datetime.datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
        await progress_msg.edit_text('Подвожу итоги...')
        sum_stat_for_url = await ym_request.get_sum_statistics(raw_processed_urls.keys(),
                                                               raw_processed_urls.values(), date1, date2)
        await progress_msg.edit_text('Формирую ответ...')
        file: bytes = xlsx_writter(result, filename, sum_stat_for_url, header)
        await progress_msg.delete()
        await bot.send_document(chat_id=message.chat.id, document=BufferedInputFile(file=file, filename=filename),
                                caption=f'Обработка завершена успешно!\n\nОбработано <u><b>{len(raw_processed_urls)}</b></u> URL-адресов.',
                                parse_mode='html')

    except BadRequestError as err:
        await message.answer(str(err))
    except ClientResponseError as err:
        await message.answer(
            'Ошибка выполнения запроса к Яндекс Метрике.' \
            'Пожалуйста, сверьте вводимые даты начала и окончания периода. Если вы не вводили даты вручную и (или)' \
            ' проблема повторяется, пожалуйста, обратитесь к администратору @antoxaSV'
        )
    except Exception as err:
        # тут стоит логировать ошибку в таблицу БД
        print(traceback.format_exc())
        await message.answer(f'Произошла непредвиденная ошибка\n\n{str(err)[:63]}')


async def main():
    print('Бот запущен')
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
