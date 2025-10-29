import asyncio
import os
import re
import sys
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from aiogram.filters import Command
import xlsxwriter
from aiohttp import ClientSession
from sqlalchemy import select, insert

from utils.ym_api import YMRequest
from utils.url_processing import urls_processing, IncorrectUrl, extract_urls_from_message
from settings import tg_token, ym_token
from database.db import async_session_maker
from database.models import User, RequestsLog
from utils.ym_api import statistic

bot = Bot(token=tg_token)
dp = Dispatcher()


class NotAccesUserError(Exception):
    def __init__(self, message='Отказано в доступе'):
        self.message = message

    def __str(self):
        return self.message


async def check_user(user_tg_id):
    print('проверка пользователя')
    async with async_session_maker() as session:
        result = await session.execute(select(User).where(User.telegram_id == user_tg_id, User.active == True))
    return result.scalar()


def xlsx_writter(statistics: list[statistic], filename: str, sum_stat):
    """
    Функция записывает данные статистики с excel-файл
    :param statistics: список с объектами statistic (namedtuple)
    :param filename: имя выходного файла
    :return: None
    """
    workbook = xlsxwriter.Workbook(f'../{filename}', {'in_memory': True})
    worksheet = workbook.add_worksheet()
    worksheet.merge_range(
        'A1:I1',
        'СТАТИСТИКА',
        workbook.add_format({'bold': True, 'align': 'center', 'font_size': 14, 'border': 2, 'bg_color': '#B0E0E6'}))
    headers = [
        '№', 'URL-адрес', 'Визитов', 'Посещений', 'Просмотров', 'Глубина просмотра', 'Время на сайте', 'Доля отказов',
        'Доля новых'
    ]
    # запись заголовков таблицы
    for col, header in enumerate(headers):
        worksheet.write(1, col, header, workbook.add_format({'bold': True, 'border': 2, 'align': 'center'}))

    # форматы записи статистики в ячейки
    default_format = workbook.add_format({'border': 1, 'align': 'center'})
    url_format = workbook.add_format({'border': 1, 'align': 'left'})
    time_format = workbook.add_format({'num_format': 'hh:mm:ss', 'align': 'center', 'border': 1})
    percent_format = workbook.add_format({'num_format': '0.00%', 'align': 'center', 'border': 1})

    # запись данных
    for row, row_stat in enumerate(statistics, start=2):
        worksheet.write(row, 0, row - 1, workbook.add_format({'border': 2, 'align': 'center'}))
        worksheet.write(row, 1, row_stat.raw_url, url_format)
        worksheet.write(row, 2, row_stat.visits, default_format)
        worksheet.write(row, 3, row_stat.users, default_format)
        worksheet.write(row, 4, row_stat.pageViews, default_format)
        worksheet.write(row, 5, row_stat.pageDepth, default_format)
        worksheet.write(row, 6, row_stat.visitDuration, time_format)
        worksheet.write(row, 7, row_stat.bounceRate / 100, percent_format)
        worksheet.write(row, 8, row_stat.newUsers / 100, percent_format)

    # Запись итогов
    print('суммарная стата', sum_stat)
    # № строки для записи итогов (+2 строки с учетом заголовков)
    itog_row = len(statistics) + 2
    print('itog_row', itog_row)
    worksheet.merge_range(f'A{itog_row + 1}:B{itog_row + 1}', 'ИТОГО',
                          worksheet.workbook_add_format({'bold': True, 'align': 'center', 'border': 1}))
    # worksheet.write(itog_row, 0, 'Итого', workbook.add_format({'bold': True, 'border': 2, 'align': 'center'}))
    worksheet.write(itog_row, 1, '', default_format)
    worksheet.write(itog_row, 2, sum_stat.visits, default_format)
    worksheet.write(itog_row, 3, sum_stat.users, default_format)
    worksheet.write(itog_row, 4, sum_stat.pageViews, default_format)
    worksheet.write(itog_row, 5, sum_stat.pageDepth, default_format)
    worksheet.write(itog_row, 6, sum_stat.visitDuration, time_format)
    worksheet.write(itog_row, 7, sum_stat.bounceRate / 100, percent_format)
    worksheet.write(itog_row, 8, sum_stat.newUsers / 100, percent_format)

    # применение условного форматирования к заполненным данным
    for cell in ('C', 'D', 'E', 'F', 'G', 'H', 'I'):
        # для доли отказов применяем инвертированные цвета
        if cell == 'H':
            worksheet.conditional_format(f'{cell}3:{cell}{itog_row}', {
                'type': '3_color_scale',
                'min_color': '#63BE7B',
                'mid_color': '#FFEB84',
                'max_color': '#F8696B'})
        else:
            worksheet.conditional_format(f'{cell}3:{cell}{itog_row}', {'type': '3_color_scale'})

    # выравнивание ширины ячеек по контенту (макс ширина = 450 px)
    worksheet.autofit(450)
    worksheet.set_column(0, 0, 6)
    workbook.close()


@dp.message(Command('start'))
async def start_handler(message: Message):
    await message.answer(
        "Привет! Я выдаю статистику посещаемости для URL-адреса(ов). URL-адреса можно вводить по одному или сразу несколько.\n" \
        "\nПри вводе нескольких URL в качестве разделителей допустимо использовать: " \
        "многострочный ввод (каждый новый URL начинается с новой строки), проблелы или запятые. "
    )


@dp.message(F.text)
async def get_message(message: Message):
    user = await check_user(message.from_user.id)
    # если пользователя нет в БД, не берем его запрос в обработку
    if not bool(user):
        err_msg = f'К сожалению, у вас нет доступа к этому боту. Пожалуйста, обратитесь к администратору @antoxaSV'
        raise NotAccesUserError(err_msg)

    async with async_session_maker() as session:
        await session.execute(insert(RequestsLog).values(
            user_id=user.id, request=message.text, message_id=message.message_id))
        await session.commit()

    button_1 = InlineKeyboardButton(
        text="За всё время", callback_data="all_time_statistics"
    )
    button_2 = InlineKeyboardButton(text='Дата начала - дата окончания', callback_data='date_from-date_to')
    # Создаем объект инлайн-клавиатуры
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[button_1]])
    await message.answer('Задайте временной интервал сбора статистики:', reply_markup=keyboard)

@dp.callback_query(F.data == 'date_from-date_to')
async def state_from_to(callback: CallbackQuery):
    pass


@dp.callback_query(F.data == 'all_time_statistics')
async def stat_all_time(callback: CallbackQuery):
    async with async_session_maker() as session:
        message = await session.execute(
            select(RequestsLog.request).where(RequestsLog.message_id == callback.message.message_id - 1))
        message = message.scalar()

    try:

        # словарь в виде - сырой юрл: обработанный юрл
        raw_processed_urls = await extract_urls_from_message(message)
        await callback.message.delete()

        ym_request = YMRequest(ym_token)

        progress_msg = await callback.message.answer(f'Получено <u><b>{len(raw_processed_urls)}</b></u> URL-адресов. Сбор статистики...', parse_mode='html')


        async with ClientSession() as session:
            tasks = [ym_request.get_statistics(session, raw_url, raw_processed_urls[raw_url]) for
                     raw_url in raw_processed_urls]
            result = await asyncio.gather(*tasks)

        # for count, raw_url in enumerate(raw_processed_urls, start=1):
        #     stat_for_url = await ym_request.get_statistics(raw_url, raw_processed_urls[raw_url])
        #     result.append(stat_for_url)
        #     await progress_msg.edit_text(
        #         f'Сбор статистики: {count}/{len(processed_urls)} ({round((count / len(processed_urls)) * 100)} %) обработано...')
        filename = f"{callback.from_user.username}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
        await progress_msg.edit_text('Подвожу итоги...')
        sum_stat_for_url = await ym_request.get_sum_statistics(raw_processed_urls.keys(),
                                                               raw_processed_urls.values())
        await progress_msg.edit_text('Формирую ответ...')
        xlsx_writter(result, filename, sum_stat_for_url)
        await progress_msg.delete()
        await bot.send_document(chat_id=callback.message.chat.id, document=FSInputFile(f'../{filename}'),
                                caption=f'Обработка завершена успешно!\n\nОбработано <u><b>{len(raw_processed_urls)}</b></u> URL-адресов.', parse_mode='html')
    except IncorrectUrl as err:
        await message.answer(str(err))
    except NotAccesUserError as err:
        print('ошибка доступа', err)
        await message.answer(str(err))


@dp.message()
async def uncorrect_message(message: Message):
    return message.answer('Похоже что вы ввели не URL. Справку по использованию бота можно получить по команде /start')


async def main():
    print('Бот запущен')
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
