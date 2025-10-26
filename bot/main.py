import asyncio
import os

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, FSInputFile
from aiogram.filters import Command
import xlsxwriter
from dotenv import load_dotenv
from sqlalchemy import select, insert

from utils.ym_api import YMRequest
from utils.url_processing import urls_procissing, IncorrectUrl
from settings import tg_token, ym_token
from database.db import get_db
from database.models import User, RequestsLog

bot = Bot(token=tg_token)
dp = Dispatcher()


def write_log(user, request):
    db = next(get_db())


def check_user(user_tg_username):
    try:
        db = next(get_db())
        return bool(db.execute(select(User).
                               where(User.username == user_tg_username,
                                     User.active == True)).scalar())
    finally:
        db.close()


async def xlsx_writter(statistics):
    workbook = xlsxwriter.Workbook('../demo.xlsx')
    worksheet = workbook.add_worksheet()
    worksheet.merge_range(
        'A1:I1',
        'СТАТИСТИКА',
        workbook.add_format({'bold': True, 'align': 'center', 'font_size': 14, 'border': 2}))
    headers = [
        '№', 'URL-адрес', 'Визитов', 'Посещений', 'Просмотров', 'глубина', 'время', '% отказов', '% новых'
    ]
    # запись заголовков таблицы
    for col, header in enumerate(headers):
        worksheet.write(1, col, header, workbook.add_format({'bold': True, 'border': 2}))
        worksheet.set_column(1, col, 15)

    time_format = workbook.add_format({'num_format': 'hh:mm:ss', 'align': 'center', 'border': 1})
    # запись данных
    for row, row_stat in enumerate(statistics, start=2):
        for col, data in enumerate(row_stat, start=1):
            if col == 1:
                worksheet.write(row, 0, row - 1, workbook.add_format({'border': 2}))
            if col == 6:
                worksheet.write(row, col, data, time_format)
            else:
                worksheet.write(row, col, data, workbook.add_format({'border': 1}))

    for cell in ('C', 'D', 'E', 'F', 'G', 'H', 'I'):
        if cell == 'H':
            worksheet.conditional_format(f'{cell}3:{cell}9', {
                'type': '3_color_scale',
                'min_color': '#63BE7B',
                'mid_color': '#FFEB84',
                'max_color': '#F8696B'})
        else:
            worksheet.conditional_format(f'{cell}3:{cell}9', {'type': '3_color_scale'})
    workbook.close()


@dp.message(Command('start'))
async def start_handler(message: Message):
    await message.answer(
        "Привет! Я выдаю статистику по URL. URL можно вводить по одному или сразу несколько." \
        "Если вводите несколько URL, пожалуйста, каждый URL начинайте с новой строки, иначе я не пойму :("
    )


@dp.message(F.text)
async def url_handler(message: Message):
    print(message.from_user.id)
    try:
        user_tg_username = message.from_user.username
        if not check_user(user_tg_username):
            await message.answer(
                f'К сожалению, у вас нет доступа к этому боту. Пожалуйста, обратитесь к администратору @antoxaSV'
            )

        else:
            raw_urls = list(map(lambda raw_url: raw_url.strip(), filter(bool, message.text.split('\n'))))
            processed_urls = urls_procissing(raw_urls)
            raw_processed_urls = dict(zip(raw_urls, processed_urls))
            ym_request = YMRequest(ym_token)
            result = []
            progress_msg = await message.answer(f'Сбор статистики: 0/{len(processed_urls)} (0 %) обработано.')
            for count, raw_url in enumerate(raw_processed_urls, start=1):
                stat_for_url = await ym_request.get_statistics(raw_url, raw_processed_urls[raw_url])
                result.append(stat_for_url)
                await progress_msg.edit_text(
                    f'Сбор статистики: {count}/{len(processed_urls)} ({round((count / len(processed_urls)) * 100)} %) обработано.')
            await progress_msg.delete()
            await xlsx_writter(result)
            await bot.send_document(chat_id=message.chat.id, document=FSInputFile('../demo.xlsx'),
                                    caption='Обработка завершена успешно!')
    except IncorrectUrl as err:
        await message.answer(str(err))
    finally:
        db = next(get_db())
        user = db.execute(select(User).where(User.username == message.from_user.username)).scalar()
        db.execute(insert(RequestsLog).values(user_id=user.id, request=message.text))
        db.commit()
        db.close()
    # except IndexError as err:
    #     print(err)
    #     await message.answer('Произошла ошибка. Похоже был введён некорректный url.')
    # except Exception as err:
    #     print(sys.exc_info())
    #     await message.answer('Произошла непредвиденная ошибка.')


@dp.message()
async def uncorrect_message(message: Message):
    return message.answer('Похоже что вы ввели не URL. Справку по использованию бота можно получить по команде /start')


async def main():
    print('Бот запущен')
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
