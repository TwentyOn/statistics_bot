import asyncio
import os
import sys
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, FSInputFile
from aiogram.filters import Command
import xlsxwriter
from dotenv import load_dotenv

from YD_API import YandexMetricaApi
load_dotenv()

tg_token = os.getenv('TG_TOKEN')
ym_token = os.getenv('YM_TOKEN')

bot = Bot(token=tg_token)
dp = Dispatcher()


def xlsx_writter(statistics):
    workbook = xlsxwriter.Workbook('demo.xlsx')
    worksheet = workbook.add_worksheet()
    worksheet.merge_range(
        'A1:I1',
        'СТАТИСТИКА',
        workbook.add_format({'bold': True, 'align': 'center', 'font_size': 14}))
    headers = [
        '№', 'URL-адрес', 'Визитов', 'Посещений', 'Просмотров', 'глубина', 'время', '% отказов', '% новых'
    ]
    for col, header in enumerate(headers):
        worksheet.write(1, col, header, workbook.add_format({'bold': True}))
        worksheet.set_column(1, col, 15)

    for row, row_stat in enumerate(statistics, start=2):
        for col, data in enumerate(row_stat, start=1):
            if col == 1:
                worksheet.write(row, 0, row - 1)
            worksheet.write(row, col, data)
    workbook.close()


@dp.message(Command('start'))
async def start_handler(message: Message):
    await message.answer(
        "Привет! Я выдаю статистику по URL. URL можно вводить по одному или сразу несколько." \
        "Если вводите несколько URL, пожалуйста, каждый URL начинайте с новой строки, иначе я не пойму :("
    )


@dp.message(F.text)
async def url_handler(message: Message):
    raw_urls = list(map(lambda raw_url: raw_url.strip(), filter(bool, message.text.split('\n'))))
    ym_api = YandexMetricaApi(ym_token, raw_urls)
    result = await ym_api.get_statistics(message, bot)
    xlsx_writter(result)
    await bot.send_document(chat_id=message.chat.id, document=FSInputFile('demo.xlsx'),
                            caption='Обработка завершена успешно!')
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
