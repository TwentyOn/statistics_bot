import io

import xlsxwriter


def xlsx_writter(statistics, filename: str, sum_stat, header):
    """
    Функция записывает данные статистики с excel-файл
    :param statistics: список с объектами statistic (namedtuple)
    :param filename: имя выходного файла
    :return: None
    """
    with io.BytesIO() as out_file_bytes:
        workbook = xlsxwriter.Workbook(out_file_bytes, {'in_memory': True})
        worksheet = workbook.add_worksheet()
        worksheet.merge_range(
            'A1:I1',
            header,
            workbook.add_format({'bold': True, 'align': 'center', 'font_size': 14, 'border': 2, 'bg_color': '#B0E0E6'}))
        headers = [
            '№', 'URL-адрес', 'Количество визитов', 'Количество посещений', 'Количество просмотров', 'Глубина просмотра', 'Время на сайте',
            'Доля отказов',
            'Доля новых'
        ]
        # запись заголовков таблицы
        for col, header in enumerate(headers):
            worksheet.write(1, col, header, workbook.add_format({'bold': True, 'border': 2, 'align': 'center'}))

        # форматы записи статистики в ячейки
        default_format = workbook.add_format({'border': 1, 'align': 'center'})
        number_format = workbook.add_format({'num_format': '#,##0', 'align': 'center', 'border': 1})
        url_format = workbook.add_format({'border': 1, 'align': 'left'})
        time_format = workbook.add_format({'num_format': 'hh:mm:ss', 'align': 'center', 'border': 1})
        percent_format = workbook.add_format({'num_format': '0.00%', 'align': 'center', 'border': 1})

        # запись данных
        for row, row_stat in enumerate(statistics, start=2):
            worksheet.write(row, 0, row - 1, workbook.add_format({'border': 2, 'align': 'center'}))
            worksheet.write(row, 1, row_stat.raw_url, url_format)
            worksheet.write(row, 2, row_stat.visits, number_format)
            worksheet.write(row, 3, row_stat.users, number_format)
            worksheet.write(row, 4, row_stat.pageViews, number_format)
            worksheet.write(row, 5, row_stat.pageDepth, default_format)
            worksheet.write(row, 6, row_stat.visitDuration, time_format)
            worksheet.write(row, 7, row_stat.bounceRate / 100, percent_format)
            worksheet.write(row, 8, row_stat.newUsers / 100, percent_format)

        # Запись итогов
        # № строки для записи итогов (+2 строки с учетом заголовков)
        itog_row = len(statistics) + 2
        worksheet.merge_range(f'A{itog_row + 1}:B{itog_row + 1}', 'ИТОГО',
                              worksheet.workbook_add_format({'bold': True, 'align': 'center', 'border': 1}))
        worksheet.write(itog_row, 1, '', default_format)
        worksheet.write(itog_row, 2, sum_stat.visits, number_format)
        worksheet.write(itog_row, 3, sum_stat.users, number_format)
        worksheet.write(itog_row, 4, sum_stat.pageViews, number_format)
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
        out_file_bytes.seek(0)
        out_file_bytes = out_file_bytes.getvalue()
    return out_file_bytes
