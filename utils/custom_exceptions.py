class NotAccessUserError(Exception):
    def __init__(self, message='Отказано в доступе'):
        self.message = message

    def __str(self):
        return self.message


class IncorrectUrl(Exception):
    def __init__(self, message='Ошибка обработки url-адреса'):
        self.message = message
        self.default_correct_message = 'Пример корректного URL: https://um.mos.ru/quizzes/kvest-kosmonavtiki/'

    def __str__(self):
        return self.message + '\n\n' + self.default_correct_message


class MaxCountUrlError(Exception):
    def __init__(self, url_count):
        self.url_count = url_count
        self.message = f'Превышено максимально допустимое количество одновременно обрабатываемых URL.' \
                       f'\n\n<u>Максимально допустимое количесво URL за один запрос = 20. Получено {self.url_count}</u>.'

    def __str__(self):
        return self.message


class BadRequestError(Exception):
    def __init__(self):
        self.message = 'В полученном запросе не найдено не одного URL-адреса.' \
                       '\n\nДля получения справки по формирование запроса воспользуйтесь командой /help'

    def __str(self):
        return self.message
