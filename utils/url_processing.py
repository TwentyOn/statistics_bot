import re
from urllib.parse import urlparse


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


async def extract_urls_from_message(text: str) -> dict:
    url_list = re.sub(r'[ ,\n]', ' ', text).split()
    if len(url_list) > 20:
        raise MaxCountUrlError(len(url_list))
    raw_processed_urls = urls_processing(url_list)
    if not raw_processed_urls:
        raise BadRequestError
    return raw_processed_urls


def urls_processing(raw_urls: list) -> dict:
    processed_urls = []

    for raw_url in raw_urls:
        parse_url = urlparse(raw_url)
        scheme, netloc, path = parse_url.scheme, parse_url.netloc, parse_url.path

        if not all((scheme, netloc, path)):
            raise IncorrectUrl(f'Получен некорректный url-адрес: {raw_url}')

        process_url = netloc + path
        if not process_url.endswith('/'):
            process_url += '/'

        processed_urls.append(process_url)

    raw_processed_urls = dict(zip(raw_urls, processed_urls))

    return raw_processed_urls
