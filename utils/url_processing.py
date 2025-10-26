from urllib.parse import urlparse


class IncorrectUrl(Exception):
    def __init__(self, message='Ошибка обработки url-адреса'):
        self.message = message
        self.default_correct_message = 'Пример корректного URL: https://um.mos.ru/quizzes/kvest-kosmonavtiki/'

    def __str__(self):
        return self.message + '\n' + self.default_correct_message


def urls_procissing(raw_urls):
    result = []
    for raw_url in raw_urls:
        parse_url = urlparse(raw_url)
        scheme, netloc, path = parse_url.scheme, parse_url.netloc, parse_url.path
        print(scheme, netloc, path)
        if not all((scheme, netloc, path)):
            raise IncorrectUrl(f'Получен некорректный url-адрес: {raw_url}')
        process_url = netloc + path
        if not process_url.endswith('/'):
            process_url += '/'
        result.append(process_url)
    return result
