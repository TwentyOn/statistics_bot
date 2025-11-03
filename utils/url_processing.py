import re
from urllib.parse import urlparse
from utils.custom_exceptions import MaxCountUrlError, IncorrectUrl, BadRequestError



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
            raise IncorrectUrl(f'Получен некорректный url-адрес: <u>{raw_url}</u>')

        process_url = netloc + path
        if not process_url.endswith('/'):
            process_url += '/'

        processed_urls.append(process_url)

    raw_processed_urls = dict(zip(raw_urls, processed_urls))

    return raw_processed_urls
