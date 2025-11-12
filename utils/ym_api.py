import asyncio
import datetime
import re
from datetime import timedelta
from urllib.parse import urlparse
from collections import namedtuple
import logging

import requests
from sqlalchemy import select

from database.db import async_session_maker
from database.models import DomainCounter
from utils.custom_exceptions import BadRequestError

# namedtuple для хранения данных (по-умолчанию все параметры=0)
statistic = namedtuple('Statistic', [
    'raw_url', 'visits', 'users', 'pageViews', 'pageDepth', 'visitDuration', 'bounceRate', 'newUsers'],
                       defaults=[0 for _ in range(8)])

logger = logging.getLogger(__name__)


class YMRequest:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance.semaphore = asyncio.Semaphore(5)
        return cls._instance

    def __init__(self, oauth_token):
        self.token = oauth_token
        self.api_url = 'https://api-metrika.yandex.net/stat/v1/data'
        self.headers = {'Authorization': self.token}
        # минимальная дата начала интервала сбора статистики
        self.min_date = datetime.date(2020, 1, 1)
        self.sampling = ['full', 'high', 'medium', 'low']

    async def _get_counter(self, raw_url: str):
        """
        Получает один счётчик для raw_url
        :param raw_url:
        :return:
        """
        # домен
        netloc = re.sub('www.', '', urlparse(raw_url).netloc)
        # сессия базы данных
        async with async_session_maker() as session:
            domain_counter_obj = await session.execute(
                select(DomainCounter.counter).where(DomainCounter.domain_name == netloc))
            # получаем счётчик по домену
            counter = domain_counter_obj.scalar_one_or_none()
        if not counter:
            raise BadRequestError(
                f'Не удалось найти счётчик Яндекс Метрики по домену: {netloc}.'
            )
        return counter

    async def _get_counters(self, raw_urls: list[str]):
        """
        Получает № счётчиков для всех переданных URL
        :param raw_urls: список URL-ов
        :return:
        """
        # домены из сырых URL
        netlocs = map(lambda url: re.sub('www.', '', urlparse(url).netloc), raw_urls)

        async with async_session_maker() as session:
            smtm = select(DomainCounter.counter).where(DomainCounter.domain_name.in_(netlocs))
            domain_counters_objects = await session.execute(smtm)
            counters_dates = domain_counters_objects.scalars().all()
        return counters_dates

    def statistic_placeholder(self, stat, raw_url=None):
        stat = statistic(
            raw_url=raw_url, visits=int(stat[0]), users=int(stat[1]), pageViews=int(stat[2]),
            pageDepth=round(float(stat[3]), 2), visitDuration=timedelta(seconds=round(stat[4])),
            bounceRate=round(stat[5], 2), newUsers=round(stat[6], 2))
        return stat

    async def get_statistics(self, session, raw_url, cleaned_url, date1, date2):
        counter_id = await self._get_counter(raw_url)

        # если дата начала периода < минимально установленого порога, берём дату установленного порога
        if date1 is None or datetime.datetime.strptime(date1, '%Y-%m-%d').date() < self.min_date:
            date1 = str(self.min_date)
        if date2 is None:
            date2 = str(datetime.date.today())

        parameters = {
            'id': counter_id,
            'metrics': 'ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:pageDepth,ym:s:avgVisitDurationSeconds,ym:s:bounceRate,ym:s:percentNewVisitors',
            'filters': f"EXISTS(ym:pv:URL=*'*{cleaned_url}*')",
            'date1': date1,
            'date2': date2,
            'accuracy': 'full'
        }

        async with self.semaphore:
            status = None
            message = None
            # 4 попытки получить данные с яндекс метрики
            for _ in range(4):
                acc = self.sampling[_]
                # понижение точности с каждой попыткой
                parameters['accuracy'] = acc
                async with session.get(self.api_url, headers=self.headers, params=parameters) as response:
                    # если данные получены успешно
                    if response.status == 200:
                        stat = await response.json()
                        stat = stat.get('data')
                        status = None
                        message = None
                        break
                    status = response.status
                    error = await response.json()
                    message = error.get('message')
                    logger.error(f'Ошибка получения данных для URL: {raw_url}; Точность: {acc}; Попытка: {_ + 1}; Ошибка: {status}:{message}')
            if status == 400:
                raise BadRequestError(
                    f'Не удалось получить данные от API Яндекс Метрики. \n\n error_message: {message}. '
                    f'\n\nВозможное решение: уменьшите период формирования статистики или попробуйте позднее.')
            elif status == 429:
                raise BadRequestError(
                    f'Не удалось получить данные от API Яндекс Метрики: {message}. Попробуйте повторить запрос')
        if stat:
            # т.к группировки не используются всегда берём первый элемент из data
            stat = stat[0]['metrics']
            # передаём статистику для заполнения namedtuple
            stat = self.statistic_placeholder(stat, raw_url)
        else:
            # иначе берем namedtuple по-умолчанию
            stat = statistic(raw_url=raw_url)
        # new_progress_msg = f'Сбор статистики: {count}/{100} ({round((count / 100) * 100)} %) обработано...'
        # if new_progress_msg != progress_msg:
        #     await progress_msg.edit_text(new_progress_msg)
        return stat

    async def get_sum_statistics(self, raw_urls, cleaned_urls, date1, date2):
        """
        Метод для получения итоговой суммы статистики для всех полученных URL
        :param raw_urls:
        :param cleaned_urls:
        :param date1:
        :param date2:
        :return:
        """
        counters = await self._get_counters(raw_urls)
        counter_ids = ','.join(counters)
        if date1 is None or datetime.datetime.strptime(date1, '%Y-%m-%d').date() < self.min_date:
            date1 = str(self.min_date)
        if date2 is None:
            date2 = str(datetime.date.today())
        filters = ' OR '.join([f"EXISTS(ym:pv:URL=*'*{cleaned_url}*')" for cleaned_url in cleaned_urls])
        parameters = {
            'ids': counter_ids,
            'metrics': 'ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:pageDepth,ym:s:avgVisitDurationSeconds,ym:s:bounceRate,ym:s:percentNewVisitors',
            'filters': filters,
            'date1': date1,
            'date2': date2,
            'accuracy': 'full'
        }
        message = None
        # 4 попытки получить данные с яндекс метрики
        for _ in range(4):
            # понижение точности с каждой попыткой
            acc = self.sampling[_]
            parameters['accuracy'] = acc
            stat = requests.get(self.api_url, headers=self.headers, params=parameters)
            if stat.status_code == 200:
                stat = stat.json().get('data')
                if stat:
                    # т.к группировки не используются всегда берём первый элемент из data
                    stat = stat[0]['metrics']
                    # передаём статистику для заполнения namedtuple
                    stat = self.statistic_placeholder(stat)
                else:
                    # иначе берем namedtuple по-умолчанию
                    stat = statistic()
                return stat
            message = stat.json().get('message')
            logger.error(
                f'Ошибка получения суммы данных точность: {acc}; Попытка: {_ + 1}; Ошибка: {message}')

        if 'Quota exceeded for quantity of parallel user requests' in message:
            raise BadRequestError(
                f'Не удалось получить итоговые данные от Яндекс Метрики.' \
                f'\n\nerror_message: {message}' \
                '\n\nПохоже, сейчас выполняется слишком много запросов к Яндекс Метрике. Попробуйте повторить запрос через пару минут.'
            )
        else:
            raise BadRequestError(
                f'Не удалось получить итоговые данные от Яндекс Метрики.' \
                f'\n\n error_message:{message}' \
                '\n\nВозможное решение: уменьшите период формирования статистики или попробуйте позднее.'
            )
