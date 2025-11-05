import asyncio
import datetime
from datetime import timedelta
from urllib.parse import urlparse
from collections import namedtuple, deque

import requests
from sqlalchemy import select
from aiohttp import ClientSession
import time

from database.db import async_session_maker, connection
from database.models import DomainCounter
from utils.custom_exceptions import BadRequestError

# namedtuple (по-умолчанию все параметры=0)
statistic = namedtuple('Statistic', [
    'raw_url', 'visits', 'users', 'pageViews', 'pageDepth', 'visitDuration', 'bounceRate', 'newUsers'],
                       defaults=[0 for _ in range(8)])


class GlobalRateLimiter:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.requests = deque()
            cls._instance.lock = asyncio.Lock()
        return cls._instance

    async def acquire(self, max_requests=5, period=1.0):
        while True:
            async with self.lock:
                current_time = time.time()

                # Удаляем старые запросы
                while self.requests and self.requests[0] <= current_time - period:
                    self.requests.popleft()

                # Проверяем лимит
                if len(self.requests) < max_requests:
                    self.requests.append(current_time)
                    return  # Выходим когда добавляем запрос

                # Если лимит превышен, вычисляем время ожидания
                oldest_timestamp = self.requests[0]
                sleep_time = period - (current_time - oldest_timestamp)

            # Ждем ВНЕ блокировки, чтобы другие корутины могли работать
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)


# Глобальный лимитер
global_limiter = GlobalRateLimiter()


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

    async def _get_counter(self, raw_url: str):
        # домен
        netloc = urlparse(raw_url).netloc
        # сессия базы данных
        async with async_session_maker() as session:
            domain_counter_obj = await session.execute(
                select(DomainCounter.counter, DomainCounter.created_at).where(DomainCounter.domain_name == netloc))
        # получаем счётчик по домену
        data = domain_counter_obj.first()
        if not data:
            raise BadRequestError(
                f'Не удалось найти счётчик Яндекс Метрики по домену: {netloc}.'
            )
        counter, created_at = data
        return counter, created_at

    async def _get_counters(self, raw_urls: list[str]):
        # домены из сырых URL
        netlocs = map(lambda url: urlparse(url).netloc, raw_urls)

        async with async_session_maker() as session:
            smtm = select(DomainCounter.counter, DomainCounter.created_at).where(DomainCounter.domain_name.in_(netlocs))
            domain_counters_objects = await session.execute(smtm)
        return domain_counters_objects.all()

    def statistic_placeholder(self, stat, raw_url=None):
        stat = statistic(
            raw_url=raw_url, visits=int(stat[0]), users=int(stat[1]), pageViews=int(stat[2]),
            pageDepth=round(float(stat[3]), 2), visitDuration=timedelta(seconds=round(stat[4])),
            bounceRate=round(stat[5], 2), newUsers=round(stat[6], 2))
        return stat

    async def get_statistics(self, session, raw_url, cleaned_url, date1, date2):
        await global_limiter.acquire()
        counter_id, created_at = await self._get_counter(raw_url)

        # если дата начала периода < даты создания счётчика, берём дату создания счётчика
        if date1 is None or datetime.datetime.strptime(date1, '%Y-%m-%d').date() < created_at:
            date1 = str(created_at)
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
            async with session.get(self.api_url, headers=self.headers, params=parameters) as response:
                if response.status == 400:
                    error = await response.json()
                    error = error.get('message')
                    raise BadRequestError(f'Не удалось получить данные от API Яндекс Метрики: {error}')
                elif response.status == 429:
                    error = await response.json()
                    error = error.get('message')
                    raise BadRequestError(f'Не удалось получить данные от API Яндекс Метрики: {error}')
                stat = await response.json()
                stat = stat.get('data')

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
        counters_data = await self._get_counters(raw_urls)
        counters = map(lambda t: t[0], counters_data)
        created_at = map(lambda t: t[1], counters_data)
        counter_ids = ','.join(counters)
        min_date = min(created_at)
        if date1 is None or datetime.datetime.strptime(date1, '%Y-%m-%d').date() < min_date:
            date1 = str(min_date)
        if date2 is None:
            date2 = str(datetime.date.today())
        filters = ' OR '.join([f"EXISTS(ym:pv:URL=*'*{cleaned_url}*')" for cleaned_url in cleaned_urls])
        parameters = {
            'ids': counter_ids,
            'metrics': 'ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:pageDepth,ym:s:avgVisitDurationSeconds,ym:s:bounceRate,ym:s:percentNewVisitors',
            'filters': filters,
            'date1': date1,
            'date2': str(date2),
            'accuracy': 'full'
        }
        stat = requests.get(self.api_url, headers=self.headers, params=parameters)
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
