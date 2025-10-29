import asyncio
import csv
import datetime
import os.path
from datetime import timedelta
from urllib.parse import urlparse
from collections import namedtuple

import requests
from aiogram import Bot
from aiogram.types import Message
from sqlalchemy import select
from aiohttp import ClientSession

from database.db import async_session_maker, connection
from database.models import DomainCounter

# namedtuple (по-умолчанию все параметры=0)
statistic = namedtuple('Statistic', [
    'raw_url', 'visits', 'users', 'pageViews', 'pageDepth', 'visitDuration', 'bounceRate', 'newUsers'],
                       defaults=[0 for _ in range(8)])


class YMRequest:
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
                select(DomainCounter.counter).where(DomainCounter.domain_name == netloc))
        # получаем счётчик по домену
        counter = domain_counter_obj.scalar()
        return counter

    async def _get_counters(self, raw_urls: list[str]):
        # доменты из сырых URL
        netlocs = map(lambda url: urlparse(url).netloc, raw_urls)

        async with async_session_maker() as session:
            smtm = select(DomainCounter.counter).where(DomainCounter.domain_name.in_(netlocs))
            domain_counters_objects = await session.execute(smtm)
        return domain_counters_objects.scalars().all()

    def statistic_placeholder(self, stat, raw_url=None):
        stat = statistic(
            raw_url=raw_url, visits=int(stat[0]), users=int(stat[1]), pageViews=int(stat[2]),
            pageDepth=round(float(stat[3]), 2), visitDuration=timedelta(seconds=round(stat[4])),
            bounceRate=round(stat[5], 2), newUsers=round(stat[6], 2))
        return stat

    async def get_statistics(self, session, raw_url, cleaned_url, date1='2021-04-12', date2=datetime.date.today()):
        await asyncio.sleep(1)
        counter_id = await self._get_counter(raw_url)
        parameters = {
            'id': counter_id,
            'metrics': 'ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:pageDepth,ym:s:avgVisitDurationSeconds,ym:s:bounceRate,ym:s:percentNewVisitors',
            'filters': f"EXISTS(ym:pv:URL=*'*{cleaned_url}*')",
            'date1': '2021-04-12',
            'date2': '2025-09-30',
            'accuracy': 'full'
        }
        # stat = requests.get(self.api_url, headers=self.headers, params=parameters)
        # stat = stat.json().get('data')
        async with session.get(self.api_url, headers=self.headers, params=parameters) as response:
            response.raise_for_status()
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

    async def get_sum_statistics(self, raw_urls, cleaned_urls, date1='2021-04-12', date2=datetime.date.today()):
        counters = await self._get_counters(raw_urls)
        counter_ids = ','.join(counters)
        filters = ' OR '.join([f"EXISTS(ym:pv:URL=*'*{cleaned_url}*')" for cleaned_url in cleaned_urls])
        parameters = {
            'ids': counter_ids,
            'metrics': 'ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:pageDepth,ym:s:avgVisitDurationSeconds,ym:s:bounceRate,ym:s:percentNewVisitors',
            'filters': filters,
            'date1': date1,
            'date2': date2,
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
