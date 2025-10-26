import csv
import os.path
from datetime import timedelta
from urllib.parse import urlparse
from collections import namedtuple

import requests
from aiogram import Bot
from aiogram.types import Message
from sqlalchemy import select

from database.db import get_db
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

    def get_counter(self, raw_url: str):
        # домен
        netloc = urlparse(raw_url).netloc
        # сессия базы данных
        db = next(get_db())
        # получаем счётчик по домену
        counter = db.execute(select(DomainCounter.counter).where(DomainCounter.domain_name == netloc))
        return counter

    def statistic_placeholder(self, raw_url, stat):
        stat = statistic(
            raw_url=raw_url, visits=int(stat[0]), users=int(stat[1]), pageViews=int(stat[2]),
            pageDepth=round(float(stat[3]), 2), visitDuration=timedelta(seconds=int(stat[4])),
            bounceRate=round(stat[5], 2), newUsers=round(stat[6], 2))
        return stat

    async def get_statistics(self, raw_url, cleaned_url):
        result = []
        counter_id = self.get_counter(raw_url)
        parameters = {
            'id': counter_id,
            'metrics': 'ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:pageDepth,ym:s:avgVisitDurationSeconds,ym:s:bounceRate,ym:s:percentNewVisitors',
            'filters': f"EXISTS(ym:pv:URL=*'*{cleaned_url}*')",
            'date1': '2021-04-12',
            'date2': '2025-09-30',
            'accuracy': 'full'
        }
        stat = requests.get(self.api_url, headers=self.headers, params=parameters)
        stat = stat.json().get('data')
        if stat:
            # т.к группировки не используются всегда берём первый элемент из data
            stat = stat[0]['metrics']
            # передаём статистику для заполнения namedtuple
            stat = self.statistic_placeholder(raw_url, stat)
        else:
            # иначе берем namedtuple по-умолчанию
            stat = statistic(raw_url=raw_url)
        return stat
