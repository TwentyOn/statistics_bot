import csv
from urllib.parse import urlparse
from collections import namedtuple
from functools import singledispatchmethod

import requests
from aiogram import Bot
from aiogram.types import Message

statistic = namedtuple('Statistic', [
    'raw_url', 'visits', 'users', 'pageViews', 'pageDepth', 'visitDuration', 'bounceRate', 'newUsers'])


class YandexMetricaApi:
    def __init__(self, oauth_token, raw_urls):
        self.token = oauth_token
        self.api_url = 'https://api-metrika.yandex.net/stat/v1/data.csv'
        self.headers = {'Authorization': self.token}
        processed_urls = self.urls_procissing([raw_url.strip() for raw_url in raw_urls if raw_url.strip()])
        self.processed_urls = dict(zip(raw_urls, processed_urls))

    def get_counter(self, raw_url):
        netloc = urlparse(raw_url).netloc
        with open('counters.csv', encoding='utf-8') as file:
            for row in csv.DictReader(file):
                if netloc == row['allowed_domain']:
                    return row['counter_id']
        return None

    def urls_procissing(self, raw_urls):
        result = []
        for raw_url in raw_urls:
            parse_url = urlparse(raw_url)
            process_url = parse_url.netloc + parse_url.path
            if not process_url.endswith('/'):
                process_url += '/'
            result.append(process_url)
        return result

    def statistic_placeholder(self, raw_url, stat):
        if stat[1] != 'нет данных':
            stat = statistic(
                raw_url=raw_url, visits=int(stat[0]), users=int(stat[1]), pageViews=int(stat[2]),
                pageDepth=round(float(stat[3]), 2), visitDuration=stat[4], bounceRate=round(float(stat[5]) * 100, 2),
                newUsers=round(float(stat[6]) * 100, 2))
        return stat

    async def get_statistics(self, message: Message, bot: Bot):
        result = []
        progress_msg = await message.answer(f'Сбор статистики: 0/{len(self.processed_urls)} обработано.')
        for i, raw_url in enumerate(self.processed_urls, 1):
            request_url = self.processed_urls[raw_url]
            counter_id = self.get_counter(raw_url)
            parameters = {
                'id': counter_id,
                'metrics': 'ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:pageDepth,ym:s:avgVisitDurationSeconds,ym:s:bounceRate,ym:s:percentNewVisitors',
                'filters': f"EXISTS(ym:pv:URL=*'*{request_url}*')",
                'date1': '2021-04-12',
                'date2': '2025-10-09',
                'accuracy': 'full'
            }
            stat = requests.get(self.api_url, headers=self.headers, params=parameters)
            stat = [i for i in stat.text.split('\n') if i]
            if len(stat) == 1:
                stat = [raw_url]
                stat.extend(['нет данных' for i in range(7)])
            else:
                stat = stat[1].split(',')
                stat = [i.strip('"') for i in stat]
                stat = self.statistic_placeholder(raw_url, stat)
            result.append(stat)
            await progress_msg.edit_text(
                f'Сбор статистики: {i}/{len(self.processed_urls)} ({int((i / len(self.processed_urls)) * 100)} %) обработано.')
        await progress_msg.delete()
        return result
