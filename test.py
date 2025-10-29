# import requests
# from aiohttp import ClientSession
# from settings import tg_token, ym_token
# from urllib.parse import urlparse
# import asyncio
#
#
# api_url = 'https://api-metrika.yandex.net/stat/v1/data'
# headers = {'Authorization': ym_token}
# counter_ids = '21093856'
# async def make_request(session: ClientSession, url):
#     parameters = {
#         'id': counter_ids,
#         'metrics': 'ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:pageDepth,ym:s:avgVisitDurationSeconds,ym:s:bounceRate,ym:s:percentNewVisitors',
#         'filters': f"EXISTS(ym:pv:URL=*'*{url}*')",
#         'date1': '2021-04-12',
#         'date2': '2025-09-30',
#         'accuracy': 'full'
#     }
#     result = await session.get(api_url, headers=headers, params=parameters)
#     result = await result.json()
#     return result
#
# async def main():
#     raw_urls = [
#         'https://um.mos.ru/novogodnie-katki/',
#         'https://karta.mos.ru/transport/'
#     ]
#
#     cleaned_urls = [urlparse(url).netloc + urlparse(url).path for url in raw_urls]
#     async with ClientSession() as session:
#         tasks = [make_request(session, url) for url in cleaned_urls]
#         results = await asyncio.gather(*tasks)
#     print(results)
#     return results
#
# asyncio.run(main())
import re
text = """https://um.mos.ru/novogodnie-katki/, https://um.mos.ru/routes/istoriya-novogo-goda-tradicii-prazdnovaniya-v-moskve/
https://um.mos.ru/new_year/

https://um.mos.ru/quests/mgu-270-let-kviz-ko-dnyu-osnovaniya-moskovskogo-gosudarstvennogo-universiteta-imeni-m-v-lomonosova
https://um.mos.ru/quizzes/kvest-kosmonavtiki/

https://um.mos.ru/mobile/houses/3287
"""

def extract_urls_from_message(message):
    url_list = re.sub(r'[ ,\n]', ' ', message).split()
    return url_list

print(extract_urls_from_message(text))