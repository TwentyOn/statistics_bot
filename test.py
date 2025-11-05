from urllib.parse import urlparse
import re
import aiohttp


class Session:
    pass


class MyClass:
    def __init__(self):
        self._session = Session()

    def get_session(self):
        yield self._session

m = MyClass()

a = next(m.get_session())
b = next(m.get_session())
print(a, b, a is b)
