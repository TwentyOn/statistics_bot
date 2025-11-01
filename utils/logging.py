from database.models import RequestsLog
from sqlalchemy.ext.asyncio import AsyncSession
from database.db import async_session_maker
from sqlalchemy import update
from utils.url_processing import IncorrectUrl, BadRequestError, MaxCountUrlError
from aiogram.types import Message


def error_logging(func):
    def decorator(message: Message, *args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as err:
            if isinstance(err, IncorrectUrl):
                pass


async def write_error_to_db(request_id: int, trace: str):
    async with async_session_maker() as session:
        await session.execute(update(RequestsLog).where(RequestsLog.id == request_id).values(error_msg=trace))
        await session.commit()
