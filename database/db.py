from collections.abc import AsyncGenerator, Generator
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
import os

from settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME


class Base(DeclarativeBase):
    pass


DATABASE_URL = f'postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
async_engine = create_async_engine(DATABASE_URL, echo=True)
async_session_maker = async_sessionmaker(bind=async_engine, expire_on_commit=False, class_=AsyncSession)

# engine = create_engine(DATABASE_URL, echo=True)
# session_maker = sessionmaker(bind=engine)


# def get_db() -> Generator[Session, None, None]:
#     db: Session = session_maker()
#     try:
#         yield db
#     finally:
#         print('соединение с БД закрыто')
#         db.close()

async def connection(func):
    async def wrapper(*args, **kwargs):
        async with async_session_maker() as session:
            result = await func(session, *args, **kwargs)
        return result
    return wrapper
async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_maker() as session:
        yield session


