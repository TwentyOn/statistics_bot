from collections.abc import AsyncGenerator
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME


class Base(DeclarativeBase):
    pass


DATABASE_URL = f'postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
async_engine = create_async_engine(DATABASE_URL)
async_session_maker = async_sessionmaker(bind=async_engine, expire_on_commit=False, class_=AsyncSession)
