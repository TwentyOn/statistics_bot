from sqlalchemy import Column, Integer, Boolean, DateTime, ForeignKey, BIGINT, TEXT, ARRAY, String
from database.db import Base, async_session_maker

from sqlalchemy import select

import asyncio


class DomainCounter(Base):
    __tablename__ = 'ym_domain_counter'
    __table_args__ = {
        'schema': 'bot_tg_url_stats',
        'comment': 'Таблица для определения счётчика Яндекс Метрики по домену'}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    domain_name = Column(TEXT(50), nullable=False)
    counter = Column(BIGINT, nullable=False)


class User(Base):
    __tablename__ = 'user'
    __table_args__ = {
        'schema': 'bot_tg_url_stats',
        'comment': 'Таблица пользователей, которым разрешен доступ к боту'
    }

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    telegram_id = Column(BIGINT, nullable=True)
    username = Column(TEXT, nullable=False)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime)


class RequestsLog(Base):
    __tablename__ = 'requests_log'
    __table_args__ = {
        'schema': 'bot_tg_url_stats',
        'comment': 'Таблица для хранения истории запросов пользователей'
    }

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('user.id', ondelete='RESTRICT'), nullable=False)
    request = Column(TEXT(5000), nullable=False)
    error_msg = Column(TEXT, nullable=True)
    s3_file_path = Column(String(100), nullable=True)
    created_at = Column(DateTime, nullable=False)
