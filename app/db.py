from datetime import datetime
from typing import cast

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings


def create_sessions(settings: Settings) -> async_sessionmaker[AsyncSession]:
    engine = create_async_engine(settings.database_url, pool_size=5, max_overflow=5)
    return async_sessionmaker(engine, expire_on_commit=False)


async def database_now(session: AsyncSession) -> datetime:
    return cast(datetime, await session.scalar(select(func.clock_timestamp())))
