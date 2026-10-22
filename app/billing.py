from fastapi import HTTPException
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import LedgerEntry, User
from app.schemas import BalanceView, LedgerView, PaymentEvent


async def lock_user(session: AsyncSession, user_id: int) -> User:
    user = await session.scalar(select(User).where(User.id == user_id).with_for_update())
    if user is None:
        raise HTTPException(404, "User not found")
    return user


async def balance_of(session: AsyncSession, user_id: int) -> int:
    amount = await session.scalar(
        select(func.coalesce(func.sum(LedgerEntry.amount), 0)).where(LedgerEntry.user_id == user_id)
    )
    return int(amount or 0)


async def apply_payment(sessions: async_sessionmaker[AsyncSession], event: PaymentEvent) -> None:
    async with sessions.begin() as session:
        await lock_user(session, event.user_id)
        if event.status != "succeeded":
            return
        await session.execute(
            insert(LedgerEntry)
            .values(
                user_id=event.user_id,
                operation_type="payment",
                amount=event.tokens,
                payment_id=event.payment_id,
            )
            .on_conflict_do_nothing(index_elements=[LedgerEntry.payment_id])
        )
        entry = await session.scalar(
            select(LedgerEntry).where(LedgerEntry.payment_id == event.payment_id)
        )
        if entry is None or (entry.user_id, entry.amount) != (event.user_id, event.tokens):
            raise HTTPException(409, "Payment ID already has different details")


async def get_balance(sessions: async_sessionmaker[AsyncSession], user_id: int) -> BalanceView:
    async with sessions.begin() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        if await session.get(User, user_id) is None:
            raise HTTPException(404, "User not found")
        balance = await balance_of(session, user_id)
        entries = await session.scalars(
            select(LedgerEntry)
            .where(LedgerEntry.user_id == user_id)
            .order_by(LedgerEntry.id.desc())
            .limit(20)
        )
        return BalanceView(
            user_id=user_id,
            balance=balance,
            entries=[LedgerView.model_validate(entry) for entry in entries],
        )
