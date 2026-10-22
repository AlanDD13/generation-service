import logging
from datetime import datetime
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.billing import balance_of, lock_user
from app.db import database_now
from app.models import Attempt, LedgerEntry, Model, ModelLeg, Task
from app.schemas import AttemptView, GenerateRequest, ProviderEvent, TaskView

logger = logging.getLogger(__name__)
ACTIVE_ATTEMPTS = ("queued", "dispatching", "awaiting_webhook")


class GenerationService:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]):
        self.sessions = sessions

    async def create(self, data: GenerateRequest) -> UUID:
        async with self.sessions.begin() as session:
            model = await session.scalar(select(Model).where(Model.slug == data.model_slug))
            if model is None:
                raise HTTPException(404, "Model not found")
            leg = await session.scalar(
                select(ModelLeg)
                .where(
                    ModelLeg.model_id == model.id, ModelLeg.max_ref_images >= len(data.ref_images)
                )
                .order_by(ModelLeg.priority)
                .limit(1)
            )
            if leg is None:
                raise HTTPException(400, "No leg accepts this number of reference images")
            await lock_user(session, data.user_id)
            active = await session.scalar(
                select(func.count())
                .select_from(Task)
                .where(Task.user_id == data.user_id, Task.status == "pending")
            )
            if active is not None and active >= 3:
                raise HTTPException(429, "Three tasks are already active")
            if await balance_of(session, data.user_id) < model.price:
                raise HTTPException(402, "Insufficient tokens")
            task = Task(
                user_id=data.user_id,
                model_id=model.id,
                price=model.price,
                prompt=data.prompt,
                ref_images=data.ref_images,
            )
            session.add(task)
            await session.flush()
            session.add(
                LedgerEntry(
                    user_id=task.user_id,
                    operation_type="debit",
                    amount=-task.price,
                    task_id=task.id,
                )
            )
            await self._enqueue(session, task, leg)
            return task.id

    async def _enqueue(self, session: AsyncSession, task: Task, leg: ModelLeg) -> None:
        attempt = Attempt(task_id=task.id, leg_id=leg.id, provider=leg.provider)
        session.add(attempt)
        await session.flush()
        task.current_attempt_id = attempt.id

    async def _lock_attempt(self, session: AsyncSession, attempt_id: UUID) -> tuple[Task, Attempt]:
        owner = (
            await session.execute(
                select(Task.user_id, Task.id)
                .join(Attempt, Attempt.task_id == Task.id)
                .where(Attempt.id == attempt_id)
            )
        ).one_or_none()
        if owner is None:
            raise HTTPException(404, "Attempt not found")
        await lock_user(session, owner.user_id)
        task = await session.scalar(select(Task).where(Task.id == owner.id).with_for_update())
        attempt = await session.scalar(
            select(Attempt).where(Attempt.id == attempt_id).with_for_update()
        )
        if task is None or attempt is None:
            raise HTTPException(404, "Attempt not found")
        return task, attempt

    @staticmethod
    def _is_current(task: Task, attempt: Attempt) -> bool:
        return (
            task.status == "pending"
            and task.current_attempt_id == attempt.id
            and attempt.status in ACTIVE_ATTEMPTS
        )

    async def _fail(
        self,
        session: AsyncSession,
        task: Task,
        attempt: Attempt,
        error_type: str,
        error_code: str,
        now: datetime,
        timed_out: bool = False,
    ) -> None:
        attempt.status = "timed_out" if timed_out else "failed"
        attempt.error_type = error_type
        attempt.error_code = error_code
        attempt.finished_at = now
        attempt.lease_until = None
        attempt.lease_token = None
        await session.flush()
        leg = await session.get(ModelLeg, attempt.leg_id)
        if leg is None:
            raise RuntimeError("Attempt references a missing leg")
        next_leg = None
        if error_type != "payload":
            query = select(ModelLeg).where(
                ModelLeg.model_id == task.model_id,
                ModelLeg.priority > leg.priority,
                ModelLeg.max_ref_images >= len(task.ref_images),
            )
            if error_type == "moderation":
                query = query.where(ModelLeg.skip_on_moderation.is_(False))
            next_leg = await session.scalar(query.order_by(ModelLeg.priority).limit(1))
        if next_leg is not None:
            await self._enqueue(session, task, next_leg)
        else:
            task.status = "failed"
            task.finished_at = now
            session.add(
                LedgerEntry(
                    user_id=task.user_id,
                    operation_type="refund",
                    amount=task.price,
                    task_id=task.id,
                )
            )
        logger.info("attempt_failed task=%s attempt=%s reason=%s", task.id, attempt.id, error_code)

    async def handle_webhook(self, provider: str, event: ProviderEvent) -> None:
        async with self.sessions.begin() as session:
            task, attempt = await self._lock_attempt(session, event.attempt_id)
            if attempt.provider != provider:
                raise HTTPException(409, "Attempt belongs to another provider")
            if attempt.external_id is not None and attempt.external_id != event.external_id:
                raise HTTPException(409, "External ID does not match")
            if not self._is_current(task, attempt):
                logger.info(
                    "webhook_ignored task=%s attempt=%s status=%s",
                    task.id,
                    attempt.id,
                    attempt.status,
                )
                return
            attempt.external_id = event.external_id
            now = await database_now(session)
            if event.status == "completed":
                attempt.status = "completed"
                attempt.finished_at = now
                attempt.lease_until = None
                attempt.lease_token = None
                task.status = "completed"
                task.result_url = event.result_url
                task.finished_at = now
                logger.info("task_completed task=%s attempt=%s", task.id, attempt.id)
            else:
                if event.error_type is None:
                    raise ValueError("Failed event requires error_type")
                await self._fail(session, task, attempt, event.error_type, "provider_failed", now)

    async def accept(self, attempt_id: UUID, lease_token: UUID, external_id: str) -> None:
        async with self.sessions.begin() as session:
            task, attempt = await self._lock_attempt(session, attempt_id)
            if not self._is_current(task, attempt) or attempt.lease_token != lease_token:
                return
            if attempt.external_id is not None and attempt.external_id != external_id:
                raise HTTPException(409, "Provider changed external ID")
            attempt.external_id = external_id
            attempt.status = "awaiting_webhook"
            attempt.lease_until = None
            attempt.lease_token = None

    async def dispatch_failed(self, attempt_id: UUID, lease_token: UUID, reason: str) -> None:
        async with self.sessions.begin() as session:
            task, attempt = await self._lock_attempt(session, attempt_id)
            if not self._is_current(task, attempt) or attempt.lease_token != lease_token:
                return
            now = await database_now(session)
            await self._fail(session, task, attempt, "technical", reason, now)

    async def expire(self, attempt_id: UUID) -> None:
        async with self.sessions.begin() as session:
            task, attempt = await self._lock_attempt(session, attempt_id)
            now = await database_now(session)
            if (
                not self._is_current(task, attempt)
                or attempt.deadline is None
                or attempt.deadline > now
            ):
                return
            await self._fail(session, task, attempt, "technical", "provider_timeout", now, True)

    async def get(self, task_id: UUID) -> TaskView:
        async with self.sessions.begin() as session:
            await session.execute(
                text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            )
            task = await session.get(Task, task_id)
            if task is None:
                raise HTTPException(404, "Task not found")
            attempts = list(
                await session.scalars(
                    select(Attempt)
                    .where(Attempt.task_id == task_id)
                    .order_by(Attempt.created_at, Attempt.id)
                )
            )
            current = next((a for a in attempts if a.id == task.current_attempt_id), None)
            return TaskView(
                id=task.id,
                status=task.status,
                current_leg=current.provider if current else None,
                current_attempt_id=task.current_attempt_id,
                result_url=task.result_url,
                created_at=task.created_at,
                finished_at=task.finished_at,
                attempts=[AttemptView.model_validate(a) for a in attempts],
            )
