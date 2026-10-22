import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID, uuid4

import httpx
from sqlalchemy import func, or_, select

from app.config import Settings
from app.db import database_now
from app.generation import ACTIVE_ATTEMPTS, GenerationService
from app.models import Attempt, Model, Task
from app.schemas import ProviderSubmission

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Dispatch:
    attempt_id: UUID
    lease_token: UUID
    provider: str
    payload: ProviderSubmission


class Workers:
    def __init__(self, service: GenerationService, settings: Settings, client: httpx.AsyncClient):
        self.service = service
        self.sessions = service.sessions
        self.settings = settings
        self.client = client

    async def claim(self, task_id: UUID | None = None) -> Dispatch | None:
        async with self.sessions.begin() as session:
            now = await database_now(session)
            query = (
                select(Attempt)
                .join(Task, Task.id == Attempt.task_id)
                .where(
                    Task.status == "pending",
                    Task.current_attempt_id == Attempt.id,
                    or_(
                        Attempt.status == "queued",
                        (Attempt.status == "dispatching") & (Attempt.lease_until <= now),
                    ),
                    or_(Attempt.deadline.is_(None), Attempt.deadline > now),
                )
                .order_by(Attempt.created_at)
                .limit(1)
                .with_for_update(of=Attempt, skip_locked=True)
            )
            if task_id is not None:
                query = query.where(Task.id == task_id)
            attempt = await session.scalar(query)
            if attempt is None:
                return None
            task = await session.get(Task, attempt.task_id)
            if task is None:
                raise RuntimeError("Attempt references a missing task")
            model = await session.get(Model, task.model_id)
            if model is None:
                raise RuntimeError("Task references a missing model")
            token = uuid4()
            attempt.status = "dispatching"
            attempt.lease_token = token
            attempt.lease_until = now + timedelta(seconds=self.settings.dispatch_lease)
            if attempt.started_at is None:
                attempt.started_at = now
                attempt.deadline = now + timedelta(seconds=self.settings.provider_timeout)
            return Dispatch(
                attempt.id,
                token,
                attempt.provider,
                ProviderSubmission(
                    attempt_id=attempt.id,
                    model_slug=model.slug,
                    prompt=task.prompt,
                    ref_images=task.ref_images,
                    callback_url=(
                        f"{self.settings.callback_base_url}/webhooks/providers/{attempt.provider}"
                    ),
                ),
            )

    async def send(self, dispatch: Dispatch) -> None:
        base_url = (
            self.settings.alpha_url if dispatch.provider == "alpha" else self.settings.beta_url
        )
        try:
            response = await self.client.post(
                f"{base_url}/tasks",
                json=dispatch.payload.model_dump(mode="json"),
                headers={"Idempotency-Key": str(dispatch.attempt_id)},
                timeout=self.settings.http_timeout,
            )
            if response.status_code != 202:
                await self.service.dispatch_failed(
                    dispatch.attempt_id, dispatch.lease_token, f"http_{response.status_code}"
                )
                return
            body = response.json()
            external_id = body.get("external_id") if isinstance(body, dict) else None
            if not isinstance(external_id, str) or not 0 < len(external_id) <= 100:
                raise ValueError("Invalid provider response")
        except (httpx.HTTPError, ValueError) as exc:
            reason = "http_timeout" if isinstance(exc, httpx.TimeoutException) else "dispatch_error"
            await self.service.dispatch_failed(dispatch.attempt_id, dispatch.lease_token, reason)
            return
        await self.service.accept(dispatch.attempt_id, dispatch.lease_token, external_id)

    async def dispatch_chain(self, task_id: UUID) -> None:
        while dispatch := await self.claim(task_id):
            await self.send(dispatch)

    async def watchdog_once(self) -> None:
        async with self.sessions() as session:
            ids = list(
                await session.scalars(
                    select(Attempt.id)
                    .where(Attempt.status.in_(ACTIVE_ATTEMPTS), Attempt.deadline <= func.now())
                    .order_by(Attempt.deadline)
                    .limit(100)
                )
            )
        for attempt_id in ids:
            await self.service.expire(attempt_id)

    async def run_dispatcher(self) -> None:
        while True:
            try:
                if dispatch := await self.claim():
                    await self.send(dispatch)
                    continue
            except Exception:
                logger.exception("dispatch_loop_failed")
            await asyncio.sleep(self.settings.poll_interval)

    async def run_watchdog(self) -> None:
        while True:
            try:
                await self.watchdog_once()
            except Exception:
                logger.exception("watchdog_loop_failed")
            await asyncio.sleep(self.settings.poll_interval)
