import asyncio
import json
import logging
import random
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import uuid4

import httpx
from fastapi import FastAPI, Header, HTTPException
from sqlalchemy import or_, select, update

from app.config import get_settings
from app.db import create_sessions, database_now
from app.models import FakeSettings, FakeTask
from app.schemas import FakeBehavior, ProviderEvent, ProviderSubmission
from app.security import sign_body

logger = logging.getLogger(__name__)
settings = get_settings()
sessions = create_sessions(settings)


async def claim_webhook() -> FakeTask | None:
    async with sessions.begin() as session:
        now = await database_now(session)
        task = await session.scalar(
            select(FakeTask)
            .where(
                FakeTask.provider == settings.provider_name,
                FakeTask.delivered.is_(False),
                FakeTask.deliver_at <= now,
                or_(FakeTask.lease_until.is_(None), FakeTask.lease_until <= now),
            )
            .order_by(FakeTask.deliver_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if task is not None:
            task.lease_until = now + timedelta(seconds=30)
            task.lease_token = uuid4()
        return task


async def deliver(client: httpx.AsyncClient, task: FakeTask) -> None:
    body = json.dumps(task.webhook_payload, separators=(",", ":")).encode()
    headers = {
        "X-Signature": sign_body(body, settings.webhook_secret),
        "Content-Type": "application/json",
    }

    async def send() -> bool:
        try:
            response = await client.post(
                task.callback_url, content=body, headers=headers, timeout=5
            )
            return response.is_success
        except httpx.HTTPError:
            return False

    if task.duplicate_webhook or task.concurrent_duplicates:
        if task.concurrent_duplicates:
            results = list(await asyncio.gather(send(), send()))
        else:
            results = [await send(), await send()]
        success = all(results)
    else:
        success = await send()
    async with sessions.begin() as session:
        now = await database_now(session)
        await session.execute(
            update(FakeTask)
            .where(FakeTask.id == task.id, FakeTask.lease_token == task.lease_token)
            .values(
                delivered=success,
                lease_token=None,
                lease_until=None,
                deliver_at=now + timedelta(seconds=2),
            )
        )


async def webhook_loop(client: httpx.AsyncClient) -> None:
    while True:
        try:
            if task := await claim_webhook():
                await deliver(client, task)
                continue
        except Exception:
            logger.exception("fake_webhook_loop_failed")
        await asyncio.sleep(settings.poll_interval)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logging.basicConfig(level=logging.INFO)
    async with httpx.AsyncClient() as client:
        worker = asyncio.create_task(webhook_loop(client))
        try:
            yield
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
    await sessions.kw["bind"].dispose()


app = FastAPI(title=f"Fake provider {settings.provider_name}", lifespan=lifespan)


@app.get("/settings")
async def get_behavior() -> FakeBehavior:
    async with sessions() as session:
        row = await session.get(FakeSettings, settings.provider_name)
        if row is None:
            raise HTTPException(503, "Migrations have not been applied")
        return FakeBehavior.model_validate(row.settings)


@app.put("/settings")
async def set_behavior(behavior: FakeBehavior) -> FakeBehavior:
    async with sessions.begin() as session:
        await session.execute(
            update(FakeSettings)
            .where(FakeSettings.provider == settings.provider_name)
            .values(settings=behavior.model_dump())
        )
    return behavior


@app.post("/tasks", status_code=202)
async def submit(
    payload: ProviderSubmission,
    idempotency_key: str = Header(alias="Idempotency-Key"),
) -> dict[str, str]:
    if idempotency_key != str(payload.attempt_id):
        raise HTTPException(400, "Idempotency-Key must equal attempt_id")
    async with sessions.begin() as session:
        row = await session.scalar(
            select(FakeSettings)
            .where(FakeSettings.provider == settings.provider_name)
            .with_for_update()
        )
        if row is None:
            raise HTTPException(503, "Migrations have not been applied")
        existing = await session.scalar(
            select(FakeTask).where(
                FakeTask.provider == settings.provider_name,
                FakeTask.attempt_id == payload.attempt_id,
            )
        )
        request_payload = payload.model_dump(mode="json")
        if existing is not None:
            if existing.request_payload != request_payload:
                raise HTTPException(409, "Idempotency key reused with another payload")
            return {"external_id": str(existing.id)}
        behavior = FakeBehavior.model_validate(row.settings)
        if behavior.sync_mode == "503":
            raise HTTPException(503, "Simulated provider outage")
        external_id = uuid4()
        failed = random.random() < behavior.failure_rate
        event = ProviderEvent(
            attempt_id=payload.attempt_id,
            external_id=str(external_id),
            status="failed" if failed else "completed",
            result_url=None
            if failed
            else f"https://files.example/{settings.provider_name}/{external_id}",
            error_type=behavior.error_type if failed else None,
        )
        now = await database_now(session)
        delay = random.uniform(behavior.delay_min, behavior.delay_max)
        session.add(
            FakeTask(
                id=external_id,
                provider=settings.provider_name,
                attempt_id=payload.attempt_id,
                callback_url=payload.callback_url,
                request_payload=request_payload,
                webhook_payload=event.model_dump(mode="json"),
                duplicate_webhook=behavior.duplicate_webhook,
                concurrent_duplicates=behavior.concurrent_duplicates,
                deliver_at=None if behavior.no_webhook else now + timedelta(seconds=delay),
            )
        )
    if behavior.sync_mode == "timeout":
        await asyncio.sleep(settings.http_timeout + 1)
    return {"external_id": str(external_id)}
