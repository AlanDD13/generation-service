import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import ValidationError
from sqlalchemy import text

from app.billing import apply_payment, get_balance
from app.config import Settings, get_settings
from app.db import create_sessions
from app.generation import GenerationService
from app.schemas import (
    BalanceView,
    GenerateRequest,
    GenerateResponse,
    PaymentEvent,
    ProviderEvent,
    TaskView,
)
from app.security import verify_signature
from app.workers import Workers


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    sessions = create_sessions(settings)
    service = GenerationService(sessions)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(process)d %(name)s %(message)s"
        )
        async with httpx.AsyncClient() as client:
            workers = Workers(service, settings, client)
            app.state.workers = workers
            tasks = []
            if settings.workers_enabled:
                tasks = [
                    asyncio.create_task(workers.run_dispatcher()),
                    asyncio.create_task(workers.run_watchdog()),
                ]
            try:
                yield
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        await sessions.kw["bind"].dispose()

    app = FastAPI(title="Generation service", lifespan=lifespan)
    app.state.sessions = sessions
    app.state.service = service

    @app.get("/health")
    async def health() -> dict[str, str]:
        async with sessions() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ok"}

    @app.post("/generate", status_code=202, response_model=GenerateResponse)
    async def generate(data: GenerateRequest, request: Request) -> GenerateResponse:
        task_id = await service.create(data)
        await request.app.state.workers.dispatch_chain(task_id)
        return GenerateResponse(task_id=task_id)

    @app.post("/webhooks/payments")
    async def payment_webhook(request: Request) -> dict[str, str]:
        body = await request.body()
        verify_signature(body, request.headers.get("X-Signature"), settings.webhook_secret)
        try:
            event = PaymentEvent.model_validate_json(body)
        except ValidationError as exc:
            raise HTTPException(422, "Invalid payment event") from exc
        await apply_payment(sessions, event)
        return {"status": "ok"}

    @app.post("/webhooks/providers/{name}")
    async def provider_webhook(name: str, request: Request) -> dict[str, str]:
        if name not in {"alpha", "beta"}:
            raise HTTPException(404, "Provider not found")
        body = await request.body()
        verify_signature(body, request.headers.get("X-Signature"), settings.webhook_secret)
        try:
            event = ProviderEvent.model_validate_json(body)
        except ValidationError as exc:
            raise HTTPException(422, "Invalid provider event") from exc
        await service.handle_webhook(name, event)
        return {"status": "ok"}

    @app.get("/tasks/{task_id}", response_model=TaskView)
    async def get_task(task_id: UUID) -> TaskView:
        return await service.get(task_id)

    @app.get("/users/{user_id}/balance", response_model=BalanceView)
    async def user_balance(user_id: int) -> BalanceView:
        return await get_balance(sessions, user_id)

    return app


app = create_app()
