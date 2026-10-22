import asyncio
import json
import os
import re
import subprocess
from uuid import uuid4

import asyncpg
import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from app.config import Settings
from app.main import create_app
from app.models import LedgerEntry, User

TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://generation:generation@localhost:55432/generation_test",
)


@pytest.fixture(scope="session")
def migrated_database():
    url = make_url(TEST_DATABASE_URL)
    database = url.database or ""
    if not re.fullmatch(r"[a-z_]+_test", database):
        raise RuntimeError("Tests require a separate database with a name ending in _test")

    async def prepare():
        connection = await asyncpg.connect(
            url.set(drivername="postgresql", database="postgres").render_as_string(
                hide_password=False
            )
        )
        try:
            if not await connection.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1", database
            ):
                await connection.execute(f'CREATE DATABASE "{database}"')
        finally:
            await connection.close()

    asyncio.run(prepare())
    subprocess.run(
        ["uv", "run", "--no-sync", "alembic", "upgrade", "head"],
        env={**os.environ, "DATABASE_URL": TEST_DATABASE_URL},
        check=True,
    )
    return TEST_DATABASE_URL


@pytest.fixture
async def api(migrated_database):
    settings = Settings(database_url=migrated_database, workers_enabled=False)
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with app.state.sessions.begin() as session:
            await session.execute(
                text(
                    "TRUNCATE ledger_entries, attempts, tasks, users, fake_tasks "
                    "RESTART IDENTITY CASCADE"
                )
            )
            session.add(User(id=1))
            await session.flush()
            session.add(
                LedgerEntry(
                    user_id=1,
                    operation_type="payment",
                    amount=100,
                    payment_id=str(uuid4()),
                )
            )

        async def provider(request):
            payload = json.loads(request.content)
            return httpx.Response(202, json={"external_id": f"ext-{payload['attempt_id']}"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as providers:
            app.state.workers.client = providers
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                yield client, app
