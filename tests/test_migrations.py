import asyncio
import os
import subprocess

from sqlalchemy import text

from app.config import Settings
from app.db import create_sessions


async def test_migrations_round_trip_and_match_models(migrated_database):
    sessions = create_sessions(Settings(database_url=migrated_database))
    try:
        async with sessions.begin() as session:
            await session.execute(
                text(
                    "TRUNCATE ledger_entries, attempts, tasks, users, fake_tasks "
                    "RESTART IDENTITY CASCADE"
                )
            )
        environment = {**os.environ, "DATABASE_URL": migrated_database}
        for command in (["downgrade", "base"], ["upgrade", "head"], ["check"]):
            process = await asyncio.create_subprocess_exec(
                "uv",
                "run",
                "--no-sync",
                "alembic",
                *command,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            assert process.returncode == 0, (stdout + stderr).decode()
        async with sessions() as session:
            assert await session.scalar(text("SELECT count(*) FROM models")) == 2
            assert await session.scalar(text("SELECT count(*) FROM model_legs")) == 4
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM pg_constraint "
                        "WHERE conname = 'fk_task_current_attempt'"
                    )
                )
                == 1
            )
    finally:
        await sessions.kw["bind"].dispose()
