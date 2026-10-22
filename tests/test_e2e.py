import asyncio
import json
import os
import time
from datetime import timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import func, select, update

from app.config import Settings, get_settings
from app.db import create_sessions, database_now
from app.models import Attempt, FakeTask, LedgerEntry, User
from app.schemas import FakeBehavior
from app.security import sign_body

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(not os.getenv("TEST_SERVICE_URL"), reason="Set TEST_SERVICE_URL for E2E"),
]


async def signed_post(client, path, payload):
    body = json.dumps(payload, ensure_ascii=False).encode()
    secret = get_settings().webhook_secret
    return await client.post(
        path,
        content=body,
        headers={
            "X-Signature": sign_body(body, secret),
            "Content-Type": "application/json",
        },
    )


async def wait_task(client, task_id, predicate, wait_seconds=12):
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline:
        response = await client.get(f"/tasks/{task_id}")
        assert response.status_code == 200, response.text
        task = response.json()
        if predicate(task):
            return task
        await asyncio.sleep(0.05)
    raise AssertionError(f"Task {task_id} did not reach expected state: {task}")


@pytest.fixture
async def live():
    settings = Settings()
    sessions = create_sessions(settings)
    user_id = uuid4().int % 10**12 + 1000
    async with sessions.begin() as session:
        session.add(User(id=user_id))
    provider_urls = {
        "alpha": os.getenv("TEST_ALPHA_URL", "http://localhost:8001"),
        "beta": os.getenv("TEST_BETA_URL", "http://localhost:8002"),
    }
    original = {}
    async with httpx.AsyncClient(base_url=os.environ["TEST_SERVICE_URL"], timeout=20) as client:
        try:
            for name, url in provider_urls.items():
                for retry in range(100):
                    try:
                        response = await client.get(f"{url}/settings")
                        response.raise_for_status()
                        break
                    except httpx.HTTPError:
                        if retry == 99:
                            raise
                        await asyncio.sleep(0.1)
                original[name] = response.json()
                response = await client.put(
                    f"{url}/settings",
                    json=FakeBehavior(
                        no_webhook=True,
                        delay_min=0.1,
                        delay_max=0.1,
                    ).model_dump(),
                )
                assert response.status_code == 200
            response = await signed_post(
                client,
                "/webhooks/payments",
                {
                    "payment_id": str(uuid4()),
                    "user_id": user_id,
                    "tokens": 100,
                    "status": "succeeded",
                },
            )
            assert response.status_code == 200, response.text
            yield client, sessions, user_id, provider_urls
        finally:
            for name, behavior in original.items():
                await client.put(f"{provider_urls[name]}/settings", json=behavior)
            await sessions.kw["bind"].dispose()


async def new_task(live, **overrides):
    client, sessions, user_id, urls = live
    response = await client.post(
        "/generate",
        json={
            "user_id": user_id,
            "model_slug": "image-fast",
            "prompt": "A quiet sea",
            **overrides,
        },
    )
    assert response.status_code == 202, response.text
    return await wait_task(
        client, response.json()["task_id"], lambda t: bool(t["attempts"][-1]["external_id"])
    )


async def operation_counts(sessions, task_id):
    async with sessions() as session:
        return dict(
            (
                await session.execute(
                    select(LedgerEntry.operation_type, func.count())
                    .where(LedgerEntry.task_id == UUID(task_id))
                    .group_by(LedgerEntry.operation_type)
                )
            ).all()
        )


async def test_real_twenty_payment_requests_credit_once(live):
    client, sessions, user_id, urls = live
    payment_id = str(uuid4())
    payload = {"payment_id": payment_id, "user_id": user_id, "tokens": 17, "status": "succeeded"}
    responses = await asyncio.gather(
        *[signed_post(client, "/webhooks/payments", payload) for _ in range(20)]
    )
    assert {r.status_code for r in responses} == {200}
    assert (await client.get(f"/users/{user_id}/balance")).json()["balance"] == 117
    async with sessions() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(LedgerEntry)
                .where(LedgerEntry.payment_id == payment_id)
            )
            == 1
        )


async def test_real_ten_failed_webhooks_refund_once(live):
    client, sessions, user_id, urls = live
    task = await new_task(live, ref_images=["ref"] * 5)
    attempt = task["attempts"][0]
    payload = {
        "attempt_id": attempt["id"],
        "external_id": attempt["external_id"],
        "status": "failed",
        "error_type": "technical",
    }
    responses = await asyncio.gather(
        *[signed_post(client, "/webhooks/providers/beta", payload) for _ in range(10)]
    )
    assert {r.status_code for r in responses} == {200}
    task = await wait_task(client, task["id"], lambda t: t["status"] == "failed")
    assert task["result_url"] is None
    assert await operation_counts(sessions, task["id"]) == {"debit": 1, "refund": 1}
    assert (await client.get(f"/users/{user_id}/balance")).json()["balance"] == 100


async def test_real_provider_fallback_and_simultaneous_duplicates(live):
    client, sessions, user_id, urls = live
    for name in ("alpha", "beta"):
        response = await client.put(
            f"{urls[name]}/settings",
            json=FakeBehavior(
                failure_rate=1 if name == "alpha" else 0,
                delay_min=0.1,
                delay_max=0.1,
                duplicate_webhook=True,
                concurrent_duplicates=True,
            ).model_dump(),
        )
        assert response.status_code == 200
    task = await new_task(live)
    task = await wait_task(client, task["id"], lambda t: t["status"] == "completed")
    assert [a["provider"] for a in task["attempts"]] == ["alpha", "beta"]
    assert [a["status"] for a in task["attempts"]] == ["failed", "completed"]
    assert await operation_counts(sessions, task["id"]) == {"debit": 1}
    assert (await client.get(f"/users/{user_id}/balance")).json()["balance"] == 98


async def test_real_late_provider_completion_after_watchdog(live):
    client, sessions, user_id, urls = live
    await client.put(
        f"{urls['beta']}/settings",
        json=FakeBehavior(
            delay_min=3,
            delay_max=3,
            duplicate_webhook=True,
            concurrent_duplicates=True,
        ).model_dump(),
    )
    task = await new_task(live, ref_images=["ref"] * 5)
    attempt_id = UUID(task["attempts"][0]["id"])
    async with sessions.begin() as session:
        now = await database_now(session)
        await session.execute(
            update(Attempt)
            .where(Attempt.id == attempt_id)
            .values(deadline=now - timedelta(seconds=1))
        )
    task = await wait_task(client, task["id"], lambda t: t["status"] == "failed")
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        async with sessions() as session:
            delivered = await session.scalar(
                select(FakeTask.delivered).where(FakeTask.attempt_id == attempt_id)
            )
        if delivered:
            break
        await asyncio.sleep(0.1)
    assert delivered, "Fake provider did not deliver the late callback"
    task = (await client.get(f"/tasks/{task['id']}")).json()
    assert task["status"] == "failed" and task["result_url"] is None
    assert task["attempts"][0]["status"] == "timed_out"
    assert await operation_counts(sessions, task["id"]) == {"debit": 1, "refund": 1}
    assert (await client.get(f"/users/{user_id}/balance")).json()["balance"] == 100


@pytest.mark.parametrize("mode", ["503", "timeout"])
async def test_real_sync_error_uses_second_provider(live, mode):
    client, sessions, user_id, urls = live
    await client.put(
        f"{urls['alpha']}/settings",
        json=FakeBehavior(
            sync_mode=mode,
            no_webhook=True,
        ).model_dump(),
    )
    await client.put(
        f"{urls['beta']}/settings",
        json=FakeBehavior(
            delay_min=0.1,
            delay_max=0.1,
        ).model_dump(),
    )
    task = await new_task(live)
    task = await wait_task(client, task["id"], lambda t: t["status"] == "completed")
    assert [a["status"] for a in task["attempts"]] == ["failed", "completed"]
    assert task["attempts"][0]["error_code"] == ("http_503" if mode == "503" else "http_timeout")
    assert await operation_counts(sessions, task["id"]) == {"debit": 1}


async def test_real_silent_first_provider_is_replaced_by_watchdog(live):
    client, sessions, user_id, urls = live
    await client.put(
        f"{urls['beta']}/settings",
        json=FakeBehavior(
            delay_min=0.1,
            delay_max=0.1,
        ).model_dump(),
    )
    task = await new_task(live)
    async with sessions.begin() as session:
        now = await database_now(session)
        await session.execute(
            update(Attempt)
            .where(Attempt.id == UUID(task["attempts"][0]["id"]))
            .values(deadline=now - timedelta(seconds=1))
        )
    task = await wait_task(client, task["id"], lambda t: t["status"] == "completed")
    assert [a["status"] for a in task["attempts"]] == ["timed_out", "completed"]
    assert await operation_counts(sessions, task["id"]) == {"debit": 1}


async def test_real_provider_deduplicates_concurrent_submissions(live):
    client, sessions, user_id, urls = live
    attempt_id = str(uuid4())
    payload = {
        "attempt_id": attempt_id,
        "model_slug": "image-fast",
        "prompt": "x",
        "ref_images": [],
        "callback_url": "http://service:8000/webhooks/providers/alpha",
    }
    responses = await asyncio.gather(
        *[
            client.post(
                f"{urls['alpha']}/tasks", json=payload, headers={"Idempotency-Key": attempt_id}
            )
            for _ in range(20)
        ]
    )
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["external_id"] for r in responses}) == 1
    response = await client.post(
        f"{urls['alpha']}/tasks",
        json={**payload, "prompt": "changed"},
        headers={"Idempotency-Key": attempt_id},
    )
    assert response.status_code == 409
    async with sessions() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(FakeTask)
                .where(FakeTask.attempt_id == UUID(attempt_id))
            )
            == 1
        )


async def test_real_worker_recovers_submission_without_saved_response(live):
    client, sessions, user_id, urls = live
    task = await new_task(live)
    task_id = UUID(task["id"])
    attempt_id = UUID(task["attempts"][0]["id"])
    external_id = task["attempts"][0]["external_id"]
    async with sessions.begin() as session:
        now = await database_now(session)
        await session.execute(
            update(Attempt)
            .where(Attempt.id == attempt_id)
            .values(
                status="dispatching",
                external_id=None,
                lease_until=now - timedelta(seconds=1),
                lease_token=uuid4(),
            )
        )
    task = await wait_task(
        client, str(task_id), lambda t: t["attempts"][0]["status"] == "awaiting_webhook"
    )
    assert len(task["attempts"]) == 1
    assert task["attempts"][0]["external_id"] == external_id
    async with sessions() as session:
        assert (
            await session.scalar(
                select(func.count()).select_from(FakeTask).where(FakeTask.attempt_id == attempt_id)
            )
            == 1
        )
    await signed_post(
        client,
        "/webhooks/providers/alpha",
        {
            "attempt_id": str(attempt_id),
            "external_id": external_id,
            "status": "completed",
            "result_url": "https://files.example/recovered.png",
        },
    )
    task = await wait_task(client, str(task_id), lambda t: t["status"] == "completed")
    assert await operation_counts(sessions, task["id"]) == {"debit": 1}
