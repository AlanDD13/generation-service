import asyncio
import json
from datetime import timedelta
from uuid import UUID

import httpx
import pytest
from sqlalchemy import func, select, update

from app.billing import apply_payment
from app.config import get_settings
from app.db import database_now
from app.models import Attempt, LedgerEntry, Task, User
from app.schemas import GenerateRequest, PaymentEvent, ProviderEvent
from app.security import sign_body
from app.workers import Workers


async def signed_post(client, path, payload):
    body = json.dumps(payload, ensure_ascii=False).encode()
    return await client.post(
        path,
        content=body,
        headers={
            "X-Signature": sign_body(body, get_settings().webhook_secret),
            "Content-Type": "application/json",
        },
    )


async def generate(client, **overrides):
    response = await client.post(
        "/generate",
        json={
            "user_id": 1,
            "model_slug": "image-fast",
            "prompt": "A quiet sea",
            **overrides,
        },
    )
    assert response.status_code == 202, response.text
    task_id = response.json()["task_id"]
    return (await client.get(f"/tasks/{task_id}")).json()


def event_for(attempt, status="failed", error_type="technical"):
    return {
        "attempt_id": attempt["id"],
        "external_id": attempt["external_id"],
        "status": status,
        **(
            {"error_type": error_type}
            if status == "failed"
            else {"result_url": "https://files.example/result.png"}
        ),
    }


async def counts(app, task_id):
    async with app.state.sessions() as session:
        rows = await session.execute(
            select(LedgerEntry.operation_type, func.count())
            .where(LedgerEntry.task_id == task_id)
            .group_by(LedgerEntry.operation_type)
        )
        return dict(rows.all())


async def make_due(app, attempt_id):
    async with app.state.sessions.begin() as session:
        now = await database_now(session)
        await session.execute(
            update(Attempt)
            .where(Attempt.id == UUID(attempt_id))
            .values(deadline=now - timedelta(seconds=1))
        )


async def test_twenty_concurrent_payment_webhooks_credit_once(api):
    client, app = api
    payload = {"payment_id": "same-payment", "user_id": 1, "tokens": 50, "status": "succeeded"}
    responses = await asyncio.gather(
        *[signed_post(client, "/webhooks/payments", payload) for _ in range(20)]
    )
    assert {r.status_code for r in responses} == {200}
    assert (await client.get("/users/1/balance")).json()["balance"] == 150
    async with app.state.sessions() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(LedgerEntry)
                .where(LedgerEntry.payment_id == "same-payment")
            )
            == 1
        )


async def test_ten_concurrent_failures_refund_once(api):
    client, app = api
    task = await generate(client, ref_images=["ref"] * 5)
    assert task["current_leg"] == "beta"
    responses = await asyncio.gather(
        *[
            signed_post(client, "/webhooks/providers/beta", event_for(task["attempts"][0]))
            for _ in range(10)
        ]
    )
    assert {r.status_code for r in responses} == {200}
    assert await counts(app, UUID(task["id"])) == {"debit": 1, "refund": 1}
    assert (await client.get("/users/1/balance")).json()["balance"] == 100
    assert (await client.get(f"/tasks/{task['id']}")).json()["status"] == "failed"


async def test_first_failure_then_second_success(api):
    client, app = api
    task = await generate(client)
    assert (
        await signed_post(client, "/webhooks/providers/alpha", event_for(task["attempts"][0]))
    ).status_code == 200
    await app.state.workers.dispatch_chain(UUID(task["id"]))
    task = (await client.get(f"/tasks/{task['id']}")).json()
    assert [a["provider"] for a in task["attempts"]] == ["alpha", "beta"]
    assert (
        await signed_post(
            client, "/webhooks/providers/beta", event_for(task["attempts"][1], "completed")
        )
    ).status_code == 200
    task = (await client.get(f"/tasks/{task['id']}")).json()
    assert task["status"] == "completed"
    assert task["result_url"]
    assert await counts(app, UUID(task["id"])) == {"debit": 1}


async def test_late_completion_after_refund_is_ignored(api):
    client, app = api
    task = await generate(client, ref_images=["ref"] * 5)
    await make_due(app, task["attempts"][0]["id"])
    await app.state.workers.watchdog_once()
    response = await signed_post(
        client, "/webhooks/providers/beta", event_for(task["attempts"][0], "completed")
    )
    assert response.status_code == 200
    task = (await client.get(f"/tasks/{task['id']}")).json()
    assert task["status"] == "failed" and task["result_url"] is None
    assert task["attempts"][0]["status"] == "timed_out"
    assert await counts(app, UUID(task["id"])) == {"debit": 1, "refund": 1}


async def test_late_completion_cannot_replace_second_attempt(api):
    client, app = api
    task = await generate(client)
    first = task["attempts"][0]
    await make_due(app, first["id"])
    await app.state.workers.watchdog_once()
    await signed_post(client, "/webhooks/providers/alpha", event_for(first, "completed"))
    task = (await client.get(f"/tasks/{task['id']}")).json()
    assert task["status"] == "pending" and task["current_leg"] == "beta"
    assert task["result_url"] is None
    assert await counts(app, UUID(task["id"])) == {"debit": 1}


@pytest.mark.parametrize("error_type,expected_attempts", [("moderation", 1), ("payload", 1)])
async def test_video_moderation_or_payload_stops_chain(api, error_type, expected_attempts):
    client, app = api
    task = await generate(client, model_slug="video-long")
    await signed_post(
        client, "/webhooks/providers/beta", event_for(task["attempts"][0], error_type=error_type)
    )
    task = (await client.get(f"/tasks/{task['id']}")).json()
    assert task["status"] == "failed" and len(task["attempts"]) == expected_attempts
    assert await counts(app, UUID(task["id"])) == {"debit": 1, "refund": 1}


async def test_image_moderation_uses_beta(api):
    client, app = api
    task = await generate(client)
    await signed_post(
        client, "/webhooks/providers/alpha", event_for(task["attempts"][0], error_type="moderation")
    )
    task = (await client.get(f"/tasks/{task['id']}")).json()
    assert task["status"] == "pending" and task["current_leg"] == "beta"


async def test_next_leg_is_filtered_by_reference_count(api):
    client, app = api
    task = await generate(client, model_slug="video-long", ref_images=["ref", "ref"])
    await signed_post(client, "/webhooks/providers/beta", event_for(task["attempts"][0]))
    task = (await client.get(f"/tasks/{task['id']}")).json()
    assert task["status"] == "failed" and len(task["attempts"]) == 1
    assert await counts(app, UUID(task["id"])) == {"debit": 1, "refund": 1}
    assert (await client.get("/users/1/balance")).json()["balance"] == 100


@pytest.mark.parametrize("ref_count", [11, 101])
async def test_no_matching_leg_does_not_debit(api, ref_count):
    client, app = api
    response = await client.post(
        "/generate",
        json={
            "user_id": 1,
            "model_slug": "image-fast",
            "prompt": "x",
            "ref_images": ["x"] * ref_count,
        },
    )
    assert response.status_code == 400
    balance = (await client.get("/users/1/balance")).json()
    assert balance["balance"] == 100 and len(balance["entries"]) == 1


async def test_concurrent_orders_cannot_exceed_three_active_tasks(api):
    client, app = api
    responses = await asyncio.gather(
        *[
            client.post(
                "/generate",
                json={
                    "user_id": 1,
                    "model_slug": "image-fast",
                    "prompt": "x",
                },
            )
            for _ in range(10)
        ]
    )
    assert sum(r.status_code == 202 for r in responses) == 3
    assert sum(r.status_code == 429 for r in responses) == 7
    assert (await client.get("/users/1/balance")).json()["balance"] == 94


async def test_concurrent_orders_cannot_overspend(api):
    client, app = api
    async with app.state.sessions.begin() as session:
        session.add(User(id=2))
        await session.flush()
        session.add(LedgerEntry(user_id=2, operation_type="payment", amount=2, payment_id="small"))
    responses = await asyncio.gather(
        *[
            client.post(
                "/generate",
                json={
                    "user_id": 2,
                    "model_slug": "image-fast",
                    "prompt": "x",
                },
            )
            for _ in range(10)
        ]
    )
    assert sum(r.status_code == 202 for r in responses) == 1
    assert sum(r.status_code == 402 for r in responses) == 9
    assert (await client.get("/users/2/balance")).json()["balance"] == 0


@pytest.mark.parametrize("mode", ["503", "timeout", "invalid_response"])
async def test_synchronous_errors_fall_back(api, mode):
    client, app = api

    async def provider(request):
        if request.url == httpx.URL(f"{app.state.workers.settings.alpha_url}/tasks"):
            if mode == "timeout":
                raise httpx.ReadTimeout("simulated", request=request)
            return httpx.Response(503 if mode == "503" else 202, json={})
        payload = json.loads(request.content)
        return httpx.Response(202, json={"external_id": f"ext-{payload['attempt_id']}"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as providers:
        app.state.workers.client = providers
        task = await generate(client)
    assert [a["provider"] for a in task["attempts"]] == ["alpha", "beta"]
    assert task["attempts"][0]["status"] == "failed"
    assert task["attempts"][1]["status"] == "awaiting_webhook"


async def test_multiple_watchdogs_and_webhooks_have_one_outcome(api):
    client, app = api
    task = await generate(client, ref_images=["ref"] * 5)
    first = task["attempts"][0]
    await make_due(app, first["id"])
    await asyncio.gather(
        *[app.state.workers.watchdog_once() for _ in range(4)],
        *[
            signed_post(client, "/webhooks/providers/beta", event_for(first, "completed"))
            for _ in range(10)
        ],
    )
    task = (await client.get(f"/tasks/{task['id']}")).json()
    operations = await counts(app, UUID(task["id"]))
    assert (task["status"], bool(task["result_url"]), operations.get("refund", 0)) in {
        ("completed", True, 0),
        ("failed", False, 1),
    }


async def test_completion_before_post_response_is_not_overwritten(api):
    client, app = api

    async def provider(request):
        payload = json.loads(request.content)
        await app.state.service.handle_webhook(
            "alpha",
            ProviderEvent(
                attempt_id=payload["attempt_id"],
                external_id="early",
                status="completed",
                result_url="https://files.example/early.png",
            ),
        )
        return httpx.Response(202, json={"external_id": "early"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as providers:
        app.state.workers.client = providers
        task = await generate(client)
    assert task["status"] == "completed"
    assert task["attempts"][0]["status"] == "completed"


async def test_expired_dispatch_lease_reuses_attempt(api):
    client, app = api
    task_id = await app.state.service.create(
        GenerateRequest(
            user_id=1,
            model_slug="image-fast",
            prompt="x",
        )
    )
    first = await app.state.workers.claim(task_id)
    assert first is not None
    async with app.state.sessions.begin() as session:
        attempt = await session.get(Attempt, first.attempt_id)
        deadline = attempt.deadline
        attempt.lease_until = await database_now(session) - timedelta(seconds=1)
    workers = Workers(app.state.service, app.state.workers.settings, app.state.workers.client)
    second = await workers.claim(task_id)
    assert second.attempt_id == first.attempt_id and second.lease_token != first.lease_token
    await app.state.service.accept(first.attempt_id, first.lease_token, "obsolete")
    await workers.send(second)
    task = (await client.get(f"/tasks/{task_id}")).json()
    assert len(task["attempts"]) == 1 and task["attempts"][0]["status"] == "awaiting_webhook"
    async with app.state.sessions() as session:
        assert (await session.get(Attempt, first.attempt_id)).deadline == deadline


async def test_payment_conflict_and_non_success_statuses(api):
    client, app = api
    payload = {"payment_id": "id", "user_id": 1, "tokens": 10, "status": "pending"}
    assert (await signed_post(client, "/webhooks/payments", payload)).status_code == 200
    payload["status"] = "failed"
    assert (await signed_post(client, "/webhooks/payments", payload)).status_code == 200
    assert (await client.get("/users/1/balance")).json()["balance"] == 100
    payload["status"] = "succeeded"
    assert (await signed_post(client, "/webhooks/payments", payload)).status_code == 200
    payload["tokens"] = 20
    assert (await signed_post(client, "/webhooks/payments", payload)).status_code == 409
    assert (await client.get("/users/1/balance")).json()["balance"] == 110


async def test_invalid_signature_and_body_have_no_effect(api):
    client, app = api
    payload = {"payment_id": "bad", "user_id": 1, "tokens": 10, "status": "succeeded"}
    assert (await client.post("/webhooks/payments", json=payload)).status_code == 401
    payload["tokens"] = -1
    assert (await signed_post(client, "/webhooks/payments", payload)).status_code == 422
    assert (await client.get("/users/1/balance")).json()["balance"] == 100


async def test_exception_before_commit_rolls_back_payment(api):
    client, app = api
    async with app.state.sessions() as session:
        with pytest.raises(RuntimeError):
            async with session.begin():
                session.add(
                    LedgerEntry(
                        user_id=1,
                        operation_type="payment",
                        amount=7,
                        payment_id="rolled-back",
                    )
                )
                await session.flush()
                raise RuntimeError("process stopped before commit")
    await apply_payment(
        app.state.sessions,
        PaymentEvent(
            payment_id="rolled-back",
            user_id=1,
            tokens=7,
            status="succeeded",
        ),
    )
    assert (await client.get("/users/1/balance")).json()["balance"] == 107


async def test_failed_transition_rolls_back_both_task_and_refund(api, monkeypatch):
    client, app = api
    task = await generate(client, ref_images=["ref"] * 5)
    attempt_id = UUID(task["attempts"][0]["id"])
    original = app.state.service._fail

    async def interrupted(*args, **kwargs):
        await original(*args, **kwargs)
        await args[0].flush()
        raise RuntimeError("interrupted before commit")

    monkeypatch.setattr(app.state.service, "_fail", interrupted)
    event = ProviderEvent.model_validate(event_for(task["attempts"][0]))
    with pytest.raises(RuntimeError):
        await app.state.service.handle_webhook("beta", event)
    async with app.state.sessions() as session:
        assert (await session.get(Task, UUID(task["id"]))).status == "pending"
        assert (await session.get(Attempt, attempt_id)).status == "awaiting_webhook"
    assert await counts(app, UUID(task["id"])) == {"debit": 1}
    monkeypatch.setattr(app.state.service, "_fail", original)
    await app.state.service.handle_webhook("beta", event)
    assert await counts(app, UUID(task["id"])) == {"debit": 1, "refund": 1}
