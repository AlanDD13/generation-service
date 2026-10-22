from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)


class Model(Base):
    __tablename__ = "models"
    __table_args__ = (CheckConstraint("price > 0", name="positive_model_price"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    slug: Mapped[str] = mapped_column(String(100), unique=True)
    price: Mapped[int]


class ModelLeg(Base):
    __tablename__ = "model_legs"
    __table_args__ = (
        UniqueConstraint("model_id", "priority"),
        CheckConstraint("max_ref_images >= 0", name="nonnegative_max_refs"),
        CheckConstraint("provider IN ('alpha', 'beta')", name="known_leg_provider"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    model_id: Mapped[int] = mapped_column(ForeignKey("models.id"))
    provider: Mapped[str] = mapped_column(String(20))
    priority: Mapped[int]
    max_ref_images: Mapped[int]
    skip_on_moderation: Mapped[bool] = mapped_column(default=False)


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        CheckConstraint("status IN ('pending', 'completed', 'failed')", name="task_status"),
        CheckConstraint("price > 0", name="positive_task_price"),
        CheckConstraint(
            "(status = 'completed') = (result_url IS NOT NULL)", name="completed_has_result"
        ),
        Index("ix_tasks_active_user", "user_id", postgresql_where=text("status = 'pending'")),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    model_id: Mapped[int] = mapped_column(ForeignKey("models.id"))
    price: Mapped[int]
    prompt: Mapped[str]
    ref_images: Mapped[list[str]] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    current_attempt_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("attempts.id", use_alter=True, name="fk_task_current_attempt")
    )
    result_url: Mapped[str | None]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Attempt(Base):
    __tablename__ = "attempts"
    __table_args__ = (
        UniqueConstraint("task_id", "leg_id"),
        UniqueConstraint("provider", "external_id"),
        CheckConstraint(
            "status IN ('queued', 'dispatching', 'awaiting_webhook', "
            "'completed', 'failed', 'timed_out')",
            name="attempt_status",
        ),
        Index(
            "ix_attempt_active_task",
            "task_id",
            unique=True,
            postgresql_where=text("status IN ('queued', 'dispatching', 'awaiting_webhook')"),
        ),
        Index("ix_attempt_dispatch", "status", "lease_until"),
        Index("ix_attempt_deadline", "deadline"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    task_id: Mapped[UUID] = mapped_column(ForeignKey("tasks.id"))
    leg_id: Mapped[int] = mapped_column(ForeignKey("model_legs.id"))
    provider: Mapped[str] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(30), default="queued")
    external_id: Mapped[str | None] = mapped_column(String(100))
    error_type: Mapped[str | None] = mapped_column(String(20))
    error_code: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_token: Mapped[UUID | None]


class LedgerEntry(Base):
    __tablename__ = "ledger_entries"
    __table_args__ = (
        UniqueConstraint("payment_id"),
        UniqueConstraint("task_id", "operation_type"),
        CheckConstraint(
            "(operation_type = 'payment' AND amount > 0 AND payment_id IS NOT NULL "
            "AND task_id IS NULL) OR "
            "(operation_type = 'debit' AND amount < 0 AND task_id IS NOT NULL "
            "AND payment_id IS NULL) OR "
            "(operation_type = 'refund' AND amount > 0 AND task_id IS NOT NULL "
            "AND payment_id IS NULL)",
            name="ledger_operation_shape",
        ),
        Index("ix_ledger_user", "user_id", "id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    operation_type: Mapped[str] = mapped_column(String(20))
    amount: Mapped[int] = mapped_column(BigInteger)
    payment_id: Mapped[str | None] = mapped_column(String(200))
    task_id: Mapped[UUID | None] = mapped_column(ForeignKey("tasks.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class FakeSettings(Base):
    __tablename__ = "fake_settings"

    provider: Mapped[str] = mapped_column(String(20), primary_key=True)
    settings: Mapped[dict[str, Any]] = mapped_column(JSONB)


class FakeTask(Base):
    __tablename__ = "fake_tasks"
    __table_args__ = (UniqueConstraint("provider", "attempt_id"),)

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    provider: Mapped[str] = mapped_column(String(20))
    attempt_id: Mapped[UUID]
    callback_url: Mapped[str]
    request_payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    webhook_payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    duplicate_webhook: Mapped[bool]
    concurrent_duplicates: Mapped[bool]
    deliver_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivered: Mapped[bool] = mapped_column(default=False)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_token: Mapped[UUID | None]
