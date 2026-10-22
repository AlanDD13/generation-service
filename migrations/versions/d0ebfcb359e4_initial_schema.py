from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "d0ebfcb359e4"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "fake_settings",
        sa.Column("provider", sa.String(length=20), nullable=False),
        sa.Column("settings", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("provider"),
    )
    op.create_table(
        "fake_tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(length=20), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("callback_url", sa.String(), nullable=False),
        sa.Column("request_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("webhook_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("duplicate_webhook", sa.Boolean(), nullable=False),
        sa.Column("concurrent_duplicates", sa.Boolean(), nullable=False),
        sa.Column("deliver_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivered", sa.Boolean(), nullable=False),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_token", sa.Uuid(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("provider", "attempt_id"),
    )
    op.create_table(
        "models",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("slug", sa.String(length=100), nullable=False),
        sa.Column("price", sa.Integer(), nullable=False),
        sa.CheckConstraint("price > 0", name="positive_model_price"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )
    op.create_table(
        "users",
        sa.Column("id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "model_legs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("model_id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=20), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("max_ref_images", sa.Integer(), nullable=False),
        sa.Column("skip_on_moderation", sa.Boolean(), nullable=False),
        sa.CheckConstraint("provider IN ('alpha', 'beta')", name="known_leg_provider"),
        sa.CheckConstraint("max_ref_images >= 0", name="nonnegative_max_refs"),
        sa.ForeignKeyConstraint(
            ["model_id"],
            ["models.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("model_id", "priority"),
    )
    op.create_table(
        "tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("model_id", sa.Integer(), nullable=False),
        sa.Column("price", sa.Integer(), nullable=False),
        sa.Column("prompt", sa.String(), nullable=False),
        sa.Column("ref_images", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("current_attempt_id", sa.Uuid(), nullable=True),
        sa.Column("result_url", sa.String(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "(status = 'completed') = (result_url IS NOT NULL)", name="completed_has_result"
        ),
        sa.CheckConstraint("status IN ('pending', 'completed', 'failed')", name="task_status"),
        sa.CheckConstraint("price > 0", name="positive_task_price"),
        sa.ForeignKeyConstraint(
            ["model_id"],
            ["models.id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_tasks_active_user",
        "tasks",
        ["user_id"],
        unique=False,
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_table(
        "attempts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("leg_id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("external_id", sa.String(length=100), nullable=True),
        sa.Column("error_type", sa.String(length=20), nullable=True),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deadline", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_token", sa.Uuid(), nullable=True),
        sa.CheckConstraint(
            "status IN ('queued', 'dispatching', 'awaiting_webhook', "
            "'completed', 'failed', 'timed_out')",
            name="attempt_status",
        ),
        sa.ForeignKeyConstraint(
            ["leg_id"],
            ["model_legs.id"],
        ),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["tasks.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("provider", "external_id"),
        sa.UniqueConstraint("task_id", "leg_id"),
    )
    op.create_index(
        "ix_attempt_active_task",
        "attempts",
        ["task_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('queued', 'dispatching', 'awaiting_webhook')"),
    )
    op.create_foreign_key(
        "fk_task_current_attempt", "tasks", "attempts", ["current_attempt_id"], ["id"]
    )
    op.create_index("ix_attempt_deadline", "attempts", ["deadline"], unique=False)
    op.create_index("ix_attempt_dispatch", "attempts", ["status", "lease_until"], unique=False)
    op.create_table(
        "ledger_entries",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("operation_type", sa.String(length=20), nullable=False),
        sa.Column("amount", sa.BigInteger(), nullable=False),
        sa.Column("payment_id", sa.String(length=200), nullable=True),
        sa.Column("task_id", sa.Uuid(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(operation_type = 'payment' AND amount > 0 AND payment_id IS NOT NULL "
            "AND task_id IS NULL) OR "
            "(operation_type = 'debit' AND amount < 0 AND task_id IS NOT NULL "
            "AND payment_id IS NULL) OR "
            "(operation_type = 'refund' AND amount > 0 AND task_id IS NOT NULL "
            "AND payment_id IS NULL)",
            name="ledger_operation_shape",
        ),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["tasks.id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("payment_id"),
        sa.UniqueConstraint("task_id", "operation_type"),
    )
    op.create_index("ix_ledger_user", "ledger_entries", ["user_id", "id"], unique=False)


def downgrade() -> None:
    op.drop_constraint("fk_task_current_attempt", "tasks", type_="foreignkey")
    op.drop_index("ix_ledger_user", table_name="ledger_entries")
    op.drop_table("ledger_entries")
    op.drop_index("ix_attempt_dispatch", table_name="attempts")
    op.drop_index("ix_attempt_deadline", table_name="attempts")
    op.drop_index(
        "ix_attempt_active_task",
        table_name="attempts",
        postgresql_where=sa.text("status IN ('queued', 'dispatching', 'awaiting_webhook')"),
    )
    op.drop_table("attempts")
    op.drop_index(
        "ix_tasks_active_user", table_name="tasks", postgresql_where=sa.text("status = 'pending'")
    )
    op.drop_table("tasks")
    op.drop_table("model_legs")
    op.drop_table("users")
    op.drop_table("models")
    op.drop_table("fake_tasks")
    op.drop_table("fake_settings")
