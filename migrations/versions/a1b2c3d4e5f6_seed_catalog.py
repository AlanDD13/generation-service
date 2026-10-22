from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a1b2c3d4e5f6"
down_revision: str | None = "d0ebfcb359e4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    models = sa.table("models", sa.column("id"), sa.column("slug"), sa.column("price"))
    legs = sa.table(
        "model_legs",
        sa.column("id"),
        sa.column("model_id"),
        sa.column("provider"),
        sa.column("priority"),
        sa.column("max_ref_images"),
        sa.column("skip_on_moderation"),
    )
    op.bulk_insert(
        models,
        [
            {"id": 1, "slug": "image-fast", "price": 2},
            {"id": 2, "slug": "video-long", "price": 25},
        ],
    )
    op.bulk_insert(
        legs,
        [
            {
                "id": 1,
                "model_id": 1,
                "provider": "alpha",
                "priority": 1,
                "max_ref_images": 4,
                "skip_on_moderation": False,
            },
            {
                "id": 2,
                "model_id": 1,
                "provider": "beta",
                "priority": 2,
                "max_ref_images": 10,
                "skip_on_moderation": False,
            },
            {
                "id": 3,
                "model_id": 2,
                "provider": "beta",
                "priority": 1,
                "max_ref_images": 2,
                "skip_on_moderation": False,
            },
            {
                "id": 4,
                "model_id": 2,
                "provider": "alpha",
                "priority": 2,
                "max_ref_images": 1,
                "skip_on_moderation": True,
            },
        ],
    )
    op.execute("INSERT INTO users (id) VALUES (1)")
    op.execute(
        "INSERT INTO fake_settings (provider, settings) VALUES ('alpha', '{}'), ('beta', '{}')"
    )


def downgrade() -> None:
    op.execute("DELETE FROM fake_settings WHERE provider IN ('alpha', 'beta')")
    op.execute("DELETE FROM users WHERE id = 1")
    op.execute("DELETE FROM model_legs WHERE model_id IN (1, 2)")
    op.execute("DELETE FROM models WHERE id IN (1, 2)")
