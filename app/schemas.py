from datetime import datetime
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator

ErrorType = Literal["technical", "moderation", "payload"]


class GenerateRequest(BaseModel):
    user_id: int = Field(gt=0)
    model_slug: str = Field(min_length=1, max_length=100)
    prompt: str = Field(min_length=1, max_length=10000)
    ref_images: list[str] = Field(default_factory=list)


class GenerateResponse(BaseModel):
    task_id: UUID


class PaymentEvent(BaseModel):
    payment_id: str = Field(min_length=1, max_length=200)
    user_id: int = Field(gt=0)
    tokens: StrictInt = Field(gt=0, le=10**12)
    status: Literal["succeeded", "pending", "failed"]


class ProviderEvent(BaseModel):
    attempt_id: UUID
    external_id: str = Field(min_length=1, max_length=100)
    status: Literal["completed", "failed"]
    result_url: str | None = Field(default=None, min_length=1)
    error_type: ErrorType | None = None

    @model_validator(mode="after")
    def validate_result(self) -> Self:
        if self.status == "completed" and (not self.result_url or self.error_type):
            raise ValueError("completed requires result_url and no error_type")
        if self.status == "failed" and (not self.error_type or self.result_url):
            raise ValueError("failed requires error_type and no result_url")
        return self


class LedgerView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    operation_type: str
    amount: int
    payment_id: str | None
    task_id: UUID | None
    created_at: datetime


class BalanceView(BaseModel):
    user_id: int
    balance: int
    entries: list[LedgerView]


class AttemptView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    provider: str
    status: str
    external_id: str | None
    error_type: str | None
    error_code: str | None
    created_at: datetime
    started_at: datetime | None
    deadline: datetime | None
    finished_at: datetime | None


class TaskView(BaseModel):
    id: UUID
    status: str
    current_leg: str | None
    current_attempt_id: UUID | None
    result_url: str | None
    created_at: datetime
    finished_at: datetime | None
    attempts: list[AttemptView]


class FakeBehavior(BaseModel):
    failure_rate: float = Field(default=0, ge=0, le=1)
    error_type: ErrorType = "technical"
    sync_mode: Literal["normal", "503", "timeout"] = "normal"
    delay_min: float = Field(default=2, ge=0, le=3600)
    delay_max: float = Field(default=20, ge=0, le=3600)
    duplicate_webhook: bool = False
    concurrent_duplicates: bool = False
    no_webhook: bool = False

    @model_validator(mode="after")
    def validate_delay(self) -> Self:
        if self.delay_max < self.delay_min:
            raise ValueError("delay_max must be >= delay_min")
        return self


class ProviderSubmission(BaseModel):
    attempt_id: UUID
    model_slug: str
    prompt: str
    ref_images: list[str]
    callback_url: str
