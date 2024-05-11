from typing import Collection

from pydantic import BaseModel, Field
from urllib3.util.retry import Retry

RETRY_AFTER_STATUS_CODES = frozenset([429, 503, 504])

RETRY_METHODS = frozenset(["GET", "POST", "PUT", "DELETE", "OPTIONS"])


class RetryStrategy(BaseModel):
    total: int = Field(
        default=3,
        ge=0,
        description="The total number of retries to allow. This overrides the value of the 'connect', "
        "'read', and 'other' parameters.",
    )
    connect: int = Field(
        default=3, ge=0, description="How many connection-related errors to retry on."
    )
    read: int = Field(
        default=3, ge=0, description="How many times to retry on read errors."
    )
    other: int = Field(
        default=3, ge=0, description="How many times to retry on other errors."
    )
    methods: Collection[str] = Field(
        default_factory=lambda: RETRY_METHODS,
        description="Which HTTP methods to retry on.",
    )
    status_codes: Collection[int] = Field(
        default_factory=lambda: RETRY_AFTER_STATUS_CODES,
        description="Which HTTP status codes to retry on.",
    )
    backoff_factor: float = Field(
        default=1.1,
        ge=0,
        description="A backoff factor to apply between attempts after the "
        "second try (most errors are resolved immediately by a "
        "retry, so the default is 1.1, slightly above a linear "
        "backoff).",
    )
    backoff_max: float = Field(
        default=120, ge=0, description="The maximum back off time in seconds."
    )
    backoff_jitter: float = Field(
        default=0, ge=0, description="A jitter factor to apply to the backoff time."
    )
    respect_retry_after_header: bool = Field(
        default=True, description="Whether to respect the Retry-After header."
    )

    class Config:
        arbitrary_types_allowed = True

    def to_retry(self) -> Retry:
        return Retry(
            total=self.total,
            connect=self.connect,
            read=self.read,
            other=self.other,
            allowed_methods=self.methods,
            status_forcelist=self.status_codes,
            backoff_factor=self.backoff_factor,
            backoff_max=self.backoff_max,
            backoff_jitter=self.backoff_jitter,
            respect_retry_after_header=self.respect_retry_after_header,
        )
