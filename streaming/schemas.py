"""
streaming/schemas.py

Shared event contract for the streaming layer. Both producer and
consumer validate against this same model -- the lightweight
substitute for a full Avro/Schema Registry setup (see the master
prompt's "explicitly out of scope" section for why we skipped that).
"""

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class TransactionEvent(BaseModel):
    transaction_id: str
    account_id: str
    merchant: str
    category: str
    amount: float = Field(gt=0)
    timestamp: datetime
    location: str

    @field_validator("amount")
    @classmethod
    def amount_must_be_reasonable(cls, v: float) -> float:
        if v > 1_000_000:
            raise ValueError("amount exceeds sane upper bound")
        return v
