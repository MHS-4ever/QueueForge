"""DynamoDB-backed ownership and lifecycle operations."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

TERMINAL_STATES = frozenset({"COMPLETED", "PARTIAL_SUCCESS", "REJECTED", "FAILED"})


class WorkBusy(RuntimeError):
    """Another worker currently owns an unexpired processing lease."""


class OwnershipLost(RuntimeError):
    """The worker no longer owns the job and must not finalize it."""


def _conditional_failure(exc: Exception) -> bool:
    response = getattr(exc, "response", {})
    return response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"


def now_iso(epoch: int | None = None) -> str:
    return datetime.fromtimestamp(epoch or int(time.time()), UTC).isoformat()


def make_job_id(bucket: str, key: str, version_id: str) -> str:
    payload = json.dumps(
        {"bucket": bucket, "key": key, "version_id": version_id},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class Claim:
    job_id: str
    owner_token: str
    terminal_status: str | None = None

    @property
    def acquired(self) -> bool:
        return self.terminal_status is None


class JobStore:
    def __init__(self, table: Any, lease_seconds: int = 120, clock: Any = time.time):
        self.table = table
        self.lease_seconds = lease_seconds
        self.clock = clock

    def ensure_pending(self, job_id: str, source: dict[str, Any]) -> None:
        epoch = int(self.clock())
        item = {
            "job_id": job_id,
            "status": "PENDING",
            "source_bucket": source["bucket"],
            "source_key": source["key"],
            "source_version_id": source["version_id"],
            "created_at": now_iso(epoch),
            "updated_at": now_iso(epoch),
        }
        try:
            self.table.put_item(Item=item, ConditionExpression="attribute_not_exists(job_id)")
        except Exception as exc:
            if not _conditional_failure(exc):
                raise

    def claim(self, job_id: str) -> Claim:
        epoch = int(self.clock())
        owner = str(uuid.uuid4())
        try:
            self.table.update_item(
                Key={"job_id": job_id},
                UpdateExpression=(
                    "SET #status = :processing, owner_token = :owner, "
                    "lease_expires_at = :lease, updated_at = :updated"
                ),
                ConditionExpression=(
                    "attribute_exists(job_id) AND "
                    "(#status = :pending OR (#status = :processing AND lease_expires_at <= :now))"
                ),
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":pending": "PENDING",
                    ":processing": "PROCESSING",
                    ":owner": owner,
                    ":lease": Decimal(epoch + self.lease_seconds),
                    ":now": Decimal(epoch),
                    ":updated": now_iso(epoch),
                },
                ReturnValues="ALL_NEW",
            )
            return Claim(job_id, owner)
        except Exception as exc:
            if not _conditional_failure(exc):
                raise
        item = self.table.get_item(Key={"job_id": job_id}, ConsistentRead=True).get("Item")
        if item and item.get("status") in TERMINAL_STATES:
            return Claim(job_id, "", str(item["status"]))
        raise WorkBusy(f"job {job_id} has an active lease")

    def finalize(self, claim: Claim, status: str, fields: dict[str, Any]) -> None:
        if status not in TERMINAL_STATES:
            raise ValueError(f"invalid terminal status: {status}")
        epoch = int(self.clock())
        values = {
            ":processing": "PROCESSING",
            ":owner": claim.owner_token,
            ":status": status,
            ":updated": now_iso(epoch),
        }
        names = {"#status": "status"}
        assignments = ["#status = :status", "updated_at = :updated"]
        for index, (name, value) in enumerate(fields.items()):
            name_key = f"#field{index}"
            value_key = f":field{index}"
            names[name_key] = name
            values[value_key] = value
            assignments.append(f"{name_key} = {value_key}")
        try:
            self.table.update_item(
                Key={"job_id": claim.job_id},
                UpdateExpression="SET "
                + ", ".join(assignments)
                + " REMOVE owner_token, lease_expires_at",
                ConditionExpression="#status = :processing AND owner_token = :owner",
                ExpressionAttributeNames=names,
                ExpressionAttributeValues=values,
            )
        except Exception as exc:
            if _conditional_failure(exc):
                raise OwnershipLost(f"ownership lost for job {claim.job_id}") from exc
            raise
