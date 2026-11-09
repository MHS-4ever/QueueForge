"""Bounded DLQ inspection and explicitly requested recovery operations."""

from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Any
from urllib.parse import quote_plus, unquote_plus

from .jobs import make_job_id, now_iso


class RecoveryBlocked(RuntimeError):
    """A job is not in a state that operators may safely recover."""


def _conditional_failure(exc: Exception) -> bool:
    return (
        getattr(exc, "response", {}).get("Error", {}).get("Code")
        == "ConditionalCheckFailedException"
    )


def sources_from_message(body: str) -> list[dict[str, str]]:
    """Extract versioned S3 sources from a raw SQS message body."""
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return []
    sources = []
    if not isinstance(payload, dict):
        return sources
    for record in payload.get("Records", []):
        try:
            if record["eventSource"] != "aws:s3":
                continue
            bucket = record["s3"]["bucket"]["name"]
            obj = record["s3"]["object"]
            key = unquote_plus(obj["key"])
            version_id = obj["versionId"]
            if all(isinstance(value, str) and value for value in (bucket, key, version_id)):
                sources.append(
                    {
                        "bucket": bucket,
                        "key": key,
                        "version_id": version_id,
                        "job_id": make_job_id(bucket, key, version_id),
                    }
                )
        except (KeyError, TypeError):
            continue
    return sources


def inspect_failures(sqs: Any, queue_url: str, table: Any, limit: int = 10) -> list[dict[str, Any]]:
    """Inspect up to ten DLQ messages without deleting them."""
    if not 1 <= limit <= 10:
        raise ValueError("failure inspection limit must be between 1 and 10")
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=limit,
        MessageSystemAttributeNames=["ApproximateReceiveCount", "SentTimestamp"],
        VisibilityTimeout=0,
        WaitTimeSeconds=0,
    )
    findings = []
    for message in response.get("Messages", []):
        sources = sources_from_message(message.get("Body", ""))
        jobs = []
        for source in sources:
            item = table.get_item(Key={"job_id": source["job_id"]}, ConsistentRead=True).get("Item")
            jobs.append(
                {
                    **source,
                    "job_status": item.get("status") if item else "MISSING",
                }
            )
        findings.append(
            {
                "message_id": message.get("MessageId"),
                "receive_count": message.get("Attributes", {}).get("ApproximateReceiveCount"),
                "sent_timestamp": message.get("Attributes", {}).get("SentTimestamp"),
                "jobs": jobs,
                "unmapped_message": not jobs,
            }
        )
    return findings


def stale_jobs(table: Any, limit: int = 100, now: int | None = None) -> dict[str, Any]:
    """Return a bounded set of PROCESSING jobs whose leases have expired."""
    if not 1 <= limit <= 100:
        raise ValueError("reconciliation limit must be between 1 and 100")
    epoch = int(time.time()) if now is None else now
    response = table.query(
        IndexName="status-lease-index",
        Limit=limit,
        KeyConditionExpression="#status = :processing AND lease_expires_at <= :now",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":processing": "PROCESSING", ":now": Decimal(epoch)},
    )
    return {
        "jobs": response.get("Items", []),
        "scan_truncated": "LastEvaluatedKey" in response,
    }


def prepare_job_recovery(table: Any, job_id: str, now: int | None = None) -> dict[str, Any]:
    """Conditionally move FAILED or stale PROCESSING work back to PENDING."""
    epoch = int(time.time()) if now is None else now
    item = table.get_item(Key={"job_id": job_id}, ConsistentRead=True).get("Item")
    if not item:
        raise RecoveryBlocked(f"job not found: {job_id}")
    status = item.get("status")
    values: dict[str, Any] = {
        ":pending": "PENDING",
        ":updated": now_iso(epoch),
        ":requested": now_iso(epoch),
    }
    if status == "FAILED":
        condition = "#status = :failed"
        values[":failed"] = "FAILED"
    elif status == "PROCESSING" and int(item.get("lease_expires_at", epoch + 1)) <= epoch:
        condition = "#status = :processing AND lease_expires_at <= :now"
        values.update({":processing": "PROCESSING", ":now": Decimal(epoch)})
    elif status == "PENDING" and item.get("recovery_requested_at"):
        condition = "#status = :pending AND attribute_exists(recovery_requested_at)"
    else:
        raise RecoveryBlocked(f"job {job_id} in {status} is protected from recovery")
    try:
        response = table.update_item(
            Key={"job_id": job_id},
            UpdateExpression=(
                "SET #status = :pending, updated_at = :updated, "
                "recovery_requested_at = :requested REMOVE owner_token, lease_expires_at"
            ),
            ConditionExpression=condition,
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues=values,
            ReturnValues="ALL_NEW",
        )
    except Exception as exc:
        if _conditional_failure(exc):
            raise RecoveryBlocked(f"job {job_id} changed during recovery") from exc
        raise
    return response["Attributes"]


def recovery_message(item: dict[str, Any]) -> str:
    payload = {
        "Records": [
            {
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Recovery",
                "s3": {
                    "bucket": {"name": item["source_bucket"]},
                    "object": {
                        "key": quote_plus(item["source_key"], safe="/"),
                        "versionId": item["source_version_id"],
                    },
                },
            }
        ]
    }
    return json.dumps(payload, separators=(",", ":"))


def recover_job(
    table: Any, sqs: Any, queue_url: str, job_id: str, now: int | None = None
) -> dict[str, Any]:
    item = prepare_job_recovery(table, job_id, now)
    response = sqs.send_message(QueueUrl=queue_url, MessageBody=recovery_message(item))
    return {"job_id": job_id, "status": "PENDING", "message_id": response["MessageId"]}


def start_dlq_redrive(sqs: Any, dlq_url: str, processing_queue_url: str) -> dict[str, Any]:
    """Start AWS-native queue-wide redrive without inspecting or deleting messages."""
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    destination_arn = sqs.get_queue_attributes(
        QueueUrl=processing_queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    return sqs.start_message_move_task(
        SourceArn=dlq_arn,
        DestinationArn=destination_arn,
    )
