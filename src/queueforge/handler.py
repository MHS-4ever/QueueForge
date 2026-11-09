"""SQS Lambda adapter for versioned S3 object-created notifications."""

from __future__ import annotations

import json
import logging
import os
from typing import Any
from urllib.parse import unquote_plus

from .jobs import JobStore
from .service import process_object

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
_aws: tuple[Any, JobStore] | None = None


def _log(event: str, **fields: object) -> None:
    logger.info(json.dumps({"event": event, **fields}, ensure_ascii=True))


def _dependencies() -> tuple[Any, JobStore]:
    global _aws
    if _aws is None:
        import boto3

        s3 = boto3.client("s3")
        table = boto3.resource("dynamodb").Table(os.environ["JOB_TABLE_NAME"])
        _aws = (s3, JobStore(table, lease_seconds=int(os.environ.get("LEASE_SECONDS", "120"))))
    return _aws


def _process_object_record(
    bucket: str, key: str, version_id: str, size: int | None
) -> tuple[str, str]:
    s3, jobs = _dependencies()
    return process_object(s3, jobs, bucket, key, version_id, size)


def _process_message(message: dict[str, Any]) -> None:
    notification = json.loads(message["body"])
    if not isinstance(notification, dict):
        raise ValueError("S3 notification must be an object")
    message_id = message["messageId"]
    if notification.get("Event") == "s3:TestEvent":
        _log("s3_test_event_ignored", message_id=message_id)
        return

    records = notification.get("Records")
    if not isinstance(records, list) or not records:
        raise ValueError("S3 notification requires a non-empty Records list")
    for record in records:
        if record["eventSource"] != "aws:s3":
            raise ValueError("Expected an S3 event")
        event_name = record["eventName"]
        if not isinstance(event_name, str):
            raise ValueError("Expected a string eventName")
        if not event_name.startswith("ObjectCreated:"):
            _log("s3_event_ignored", message_id=message_id, event_name=event_name)
            continue
        bucket = record["s3"]["bucket"]["name"]
        obj = record["s3"]["object"]
        if not isinstance(bucket, str) or not bucket or not isinstance(obj["key"], str):
            raise ValueError("Expected an S3 bucket name and object key")
        key = unquote_plus(obj["key"])
        version_id = obj.get("versionId")
        _log(
            "s3_object_created",
            message_id=message_id,
            event_name=event_name,
            bucket=bucket,
            key=key,
            size=obj.get("size"),
            version_id=version_id,
            sequencer=obj.get("sequencer"),
        )
        job_id, status = _process_object_record(bucket, key, version_id, obj.get("size"))
        _log("job_terminal", message_id=message_id, job_id=job_id, status=status)


def lambda_handler(event: dict[str, Any], context: object) -> dict[str, list[dict[str, str]]]:
    """Return failed SQS message IDs so successful messages can be deleted."""
    failures = []
    for message in event["Records"]:
        message_id = message["messageId"]
        try:
            _process_message(message)
        except Exception as exc:
            _log("message_failed", message_id=message_id, error_type=type(exc).__name__)
            failures.append({"itemIdentifier": message_id})
    return {"batchItemFailures": failures}
