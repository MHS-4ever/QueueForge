"""Inspect S3 notifications without reading object contents."""

import json
import logging
from urllib.parse import unquote_plus

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _log(event: str, **fields: object) -> None:
    logger.info(json.dumps({"event": event, **fields}, ensure_ascii=True))


def _process_message(message: dict) -> None:
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
        _log(
            "s3_object_created",
            message_id=message_id,
            event_name=event_name,
            bucket=bucket,
            key=unquote_plus(obj["key"]),
            size=obj.get("size"),
            version_id=obj.get("versionId"),
            sequencer=obj.get("sequencer"),
        )


def lambda_handler(event: dict, context: object) -> dict:
    """Return failed SQS message IDs so successful messages can be deleted."""
    failures = []
    for message in event["Records"]:
        # SQS always supplies messageId. Missing IDs must fail the invocation:
        # returning an invalid partial response could acknowledge malformed input.
        message_id = message["messageId"]
        try:
            _process_message(message)
        except Exception as exc:
            # Do not log raw bodies; they may contain sensitive object metadata.
            _log("message_failed", message_id=message_id, error_type=type(exc).__name__)
            failures.append({"itemIdentifier": message_id})
    return {"batchItemFailures": failures}
