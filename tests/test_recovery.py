import json
from decimal import Decimal

import pytest

from queueforge.jobs import make_job_id
from queueforge.recovery import (
    RecoveryBlocked,
    inspect_failures,
    prepare_job_recovery,
    recover_job,
    stale_jobs,
    start_dlq_redrive,
)


class FakeTable:
    name = "jobs"

    def __init__(self, items=None):
        self.items = items or {}

    def get_item(self, Key, ConsistentRead=True):
        item = self.items.get(Key["job_id"])
        return {"Item": dict(item)} if item else {}

    def query(self, **kwargs):
        assert kwargs["IndexName"] == "status-lease-index"
        now = kwargs["ExpressionAttributeValues"][":now"]
        items = [
            dict(item)
            for item in self.items.values()
            if item.get("status") == "PROCESSING" and item.get("lease_expires_at", now + 1) <= now
        ]
        return {"Items": items[: kwargs["Limit"]]}

    def update_item(self, **kwargs):
        item = self.items[kwargs["Key"]["job_id"]]
        values = kwargs["ExpressionAttributeValues"]
        condition = kwargs["ConditionExpression"]
        if ":failed" in condition and item["status"] != "FAILED":
            raise AssertionError("condition failed")
        if ":processing" in condition and not (
            item["status"] == "PROCESSING" and item["lease_expires_at"] <= values[":now"]
        ):
            raise AssertionError("condition failed")
        item.update(
            status="PENDING",
            updated_at=values[":updated"],
            recovery_requested_at=values[":requested"],
        )
        item.pop("owner_token", None)
        item.pop("lease_expires_at", None)
        return {"Attributes": dict(item)}


class FakeSqs:
    def __init__(self, messages=None):
        self.messages = messages or []
        self.sent = []

    def receive_message(self, **kwargs):
        return {"Messages": self.messages[: kwargs["MaxNumberOfMessages"]]}

    def send_message(self, **kwargs):
        self.sent.append(kwargs)
        return {"MessageId": "recovery-message"}

    def get_queue_attributes(self, QueueUrl, AttributeNames):
        return {"Attributes": {"QueueArn": f"arn:{QueueUrl}"}}

    def start_message_move_task(self, **kwargs):
        self.redrive = kwargs
        return {"TaskHandle": "task"}


def job(status="PROCESSING", lease=50):
    return {
        "job_id": "job",
        "status": status,
        "lease_expires_at": Decimal(lease),
        "owner_token": "owner",
        "source_bucket": "bucket",
        "source_key": "incoming/a.json",
        "source_version_id": "v1",
    }


def test_stale_reconciliation_is_bounded_and_excludes_active_lease():
    table = FakeTable(
        {"stale": {**job(), "job_id": "stale"}, "active": {**job(lease=150), "job_id": "active"}}
    )
    result = stale_jobs(table, limit=10, now=100)
    assert [item["job_id"] for item in result["jobs"]] == ["stale"]
    assert not result["scan_truncated"]


def test_active_and_completed_jobs_are_protected_from_recovery():
    with pytest.raises(RecoveryBlocked, match="protected"):
        prepare_job_recovery(FakeTable({"job": job(lease=150)}), "job", now=100)
    with pytest.raises(RecoveryBlocked, match="protected"):
        prepare_job_recovery(FakeTable({"job": job(status="COMPLETED")}), "job", now=100)


def test_stale_job_recovery_is_conditional_and_sends_targeted_message():
    table = FakeTable({"job": job()})
    sqs = FakeSqs()
    result = recover_job(table, sqs, "processing-url", "job", now=100)
    assert result == {"job_id": "job", "status": "PENDING", "message_id": "recovery-message"}
    assert table.items["job"]["status"] == "PENDING"
    body = json.loads(sqs.sent[0]["MessageBody"])
    assert body["Records"][0]["s3"]["object"]["versionId"] == "v1"


def test_recovery_message_preserves_literal_plus_in_source_key():
    item = job(status="FAILED")
    item["source_key"] = "incoming/a+b file.json"
    table = FakeTable({"job": item})
    sqs = FakeSqs()
    recover_job(table, sqs, "processing-url", "job", now=100)
    encoded = json.loads(sqs.sent[0]["MessageBody"])["Records"][0]["s3"]["object"]["key"]
    assert encoded == "incoming/a%2Bb+file.json"


def test_failed_job_requires_explicit_recovery_path():
    table = FakeTable({"job": job(status="FAILED")})
    recovered = prepare_job_recovery(table, "job", now=100)
    assert recovered["status"] == "PENDING"
    assert recovered["recovery_requested_at"]


def test_dlq_inspection_reports_missing_jobs_without_deleting_messages():
    bucket, key, version = "bucket", "incoming/a.json", "v1"
    job_id = make_job_id(bucket, key, version)
    body = json.dumps(
        {
            "Records": [
                {
                    "eventSource": "aws:s3",
                    "s3": {
                        "bucket": {"name": bucket},
                        "object": {"key": key, "versionId": version},
                    },
                }
            ]
        }
    )
    sqs = FakeSqs(
        [{"MessageId": "m1", "Body": body, "Attributes": {"ApproximateReceiveCount": "6"}}]
    )
    findings = inspect_failures(sqs, "dlq", FakeTable(), limit=10)
    assert findings[0]["jobs"] == [
        {
            "bucket": bucket,
            "key": key,
            "version_id": version,
            "job_id": job_id,
            "job_status": "MISSING",
        }
    ]
    assert not hasattr(sqs, "delete_message")


def test_native_redrive_is_queue_wide_and_separate_from_targeted_recovery():
    sqs = FakeSqs()
    assert start_dlq_redrive(sqs, "dlq", "processing") == {"TaskHandle": "task"}
    assert sqs.redrive == {
        "SourceArn": "arn:dlq",
        "DestinationArn": "arn:processing",
    }
