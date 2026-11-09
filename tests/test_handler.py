import json
import logging

import pytest

import queueforge.handler as handler
from queueforge.handler import lambda_handler
from queueforge.jobs import WorkBusy


@pytest.fixture(autouse=True)
def stub_processing(monkeypatch):
    monkeypatch.setattr(handler, "_process_object_record", lambda *args: ("job-id", "COMPLETED"))


def notification(key="incoming/a+file%2Bname.csv"):
    return {
        "Records": [
            {
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "example"},
                    "object": {"key": key, "size": 10, "versionId": "v1", "sequencer": "AB"},
                },
            }
        ]
    }


def message(identifier, body):
    return {"messageId": identifier, "body": json.dumps(body)}


def test_object_created_logs_decoded_metadata(caplog):
    with caplog.at_level(logging.INFO):
        result = lambda_handler({"Records": [message("ok", notification())]}, None)
    assert result == {"batchItemFailures": []}
    entry = next(
        json.loads(record.message)
        for record in caplog.records
        if json.loads(record.message)["event"] == "s3_object_created"
    )
    assert entry == {
        "event": "s3_object_created",
        "message_id": "ok",
        "event_name": "ObjectCreated:Put",
        "bucket": "example",
        "key": "incoming/a file+name.csv",
        "size": 10,
        "version_id": "v1",
        "sequencer": "AB",
    }


def test_test_event_is_acknowledged(caplog):
    result = lambda_handler({"Records": [message("test", {"Event": "s3:TestEvent"})]}, None)
    assert result == {"batchItemFailures": []}
    assert json.loads(caplog.records[-1].message)["event"] == "s3_test_event_ignored"


@pytest.mark.parametrize("body", [None, [], {}, {"Records": []}, {"Records": [{}]}])
def test_malformed_notifications_fail_only_their_message(body):
    records = [message("bad", body), message("good", notification())]
    assert lambda_handler({"Records": records}, None) == {
        "batchItemFailures": [{"itemIdentifier": "bad"}]
    }


def test_invalid_json_and_missing_body():
    records = [{"messageId": "json", "body": "{"}, {"messageId": "missing"}]
    assert lambda_handler({"Records": records}, None) == {
        "batchItemFailures": [{"itemIdentifier": "json"}, {"itemIdentifier": "missing"}]
    }


def test_multiple_nested_records(caplog):
    body = notification()
    body["Records"].extend(notification("incoming/second.json")["Records"])
    assert lambda_handler({"Records": [message("multi", body)]}, None) == {"batchItemFailures": []}
    assert (
        sum(json.loads(record.message)["event"] == "s3_object_created" for record in caplog.records)
        == 2
    )


def test_nested_failure_retries_whole_sqs_message():
    body = notification()
    body["Records"].append({})
    assert lambda_handler({"Records": [message("retry", body)]}, None) == {
        "batchItemFailures": [{"itemIdentifier": "retry"}]
    }


def test_non_created_event_is_ignored():
    body = notification()
    body["Records"][0]["eventName"] = "ObjectRemoved:Delete"
    assert lambda_handler({"Records": [message("ignored", body)]}, None) == {
        "batchItemFailures": []
    }


def test_empty_batch():
    assert lambda_handler({"Records": []}, None) == {"batchItemFailures": []}


def test_missing_message_id_fails_invocation():
    with pytest.raises(KeyError):
        lambda_handler({"Records": [{"body": "{}"}]}, None)


def test_active_duplicate_is_returned_as_batch_failure(monkeypatch):
    def busy(*args):
        raise WorkBusy("owned")

    monkeypatch.setattr(handler, "_process_object_record", busy)
    assert lambda_handler({"Records": [message("duplicate", notification())]}, None) == {
        "batchItemFailures": [{"itemIdentifier": "duplicate"}]
    }


def test_processing_failure_does_not_fail_other_sqs_messages(monkeypatch):
    def process(bucket, key, version_id, size):
        if "bad" in key:
            raise RuntimeError("temporary failure")
        return "job-id", "COMPLETED"

    monkeypatch.setattr(handler, "_process_object_record", process)
    records = [
        message("bad", notification("incoming/bad.csv")),
        message("good", notification("incoming/good.csv")),
    ]
    assert lambda_handler({"Records": records}, None) == {
        "batchItemFailures": [{"itemIdentifier": "bad"}]
    }
