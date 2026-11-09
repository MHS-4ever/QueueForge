from decimal import Decimal

import pytest

from queueforge.jobs import JobStore, OwnershipLost, WorkBusy, make_job_id


class ConditionalError(Exception):
    response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class FakeTable:
    def __init__(self):
        self.items = {}

    def put_item(self, Item, ConditionExpression):
        if Item["job_id"] in self.items:
            raise ConditionalError
        self.items[Item["job_id"]] = dict(Item)

    def get_item(self, Key, ConsistentRead=True):
        item = self.items.get(Key["job_id"])
        return {"Item": dict(item)} if item else {}

    def update_item(self, **kwargs):
        item = self.items.get(kwargs["Key"]["job_id"])
        values = kwargs["ExpressionAttributeValues"]
        if ":pending" in values:
            now = values[":now"]
            claimable = item and (
                item["status"] == "PENDING"
                or (item["status"] == "PROCESSING" and item["lease_expires_at"] <= now)
            )
            if not claimable:
                raise ConditionalError
            item.update(
                status="PROCESSING",
                owner_token=values[":owner"],
                lease_expires_at=values[":lease"],
                updated_at=values[":updated"],
            )
            return {"Attributes": dict(item)}
        if (
            not item
            or item["status"] != "PROCESSING"
            or item.get("owner_token") != values[":owner"]
        ):
            raise ConditionalError
        item["status"] = values[":status"]
        item["updated_at"] = values[":updated"]
        names = kwargs["ExpressionAttributeNames"]
        for key, name in names.items():
            if key.startswith("#field"):
                item[name] = values[key.replace("#", ":")]
        item.pop("owner_token", None)
        item.pop("lease_expires_at", None)
        return {}


def source():
    return {"bucket": "bucket", "key": "incoming/a.json", "version_id": "v1"}


def test_job_id_is_stable_and_source_sensitive():
    first = make_job_id("bucket", "incoming/a.json", "v1")
    assert first == make_job_id("bucket", "incoming/a.json", "v1")
    assert first != make_job_id("bucket", "incoming/a.json", "v2")


def test_concurrent_claim_is_busy():
    table = FakeTable()
    store = JobStore(table, clock=lambda: 100)
    store.ensure_pending("job", source())
    assert store.claim("job").acquired
    with pytest.raises(WorkBusy):
        store.claim("job")


def test_expired_lease_can_be_reclaimed():
    table = FakeTable()
    now = [100]
    store = JobStore(table, lease_seconds=10, clock=lambda: now[0])
    store.ensure_pending("job", source())
    first = store.claim("job")
    now[0] = 110
    second = store.claim("job")
    assert first.owner_token != second.owner_token


def test_terminal_state_is_protected_and_duplicate_returns_status():
    table = FakeTable()
    store = JobStore(table, clock=lambda: 100)
    store.ensure_pending("job", source())
    claim = store.claim("job")
    store.finalize(claim, "COMPLETED", {"valid_count": Decimal(1)})
    duplicate = store.claim("job")
    assert not duplicate.acquired
    assert duplicate.terminal_status == "COMPLETED"
    with pytest.raises(OwnershipLost):
        store.finalize(claim, "FAILED", {})
