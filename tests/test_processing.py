import json

import pytest

from queueforge.processing import MAX_FILE_BYTES, InputRejected, process_file


def test_valid_csv_normalizes_records():
    content = b"record_id,customer_id,amount\nr1,c1,10.50\nr2,c2,2\n"
    result = process_file(content, "incoming/data.csv")
    assert result.status == "COMPLETED"
    assert result.records == [
        {"record_id": "r1", "customer_id": "c1", "amount": "10.5"},
        {"record_id": "r2", "customer_id": "c2", "amount": "2"},
    ]
    assert result.errors == []


def test_valid_json_uses_decimal_without_float_rounding():
    content = b'[{"record_id":"r1","customer_id":"c1","amount":0.10}]'
    result = process_file(content, "incoming/data.JSON")
    assert result.records[0]["amount"] == "0.1"


def test_mixed_invalid_and_duplicate_records():
    records = [
        {"record_id": "r1", "customer_id": "c1", "amount": "3.00"},
        {"record_id": "r2", "customer_id": "", "amount": -1},
        {"record_id": "r1", "customer_id": "c3", "amount": 7},
        {"customer_id": "c4", "amount": "not-a-number"},
    ]
    result = process_file(json.dumps(records).encode(), "incoming/data.json")
    assert result.status == "PARTIAL_SUCCESS"
    assert [record["record_id"] for record in result.records] == ["r1"]
    assert result.total_records == 4
    assert "duplicate record_id" in result.errors[1]["errors"][0]
    assert result.errors[0]["record_id"] == "r2"


@pytest.mark.parametrize(
    ("content", "key", "message"),
    [
        (b"", "incoming/a.json", "empty"),
        (b"{", "incoming/a.json", "malformed JSON"),
        (b"{}", "incoming/a.json", "must be an array"),
        (b"[]", "incoming/a.json", "no records"),
        (b"x", "incoming/a.txt", "unsupported"),
    ],
)
def test_rejected_files(content, key, message):
    with pytest.raises(InputRejected, match=message):
        process_file(content, key)


def test_file_size_limit():
    with pytest.raises(InputRejected, match="exceeds"):
        process_file(b" " * (MAX_FILE_BYTES + 1), "incoming/a.json")


def test_record_limit():
    records = [{"record_id": str(i), "customer_id": "c", "amount": 1} for i in range(1001)]
    with pytest.raises(InputRejected, match="1000 records"):
        process_file(json.dumps(records).encode(), "incoming/a.json")


def test_all_invalid_is_rejected_without_aborting_validation():
    content = b'[{"record_id":"a","customer_id":"c","amount":0},42]'
    result = process_file(content, "incoming/a.json")
    assert result.status == "REJECTED"
    assert len(result.errors) == 2
