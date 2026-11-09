"""Parse and validate QueueForge transaction files."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import PurePosixPath
from typing import Any

MAX_FILE_BYTES = 1024 * 1024
MAX_RECORDS = 1000
REQUIRED_FIELDS = ("record_id", "customer_id", "amount")


class InputRejected(ValueError):
    """The source file cannot be processed as a transaction collection."""


@dataclass(frozen=True)
class ProcessingResult:
    records: list[dict[str, str]]
    errors: list[dict[str, Any]]
    total_records: int

    @property
    def status(self) -> str:
        if not self.records:
            return "REJECTED"
        if self.errors:
            return "PARTIAL_SUCCESS"
        return "COMPLETED"


def _decimal_text(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("must be a number or numeric string")
    text = str(value).strip()
    if not text:
        raise ValueError("must not be empty")
    try:
        amount = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError("must be a valid decimal number") from exc
    if not amount.is_finite() or amount <= 0:
        raise ValueError("must be a positive finite amount")
    normalized = format(amount, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized


def _parse(content: bytes, key: str) -> list[object]:
    if not content:
        raise InputRejected("file is empty")
    if len(content) > MAX_FILE_BYTES:
        raise InputRejected(f"file exceeds {MAX_FILE_BYTES} bytes")
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise InputRejected("file must be UTF-8 encoded") from exc
    if not text.strip():
        raise InputRejected("file is empty")

    suffix = PurePosixPath(key).suffix.lower()
    if suffix == ".json":
        try:
            parsed = json.loads(text, parse_float=Decimal, parse_int=Decimal)
        except (json.JSONDecodeError, RecursionError) as exc:
            raise InputRejected("malformed JSON") from exc
        if not isinstance(parsed, list):
            raise InputRejected("JSON input must be an array of transaction objects")
        records = parsed
    elif suffix == ".csv":
        try:
            reader = csv.DictReader(io.StringIO(text, newline=""), strict=True)
            if reader.fieldnames is None:
                raise InputRejected("CSV must include a header row")
            if None in reader.fieldnames or any(not name.strip() for name in reader.fieldnames):
                raise InputRejected("CSV contains an empty column name")
            records = list(reader)
        except csv.Error as exc:
            raise InputRejected("malformed CSV") from exc
    else:
        raise InputRejected("unsupported file type; expected .csv or .json")

    if not records:
        raise InputRejected("file contains no records")
    if len(records) > MAX_RECORDS:
        raise InputRejected(f"file exceeds {MAX_RECORDS} records")
    return records


def process_file(content: bytes, key: str) -> ProcessingResult:
    """Return normalized valid records and per-record validation errors."""
    raw_records = _parse(content, key)
    valid: list[dict[str, str]] = []
    errors: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for index, raw in enumerate(raw_records, start=1):
        record_errors: list[str] = []
        if not isinstance(raw, dict):
            errors.append({"index": index, "errors": ["record must be an object"]})
            continue

        record_id = raw.get("record_id")
        customer_id = raw.get("customer_id")
        if not isinstance(record_id, str) or not record_id.strip():
            record_errors.append("record_id must be a non-empty string")
            normalized_id = None
        else:
            normalized_id = record_id.strip()
            if normalized_id in seen_ids:
                errors.append(
                    {
                        "index": index,
                        "record_id": normalized_id,
                        "errors": ["duplicate record_id; first occurrence wins"],
                    }
                )
                continue
            seen_ids.add(normalized_id)

        if not isinstance(customer_id, str) or not customer_id.strip():
            record_errors.append("customer_id must be a non-empty string")
        try:
            amount = _decimal_text(raw.get("amount"))
        except ValueError as exc:
            record_errors.append(f"amount {exc}")

        for field in REQUIRED_FIELDS:
            if field not in raw:
                message = f"missing required field: {field}"
                if message not in record_errors:
                    record_errors.append(message)

        if record_errors:
            error: dict[str, Any] = {"index": index, "errors": record_errors}
            if normalized_id is not None:
                error["record_id"] = normalized_id
            errors.append(error)
            continue
        valid.append(
            {
                "record_id": normalized_id,
                "customer_id": customer_id.strip(),
                "amount": amount,
            }
        )

    return ProcessingResult(valid, errors, len(raw_records))
