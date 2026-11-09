"""Orchestrate exact-version reads, processing, durable reports, and finalization."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from .jobs import JobStore, make_job_id
from .processing import MAX_FILE_BYTES, InputRejected, ProcessingResult, process_file


def _aws_error(exc: Exception, code: str) -> bool:
    return getattr(exc, "response", {}).get("Error", {}).get("Code") == code


class ReportConflict(RuntimeError):
    """An existing report does not match the deterministic report for this job."""


def _report_bytes(
    job_id: str,
    source: dict[str, Any],
    result: ProcessingResult | None,
    rejection: str | None,
) -> tuple[bytes, str, dict[str, int]]:
    if result is None:
        status = "REJECTED"
        counts = {"total": 0, "valid": 0, "invalid": 0}
        records: list[dict[str, str]] = []
        errors: list[dict[str, Any]] = [{"errors": [rejection]}]
    else:
        status = result.status
        counts = {
            "total": result.total_records,
            "valid": len(result.records),
            "invalid": len(result.errors),
        }
        records = result.records
        errors = result.errors
    report = {
        "job_id": job_id,
        "source": source,
        "status": status,
        "counts": counts,
        "records": records,
        "errors": errors,
    }
    return (
        json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(),
        status,
        counts,
    )


def _write_report(s3: Any, bucket: str, key: str, content: bytes) -> None:
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=content,
            ContentType="application/json",
            IfNoneMatch="*",
        )
        return
    except Exception as exc:
        if not (_aws_error(exc, "PreconditionFailed") or _aws_error(exc, "412")):
            raise
    existing = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    if existing != content:
        raise ReportConflict(f"existing report differs at s3://{bucket}/{key}")


def process_object(
    s3: Any,
    jobs: JobStore,
    bucket: str,
    key: str,
    version_id: str,
    notification_size: int | None = None,
) -> tuple[str, str]:
    """Process one versioned object, returning (job_id, terminal status)."""
    if not version_id:
        raise ValueError("version_id is required for deterministic processing")
    source = {"bucket": bucket, "key": key, "version_id": version_id}
    job_id = make_job_id(bucket, key, version_id)
    jobs.ensure_pending(job_id, source)
    claim = jobs.claim(job_id)
    if not claim.acquired:
        return job_id, str(claim.terminal_status)

    result: ProcessingResult | None = None
    rejection: str | None = None
    try:
        if notification_size is not None and notification_size > MAX_FILE_BYTES:
            raise InputRejected(f"file exceeds {MAX_FILE_BYTES} bytes")
        response = s3.get_object(Bucket=bucket, Key=key, VersionId=version_id)
        content = response["Body"].read(MAX_FILE_BYTES + 1)
        result = process_file(content, key)
    except InputRejected as exc:
        rejection = str(exc)

    report, status, counts = _report_bytes(job_id, source, result, rejection)
    report_key = f"results/{job_id}.json"
    _write_report(s3, bucket, report_key, report)
    jobs.finalize(
        claim,
        status,
        {
            "total_count": Decimal(counts["total"]),
            "valid_count": Decimal(counts["valid"]),
            "invalid_count": Decimal(counts["invalid"]),
            "result_bucket": bucket,
            "result_key": report_key,
        },
    )
    return job_id, status
