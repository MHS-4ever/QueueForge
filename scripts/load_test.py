"""Manually invoked 100-file QueueForge acceptance test; never imported by Lambda."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3

from queueforge.jobs import TERMINAL_STATES, make_job_id


def _case(index: int) -> tuple[str, bytes, str, dict[str, int]]:
    kind = index % 5
    if kind == 0:
        return (
            "valid_csv",
            f"record_id,customer_id,amount\nr-{index},c-{index},10.50\n".encode(),
            "COMPLETED",
            {"total": 1, "valid": 1, "invalid": 0},
        )
    if kind == 1:
        body = [{"record_id": f"r-{index}", "customer_id": f"c-{index}", "amount": 2}]
        return (
            "valid_json",
            json.dumps(body).encode(),
            "COMPLETED",
            {"total": 1, "valid": 1, "invalid": 0},
        )
    if kind == 2:
        body = (
            f"record_id,customer_id,amount\nr-{index}-ok,c-{index},3\nr-{index}-bad,c-{index},-1\n"
        )
        return (
            "mixed_csv",
            body.encode(),
            "PARTIAL_SUCCESS",
            {"total": 2, "valid": 1, "invalid": 1},
        )
    if kind == 3:
        return (
            "malformed_json",
            b"{",
            "REJECTED",
            {"total": 0, "valid": 0, "invalid": 0},
        )
    record_id = f"r-{index}"
    body = [
        {"record_id": record_id, "customer_id": f"c-{index}", "amount": 4},
        {"record_id": record_id, "customer_id": f"c-{index}-dup", "amount": 5},
    ]
    return (
        "duplicate_json",
        json.dumps(body).encode(),
        "PARTIAL_SUCCESS",
        {"total": 2, "valid": 1, "invalid": 1},
    )


def _batch_jobs(table: Any, job_ids: list[str]) -> dict[str, dict[str, Any]]:
    request = {
        table.name: {"Keys": [{"job_id": job_id} for job_id in job_ids], "ConsistentRead": True}
    }
    found: dict[str, dict[str, Any]] = {}
    attempts = 0
    while request and attempts < 5:
        response = table.meta.client.batch_get_item(RequestItems=request)
        for item in response.get("Responses", {}).get(table.name, []):
            found[item["job_id"]] = item
        request = response.get("UnprocessedKeys", {})
        attempts += 1
        if request:
            time.sleep(0.2 * attempts)
    return found


def verify_results(
    expected: dict[str, dict[str, Any]],
    jobs: dict[str, dict[str, Any]],
    reports: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    missing = sorted(set(expected) - set(jobs))
    unresolved = sorted(
        job_id for job_id, item in jobs.items() if item.get("status") not in TERMINAL_STATES
    )
    mismatches = []
    for job_id, wanted in expected.items():
        job = jobs.get(job_id)
        if not job:
            continue
        actual_counts = {
            "total": int(job.get("total_count", -1)),
            "valid": int(job.get("valid_count", -1)),
            "invalid": int(job.get("invalid_count", -1)),
        }
        if job.get("status") != wanted["status"] or actual_counts != wanted["counts"]:
            mismatches.append(
                {
                    "job_id": job_id,
                    "expected_status": wanted["status"],
                    "actual_status": job.get("status"),
                    "expected_counts": wanted["counts"],
                    "actual_counts": actual_counts,
                }
            )
            continue
        report = reports.get(job_id)
        if (
            not report
            or report.get("job_id") != job_id
            or report.get("status") != wanted["status"]
            or report.get("counts") != wanted["counts"]
        ):
            mismatches.append({"job_id": job_id, "error": "result report mismatch or missing"})
    return {"missing_jobs": missing, "unresolved_jobs": unresolved, "mismatches": mismatches}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="required acknowledgement")
    parser.add_argument("--bucket", default=os.environ.get("QUEUEFORGE_BUCKET"))
    parser.add_argument("--table", default=os.environ.get("QUEUEFORGE_TABLE"))
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--summary", type=Path, default=Path("build/load-test-summary.json"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not args.execute:
        raise SystemExit("refusing live uploads without --execute")
    if not args.bucket or not args.table:
        raise SystemExit("set --bucket/QUEUEFORGE_BUCKET and --table/QUEUEFORGE_TABLE")
    if args.timeout <= 0 or args.interval <= 0:
        raise SystemExit("timeout and interval must be positive")

    session = boto3.Session(region_name=args.region)
    s3 = session.client("s3")
    table = session.resource("dynamodb").Table(args.table)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    started = time.monotonic()
    expected: dict[str, dict[str, Any]] = {}
    files = []

    for index in range(100):
        case, content, status, counts = _case(index)
        suffix = ".csv" if "csv" in case else ".json"
        key = f"incoming/acceptance/{run_id}/{index:03d}-{case}{suffix}"
        response = s3.put_object(Bucket=args.bucket, Key=key, Body=content)
        version_id = response.get("VersionId")
        if not version_id:
            raise RuntimeError(f"upload returned no VersionId for {key}")
        job_id = make_job_id(args.bucket, key, version_id)
        expected[job_id] = {"status": status, "counts": counts, "key": key, "case": case}
        files.append({"job_id": job_id, **expected[job_id], "version_id": version_id})

    upload_seconds = round(time.monotonic() - started, 3)
    duplicate_job_ids = sorted(
        job_id for job_id in expected if sum(f["job_id"] == job_id for f in files) > 1
    )
    deadline = time.monotonic() + args.timeout
    jobs: dict[str, dict[str, Any]] = {}
    while time.monotonic() < deadline:
        jobs = _batch_jobs(table, list(expected))
        if len(jobs) == 100 and all(
            item.get("status") in TERMINAL_STATES for item in jobs.values()
        ):
            break
        time.sleep(args.interval)
    if len(jobs) < 100 or any(item.get("status") not in TERMINAL_STATES for item in jobs.values()):
        jobs = _batch_jobs(table, list(expected))
    timed_out = not (
        len(jobs) == 100 and all(item.get("status") in TERMINAL_STATES for item in jobs.values())
    )

    reports = {}
    for job_id, item in jobs.items():
        if item.get("status") in TERMINAL_STATES and item.get("result_key"):
            body = s3.get_object(
                Bucket=item.get("result_bucket", args.bucket), Key=item["result_key"]
            )["Body"].read()
            reports[job_id] = json.loads(body)
    verification = verify_results(expected, jobs, reports)
    total_seconds = round(time.monotonic() - started, 3)
    summary = {
        "run_id": run_id,
        "bucket": args.bucket,
        "table": args.table,
        "expected_files": 100,
        "uploaded_files": len(files),
        "terminal_jobs": sum(item.get("status") in TERMINAL_STATES for item in jobs.values()),
        "timed_out": timed_out,
        "duplicate_job_ids": duplicate_job_ids,
        "measurements": {
            "upload_seconds": upload_seconds,
            "processing_and_verification_seconds": round(total_seconds - upload_seconds, 3),
            "total_seconds": total_seconds,
        },
        **verification,
        "files": files,
    }
    summary["passed"] = not (
        timed_out
        or duplicate_job_ids
        or verification["missing_jobs"]
        or verification["unresolved_jobs"]
        or verification["mismatches"]
    )
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    args.summary.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(
        f"QueueForge acceptance {run_id}: "
        f"{summary['terminal_jobs']}/100 terminal, passed={summary['passed']}, "
        f"elapsed={total_seconds}s"
    )
    print(f"Machine-readable summary: {args.summary}")
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
