"""Minimal operator CLI for QueueForge."""

from __future__ import annotations

import argparse
import json
import os
from decimal import Decimal
from pathlib import Path
from typing import Any

from .jobs import make_job_id
from .recovery import inspect_failures, recover_job, stale_jobs, start_dlq_redrive


def _json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral() else str(value)
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _print(value: object) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, default=_json_default))


def _settings(args: argparse.Namespace) -> tuple[str, str, str, str | None, str | None]:
    bucket = args.bucket or os.environ.get("QUEUEFORGE_BUCKET")
    table = args.table or os.environ.get("QUEUEFORGE_TABLE")
    region = args.region or os.environ.get("AWS_REGION", "us-east-1")
    dlq_url = args.dlq_url or os.environ.get("QUEUEFORGE_DLQ_URL")
    processing_queue_url = args.processing_queue_url or os.environ.get(
        "QUEUEFORGE_PROCESSING_QUEUE_URL"
    )
    if not bucket or not table:
        raise SystemExit("set --bucket/QUEUEFORGE_BUCKET and --table/QUEUEFORGE_TABLE")
    return bucket, table, region, dlq_url, processing_queue_url


def _clients(region: str) -> tuple[Any, Any, Any]:
    import boto3

    session = boto3.Session(region_name=region)
    return session.client("s3"), session.resource("dynamodb"), session.client("sqs")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="queueforge")
    parser.add_argument("--bucket")
    parser.add_argument("--table")
    parser.add_argument("--region")
    parser.add_argument("--dlq-url")
    parser.add_argument("--processing-queue-url")
    commands = parser.add_subparsers(dest="command", required=True)
    upload = commands.add_parser("upload")
    upload.add_argument("file", type=Path)
    upload.add_argument("--key")
    jobs = commands.add_parser("jobs")
    jobs.add_argument("--limit", type=int, default=25)
    status = commands.add_parser("status")
    status.add_argument("job_id")
    report = commands.add_parser("report")
    report.add_argument("job_id")
    failures = commands.add_parser("failures")
    failures.add_argument("--limit", type=int, default=10)
    reconcile = commands.add_parser("reconcile")
    reconcile.add_argument("--limit", type=int, default=100)
    recover = commands.add_parser("recover")
    mode = recover.add_mutually_exclusive_group(required=True)
    mode.add_argument("--job-id")
    mode.add_argument("--redrive-dlq", action="store_true")
    recover.add_argument("--confirm", required=True)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    bucket, table_name, region, dlq_url, processing_queue_url = _settings(args)
    s3, dynamodb, sqs = _clients(region)
    table = dynamodb.Table(table_name)

    if args.command == "upload":
        if not args.file.is_file():
            raise SystemExit(f"file not found: {args.file}")
        key = args.key or f"incoming/{args.file.name}"
        if not key.startswith("incoming/"):
            raise SystemExit("upload key must start with incoming/")
        with args.file.open("rb") as body:
            response = s3.put_object(Bucket=bucket, Key=key, Body=body)
        version_id = response.get("VersionId")
        if not version_id:
            raise SystemExit("upload returned no VersionId; verify bucket versioning")
        _print(
            {
                "bucket": bucket,
                "key": key,
                "version_id": version_id,
                "job_id": make_job_id(bucket, key, version_id),
            }
        )
    elif args.command == "jobs":
        if not 1 <= args.limit <= 100:
            raise SystemExit("--limit must be between 1 and 100")
        response = table.scan(Limit=args.limit)
        _print({"jobs": response.get("Items", []), "count": response.get("Count", 0)})
    elif args.command == "status":
        item = table.get_item(Key={"job_id": args.job_id}, ConsistentRead=True).get("Item")
        if item is None:
            raise SystemExit(f"job not found: {args.job_id}")
        _print(item)
    elif args.command == "report":
        item = table.get_item(Key={"job_id": args.job_id}, ConsistentRead=True).get("Item")
        if item is None:
            raise SystemExit(f"job not found: {args.job_id}")
        if not item.get("result_key"):
            raise SystemExit(f"job has no report: {item.get('status', 'UNKNOWN')}")
        report = s3.get_object(Bucket=item.get("result_bucket", bucket), Key=item["result_key"])[
            "Body"
        ].read()
        print(report.decode("utf-8"))
    elif args.command == "failures":
        if not dlq_url:
            raise SystemExit("set --dlq-url/QUEUEFORGE_DLQ_URL")
        _print({"failures": inspect_failures(sqs, dlq_url, table, args.limit)})
    elif args.command == "reconcile":
        if not dlq_url:
            raise SystemExit("set --dlq-url/QUEUEFORGE_DLQ_URL")
        _print(
            {
                "stale_processing_jobs": stale_jobs(table, args.limit),
                "dlq_failures": inspect_failures(sqs, dlq_url, table),
            }
        )
    elif args.command == "recover":
        if args.job_id:
            if args.confirm != args.job_id:
                raise SystemExit("targeted recovery requires --confirm <job-id>")
            if not processing_queue_url:
                raise SystemExit("set --processing-queue-url/QUEUEFORGE_PROCESSING_QUEUE_URL")
            _print(recover_job(table, sqs, processing_queue_url, args.job_id))
        else:
            if args.confirm != "REDRIVE-DLQ":
                raise SystemExit("queue-wide redrive requires --confirm REDRIVE-DLQ")
            if not dlq_url or not processing_queue_url:
                raise SystemExit("set both DLQ and processing queue URLs")
            response = start_dlq_redrive(sqs, dlq_url, processing_queue_url)
            _print({"redrive": "started", "task_handle": response.get("TaskHandle")})
