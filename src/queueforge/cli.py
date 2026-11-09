"""Minimal operator CLI for QueueForge."""

from __future__ import annotations

import argparse
import json
import os
from decimal import Decimal
from pathlib import Path
from typing import Any

from .jobs import make_job_id


def _json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral() else str(value)
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _print(value: object) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, default=_json_default))


def _settings(args: argparse.Namespace) -> tuple[str, str, str]:
    bucket = args.bucket or os.environ.get("QUEUEFORGE_BUCKET")
    table = args.table or os.environ.get("QUEUEFORGE_TABLE")
    region = args.region or os.environ.get("AWS_REGION", "us-east-1")
    if not bucket or not table:
        raise SystemExit("set --bucket/QUEUEFORGE_BUCKET and --table/QUEUEFORGE_TABLE")
    return bucket, table, region


def _clients(region: str) -> tuple[Any, Any]:
    import boto3

    session = boto3.Session(region_name=region)
    return session.client("s3"), session.resource("dynamodb")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="queueforge")
    parser.add_argument("--bucket")
    parser.add_argument("--table")
    parser.add_argument("--region")
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
    return parser


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    bucket, table_name, region = _settings(args)
    s3, dynamodb = _clients(region)
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
