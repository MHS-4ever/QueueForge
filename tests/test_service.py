import io
import json

from queueforge.jobs import Claim
from queueforge.service import process_object


class AwsError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self, source):
        self.source = source
        self.reports = {}
        self.get_calls = []

    def get_object(self, Bucket, Key, VersionId=None):
        self.get_calls.append((Bucket, Key, VersionId))
        if Key.startswith("results/"):
            return {"Body": io.BytesIO(self.reports[Key])}
        return {"Body": io.BytesIO(self.source)}

    def put_object(self, Bucket, Key, Body, **kwargs):
        if Key in self.reports and kwargs.get("IfNoneMatch") == "*":
            raise AwsError("PreconditionFailed")
        self.reports[Key] = Body


class FakeJobs:
    def __init__(self, terminal=None):
        self.terminal = terminal
        self.finalized = []

    def ensure_pending(self, job_id, source):
        self.job_id = job_id

    def claim(self, job_id):
        return Claim(job_id, "owner", self.terminal)

    def finalize(self, claim, status, fields):
        self.finalized.append((claim, status, fields))


def test_processes_exact_version_and_writes_report_before_completion():
    s3 = FakeS3(b'[{"record_id":"r1","customer_id":"c1","amount":"2.50"}]')
    jobs = FakeJobs()
    job_id, status = process_object(s3, jobs, "bucket", "incoming/a.json", "v1", 60)
    assert status == "COMPLETED"
    assert s3.get_calls[0] == ("bucket", "incoming/a.json", "v1")
    report = json.loads(s3.reports[f"results/{job_id}.json"])
    assert report["records"][0]["amount"] == "2.5"
    assert jobs.finalized[0][1] == "COMPLETED"


def test_validation_rejection_is_terminal_with_durable_report():
    s3 = FakeS3(b"{")
    jobs = FakeJobs()
    job_id, status = process_object(s3, jobs, "bucket", "incoming/a.json", "v1")
    assert status == "REJECTED"
    report = json.loads(s3.reports[f"results/{job_id}.json"])
    assert report["errors"] == [{"errors": ["malformed JSON"]}]
    assert jobs.finalized[0][1] == "REJECTED"


def test_safe_retry_after_identical_result_creation():
    source = b'[{"record_id":"r1","customer_id":"c1","amount":1}]'
    s3 = FakeS3(source)
    first_jobs = FakeJobs()
    job_id, _ = process_object(s3, first_jobs, "bucket", "incoming/a.json", "v1")
    second_jobs = FakeJobs()
    retry_id, status = process_object(s3, second_jobs, "bucket", "incoming/a.json", "v1")
    assert retry_id == job_id
    assert status == "COMPLETED"
    assert second_jobs.finalized[0][1] == "COMPLETED"


def test_terminal_duplicate_skips_s3_read_and_write():
    s3 = FakeS3(b"unused")
    jobs = FakeJobs(terminal="REJECTED")
    _, status = process_object(s3, jobs, "bucket", "incoming/a.json", "v1")
    assert status == "REJECTED"
    assert s3.get_calls == []
    assert s3.reports == {}
