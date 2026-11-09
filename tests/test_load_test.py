from scripts.load_test import verify_results


def test_load_test_verification_reports_success():
    expected = {
        "job": {
            "status": "COMPLETED",
            "counts": {"total": 1, "valid": 1, "invalid": 0},
        }
    }
    jobs = {
        "job": {
            "job_id": "job",
            "status": "COMPLETED",
            "total_count": 1,
            "valid_count": 1,
            "invalid_count": 0,
        }
    }
    reports = {
        "job": {
            "job_id": "job",
            "status": "COMPLETED",
            "counts": {"total": 1, "valid": 1, "invalid": 0},
        }
    }
    assert verify_results(expected, jobs, reports) == {
        "missing_jobs": [],
        "unresolved_jobs": [],
        "mismatches": [],
    }


def test_load_test_verification_never_hides_missing_or_unresolved_work():
    expected = {
        "missing": {"status": "REJECTED", "counts": {}},
        "running": {"status": "COMPLETED", "counts": {}},
    }
    jobs = {"running": {"status": "PROCESSING"}}
    result = verify_results(expected, jobs, {})
    assert result["missing_jobs"] == ["missing"]
    assert result["unresolved_jobs"] == ["running"]
