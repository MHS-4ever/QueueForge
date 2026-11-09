from queueforge.faults import FAULT_PREFIX, fault_point


def test_faults_are_disabled_by_default_and_isolated_to_fixed_prefix():
    assert fault_point(False, f"{FAULT_PREFIX}persistent/a.json", 1) is None
    assert fault_point(True, "incoming/normal.json", 1) is None


def test_first_attempt_fault_then_success():
    key = f"{FAULT_PREFIX}first/a.json"
    assert fault_point(True, key, 1) == "before_processing"
    assert fault_point(True, key, 2) is None


def test_persistent_and_after_report_faults():
    assert fault_point(True, f"{FAULT_PREFIX}persistent/a.json", 5) == "before_processing"
    key = f"{FAULT_PREFIX}after-report/a.json"
    assert fault_point(True, key, 1) == "after_report"
    assert fault_point(True, key, 2) is None
