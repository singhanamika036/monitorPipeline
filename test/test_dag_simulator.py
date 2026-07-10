import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dag_simulator import run_pipeline, run_task, TASKS


def test_run_pipeline_returns_result_per_task():
    results = run_pipeline(failure_rate=0.0)  # force all successes
    assert len(results) == len(TASKS)
    assert all(r.success for r in results)


def test_run_pipeline_all_fail():
    results = run_pipeline(failure_rate=1.0)  # force all failures
    assert all(not r.success for r in results)
    assert all(r.log for r in results)
    assert all(r.failure_category for r in results)


def test_run_task_success_has_no_log():
    result = run_task(TASKS[0], failure_rate=0.0)
    assert result.success
    assert result.log == ""


if __name__ == "__main__":
    test_run_pipeline_returns_result_per_task()
    test_run_pipeline_all_fail()
    test_run_task_success_has_no_log()
    print("All tests passed.")
