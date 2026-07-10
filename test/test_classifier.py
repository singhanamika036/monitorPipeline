import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ai_classifier import FailureClassifier
from dag_simulator import FAILURE_SIGNATURES


def test_schema_drift_classification():
    clf = FailureClassifier(use_llm=False)
    diagnosis = clf.classify(
        task_id="transform_customer_dim",
        log_excerpt=FAILURE_SIGNATURES["schema_drift"],
        task_metadata={"dag": "orders_pipeline"},
    )
    assert diagnosis.category == "schema_drift"
    assert diagnosis.source == "rule_based_fallback"
    assert 0 <= diagnosis.confidence <= 1


def test_timeout_classification():
    clf = FailureClassifier(use_llm=False)
    diagnosis = clf.classify(
        task_id="load_snowflake_fact_orders",
        log_excerpt=FAILURE_SIGNATURES["timeout"],
        task_metadata={"dag": "orders_pipeline"},
    )
    assert diagnosis.category == "timeout"


def test_unknown_failure_falls_back_gracefully():
    clf = FailureClassifier(use_llm=False)
    diagnosis = clf.classify(
        task_id="mystery_task",
        log_excerpt="ERROR - something completely unrecognized happened",
        task_metadata={"dag": "orders_pipeline"},
    )
    assert diagnosis.category == "unknown"
    assert diagnosis.confidence < 0.5


def test_all_failure_signatures_are_classifiable():
    clf = FailureClassifier(use_llm=False)
    for category, log in FAILURE_SIGNATURES.items():
        diagnosis = clf.classify(task_id="t", log_excerpt=log, task_metadata={})
        assert diagnosis.category == category, f"expected {category}, got {diagnosis.category}"


if __name__ == "__main__":
    test_schema_drift_classification()
    test_timeout_classification()
    test_unknown_failure_falls_back_gracefully()
    test_all_failure_signatures_are_classifiable()
    print("All tests passed.")
