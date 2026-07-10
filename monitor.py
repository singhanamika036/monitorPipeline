"""
monitor.py

Orchestrates the demo end-to-end:
  1. Runs a simulated pipeline across N cycles (like N days of production runs).
  2. On each task failure, calls the AI classifier to diagnose it.
  3. Tracks a simple MTTR (mean time to triage) model:
       - baseline: manual log review time, varies by failure type
       - ai_assisted: fixed, small overhead of reading a structured summary
  4. Prints a report: failures by category, classifier confidence, and the
     MTTR improvement the AI layer would produce over manual triage.

Run:
    python monitor.py                 # rule-based fallback (no API key needed)
    ANTHROPIC_API_KEY=sk-...  python monitor.py   # real LLM classification
"""

import random
import statistics
from collections import Counter, defaultdict

from dag_simulator import run_pipeline
from ai_classifier import FailureClassifier

# Rough manual-triage time (minutes) by failure category — an on-call
# engineer grepping logs to figure out "what even broke". These are
# illustrative estimates for the demo, not measured production data.
MANUAL_TRIAGE_MINUTES = {
    "schema_drift": 22,
    "timeout": 15,
    "resource_exhaustion": 18,
    "connection_error": 12,
    "data_quality": 25,
    "upstream_dependency": 10,
    "unknown": 30,
}

AI_ASSISTED_TRIAGE_MINUTES = 4  # time to read a structured AI summary and act


def simulate(num_cycles: int = 20, failure_rate: float = 0.35, seed: int = 42):
    random.seed(seed)
    classifier = FailureClassifier()  # auto-detects ANTHROPIC_API_KEY

    all_failures = []
    for _ in range(num_cycles):
        results = run_pipeline(failure_rate=failure_rate)
        for r in results:
            if not r.success:
                diagnosis = classifier.classify(
                    task_id=r.task_id,
                    log_excerpt=r.log,
                    task_metadata={"dag": r.dag},
                )
                all_failures.append((r, diagnosis))
    return all_failures, classifier


def print_report(all_failures, classifier):
    print("=" * 72)
    print("AI-AUGMENTED PIPELINE MONITORING — DEMO RUN REPORT")
    print("=" * 72)
    print(f"Classifier mode: {'LLM (Claude)' if classifier.use_llm else 'rule-based fallback (no API key set)'}")
    print(f"Total failures observed: {len(all_failures)}\n")

    by_category = Counter(d.category for _, d in all_failures)
    print("Failures by category:")
    for cat, count in by_category.most_common():
        print(f"  {cat:22s} {count}")

    print("\nSample diagnoses:")
    for r, d in all_failures[:5]:
        print(f"  [{r.task_id}] category={d.category} confidence={d.confidence:.2f} source={d.source}")
        print(f"      root cause : {d.root_cause}")
        print(f"      suggested  : {d.suggested_fix}")

    # MTTR comparison
    baseline_times = [MANUAL_TRIAGE_MINUTES.get(d.category, 30) for _, d in all_failures]
    ai_times = [AI_ASSISTED_TRIAGE_MINUTES for _ in all_failures]

    if baseline_times:
        baseline_avg = statistics.mean(baseline_times)
        ai_avg = statistics.mean(ai_times)
        improvement = (baseline_avg - ai_avg) / baseline_avg * 100

        print("\nMTTR (mean time to triage) comparison:")
        print(f"  Manual log review (baseline) : {baseline_avg:.1f} min/incident")
        print(f"  AI-assisted triage            : {ai_avg:.1f} min/incident")
        print(f"  Improvement                   : {improvement:.0f}% faster triage")
    print("=" * 72)


if __name__ == "__main__":
    failures, classifier = simulate(num_cycles=20, failure_rate=0.35)
    print_report(failures, classifier)
