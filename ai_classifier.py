"""
ai_classifier.py

Classifies pipeline task failures using an LLM (Claude). Given a failure log
and task metadata, returns a structured diagnosis: category, likely root
cause, suggested fix, and a confidence score.

If ANTHROPIC_API_KEY is not set, falls back to a deterministic rule-based
classifier so the rest of the system (metrics, alerting, reporting) can
still be demoed end-to-end without network access or an API key.
"""

import json
import os
import re
from dataclasses import dataclass, asdict


@dataclass
class Diagnosis:
    category: str
    root_cause: str
    suggested_fix: str
    confidence: float
    source: str  # "llm" or "rule_based_fallback"

    def to_dict(self):
        return asdict(self)


SYSTEM_PROMPT = """You are an expert data platform SRE. You will be given a
failed Airflow task's log output and metadata. Respond ONLY with a JSON
object with exactly these keys:
  "category": one of ["schema_drift", "timeout", "resource_exhaustion",
              "upstream_dependency", "data_quality", "connection_error", "unknown"]
  "root_cause": one sentence, specific, plain-English explanation
  "suggested_fix": one sentence, actionable next step for the on-call engineer
  "confidence": a float between 0 and 1

No prose, no markdown fences, JSON only.
"""


class FailureClassifier:
    def __init__(self, use_llm: bool | None = None):
        """
        use_llm=None -> auto-detect based on ANTHROPIC_API_KEY env var.
        use_llm=True/False -> force on/off (False is useful for offline demos/tests).
        """
        self.api_key = os.environ.get("ANTHROPIC_API_KEY")
        self.use_llm = use_llm if use_llm is not None else bool(self.api_key)

    def classify(self, task_id: str, log_excerpt: str, task_metadata: dict) -> Diagnosis:
        if self.use_llm:
            try:
                return self._classify_with_llm(task_id, log_excerpt, task_metadata)
            except Exception:
                # Network/API issues shouldn't take down the monitoring layer itself.
                return self._classify_with_rules(task_id, log_excerpt, task_metadata, source="rule_based_fallback")
        return self._classify_with_rules(task_id, log_excerpt, task_metadata, source="rule_based_fallback")

    def _classify_with_llm(self, task_id, log_excerpt, task_metadata) -> Diagnosis:
        import anthropic

        client = anthropic.Anthropic(api_key=self.api_key)
        user_content = (
            f"Task ID: {task_id}\n"
            f"Task metadata: {json.dumps(task_metadata)}\n"
            f"Log excerpt:\n{log_excerpt}"
        )
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=300,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_content}],
        )
        text = resp.content[0].text.strip()
        text = re.sub(r"^```json|```$", "", text).strip()
        data = json.loads(text)
        return Diagnosis(
            category=data["category"],
            root_cause=data["root_cause"],
            suggested_fix=data["suggested_fix"],
            confidence=float(data["confidence"]),
            source="llm",
        )

    def _classify_with_rules(self, task_id, log_excerpt, task_metadata, source) -> Diagnosis:
        """Deterministic keyword-based fallback. Not as nuanced as the LLM
        path, but keeps the system fully functional offline."""
        log_lower = log_excerpt.lower()

        rules = [
            ("upstream_dependency", ["sensor timeout", "dependency not met", "upstream task failed"],
             "An upstream task or sensor did not complete in time.",
             "Check upstream DAG status before re-triggering this task."),
            ("timeout", ["timeout", "timed out", "deadline exceeded"],
             "Task exceeded its allotted execution window.",
             "Increase task timeout or investigate upstream slowness."),
            ("resource_exhaustion", ["out of memory", "oom", "memoryerror", "disk full"],
             "Task ran out of memory or disk during execution.",
             "Increase worker resources or optimize the transform for lower memory use."),
            ("schema_drift", ["column not found", "schema mismatch", "keyerror", "field not found"],
             "An expected column/field was missing or renamed upstream.",
             "Check upstream schema changes and update the pipeline's expected schema."),
            ("connection_error", ["connection refused", "connection reset", "could not connect", "timeout connecting"],
             "The task could not establish a connection to a downstream/upstream service.",
             "Verify service health, credentials, and network connectivity."),
            ("data_quality", ["null constraint", "validation failed", "unexpected null", "duplicate key"],
             "Incoming data failed a validation or integrity check.",
             "Inspect the source data batch and quarantine invalid records."),
        ]

        for category, keywords, root_cause, fix in rules:
            if any(k in log_lower for k in keywords):
                return Diagnosis(category, root_cause, fix, 0.75, source)

        return Diagnosis(
            category="unknown",
            root_cause="Failure did not match a known pattern.",
            suggested_fix="Escalate to the pipeline owner for manual log review.",
            confidence=0.3,
            source=source,
        )
