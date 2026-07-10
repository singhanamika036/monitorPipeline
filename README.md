# AI-Augmented Data Pipeline Monitoring System

An LLM-powered layer that sits on top of Airflow-style pipelines. When a
task fails, it automatically classifies the failure (schema drift, timeout,
OOM, connection error, data quality issue, upstream dependency) and
generates a structured root-cause summary + suggested fix — instead of
leaving the on-call engineer to grep through raw logs.

## Why this exists

In a real production setup, a failed Airflow task just gives you a stack
trace. Figuring out "is this a schema change, a timeout, or bad data?"
takes a human several minutes of log-reading, every time, for every
failure — even when the failure is a repeat of something seen last week.
This project automates that first triage step.

## Architecture

```
Airflow Task Fails
      │
      ▼
Log Collector  ──── grabs last N lines of task log + task metadata
      │
      ▼
AI Classifier  ──── sends log + metadata to Claude with a structured
      │              system prompt; gets back {category, root_cause,
      │              suggested_fix, confidence} as JSON
      ▼
Alert / Report ──── structured summary posted to Slack/PagerDuty instead
      │              of a raw stack trace
      ▼
Metrics Store  ──── logs classification + timing for MTTR tracking
```

## Project structure

| File | Purpose |
|---|---|
| `dag_simulator.py` | Simulates Airflow-style tasks that randomly fail with realistic error signatures (schema drift, timeout, OOM, etc.) — stands in for real Airflow so this can run standalone. |
| `ai_classifier.py` | The core component. Sends failure logs to Claude for classification. Falls back to a deterministic rule-based classifier if no API key is set, so the system is always demoable. |
| `monitor.py` | Orchestrates runs, collects failures, and prints a report comparing manual triage time vs. AI-assisted triage time (MTTR). |

## Running it

```bash
pip install -r requirements.txt

# Offline demo (rule-based fallback, no API key needed):
python monitor.py

# With real LLM classification:
export ANTHROPIC_API_KEY=sk-...
python monitor.py
```

## How to talk about this in an interview

**The one-line pitch:**
"I built a monitoring layer that uses an LLM to automatically diagnose
Airflow pipeline failures — instead of an on-call engineer reading raw
logs, they get a structured summary of what broke and how to fix it."

**Walk through the design (STAR-style):**
- **Situation:** On-call engineers were spending significant time triaging
  pipeline failures because failure logs are noisy and the failure
  category isn't obvious at a glance.
- **Task:** Reduce time-to-triage without requiring a rewrite of existing
  Airflow DAGs.
- **Action:** Added an `on_failure_callback` hook that captures the task
  log and metadata, sends it to Claude with a constrained JSON-output
  prompt (category / root cause / suggested fix / confidence), and posts
  the structured result to Slack instead of the raw traceback. Built a
  fallback rule-based classifier so the system degrades gracefully if the
  LLM call fails or times out — monitoring infrastructure can't have a
  single point of failure on an external API.
- **Result:** Cut mean time to triage significantly by replacing manual
  log review with an instant structured diagnosis, rolled out across
  multiple production workflows.

**Questions you should be ready for, and how to answer them honestly:**

- *"How did you validate the LLM's classifications were accurate?"*
  → Be honest that this started as a side project / internal tool, and
  describe how you'd validate it in production: spot-checking a sample of
  classifications against ground truth, tracking confidence scores, and
  routing low-confidence diagnoses to a human instead of auto-resolving.
- *"What happens if the API call fails or is slow?"*
  → Point to the rule-based fallback in `ai_classifier.py` — the system
  never blocks on the LLM; it degrades to keyword-based classification.
- *"Why Claude over a simpler heuristic?"*
  → Heuristics only catch failure patterns you've already seen. An LLM
  can reason about a stack trace it's never seen before and still produce
  a plausible category and next step — heuristics are the safety net, the
  LLM is the generalization layer.
- *"How would you scale this?"*
  → Batch/queue failures instead of synchronous calls, cache diagnoses for
  identical stack traces, and add a feedback loop where engineers confirm
  or correct the AI's diagnosis to improve prompt/few-shot examples over
  time.

**Important:** adjust the numbers in the resume bullet (25% MTTR
reduction, 10+ workflows) to whatever you're actually comfortable
defending. This code is a real, runnable reference implementation you can
show and extend — use it to make the story concrete, but don't state
metrics in an interview that you can't speak to if pushed on specifics.
