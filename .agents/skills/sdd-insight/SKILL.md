---
name: sdd-insight
description: Analyze collaboration transcripts and repo-level SDD Process Discipline from the sdd/ artifact tree.
---

# SDD Insight

Use this skill when the user asks to analyze AI fluency, review prompt collaboration patterns, or evaluate repo-level SDD Process Discipline adherence.

Invocation: `sdd-insight [TRANSCRIPT_PATH | --no-open]`.

## Purpose

Execute `scripts/sdd/insight.py` to produce a two-layer analytical report:
1. Personal AI-fluency skill map (score, archetype, competencies, growth levers) from transcripts.
2. Repo-level SDD Process Discipline panel (pipeline progression, decomposition quality, AC coverage, cycle closure, review rigor) computed deterministically from `sdd/`.

## Guardrails

- Transcripts and `sdd/` files are read strictly read-only.
- Always run within the virtual environment (`source .venv/bin/activate`).
- Reports are saved to `~/.claude/sdd-insight/sdd_insight_report.html` or workspace artifacts.

## Workflow

1. Clean previous run state in `~/.claude/sdd-insight/`.
2. Run deterministic measurement:
   ```bash
   python3 scripts/sdd/insight.py --evidence ~/.claude/sdd-insight/evidence.json --sdd-dir sdd --no-open --quiet -o ~/.claude/sdd-insight/sdd_insight_report.html
   ```
3. Run two-layer analysis if LLM workflow capability is available, or proceed with deterministic report.
4. Render final HTML report and present key findings:
   - Personal score and archetype.
   - Top growth lever.
   - Repo SDD Process Discipline score and weakest dimension.

## References

- `scripts/sdd/insight.py`
- `reference/sdd-insight/ai-fluency-framework.md`
- `sdd/WORKFLOW.md`
