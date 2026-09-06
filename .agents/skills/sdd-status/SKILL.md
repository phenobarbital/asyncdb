---
name: sdd-status
description: Aggregate task state across per-spec indexes and display the SDD task board.
---

# SDD Status

Use this skill when the user asks for SDD status, runs `sdd-status`, or wants to see the task board.

Invocation: `sdd-status [<feature-name>]`.

## Purpose

Aggregate task states across all per-spec indexes (`sdd/tasks/index/*.json`) and display a clear, human-friendly status report.

## Guardrails

- Read-only: never modifies any files.
- Honors the four exact status states: `in-progress`, `pending`, `done-with-issues`, `done`.
- Displays orphans from `_orphans.json` in a dedicated panel.

## Workflow

1. Load all per-spec indexes:
   - Glob `sdd/tasks/index/*.json`.
   - Filter by feature slug or `FEAT-NNN` if argument is provided.
2. Group tasks by feature and status:
   - `in-progress` (🔄)
   - `pending` (⏳)
   - `done-with-issues` (⚠️)
   - `done` (✅)
3. Highlight blockers:
   - Identify pending tasks blocked by incomplete dependencies.
4. Surface orphans:
   - If `sdd/tasks/index/_orphans.json` has entries, display them in an Unowned Tasks panel.
5. Print summary totals (done, done-with-issues, in-progress, pending, total).

## References

- `sdd/tasks/index/*.json`
- `sdd/WORKFLOW.md`
