---
name: sdd-codereview
description: Code review a completed SDD task against acceptance criteria, code quality, security, and adversarial cross-checks.
---

# SDD Code Review

Use this skill when the user asks to review a completed SDD task, run `sdd-codereview`, or perform an adversarial code review on completed task artifacts.

## Purpose

Reads the task file from `sdd/tasks/completed/`, loads all referenced source files and the parent spec, applies code review criteria (Correctness, Code Quality, Performance, Security, Documentation, Testing), and produces a structured review report.

## Guardrails

- Reads strictly completed tasks from `sdd/tasks/completed/`.
- Verify the chain of thought and code anchors: check actual files, classes, methods, and line ranges.
- Treat reviewer second-opinions as advisory: confirm, reject, or escalate each finding.
- Never report an unverifiable claim as a finding.

## Workflow

1. Resolve task:
   - Accept full path, `TASK-NNN`, or slug.
   - Match against `sdd/tasks/completed/TASK-*.md`.
2. Load context:
   - Read the task markdown file.
   - Read the referenced spec in `sdd/specs/`.
   - Read every file in the task's "Files to Create/Modify" section.
   - Read the Acceptance Criteria and Completion Note.
3. Review criteria:
   - **Correctness & Logic**: Acceptance criteria satisfaction, edge cases, error paths.
   - **Code Quality**: DRY, SOLID, abstraction seams, framework patterns.
   - **Performance**: N+1 queries, async blocking calls, algorithmic efficiency.
   - **Security**: Input validation, SQL/injection risks, credentials, auth.
   - **Documentation**: Docstrings, type annotations, clarity.
   - **Testing**: Test coverage against criteria, assertions, failure modes.
4. Adversarial cross-check:
   - Run external reviewer (`codex exec review`) or spawn a dedicated read-only subagent.
   - Supply only neutral brief: requirements, diff/commit, question.
   - Synthesize agreements and disagreements.
5. Generate report:
   - Summary, Critical, Major, Minor/Suggestions.
   - Acceptance Criteria check table.
   - Adversarial cross-check disposition table.
   - Positive highlights.
6. Optional save:
   - Save to `sdd/reviews/TASK-<NNN>-review.md` if requested.

## References

- `sdd/tasks/completed/`
- `sdd/tasks/index/<feature>.json`
- `sdd/WORKFLOW.md`
