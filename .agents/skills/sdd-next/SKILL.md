---
name: sdd-next
description: Suggest next unblocked SDD tasks to assign across per-spec indexes.
---

# SDD Next

Use this skill when the user asks what task to work on next, runs `sdd-next`, or needs unblocked SDD assignments.

Invocation: `sdd-next`.

## Purpose

Inspect all per-spec indexes (`sdd/tasks/index/*.json`), identify tasks whose dependencies are satisfied, and suggest optimal work assignments with worktree commands.

## Guardrails

- Read-only: does not modify index or task files.
- Skip `sdd/tasks/index/_orphans.json` (orphans are displayed only by `sdd-status`).
- Only suggest tasks with `status: "pending"` where all `depends_on` tasks are `"done"`.

## Workflow

1. Aggregate tasks:
   - Read all `sdd/tasks/index/*.json` excluding `_orphans.json`.
2. Inspect worktrees:
   - Run `git worktree list` to match active feature worktrees.
3. Compute unblocked tasks:
   - Check `status == "pending"`.
   - Verify every ID in `depends_on` has `status == "done"`.
4. Group & annotate:
   - Tasks with active worktree: suggest running `sdd-start TASK-NNN` inside the worktree.
   - Tasks needing new worktree: provide `git worktree add` command.
   - Parallel tasks: indicate `parallel: true` tasks that can run concurrently.
5. Sort:
   - Priority (high -> medium -> low), then effort (S -> M -> L -> XL).
6. Present list and show currently in-progress tasks.

## References

- `sdd/tasks/index/*.json`
- `sdd/WORKFLOW.md`
