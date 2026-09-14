---
name: sdd-start
description: Start and complete a single SDD task from a per-spec index, implementing only its scoped files and committing code plus SDD state in the worktree.
---

# SDD Start

Use this skill when the user asks to run `sdd-start`, pick up a task, or
implement a single SDD task.

Codex invocation: `$sdd-start TASK-NNN` or `$sdd-start <task-slug>`.

## Purpose

Resolve one task, validate dependencies, mark it in progress, implement it,
validate it, and close it in the same branch/worktree.

## Guardrails

- Do not start blocked tasks.
- Do not restart `done` tasks.
- Confirm before resuming an `in-progress` task unless the user explicitly asked
  to resume.
- Code and per-spec index state live together in the worktree.
- Touch only files listed in the task unless the user approves a scope update.
- Use `scripts/sdd/close_task.sh`; do not hand-copy active tasks to completed.
- Commit only scoped files at each step.

## Workflow

1. Resolve task:
   - scan `sdd/tasks/index/*.json`, excluding `_orphans.json`
   - match task `id` or `slug`
   - if no input, run the equivalent of `$sdd-next` and ask the user to choose
2. Validate:
   - status must be `pending`, or explicitly confirmed `in-progress`
   - every dependency in `depends_on` must be `done`
3. Detect context:
   - note current branch
   - prefer running inside `.claude/worktrees/`
   - if outside a worktree, confirm the current branch is intentional
4. Mark in progress:
   - update only the task entry in `sdd/tasks/index/<feature>.json`
   - set `status: in-progress`
   - set `started_at`
   - clear staging, stage only the index, verify cached names
   - commit `sdd: start TASK-NNN - <title>`
5. Read context:
   - full task file
   - referenced spec file
   - scope
   - files to create/modify
   - Codebase Contract
   - implementation notes
   - acceptance criteria
   - test specification
6. Print kickoff summary, then continue immediately.
7. Verify Codebase Contract before editing:
   - confirm every import exists
   - confirm every signature still matches
   - respect "Does NOT Exist"
   - update the task contract first if a verified reference is stale
8. Implement:
   - edit/create only task-scoped files
   - use specified classes, methods, signatures, and patterns
   - follow repo instructions in `AGENTS.md` and `.agent/CONTEXT.md`

#### Delegated implementation (only when the task has `## Delegation Contract`)

Use this branch ONLY when the task file contains a `## Delegation Contract`
section AND the `parrot-targeted-writer` MCP server is available. Otherwise
implement the task yourself — the normal route is the default.

1. Call MCP tool `writer_generate` (server `parrot-targeted-writer`) with `task_path`.
2. On `status: error` with a contract code (`stale_target`, `missing_block`,
   `placeholder_code`, `underspecified_create`, …): fix the packet in the task file
   (refresh hashes with `sha256sum`, complete the design) and retry once, or implement
   the task yourself. The workflow **never silently invokes another coder** — no other
   coding tool is substituted when delegation fails.
3. On `ok`: read `data.patch_path` with `source_read` in ranges of at most 350 lines and
   review EVERY hunk against the task's Codebase Contract. Never apply a patch you have
   not fully read. If a hunk is wrong, do not apply: fix the packet/blocks and regenerate
   at most once more, else implement normally.
4. Call `writer_apply` with `artifact_id` and `reviewed_sha256 = data.patch_sha256`
   (verify it equals `sha256sum artifacts/tool-optimizations/<id>/patch.diff`).
5. Run the task's acceptance tests yourself. The writer never runs tests; a model's claim
   that tests passed is not execution evidence.
6. Continue with the normal validate → commit → SDD state steps. SDD files
   (`sdd/tasks/index/*.json`, task files) are never edited by the writer.

9. Validate:
   - run task-specified tests
   - run task-specified lint/format checks
   - store test logs in `artifacts/logs/` when logs are persisted
   - fix failures within scope
   - stop after three failed attempts if the failure is not understood
10. Commit code:
    - clear staging with `git reset HEAD`
    - stage only files listed for the task
    - verify cached names
    - commit `feat(<feature-slug>): TASK-NNN - <title>`
11. Close task:
    - run `scripts/sdd/close_task.sh TASK-NNN <feature-slug> verified`
    - fill Completion Note in the completed task file
    - verify cached names include only the per-spec index and moved task file
    - commit `sdd: complete TASK-NNN - <title>`
12. Report next task or suggest `$sdd-done FEAT-NNN` when complete.

## Stop Conditions

Stop and report if:

- SDD files are missing.
- dependency state is inconsistent.
- task contradicts the spec.
- required changes are outside task scope.
- a necessary import/signature cannot be verified.
- tests still fail after three focused attempts.
- implementation diverges from file/class/interface fidelity.

## References

- `sdd/templates/task.md`
- `sdd/tasks/index/`
- `scripts/sdd/close_task.sh`
- `sdd/WORKFLOW.md`

