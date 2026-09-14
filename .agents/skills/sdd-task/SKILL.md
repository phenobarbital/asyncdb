---
name: sdd-task
description: Decompose an approved SDD spec into atomic task files and a per-spec task index, then create the feature or hotfix worktree.
---

# SDD Task

Use this skill when the user asks to run `sdd-task`, decompose an approved spec,
or create SDD task artifacts.

Codex invocation: `$sdd-task sdd/specs/<feature-slug>.spec.md`.

## Purpose

Create atomic, bounded, testable task artifacts from an approved spec and commit
them to the spec's base branch before creating a worktree.

## Guardrails

- Do not implement code.
- Only decompose an approved spec. If status is not `approved`, warn and ask
  for confirmation before proceeding.
- Must run from the main repo, not inside `.claude/worktrees/`.
- Must run on the spec's `base_branch`.
- Do not hand-compute `TASK-NNN` IDs for feature work.
- Use per-spec indexes under `sdd/tasks/index/`; ignore the historical
  monolithic `sdd/tasks/.index.json`.
- Commit only the task files and the per-spec index.

## Workflow

1. Read spec frontmatter with `scripts.sdd.sdd_meta.parse()`:
   - `type`
   - `base_branch`
2. Validate:
   - `hotfix` requires `main`
   - `feature` must not use `main`
   - `staging` is valid for feature stabilization during release freeze
3. Sync:
   - refuse if inside `.claude/worktrees/`
   - refuse dirty worktree
   - `git checkout <base_branch>`
   - `git pull --ff-only origin <base_branch>`
4. Read the full spec:
   - status
   - Feature ID or hotfix identity
   - title and slug
   - Module Breakdown
   - Test Specification
   - Acceptance Criteria
   - Codebase Contract
   - Worktree Strategy
5. Plan task decomposition:
   - one task per module, class, or distinct deliverable
   - target 1-4 hours per task
   - dependencies explicit
   - mark independent tasks with `parallel: true`
   - document `parallelism_notes`
6. For every task, build a task-specific Codebase Contract:
   - copy relevant verified imports/signatures from the spec
   - re-read each referenced file to verify freshness
   - add task-specific references for touched files
   - include "Does NOT Exist" entries

#### Delegation Contract (optional, per task)

Emit a `## Delegation Contract` packet ONLY for a task whose design is
complete. `design_complete: true` is a declaration the task author signs.

- List every target file with its `action` (`create`/`modify`), and give each
  `modify` target a REAL `expected_sha256` — compute it, never guess:
  `sha256sum <path>` or
  `python -c "import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" <path>`.
- Every `create` target needs a block tagged `path=<target>` holding the new
  file's full content; every referenced block id must exist in the task file.
- Never leave placeholders (`...`, `TODO`, `FIXME`, `XXX`, `<angle>`,
  `raise NotImplementedError`) in an implementation block — the validator
  rejects them and the packet is not delegated.
- Hashes are re-validated at execution time, after dependencies land. If they
  are stale then, the executor refreshes the packet in the task file FIRST and
  only then re-runs `writer_generate`.
- Omit the section entirely when the task is not eligible. Most tasks are not,
  and that is the normal, expected route.

7. Reserve task IDs:
   - For `type: feature`, run:
     `python -m scripts.sdd.reserve_ids --kind task --count <N> --base-branch <base_branch> --label <feature-slug>`.
   - Use returned IDs verbatim.
   - Stop if reservation fails.
   - For `type: hotfix`, do not reserve `TASK-NNN`; use local IDs
     `HOTFIX-<JIRA-KEY>-1`, `HOTFIX-<JIRA-KEY>-2`, and so on.
8. Create task files:
   - directory: `sdd/tasks/active/`
   - template: `sdd/templates/task.md`
   - filename: `TASK-NNN-<slug>.md` or `HOTFIX-<KEY>-N-<slug>.md`
   - `Feature` header must include `FEAT-NNN - <title>` for features
9. Create or update `sdd/tasks/index/<feature-slug>.json`:
   - preserve existing header if present
   - include `feature`, `feature_id`, `spec`, `type`, `base_branch`,
     `created_at`, `completed_at`, and `tasks[]`
   - each task entry includes id, slug, title, feature metadata, spec, status,
     priority, effort, dependencies, parallel fields, assignment timestamps,
     and file path
10. Commit:
   - clear staging with `git reset HEAD`
   - stage only `sdd/tasks/index/<feature-slug>.json` and new active task
     files
   - verify cached names
   - commit `sdd: add <N> tasks for FEAT-NNN - <feature-slug>`
11. Create worktree after commit:
   - feature:
     `git worktree add -b feat-<FEAT-ID>-<slug> .claude/worktrees/feat-<FEAT-ID>-<slug> HEAD`
   - hotfix:
     `git worktree add -b hotfix-<JIRA-KEY>-<slug> .claude/worktrees/hotfix-<JIRA-KEY>-<slug> origin/main`

## Output

Count the delegation-eligible tasks first, so the report shows how many tasks
the targeted writer will receive when the worker runs:

```bash
grep -l '^## Delegation Contract' sdd/tasks/active/TASK-*.md | wc -l
```

Report:

```text
Generated and committed <N> tasks for FEAT-NNN - <feature-slug>
Tasks created:
  TASK-NNN - <title> [priority/effort]
Delegated: <D>/<N> tasks carry a Delegation Contract (list them, or "none")
Worktree created:
  .claude/worktrees/feat-<FEAT-ID>-<slug>
Next:
  cd .claude/worktrees/<worktree-name>
  $sdd-start TASK-NNN
```

## References

- `sdd/templates/task.md`
- `sdd/WORKFLOW.md`
- `scripts/sdd/sdd_meta.py`
- `scripts/sdd/reserve_ids.py`

