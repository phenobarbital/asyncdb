---
model: haiku
---

# /sdd-done — Verify, Push, and Cleanup a Feature

Verify that a feature's tasks were implemented in its worktree, ensure the branch is
pushed, and clean up the worktree.

**This command runs on `dev` (or the main repo), NOT inside a worktree.**
It looks INTO the worktree to verify work, but modifies state only on `dev`.

## Usage
```
/sdd-done FEAT-014
/sdd-done videoreel-visual-changes
/sdd-done FEAT-014 --dry-run        # show what would change, don't change anything
/sdd-done FEAT-014 --force          # mark done even if some checks fail
```

## Guardrails
- **Must run on `dev`**, not inside a worktree.
- Do NOT mark tasks as done unless evidence exists in the worktree (commits, files).
- Do NOT modify the spec — only task statuses and task files.
- If a task has no evidence of implementation, flag it explicitly.
- Always show a verification report before making changes.

## Steps

### 1. Verify We're on `dev`
```bash
CURRENT_BRANCH=$(git branch --show-current)
```
If not on `dev`, warn:
```
⚠️  /sdd-done should run on dev, not inside a worktree.
   Current branch: <branch>
   Switch: git checkout dev
```

### 2. Resolve the Feature
1. Read `sdd/tasks/.index.json`.
2. Find all tasks belonging to the given feature. Match against:
   - `feature_id` — exact match (e.g., `"FEAT-014"`)
   - `feature` — exact match (e.g., `"videoreel-visual-changes"`)
   - `feature_id` — numeric suffix (e.g., `"014"` → `"FEAT-014"`)
   - `feature` — substring match (e.g., `"videoreel"` → `"videoreel-visual-changes"`)
   If no match, list available features and ask the user to clarify.
3. Read the spec file referenced by the tasks.

### 3. Locate the Worktree
Find the feature's worktree:
```bash
git worktree list | grep "feat-<FEAT-ID>"
```
Extract the worktree path. If no worktree found:
```
⚠️  No worktree found for FEAT-<ID>.
   Looking for branch feat-<FEAT-ID>-<slug> in remote...
```
Fall back to checking remote branches.

### 4. Gather Evidence from the Worktree
For each task in the feature, check the WORKTREE for implementation evidence:

**a) Git history check (in the worktree):**
```bash
git -C <worktree-path> log --oneline --grep="TASK-<NNN>"
git -C <worktree-path> log --oneline --grep="<task-slug>"
```

**b) File existence check (in the worktree):**
Read the task file and extract the "Files to create/modify" section.
```bash
test -f <worktree-path>/<filepath>
```

**c) Test check (optional, skip if --force):**
If the task file lists test commands, run them in the worktree:
```bash
cd <worktree-path> && npx vitest run <test-path> 2>&1 | tail -10
# or
cd <worktree-path> && pytest <test-path> -x -q 2>&1 | tail -5
```

### 5. Build Verification Report
Classify each task:

- **✅ VERIFIED** — commit found AND files exist AND tests pass (or no tests specified).
- **⚠️ PARTIAL** — commit found but some files missing or tests failing.
- **❌ NO EVIDENCE** — no matching commits, files don't exist.

Present the report:
```
📋 Verification Report: FEAT-<ID> — <title>

Worktree: .claude/worktrees/feat-<ID>-<slug>
Branch: feat-<ID>-<slug>
Commits found: <N>
Tasks: <total> total, <verified> verified, <partial> partial, <missing> missing

  ✅ TASK-096 — Scene Editor Refactor
     Commits: feat(videoreel): TASK-096 — Scene Editor Refactor (abc1234)
     Files: src/lib/components/SceneEditor.svelte ✅
     Tests: 3 passed ✅

  ⚠️ TASK-097 — Visual Transitions
     Commits: feat(videoreel): TASK-097 — Visual Transitions (def5678)
     Files: src/lib/components/Transitions.svelte ✅
     Tests: 1 failed ⚠️

  ❌ TASK-098 — Export Pipeline
     Commits: none found
     Files: src/lib/utils/export.ts ❌
```

### 6. Confirm
If all tasks are ✅ VERIFIED:
```
All tasks verified. Proceed with closing? (Y/n)
```

If any tasks are ⚠️ PARTIAL or ❌ NO EVIDENCE:
```
<N> task(s) have issues. Options:
  1. Close verified tasks only (mark others as "pending")
  2. Close all with --force (mark partial as "done-with-issues")
  3. Abort — fix issues first
```

If `--dry-run`, show the report and STOP.
If `--force`, close all tasks regardless.

### 7. Close Tasks (on `dev`)
For each task being closed, update `dev`:

```bash
# Already on dev (verified in Step 1)

# Move task files to completed
mkdir -p sdd/tasks/completed/
mv sdd/tasks/active/TASK-<NNN>-<slug>.md sdd/tasks/completed/
# Repeat for each closed task...

# Update index: set status → "done", completed_at → now, verification → verified|partial|forced
# Update task file headers: Status, Completed date, Verification

git add sdd/tasks/.index.json sdd/tasks/active/ sdd/tasks/completed/
git commit -m "sdd: close tasks for FEAT-<ID> — <title>"
```

### 8. Push the Feature Branch
If the worktree branch hasn't been pushed yet:
```bash
git -C <worktree-path> push origin feat-<FEAT-ID>-<slug>
```

### 9. Merge Feature Branch into `dev`

> **CRITICAL**: This is the step that brings the implementation code into `dev`.
> Without this merge, the task index is updated but the code changes remain
> only on the feature branch — causing "marked done but not implemented" issues.

```bash
# We're already on dev (verified in Step 1)
git merge feat-<FEAT-ID>-<slug> --no-edit
```

If the merge has conflicts:
```
⚠️  Merge conflict when merging feat-<FEAT-ID>-<slug> into dev.
   Conflicting files:
     - <file1>
     - <file2>

   Options:
     1. Resolve conflicts now (recommended)
     2. Abort merge: git merge --abort
```
If conflicts are resolved, commit the merge. If the user aborts, STOP and
do NOT proceed to cleanup.

After a successful merge, push `dev`:
```bash
git push origin dev
```

### 10. Cleanup the Worktree
```bash
git worktree remove .claude/worktrees/feat-<FEAT-ID>-<slug>
```
If there are uncommitted changes in the worktree, warn:
```
⚠️  Worktree has uncommitted changes. Force remove? (y/N)
```

If the worktree was already removed, prune stale metadata:
```bash
git worktree prune
```

Optionally delete the local feature branch (it's been merged):
```bash
git branch -d feat-<FEAT-ID>-<slug>
```

### 11. Output
```
✅ FEAT-<ID> — <title>: <N>/<total> tasks closed.

Closed:
  ✅ TASK-096 — Scene Editor Refactor (verified)
  ✅ TASK-097 — Visual Transitions (verified)

Index updated on dev and committed.
Branch pushed: feat-<ID>-<slug>
Merged into dev: feat-<ID>-<slug> ✅
Worktree removed: .claude/worktrees/feat-<ID>-<slug>
Local branch deleted: feat-<ID>-<slug>
```

If ALL tasks were closed:
```
✅ FEAT-<ID> — <title>: all <N> tasks closed and merged into dev.

Worktree cleaned up.
Feature branch merged and deleted.
```

## Reference
- Index file: `sdd/tasks/.index.json` (on `dev`)
- Active tasks: `sdd/tasks/active/` (on `dev`)
- Completed tasks: `sdd/tasks/completed/` (on `dev`)
- SDD methodology: `sdd/WORKFLOW.md`