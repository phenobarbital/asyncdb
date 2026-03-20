# /sdd-start — Start an SDD Task

Pick up a task from the SDD task index by ID or slug, validate it is ready, mark it in-progress,
and begin implementation following the task's instructions.

## Usage
```
/sdd-start TASK-004
/sdd-start lyria-music-tests
```
Accept either the full ID (`TASK-NNN`) or the slug. If nothing is provided, run `/sdd-next` logic and ask the user to pick one.

## Guardrails
- Do NOT start a task whose dependencies are not all `"done"`.
- Do NOT start a task that is already `"in-progress"` or `"done"` unless the user explicitly confirms.
- **Code changes happen in the worktree.**
- **SDD state changes (index, task file moves) happen on `dev`.**
- This separation prevents merge conflicts between parallel features.

## Steps

### 1. Resolve the Task
1. Read `sdd/tasks/.index.json`.
2. Match the user's input against `id` or `slug` (case-insensitive).
3. If no match is found, print available tasks and ask the user to pick one.

### 2. Validate Readiness
Check:
- **Status** must be `"pending"`. If `"in-progress"`, warn and ask to confirm resume; if `"done"`, abort.
- **Dependencies** — every task in `depends_on` must have status `"done"`.
  If any dependency is not done, print:
  ```
  ❌ TASK-<NNN> is blocked.
     Waiting on: TASK-<X> (<status>), TASK-<Y> (<status>)
     Resolve those first or run /sdd-status to see the full board.
  ```
  and STOP.

### 3. Detect Context
Determine where we are:
```bash
CURRENT_DIR=$(pwd)
REPO_ROOT=$(git rev-parse --show-toplevel)
```

- **In a worktree** (path contains `.claude/worktrees/`): good, proceed.
- **On `dev` directly**: warn that implementation should happen in a worktree:
  ```
  ⚠️  You're on dev. Implementation should happen in a worktree.
     Create one with:
       git worktree add -b feat-<FEAT-ID>-<slug> .claude/worktrees/feat-<FEAT-ID>-<slug> HEAD
       cd .claude/worktrees/feat-<FEAT-ID>-<slug>
     Then run /sdd-start TASK-<NNN> again.

     Continue on dev anyway? (y/N)
  ```
- **On another branch**: proceed (user knows what they're doing).

Save `REPO_ROOT` for later — we'll need the path to the main repo to update `dev`.

### 4. Mark In-Progress (on `dev`)
Switch to the main repo and update `dev`:
```bash
# Save current worktree path
WORKTREE_DIR=$(pwd)

# Go to main repo root (parent of .claude/worktrees/)
cd <REPO_ROOT>   # the main repo, NOT the worktree
git checkout dev

# Update index
# Set status → "in-progress", started_at → now
git add sdd/tasks/.index.json
git commit -m "sdd: start TASK-<NNN> — <title>"

# Return to worktree
cd "${WORKTREE_DIR}"
```

If already inside the main repo (not a worktree), just update in place.

### 5. Read Context
1. Read the **task file** at the path from the index.
2. Read the **spec file** referenced in the task header.
3. Extract:
   - Scope and implementation notes
   - Files to create/modify
   - Acceptance criteria
   - Test specification

### 6. Print Kickoff Summary
Output:
```
🚀 Starting TASK-<NNN>: <title>
   Feature: <feature>
   Branch: <current branch name>
   Priority: <priority>  |  Effort: <effort>
   Depends-on: <deps or "none">

📋 Scope:
   - <scope item 1>
   - <scope item 2>

📂 Files:
   - <file1> (CREATE)
   - <file2> (MODIFY)

✅ Acceptance Criteria:
   - <criterion 1>
   - <criterion 2>
```

> **Do NOT stop here.** The kickoff summary is informational only. Proceed immediately to Step 7.

### 7. Begin Implementation (in the worktree)

> **CRITICAL — THIS IS THE CORE PURPOSE OF `/sdd-start`.**
> Do NOT stop after printing the kickoff summary.
> You MUST proceed to actually implement the task code NOW.
> The kickoff summary is just informational; the real work starts here.

Follow the **Agent Instructions** section in the task file:

1. Read the spec for full context.
2. **Actually write the code** — create/modify the files listed in the task scope.
3. Run linting and fix any issues.
4. Run the acceptance-criteria tests from the task.
5. Verify **all** acceptance criteria are met.
6. **Commit code in the worktree:**
   ```bash
   git add <task-scoped-files-only>
   git commit -m "feat(<feature-slug>): TASK-<NNN> — <title>"
   ```

**⚠ STOP condition**: Only stop (ask the user) if:
- A dependency is missing or broken.
- The spec is ambiguous and you need clarification.
- Tests are failing and you cannot determine the fix.

Otherwise, keep going until the task is **done**.

### 8. Mark Done (on `dev`)
After the code is committed in the worktree, switch to `dev` to update SDD state:

```bash
# Save worktree path
WORKTREE_DIR=$(pwd)

# Go to main repo
cd <REPO_ROOT>
git checkout dev

# Move task file to completed
mkdir -p sdd/tasks/completed/
mv sdd/tasks/active/TASK-<NNN>-<slug>.md sdd/tasks/completed/

# Update index: set status → "done", completed_at → now
# Fill in the Completion Note section of the task file

git add sdd/tasks/.index.json sdd/tasks/active/ sdd/tasks/completed/
git commit -m "sdd: complete TASK-<NNN> — <title>"

# Return to worktree
cd "${WORKTREE_DIR}"
```

### 9. Post-Completion Hint
After marking the task done, suggest next steps:
```
✅ TASK-<NNN> completed.
   Code committed in worktree: <worktree-branch>
   Index updated on dev.

Next in this feature:
  → /sdd-start TASK-<NEXT>  (<title>)

Or see all unblocked work:
  → /sdd-next
```

If this was the **last task** for the feature:
```
✅ TASK-<NNN> completed — all tasks for FEAT-<ID> are done!

Next:
  - Run /sdd-done FEAT-<ID> to verify, push, and cleanup
```

## Reference
- Index file: `sdd/tasks/.index.json`
- Task template: `sdd/templates/task.md`
- SDD methodology: `sdd/WORKFLOW.md`