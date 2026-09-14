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
- **Code AND per-spec index live together in the worktree (FEAT-145).**
  Each feature owns its own `sdd/tasks/index/<feature>.json`, so parallel
  worktrees never collide. The merge in `/sdd-done` brings the index file
  to `base_branch` alongside the code.

## Steps

### 1. Resolve the Task
1. Glob `sdd/tasks/index/*.json` (excluding `_orphans.json`) and find the
   per-spec index whose `tasks[]` array contains the requested ID or slug.
   ```bash
   for f in sdd/tasks/index/*.json; do
       [[ "$(basename "$f")" == "_orphans.json" ]] && continue
       if jq -e --arg q "<TASK-NNN-or-slug>" '.tasks[] | select(.id == $q or .slug == $q)' "$f" > /dev/null; then
           INDEX="$f"
           break
       fi
   done
   ```
2. Resolve `feature_id`, `feature` slug, and `spec` from the per-spec index header.
3. If no match is found, print available tasks (aggregate across all per-spec indexes) and ask the user to pick one.

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

### 3. Ensure the Worktree

The worktree is created by whoever is about to write code in it — not at
planning time (FEAT-552). `/sdd-task` no longer creates one, so this step
provisions it, idempotently: already inside the right worktree, it is a no-op
that prints the path you are already in.

Everything the step needs is in the per-spec index header resolved in §1
(`feature_id`, `feature`, `spec`, `type`, `base_branch`):

```bash
WT=$(python -m scripts.sdd.ensure_worktree \
       --slug "<feature-slug>" \
       --feature-id "<FEAT-ID>" \
       --spec "<spec-path>" \
       --index "sdd/tasks/index/<feature-slug>.json")
cd "$WT"
```

For a hotfix (`type: hotfix` in the index header) pass `--jira-key <KEY>`
instead of `--feature-id`; the CLI applies the FEAT-466 naming and branches
from `origin/main`.

If the command exits non-zero, **STOP** and show its message verbatim. Do NOT
fall back to implementing on `<base_branch>` — an un-isolated implementation is
exactly what this step exists to prevent. The two messages you are most likely
to see are a leftover branch with no worktree, and task artifacts missing from
the base you branched off (fetch and re-run).

With per-spec indexes (FEAT-145), commits then land in the worktree's own
branch. Each feature owns its own index file, so parallel worktrees never
collide on shared mutable state.

### 4. Mark In-Progress (in place)

Update the per-spec index file directly in the current branch — no
directory switching needed (FEAT-145).

```bash
INDEX="sdd/tasks/index/<feature-slug>.json"
NOW=$(date -u +%Y-%m-%dT%H:%M:%S+00:00)

jq --arg id "<TASK-NNN>" --arg now "$NOW" '
  (.tasks[] | select(.id == $id) | .status) = "in-progress" |
  (.tasks[] | select(.id == $id) | .started_at) = $now
' "$INDEX" > "$INDEX.tmp" && mv "$INDEX.tmp" "$INDEX"

# CRITICAL: Unstage everything first — NEVER commit unrelated changes
git reset HEAD
# Stage ONLY the per-spec index — NEVER use "git add ." or "git add -A"
git add "$INDEX"
# Verify
git diff --cached --name-only
# If ANY other files appear, run "git reset HEAD" and start over

git commit -m "sdd: start TASK-<NNN> — <title>"
```

The commit lives on the current branch. The merge in `/sdd-done` brings it
to `base_branch` alongside the code commit — atomically, with no conflict
surface (other features touch other per-spec index files).

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
2. **Verify the Codebase Contract (Anti-Hallucination Check):**
   Before writing ANY code, verify every entry in the task's `## Codebase Contract`:
   - `grep` or `read` each file listed in "Verified Imports" to confirm the imports exist.
   - `read` each file in "Existing Signatures" to confirm class/method signatures are accurate.
   - Check the "Does NOT Exist" section — do NOT reference anything listed there.
   - If any entry is stale (file moved, method renamed), update the contract in the
     task file FIRST, then proceed with implementation using the corrected references.
   - **NEVER guess an import or attribute. If unsure, verify with `grep` or `read` first.**
3. **Actually write the code** — create/modify the files listed in the task scope.
   Use ONLY the imports and signatures from the verified Codebase Contract.
4. Run linting and fix any issues.
5. Run the acceptance-criteria tests from the task.
6. Verify **all** acceptance criteria are met.
6. **Commit code in the worktree:**
   ```bash
   # CRITICAL: Unstage everything first — NEVER commit unrelated changes
   git reset HEAD
   # Stage ONLY the files created/modified by this task — NEVER use "git add ." or "git add -A"
   git add <task-scoped-files-only>
   # Verify ONLY task files are staged
   git diff --cached --name-only
   # If ANY unrelated files appear, run "git reset HEAD" and start over
   git commit -m "feat(<feature-slug>): TASK-<NNN> — <title>"
   ```

**⚠ STOP condition**: Only stop (ask the user) if:
- A dependency is missing or broken.
- The spec is ambiguous and you need clarification.
- Tests are failing and you cannot determine the fix.

Otherwise, keep going until the task is **done**.


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

### 8. Mark Done (in place)

After the code is committed, update the per-spec index in the same branch
— no `cd` to the main repo (FEAT-145).

> **CRITICAL — use the script, do NOT hand-roll the move.** Closing a task
> means *moving* its file from `active/` to `completed/`. Agents that paraphrase
> this as a `Write`/copy leave the `active/` file behind; when the feature
> branch merges, both copies land on the base branch as a "stalled" orphan.
> `scripts/sdd/close_task.sh` does the move with `git mv` and HARD-VERIFIES that
> no `active/` copy survives (exit 3 if it does). Always call it verbatim.

```bash
# Move active → completed, stamp the index (status/completed_at/verification/file),
# stage the change, and assert active/ is clean. Idempotent.
scripts/sdd/close_task.sh TASK-<NNN> <feature-slug> verified

# Fill in the Completion Note section of the moved file (now in completed/).
# Then commit ONLY the staged SDD state — never "git add ." / "git add -A".
git diff --cached --name-only        # sanity-check: only index + task files
git commit -m "sdd: complete TASK-<NNN> — <title>"
```

### 9. Post-Completion Hint
After marking the task done, suggest next steps:
```
✅ TASK-<NNN> completed.
   Code + per-spec index committed on branch: <current branch>

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
- Per-spec index files: `sdd/tasks/index/<feature>.json`
- Task template: `sdd/templates/task.md`
- SDD methodology: `sdd/WORKFLOW.md`
- Frontmatter parser: `scripts/sdd/sdd_meta.py`
