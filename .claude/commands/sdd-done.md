---
model: haiku
description: Verify that a feature's tasks were implemented, push the branch, optionally resolve the linked Jira ticket, and clean up the worktree.
---

# /sdd-done — Verify, Push, and Cleanup a Feature

Verify that a feature's tasks were implemented in its worktree, ensure the branch is
pushed, and clean up the worktree. Optionally transitions the linked Jira ticket to
"Done" / "Resolved".

**This command runs on the spec's `base_branch`** — read from the spec's
YAML frontmatter (FEAT-145). For `type: feature` that is `dev` (default)
or `staging` (during a release freeze); for `type: hotfix` that is `main`.
NOT inside a worktree. It looks INTO the worktree to verify work, but
modifies state only on `base_branch`.

## Usage
```
/sdd-done FEAT-014
/sdd-done videoreel-visual-changes
/sdd-done FEAT-014 --dry-run           # show what would change, don't change anything
/sdd-done FEAT-014 --merge             # direct merge into base_branch (old behavior)
/sdd-done FEAT-014 --force             # mark done even if some checks fail
/sdd-done FEAT-014 --resolve-jira      # also transition the Jira ticket to Done
/sdd-done FEAT-014 --sync-down         # for hotfixes: after the user merges the PR
                                       # to main, propagate the change to staging + dev
                                       # (mostly redundant with sync-down.yml Action)
/sdd-done FEAT-014 --sync-dev          # deprecated alias for --sync-down
```

## Guardrails
- **Must run on the spec's `base_branch`** (read from spec frontmatter — `dev` for features, `main` for hotfixes), not inside a worktree.
- Do NOT mark tasks as done unless evidence exists in the worktree (commits, files).
- Do NOT modify the spec — only task statuses and task files.
- If a task has no evidence of implementation, flag it explicitly.
- Always show a verification report before making changes.

> **CRITICAL — `/sdd-done` NEVER pushes to `main` and NEVER opens a PR against `main` (FEAT-145).**
> Hotfixes go to `main` ONLY via a manually-opened PR. This rule is non-negotiable
> and applies to every flag combination — including `--merge`, `--force`, and `--resolve-jira`.
> For hotfixes, this command pushes the hotfix branch and prints a `gh pr create
> --base main` snippet. After the user merges the PR, the `.github/workflows/sync-down.yml`
> Action propagates the change to `staging` and `dev` automatically. If the Action
> fails or you are offline, re-run with `--sync-down` to propagate the change back to
> both `staging` and `dev` manually. (`--sync-dev` is a deprecated alias for `--sync-down`.)
>
> **Default behavior for features**: `/sdd-done` opens a PR against `BASE_BRANCH`
> (typically `dev` or `staging`). Pass `--merge` to merge directly instead.

## Steps

### 1. Verify We're on the Base Branch (FEAT-145)

Read the spec's frontmatter to discover `BASE_BRANCH`:

```bash
META=$(python -c "from pathlib import Path; from scripts.sdd.sdd_meta import parse; m = parse(Path('<spec-path>')); print(m.type, m.base_branch)")
TYPE=$(echo "$META" | awk '{print $1}')
BASE_BRANCH=$(echo "$META" | awk '{print $2}')
CURRENT_BRANCH=$(git branch --show-current)
```

If `CURRENT_BRANCH != BASE_BRANCH`, abort:
```
⚠️  /sdd-done must run on the spec's base_branch (got <CURRENT_BRANCH>, expected <BASE_BRANCH>).
   Switch: git checkout <BASE_BRANCH>
```

If currently inside a worktree (path contains `.claude/worktrees/`), abort:
```
⚠️  /sdd-done must run from the main repo, not inside a worktree.
   cd back to the main repo and re-run.
```

### 2. Resolve the Feature
1. Glob `sdd/tasks/index/*.json` (excluding `_orphans.json`) and find the
   per-spec index whose header matches the user's input. Match against:
   - `feature_id` — exact match (e.g., `"FEAT-014"`)
   - `feature` — exact match (e.g., `"videoreel-visual-changes"`)
   - `feature_id` — numeric suffix (e.g., `"014"` → `"FEAT-014"`)
   - `feature` — substring match (e.g., `"videoreel"` → `"videoreel-visual-changes"`)
   If no match, list available features (one per per-spec index file) and ask the user to clarify.
2. Read the spec file referenced by the per-spec index header.
3. The list of tasks for this feature is the `tasks[]` array in the matched per-spec index file.

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

**Note:** Test validation is intentionally skipped here — each task already ran
its acceptance-criteria tests during `/sdd-start` or `sdd-worker` execution.
Re-running them at close time adds latency without new signal.

### 5. Build Verification Report
Classify each task:

- **✅ VERIFIED** — commit found AND files exist.
- **⚠️ PARTIAL** — commit found but some files missing.
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

  ⚠️ TASK-097 — Visual Transitions
     Commits: feat(videoreel): TASK-097 — Visual Transitions (def5678)
     Files: src/lib/components/Transitions.svelte ✅ | src/lib/utils/transitions.ts ❌

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

### 7. Stamp Verification (on feature branch)

Stamp verification metadata on each closed task in the worktree's per-spec
index. The feature branch already carries the task files in `completed/` and
the index with `status: "done"` — this step only adds the `verification`
field and the feature-level `completed_at`.

> **Why not close_task.sh?** The feature branch already moved files
> `active/` → `completed/` and set status/completed_at/file during
> `sdd-worker`/`sdd-start`. Running `close_task.sh` again on `base_branch`
> would create duplicate state that conflicts on merge (FEAT-414).

```bash
WORKTREE_PATH=".claude/worktrees/feat-<FEAT-ID>-<slug>"
INDEX="sdd/tasks/index/${FEATURE_SLUG}.json"
NOW="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"

# Stamp verification on each task being closed.
# Use "verified" for ✅ VERIFIED tasks, "partial" for ⚠️ PARTIAL, "forced" for --force.
for TASK_ID in "${TASK_IDS[@]}"; do
  jq --arg id "$TASK_ID" --arg ver "$VERIFICATION" '
    (.tasks[] | select(.id == $id) | .verification) = $ver
  ' "$WORKTREE_PATH/$INDEX" > tmp && mv tmp "$WORKTREE_PATH/$INDEX"
done

# Stamp feature-level completed_at if all tasks are done.
jq --arg now "$NOW" '
  if all(.tasks[]; .status == "done") then .completed_at = $now else . end
' "$WORKTREE_PATH/$INDEX" > tmp && mv tmp "$WORKTREE_PATH/$INDEX"

# Commit on the feature branch (inside the worktree) — never on base_branch.
git -C "$WORKTREE_PATH" add "$INDEX"
git -C "$WORKTREE_PATH" diff --cached --name-only   # sanity-check: only the index
git -C "$WORKTREE_PATH" commit -m "sdd: close tasks for FEAT-<ID> — <slug>"
```

### 8. Push the Feature Branch
If the worktree branch hasn't been pushed yet:
```bash
git -C <worktree-path> push origin feat-<FEAT-ID>-<slug>
```

### 9. Integrate Feature Branch (FEAT-145, flow-aware)

> **CRITICAL**: This is the step that brings the implementation code into the
> base branch. The default is to open a PR; pass `--merge` to merge directly.

**Hard refusal — `BASE_BRANCH == "main"`:**

Hotfixes ALWAYS go through a PR, regardless of flags (including `--merge`):

```bash
if [[ "$BASE_BRANCH" == "main" ]]; then
    cat <<EOF
⚠️ Hotfix merging into 'main' MUST go through a PR. /sdd-done refuses to merge
   into main directly, regardless of flags.

   Open the PR manually:

     gh pr create --base main --head feat-<FEAT-ID>-<slug> \\
       --title "<hotfix title>" \\
       --body "<verification summary>"

   After the PR merges, the sync-down.yml Action propagates the change to staging
   and dev automatically. If the Action fails or you are offline, re-run with
   --sync-down to propagate the change manually:

     /sdd-done <FEAT-ID> --sync-down

EOF
    exit 0   # NOT an error — the hotfix workflow continues outside this command
fi
```

**Feature flow (`BASE_BRANCH != "main"`) — default: open a PR:**

Unless `--merge` is passed, push the feature branch and open a PR against
`BASE_BRANCH`:

```bash
# Push the feature branch (already done in Step 8, but ensure it's up to date)
git -C <worktree-path> push origin feat-<FEAT-ID>-<slug>

# Open a PR against the base branch
gh pr create \
  --base "$BASE_BRANCH" \
  --head "feat-<FEAT-ID>-<slug>" \
  --title "feat(<feature-slug>): FEAT-<ID> — <title>" \
  --body "$(cat <<EOF
## Summary

<Verification report from Step 5 — list of tasks implemented>

## Tasks

<N>/<total> tasks verified.

---
_Closed by /sdd-done_
EOF
)"
```

If `gh` is not installed or not authenticated, print the manual command:
```
ℹ️  Could not create PR automatically. Run manually:

    gh pr create --base <BASE_BRANCH> --head feat-<FEAT-ID>-<slug> \
      --title "feat(<feature-slug>): FEAT-<ID> — <title>" \
      --body "<verification summary>"
```

**Feature flow with `--merge` — direct merge (old behavior):**

When `--merge` is explicitly passed, perform a direct merge instead of a PR:

```bash
# We're already on $BASE_BRANCH (verified in Step 1)
git merge --no-edit feat-<FEAT-ID>-<slug>
```

If the merge has conflicts (e.g. code-level changes to the same files):
```
⚠️  Merge conflict when merging feat-<FEAT-ID>-<slug> into <BASE_BRANCH>.
   Resolve conflicts, then continue with: git merge --continue
   Or abort: git merge --abort
```
If the user aborts, STOP and do NOT proceed to cleanup.

**Self-heal — reap stalled `active/` orphans (runs after every `--merge`):**

> Only applies when `--merge` is used. When using PR flow, the orphan sweep
> happens on the PR merge side.

```bash
scripts/sdd/heal_orphans.sh <feature-slug>
# If it reaped anything, commit the cleanup before pushing:
if ! git diff --cached --quiet -- sdd/tasks/active sdd/tasks/completed; then
  git commit -m "sdd: reap stalled active task orphans for FEAT-<ID> — <title>"
fi
```

After a successful merge and self-heal, push `<BASE_BRANCH>`:
```bash
git push origin "$BASE_BRANCH"
```

### 9.5. Hotfix → Sync-down (FEAT-187, only with `--sync-down`)

This sub-step runs ONLY when the user passes `--sync-down` (or the deprecated
`--sync-dev` alias) AND `TYPE == "hotfix"`. It propagates a hotfix that has just
been merged into `main` (via the manual PR from §9) back into `staging` and `dev`
so both stay in sync.

If `--sync-dev` is used instead of `--sync-down`, first emit:
```
ℹ️  --sync-dev is deprecated; use --sync-down. Continuing with sync-down behaviour.
```

In normal operation, `.github/workflows/sync-down.yml` does this automatically
after every push to `main`. Run this command only when the Action has failed or
the user is operating offline.

**Pre-flight (run once):** verify the hotfix landed on `origin/main`:
```bash
git fetch origin
if ! git merge-base --is-ancestor "feat-<FEAT-ID>-<slug>" origin/main; then
    echo "⚠️  feat-<FEAT-ID>-<slug> is not yet an ancestor of origin/main."
    echo "   Open the PR and merge it first, then re-run with --sync-down."
    exit 1
fi
```

**Sync to `staging`** — optimistic auto-merge with safe abort on conflict:
```bash
git checkout staging
git pull --ff-only origin staging

if git merge --no-edit feat-<FEAT-ID>-<slug>; then
    git push origin staging
    echo "✅ staging synced with hotfix feat-<FEAT-ID>-<slug>."
    STAGING_OK=true
else
    git merge --abort
    STAGING_OK=false
    cat <<EOF
⚠️  Conflict syncing hotfix into staging. The merge has been aborted (no changes left).

    Resolve manually:
      git checkout staging
      git merge feat-<FEAT-ID>-<slug>
      # ...resolve conflicts in your editor...
      git commit
      git push origin staging

EOF
fi
```

**Sync to `dev`** — optimistic auto-merge with safe abort on conflict
(independent of `staging` outcome — always attempt):
```bash
git checkout dev
git pull --ff-only origin dev

if git merge --no-edit feat-<FEAT-ID>-<slug>; then
    git push origin dev
    echo "✅ dev synced with hotfix feat-<FEAT-ID>-<slug>."
    DEV_OK=true
else
    git merge --abort
    DEV_OK=false
    cat <<EOF
⚠️  Conflict syncing hotfix into dev. The merge has been aborted (no changes left).

    Resolve manually:
      git checkout dev
      git merge feat-<FEAT-ID>-<slug>
      # ...resolve conflicts in your editor...
      git commit
      git push origin dev

EOF
fi
```

**Return to base:** leave the user on `main` (the hotfix's base branch):
```bash
git checkout main
```

**Summary and exit code:**
```bash
if $STAGING_OK && $DEV_OK; then
    echo "✅ Sync-down complete: staging and dev are in sync with main."
    exit 0
else
    echo "⚠️  Sync-down partially failed. See above for failed targets."
    exit 1
fi
```

### 10. Transition Jira Ticket (if --resolve-jira)

If `--resolve-jira` is passed AND the spec has a Jira key (set by `/sdd-tojira`):

**a) Extract the Jira key from the spec:**
```bash
# Look for "**Jira**: [NAV-8036](...)" or a "jira:" metadata field in the spec
JIRA_KEY=$(grep -oP '(?<=\*\*Jira\*\*: \[)[A-Z]+-\d+' sdd/specs/<feature>.spec.md)
# Or from the brainstorm "## Jira Source" table
if [[ -z "$JIRA_KEY" ]]; then
    JIRA_KEY=$(grep -oP '(?<=\| Key \| )[A-Z]+-\d+' sdd/proposals/<key>-*.brainstorm.md)
fi
```

If no Jira key is found, skip this step with a note:
```
ℹ️  No Jira key found in spec — skipping Jira transition.
   To link a spec to Jira: /sdd-tojira <spec-path>
```

**b) Load Jira credentials:**
```bash
eval "$(python -c "from navconfig import config; import os; [print(f'export {k}={v}') for k,v in os.environ.items() if k.startswith('JIRA_')]")"
JIRA_INSTANCE="${JIRA_INSTANCE%/}"
```

If `JIRA_INSTANCE` or `JIRA_API_TOKEN` are not set, warn and skip.

**c) Get available transitions for the ticket:**

Jira transitions are workflow-dependent — you cannot set a status directly.
First, fetch the available transitions:

**MCP path:**
```
jira_transition_issue(issue_key="<JIRA_KEY>")  # list available transitions
```

**curl fallback:**
```bash
TRANSITIONS=$(curl -s -u "$JIRA_USERNAME:$JIRA_API_TOKEN" \
  "$JIRA_INSTANCE/rest/api/3/issue/$JIRA_KEY/transitions")
echo "$TRANSITIONS" | jq '.transitions[] | {id, name}'
```

**d) Find and execute the "Done" / "Resolved" transition:**

Look for a transition whose name matches (case-insensitive):
`Done`, `Resolved`, `Close`, `Ready for UAT`, `Complete`.

```bash
# Find the transition ID
TRANSITION_ID=$(echo "$TRANSITIONS" | jq -r '
  .transitions[] |
  select(.name | test("(?i)done|resolved|close|complete|ready for uat")) |
  .id' | head -1)
```

If found, execute it:

**MCP path:**
```
jira_transition_issue(issue_key="<JIRA_KEY>", transition_id="<TRANSITION_ID>")
```

**curl fallback:**
```bash
curl -s -u "$JIRA_USERNAME:$JIRA_API_TOKEN" \
  -H "Content-Type: application/json" \
  -X POST "$JIRA_INSTANCE/rest/api/3/issue/$JIRA_KEY/transitions" \
  -d "{\"transition\": {\"id\": \"$TRANSITION_ID\"}}"
```

If multiple matching transitions exist, prefer in this order:
1. "Done"
2. "Resolved"
3. "Ready for UAT"
4. "Complete"
5. "Close"

If no matching transition is found:
```
⚠️  No "Done" or "Resolved" transition available for <JIRA_KEY>.
   Current status: <current_status>
   Available transitions: <list>
   You may need to transition it manually in Jira.
```

**e) Optionally resolve subtasks too:**

If the ticket has subtasks (created by `--with-subtasks` in `/sdd-tojira`),
transition each one that is still open:
```bash
SUBTASKS=$(curl -s -u "$JIRA_USERNAME:$JIRA_API_TOKEN" \
  "$JIRA_INSTANCE/rest/api/3/issue/$JIRA_KEY?fields=subtasks" \
  | jq -r '.fields.subtasks[].key')

for SUBTASK in $SUBTASKS; do
    # Get transitions for this subtask, find "Done", execute
    # Same logic as above
done
```

### 11. Cleanup the Worktree
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

### 12. Output

**Default (PR flow):**
```
✅ FEAT-<ID> — <title>: <N>/<total> tasks closed.

Closed:
  ✅ TASK-096 — Scene Editor Refactor (verified)
  ✅ TASK-097 — Visual Transitions (verified)

Index updated and committed.
Branch pushed: feat-<ID>-<slug>
PR opened: feat-<ID>-<slug> → <BASE_BRANCH>  <PR-URL>
Worktree removed: .claude/worktrees/feat-<ID>-<slug>
Local branch deleted: feat-<ID>-<slug>
```

**With `--merge`:**
```
✅ FEAT-<ID> — <title>: <N>/<total> tasks closed.

Closed:
  ✅ TASK-096 — Scene Editor Refactor (verified)
  ✅ TASK-097 — Visual Transitions (verified)

Index updated and committed.
Branch pushed: feat-<ID>-<slug>
Merged into <BASE_BRANCH>: feat-<ID>-<slug> ✅
Worktree removed: .claude/worktrees/feat-<ID>-<slug>
Local branch deleted: feat-<ID>-<slug>
```

If `--resolve-jira` was used and succeeded:
```
Jira: NAV-8036 → Done ✅
  Subtasks transitioned: 4/4
```

If ALL tasks were closed:
```
✅ FEAT-<ID> — <title>: all <N> tasks closed.

Branch pushed. PR opened → <BASE_BRANCH>.
{if --merge} Merged into <BASE_BRANCH> directly. {end if}
Worktree cleaned up. Feature branch deleted.
{if --resolve-jira} Jira NAV-8036 → Done ✅ {end if}
```

## Reference
- Per-spec index files: `sdd/tasks/index/<feature>.json` (on `<base_branch>`)
- Active tasks: `sdd/tasks/active/` (on `<base_branch>`)
- Completed tasks: `sdd/tasks/completed/` (on `<base_branch>`)
- Frontmatter parser: `scripts/sdd/sdd_meta.py`
- SDD methodology: `sdd/WORKFLOW.md`
