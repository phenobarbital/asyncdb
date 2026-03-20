---
description: Use to inspect the current git worktree setup, show the active/likely-active worktree, verify cleanliness, and remind the agent to work only inside the worktree (keywords: worktree status, which worktree, where am I, sanity check, active branch).
globs:
  - ".worktrees/**"
  - "worktrees/**"
---

# Worktree Status

## Goal

Show the current worktree situation and prevent accidental edits in the primary working directory.

## Rules

- Do not make code changes.
- Do not commit, push, or open PRs.
- Only inspect and report.

## Steps

### 1. Confirm Git Repo

- If not in a git repo, report that and stop.

### 2. Show Worktrees

- Run: `git worktree list`
- Identify:
  - the primary worktree (usually the repo root)
  - any additional worktrees (candidates)

### 3. Determine the Active / Likely-Active Worktree

- If `.worktrees/_active.json` exists:
  - Read and report `branch`, `base`, `path`.
  - Verify the path exists.
  - Mark this as **Active (recorded)**.
- Else (no `_active.json`):
  - From `git worktree list`, collect non-primary worktrees.
  - If there are no additional worktrees:
    - Report: "No additional worktrees found."
    - Skip to Step 5.
  - If there is exactly one additional worktree:
    - Treat it as **Likely active (only candidate)**.
  - If there are multiple additional worktrees:
    - Determine "most likely active" using this heuristic order (no filesystem changes):
      1. Prefer a worktree whose path starts with `.worktrees/` (repo-local convention).
      2. Among those, prefer one whose branch name matches common feature patterns:
         `feat/*`, `feature/*`, `fix/*`, `bugfix/*`, `chore/*`
      3. If still multiple, pick the one that appears last in `git worktree list` output (often newest).
    - Mark it as **Likely active (heuristic)**.
  - For the chosen likely-active worktree, set:
    - `path` = candidate path
    - `branch` = candidate branch

### 4. Verify Cleanliness (Active or Likely-Active)

- If an active/likely-active worktree path is known:
  - Run: `git -C <path> status --porcelain`
  - If non-empty: report "worktree is NOT clean" and show a short summary of modified/untracked.
  - If empty: report "worktree is clean".
  - Also report the branch: `git -C <path> rev-parse --abbrev-ref HEAD`.

### 5. Guardrail Reminder

- If an active/likely-active worktree exists:
  - Explicitly state: "Do all feature edits inside: `<path>`."
- If no active/likely-active worktree exists:
  - Suggest using the `worktree-start-feature` rule to create one for a new feature.

## Output Format

- **Worktrees:** list (path + branch)
- **Active worktree:** branch/path and whether it is recorded or heuristic
- **Cleanliness:** clean / not clean (active/likely-active)
- **Reminder:** where to work
