---
description: Use ONLY after the user explicitly approves and requests a PR (keywords: open PR, create pull request, ready for main, approved, ship it). Find the active worktree, verify it is clean, push the branch, create a PR to main, and remove the worktree.
globs:
  - ".worktrees/**"
  - "worktrees/**"
---

# Worktree PR and Clean

## Goal

After user approval: push branch + open PR to main + clean up the worktree.

## Safety Rules

- Do not delete the worktree if there are uncommitted changes.
- Do not use `--force` unless the user explicitly requests it.
- If `gh` is not installed or not authenticated, guide the user to create the PR manually (or provide the exact commands needed).

## Steps

### 1. Locate the Active Worktree

- Read `.worktrees/_active.json` (if present) to get `path`, `branch`, `base`.
- If missing, run `git worktree list` and pick the non-primary worktree (or ask the user which one).

### 2. Verify State

- `git -C <path> status --porcelain` must be empty.
- Confirm the current branch is `<branch>`.
- (Optional) run tests one last time.

### 3. Push to Origin

- `git -C <path> push -u origin <branch>`

### 4. Create a PR to Main

- If GitHub CLI is available and authenticated:
  - `gh pr create --base <base> --head <branch> --title "<title>" --body "<summary>"`

### 5. Clean Up

- Remove the worktree: `git worktree remove <path>`
- Prune stale metadata: `git worktree prune`
- Delete `.worktrees/_active.json` if there is no active worktree left.

### 6. Report

- PR link (if created).
- Commands executed.
- Any remaining manual steps (if auth or tooling was missing).
