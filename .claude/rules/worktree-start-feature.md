---
description: Use when the user asks to implement a feature in an isolated git worktree / separate branch (keywords: worktree, separate folder, isolated branch, new feature branch). Create a new branch + worktree from main, store the active worktree info, and do ALL work inside the worktree. Do NOT open a PR yet.
globs:
  - ".worktrees/**"
  - "worktrees/**"
---

# Worktree Start Feature

## Goal

Implement a feature in a separate working directory (git worktree) on a new branch.

## Rules

- Do not edit feature code in the main working directory.
- Run all git commands in the worktree using: `git -C <path> ...`
- Do not create a PR yet. Leave the work ready for review and ask the user for approval.

## Steps

### 1. Preflight

- Confirm this is a git repository.
- Determine the base branch:
  - Prefer `main` if it exists, otherwise try `master`.
- If remote `origin` exists, run `git fetch origin`.

### 2. Naming

- Branch name: `feat/<slug>` (short slug derived from the feature name).
- Worktree path: `.worktrees/<slug>` (keep worktrees inside the repo).

### 3. Create the Worktree + New Branch

- Create `.worktrees/` if it does not exist.
- Create a worktree and new branch from the base ref (prefer `origin/<base>` if available):
  - `git worktree add -b <branch> .worktrees/<slug> <base_ref>`

### 4. Record the Active Worktree

- Create/update `.worktrees/_active.json` with:
  - `branch`
  - `base`
  - `path`

### 5. Implement the Feature

- Make all code changes under `.worktrees/<slug>/...`
- Run tests/lint in the worktree (choose appropriate commands based on the repo: `package.json` → `npm test`; `pyproject`/`pytest` → `pytest`; etc.)
- Make small, clear commits.

### 6. Finish

- Ensure the worktree is clean (no uncommitted changes).
- Summarize:
  - what changed
  - how to run tests
  - any risks / follow-ups
- Ask the user for approval to open a PR and clean up the worktree.
