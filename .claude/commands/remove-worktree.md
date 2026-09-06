---
description: Inspect and safely remove stalled or finished SDD worktrees, refusing any worktree with a live sdd-worker inside
argument-hint: "[<branch|FEAT-id|dir>] [--stale] [--delete-branch] [--force] [--dry-run]"
allowed-tools: Bash, Read
---

# /remove-worktree — Safe Worktree Cleanup

Remove leftover worktrees under `.claude/worktrees/` without destroying work
a running `sdd-worker` still owns.

Backed by `scripts/remove_worktree.py`. **Always run `list` first** and show
the user what is there before removing anything.

## Two kinds of leftovers

| Kind | Symptom | Cause |
|---|---|---|
| **Registered** | in `git worktree list` | feature finished or abandoned; `/sdd-done` never cleaned it |
| **Orphan directory** | on disk, *not* in `git worktree list` | git admin data was pruned or the checkout was moved — invisible to `git worktree prune`, so it just sits there |

## Usage

```
/remove-worktree                      # list everything with its state
/remove-worktree feat-310-eventbus-v2
/remove-worktree FEAT-417             # partial match on branch or dir name
/remove-worktree --stale              # every orphan directory
/remove-worktree <target> --delete-branch
/remove-worktree <target> --dry-run
```

## Step 1 — Always inspect first

```bash
source .venv/bin/activate && python scripts/remove_worktree.py list
```

State flags:

- `LIVE(n)` — n process(es) have their **cwd inside** this worktree.
  **Never remove one of these.** Detection resolves `/proc/<pid>/cwd`, so it
  catches a worker that `cd`-ed in and does not false-positive on a process
  that merely mentions the path.
- `ORPHAN-DIR` — no git record; removal is a plain directory delete.
- `dirty:n` — uncommitted changes.
- `unpushed:n` — commits origin does not have. Per this repo's history, a
  concurrent SDD process can `reset --hard` a shared branch and eat local
  commits, so unpushed work in a worktree is genuinely at risk — push it
  rather than discarding it. (The worst offender, `reserve_ids.py`'s retry
  path, no longer resets anything — but `/sdd-done` merges and release
  tooling still move shared branches under you.)

Add `--check-pr` to also resolve PR state via `gh` (slower).

If `$ARGUMENTS` is empty, **stop after this step** and report the table.

## Step 2 — Cross-check for a live worker

Belt and braces on top of the cwd check — a worker launched via tmux may have
its shell elsewhere:

```bash
ps -eo pid,etime,args | grep -i '[s]dd-worker'
tmux ls 2>/dev/null
```

If anything matches the target worktree, **abort and tell the user which
process to stop.** Do not offer `--force` as the workaround here: `--force`
overrides dirty/unpushed gates, never the live-process gate.

## Step 3 — Preview

```bash
source .venv/bin/activate && python scripts/remove_worktree.py remove <target> --dry-run
```

Show the output. If `--dry-run` was in `$ARGUMENTS`, stop here.

## Step 4 — Remove

```bash
source .venv/bin/activate && python scripts/remove_worktree.py remove <target>
```

Add `--delete-branch` to drop the local branch as well (uses `git branch -d`,
so an unmerged branch survives unless `--force` is also given).

If the script refuses on **dirty** or **unpushed** state, relay the reason and
**ask** before adding `--force` — the safe route is almost always to push
first:

```bash
git -C .claude/worktrees/<name> push -u origin <branch>
```

## Safety gates (all enforced by the script)

| Gate | Overridable |
|---|---|
| Primary repository checkout | never |
| `main` / `dev` / `staging` / `master` branch | never |
| Live process with cwd inside | **never** — stop the process |
| Uncommitted changes | `--force` |
| Commits not on origin | `--force` |

An exact match on a protected branch name is refused up front, so
`/remove-worktree dev` cannot silently resolve to
`feat-FEAT-378-devloop-enhancement` by substring.

## Step 5 — Report

```
Removed: <name>  (<branch>)
  kind:   registered worktree | orphan directory
  branch: deleted | kept | force-deleted

Remaining worktrees:
  <git worktree list output>
```

## Related

| Command | Purpose |
|---|---|
| `/remove-worktree` | Targeted, gated removal (THIS) |
| `python scripts/sweep_worktrees.py` | dev-loop sweep: removes worktrees whose PR is merged/closed, keeps open ones |
| `/sdd-done <FEAT-ID>` | Normal end-of-feature path — merges, then cleans its own worktree |
