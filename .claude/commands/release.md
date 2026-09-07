---
description: Bump every workspace package version, commit, tag and (on confirmation) create the GitHub Release that publishes to PyPI
argument-hint: "<patch|minor|major> [--only <pkg>...] [--dry-run] [--no-release]"
allowed-tools: Bash, Read, Edit
---

# /release — Monorepo Release

Drive a release of the `ai-parrot` uv workspace: bump **every** package's
independent version, keep the `ai-parrot>=` pins in lockstep, commit, tag,
push, and — only after you say yes — create the GitHub Release that triggers
the PyPI publish.

The mechanical part lives in `scripts/release.py`; this command is the
judgment around it (CHANGELOG wording, safety gates, the publish decision).

## Usage

```
/release patch                  # 0.28.1 -> 0.28.2 across all 28 packages
/release minor
/release major
/release patch --only ai-parrot ai-parrot-server
/release patch --dry-run        # preview every write, touch nothing
/release patch --no-release     # bump + commit + tag + push, stop there
```

## How versioning works here

- **28 distributions**, each with its own independent version. Ten read it
  from a `version.py` (`dynamic = ["version"]`); **`navrules` repeats the
  number in three files** — `src/navrules/__init__.py`, `pyproject.toml`,
  `rust/Cargo.toml` — and **`parrot-codec` in two** (`pyproject.toml` +
  `Cargo.toml`), which `release.py` keeps in lockstep.
- The **15 `ai-parrot-client-<provider>` LLM-client satellites** (FEAT-523)
  and `ai-parrot-openlit-bridge` keep a *static* `version = "X.Y.Z"` in
  their `pyproject.toml`. `release.py` and the Makefile discover the
  satellites by globbing `packages/ai-parrot-client-*/` — a new provider
  needs no registration. Per-satellite alias: `--only client-anthropic`.
- Twenty siblings pin `ai-parrot>=<core>` (the five classic ones plus the 15
  client satellites); a core bump re-pins all of them.
- **First publish of a NEW distribution name is manual.** `release.yml`
  publishes via PyPI trusted publishing, which can only upload to a project
  that already exists (or has a pending publisher). Bootstrap a new satellite
  with `make build-clients publish-clients` (twine, credentials from
  `~/.pypirc`) once; every later version then ships from `release.yml`.
- **The git tag is the core `ai-parrot` version** (`0.26.1`, `0.26.0`, …),
  lightweight, not annotated.
- `.github/workflows/release.yml` fires on `release: [created]`, builds all
  distributions and publishes with `skip-existing: true` — packages whose
  version did not move are skipped instead of failing the deploy.

---

## Step 1 — Parse and validate

1. `$ARGUMENTS` must contain exactly one of `patch`, `minor`, `major`.
   If missing or ambiguous, **ask** — do not guess.
2. Show the current state:
   ```bash
   source .venv/bin/activate && python scripts/release.py status
   ```
3. Confirm the branch. Releases are cut from `dev` (or `staging` during a
   freeze) — **never from a feature branch or a worktree**.
   ```bash
   git rev-parse --abbrev-ref HEAD && git rev-parse --show-toplevel
   ```
   If HEAD is not `dev`/`staging`/`main`, or the toplevel is under
   `.claude/worktrees/`, **abort** and say why.
4. The working tree must be clean:
   ```bash
   git status --porcelain
   ```
   If dirty, abort: "Commit or stash first — a release commit must contain
   only version files."
5. Check for a concurrent SDD worker before touching shared refs (a live
   worker may `merge` / `reset --hard` the branch under you):
   ```bash
   ps -eo pid,etime,args | grep -i '[s]dd-worker'
   ```
   If one is live, warn and ask whether to continue.

## Step 2 — Preview

Always dry-run first and show the table to the user:

```bash
source .venv/bin/activate && python scripts/release.py bump <part> --dry-run
```

If `--dry-run` was in `$ARGUMENTS`, **stop here** and report.

## Step 3 — CHANGELOG

`CHANGELOG.md` opens with a `## [Unreleased] — <topic>` section.

1. Read the current `[Unreleased]` block.
2. Read what actually landed since the last tag:
   ```bash
   git log --oneline $(git tag --sort=-creatordate | head -1)..HEAD
   ```
3. **Draft** the release section — do not auto-paste the git log. Retitle
   `[Unreleased]` to `## [X.Y.Z] — YYYY-MM-DD` (keep its topic suffix if it
   still describes the release), fold in anything from the log the section
   missed, and open a fresh empty `## [Unreleased]` above it.
4. **Show the draft and get approval** before writing.

## Step 4 — Bump, commit, tag, push

```bash
source .venv/bin/activate && python scripts/release.py bump <part> --commit --tag --push
```

If the CHANGELOG was edited, stage it into the same commit — amend right
after, or pass the CHANGELOG through `git add` before running the script.

The script refuses to reuse an existing tag and pushes the branch plus tags.

If `--no-release` was passed, **stop here** and print the follow-up command.

## Step 5 — GitHub Release (irreversible — confirm)

Creating the release triggers `release.yml`, which uploads to PyPI. **PyPI
never allows re-uploading a version.** So:

1. State plainly what is about to happen and which packages will publish.
2. **Ask for explicit confirmation.** Do not proceed on an implied yes.
3. Then:
   ```bash
   source .venv/bin/activate && python scripts/release.py gh-release --tag <X.Y.Z>
   ```
   Notes come from `--generate-notes` by default; pass `--notes-file` to use
   the CHANGELOG section instead. Add `--draft` to stage the release without
   triggering the publish.
4. Watch the run: `gh run watch`

## Step 6 — Report

```
Release vX.Y.Z

  Packages bumped:  11  (navrules across 3 files)
  Core pins synced: 5 pyproject.toml
  Commit:           <sha>
  Tag:              X.Y.Z  (pushed)
  GH Release:       created / skipped
  PyPI:             building — gh run watch

CHANGELOG: <2-3 bullet summary>
```

## Error handling

| Situation | Action |
|---|---|
| No bump type in `$ARGUMENTS` | Ask. Never default to `patch`. |
| Dirty working tree | Abort — release commits carry version files only. |
| On a feature branch or in a worktree | Abort. Releases come from `dev`/`staging`. |
| Tag already exists | Abort — the previous release did not finish; investigate. |
| Live `sdd-worker` detected | Warn, ask before touching shared refs. |
| `gh release view` says it exists | Abort — already published. |
| PyPI upload fails for one package | `skip-existing: true` already skips unchanged ones; a real failure needs a new patch version, never a re-upload. |

## Related

| Command | Purpose |
|---|---|
| `/release` | Bump + tag + publish (THIS) |
| `make bump-patch-<pkg>` | Bump one package by hand, no commit |
| `python scripts/release.py status` | Read-only version table |
