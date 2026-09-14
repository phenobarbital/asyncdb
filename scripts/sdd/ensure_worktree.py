"""Idempotent feature/hotfix worktree provisioning for SDD commands (FEAT-552).

Replaces the hand-rolled ``git worktree add`` blocks that used to live in
``/sdd-task``, ``/sdd-start``, ``sdd-worker``, ``sdd-planner``,
``sdd-research`` and ``sdd-autopilot``. Naming and base ref come from
``scripts.sdd.sdd_meta.plan_worktree`` — never built here.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Sequence

from scripts.sdd.sdd_meta import WorktreePlan, plan_worktree, resolve_flow

logger = logging.getLogger(__name__)


class EnsureWorktreeError(RuntimeError):
    """Raised when the worktree cannot be provisioned (CLI exit code 1)."""


def _git(*args: str, cwd: Path) -> str:
    """Run a git command, returning stdout; raise EnsureWorktreeError on failure."""
    res = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    if res.returncode != 0:
        cmd_str = " ".join(["git", *args])
        raise EnsureWorktreeError(
            f"Command '{cmd_str}' failed with exit code {res.returncode}.\n"
            f"stdout: {res.stdout.strip()}\n"
            f"stderr: {res.stderr.strip()}"
        )
    return res.stdout


def ensure(
    plan: WorktreePlan,
    *,
    repo_root: Path,
    sync: bool = True,
    require_paths: Sequence[str] = (),
    dry_run: bool = False,
) -> tuple[Path, bool]:
    """Create the worktree if absent, reuse it if present, and verify it.

    Steps, in order:
      1. Reuse — if ``git worktree list --porcelain`` already lists a worktree
         whose path basename is ``plan.name``, confirm its checked-out branch
         is ``plan.name`` and return it. A path holding a DIFFERENT branch is
         an error, never a silent reuse.
      2. Sync (skipped when ``sync`` is False) — ``git fetch origin
         <base_branch>``. No local branch is checked out; no local commit moves.
      3. Refuse when a branch named ``plan.name`` exists but is checked out
         nowhere — the operator decides (reuse it, or pick another slug).
      4. Create — ``git worktree add -b <name> <path> <base_ref>``.
      5. Verify — every ``require_paths`` entry must exist inside the worktree.
         A miss means the base does not carry the task artifacts yet.
      6. Return ``(absolute_path, created)``.

    Args:
        plan: The naming/base-ref decision from ``plan_worktree``.
        repo_root: Absolute path to the main clone.
        sync: Fetch ``origin/<base_branch>`` before creating.
        require_paths: Repo-relative paths that must exist in the worktree.
        dry_run: Resolve and report without running any mutating git command.

    Returns:
        ``(path, created)`` — ``created`` is False when an existing worktree
        was reused.

    Raises:
        EnsureWorktreeError: On any refusal above, or a failing git command.
    """
    _reject_absolute_paths(require_paths)
    target_path = (repo_root / plan.path).resolve()

    # Step 1: Reuse
    reused_path = _find_reusable_worktree(plan, target_path, repo_root=repo_root)
    if reused_path is not None:
        missing = _missing_paths(reused_path, require_paths)
        if missing:
            raise EnsureWorktreeError(f"Required path {missing[0]!r} does not exist in reused worktree {reused_path}.")
        return reused_path, False

    if dry_run:
        return target_path, True

    # Step 2: Sync — base_ref is origin/<base_branch>
    if sync:
        _git("fetch", "origin", plan.base_ref.split("/", 1)[1], cwd=repo_root)

    # Step 3: Refuse a branch that exists but is checked out nowhere. Step 1
    # already walked every active worktree, so none of them holds it.
    if _branch_exists(plan.name, repo_root=repo_root):
        raise EnsureWorktreeError(f"Branch {plan.name!r} already exists but is not checked out in any worktree.")

    # Step 4: Create
    _create_worktree(plan, target_path, repo_root=repo_root)

    # Step 5: Verify
    missing = _missing_paths(target_path, require_paths)
    if missing:
        _discard_new_worktree(plan, target_path, repo_root=repo_root)
        raise EnsureWorktreeError(
            f"Verification failed: the following required paths were missing from the new worktree: "
            f"{', '.join(missing)}"
        )

    return target_path, True


def _reject_absolute_paths(require_paths: Sequence[str]) -> None:
    """Refuse absolute ``require_paths`` entries.

    ``Path(x) / y`` silently discards ``x`` when ``y`` is absolute, which would
    make the Step 5 verification report success for a path that was never
    inside the new worktree. ``require_paths`` is documented as repo-relative —
    enforce that instead of failing open.
    """
    for req in require_paths:
        if Path(req).is_absolute():
            raise EnsureWorktreeError(f"require_paths entries must be repo-relative, got absolute path: {req!r}")


def _worktree_branches(repo_root: Path) -> dict[Path, str]:
    """Map every registered worktree path to its checked-out local branch.

    Parses ``git worktree list --porcelain``, whose blocks look like::

        worktree /path/to/worktree
        branch refs/heads/branch-name

    A worktree with no ``branch`` line (detached HEAD, bare) is left out.
    """
    out = _git("worktree", "list", "--porcelain", cwd=repo_root)
    current: Path | None = None
    branches: dict[Path, str] = {}
    for raw in out.splitlines():
        line = raw.strip()
        if line.startswith("worktree "):
            current = Path(line[len("worktree ") :]).resolve()
        elif line.startswith("branch refs/heads/") and current is not None:
            branches[current] = line[len("branch refs/heads/") :]
    return branches


def _find_reusable_worktree(plan: WorktreePlan, target_path: Path, *, repo_root: Path) -> Path | None:
    """Return the existing worktree for ``plan``, ``None`` when there is none.

    A registered worktree matches by exact path or by basename ``plan.name``.
    One that is checked out on a DIFFERENT branch is an error, never a silent
    reuse.
    """
    for wt_path, branch_name in _worktree_branches(repo_root).items():
        if wt_path != target_path and wt_path.name != plan.name:
            continue
        if branch_name != plan.name:
            raise EnsureWorktreeError(
                f"Worktree at {wt_path} is checked out on branch {branch_name!r}, "
                f"but expected branch {plan.name!r}."
            )
        return wt_path
    return None


def _missing_paths(root: Path, require_paths: Sequence[str]) -> list[str]:
    """Return the ``require_paths`` entries that do not exist under ``root``."""
    return [req for req in require_paths if not (root / req).exists()]


def _branch_exists(name: str, *, repo_root: Path) -> bool:
    """True when a local branch called ``name`` exists (checked out or not)."""
    out = _git("branch", "--list", name, cwd=repo_root)
    return any(line.strip().replace("*", "").strip() == name for line in out.splitlines())


def _create_worktree(plan: WorktreePlan, target_path: Path, *, repo_root: Path) -> None:
    """``git worktree add -b <name> <path> <base_ref>``, pruning on failure."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        _git("worktree", "add", "-b", plan.name, str(target_path), plan.base_ref, cwd=repo_root)
    except EnsureWorktreeError:
        # Drop the half-registered entry git may have left behind.
        if target_path.exists():
            try:
                _git("worktree", "prune", cwd=repo_root)
            except Exception:  # noqa: BLE001 — best effort, the original error is what matters
                pass
        raise


def _discard_new_worktree(plan: WorktreePlan, target_path: Path, *, repo_root: Path) -> None:
    """Undo a worktree ``_create_worktree`` just made, after verification failed.

    Delete the directory and let ``git worktree prune`` drop the now-stale
    registration, then remove the branch we created — a plain, non-forced
    delete of an unpushed branch that carries no commits of its own, never a
    forced delete of a branch that might hold real work. ``prune`` is
    repo-wide by git's own design but harmless here: it only forgets
    registrations whose directory is already gone from disk, so a concurrent
    process's still-live worktree is never touched (verified empirically — an
    unrelated live worktree/branch survives a run that hits this path).
    """
    try:
        if target_path.is_dir():
            shutil.rmtree(target_path)
        _git("worktree", "prune", cwd=repo_root)
        _git("branch", "-d", plan.name, cwd=repo_root)
    except Exception as cleanup_err:  # noqa: BLE001 — never mask the verification error
        logger.warning("Failed to clean up worktree/branch after verification failure: %s", cleanup_err)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Prints the worktree path to stdout; 0 on success.

    With ``--json``, prints one object instead —
    ``{"name": …, "path": …, "base_ref": …, "created": bool}`` — so
    ``sdd-planner``/``sdd-research`` can lift ``worktree_path`` straight into
    their ``PlannerOutput``/``ResearchOutput`` contracts (spec §8).
    """
    parser = argparse.ArgumentParser(description="Idempotent feature/hotfix worktree provisioning for SDD commands.")
    parser.add_argument("--slug", required=True, help="Feature slug, kebab-case.")
    parser.add_argument("--feature-id", help="FEAT-<NNN>; required for feature runs.")
    parser.add_argument(
        "--jira-key",
        help="Jira issue key; required for hotfix runs. Implies --type hotfix "
        "and --base-branch main unless either is passed explicitly.",
    )
    parser.add_argument("--spec", help="Path to spec markdown file.")
    parser.add_argument("--index", help="Path to index JSON file.")
    parser.add_argument("--base-branch", help="Override base branch.")
    parser.add_argument("--type", choices=["feature", "hotfix"], help="Override flow type.")
    parser.add_argument("--no-sync", action="store_true", help="Skip fetching origin/<base_branch>.")
    parser.add_argument("--dry-run", action="store_true", help="Resolve and report without mutating git.")
    parser.add_argument("--json", action="store_true", help="Output JSON instead of bare path.")

    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    try:
        # Resolve flow. `--jira-key` with no explicit `--type` implies a
        # hotfix (and therefore `origin/main`) — without this, the
        # documented invocation `--slug <slug> --jira-key <KEY>` (see
        # CLAUDE.md and sdd-research.md) would fail with "feature_id is
        # required", since resolve_flow() defaults to type="feature" and
        # has no other signal here that this is a hotfix run.
        type_override = args.type
        base_branch_override = args.base_branch
        if args.jira_key and not type_override:
            type_override = "hotfix"
            if not base_branch_override:
                base_branch_override = "main"

        doc_path = Path(args.spec) if args.spec else None
        meta = resolve_flow(
            doc_path=doc_path,
            type_override=type_override,
            base_branch_override=base_branch_override,
        )

        # Plan worktree
        plan = plan_worktree(
            meta,
            slug=args.slug,
            feature_id=args.feature_id,
            jira_key=args.jira_key,
        )

        # Determine require_paths
        require_paths = []
        if args.spec:
            require_paths.append(args.spec)
        if args.index:
            require_paths.append(args.index)

        # Run ensure
        repo_root = Path.cwd()
        path, created = ensure(
            plan,
            repo_root=repo_root,
            sync=not args.no_sync,
            require_paths=require_paths,
            dry_run=args.dry_run,
        )

        if args.json:
            print(
                json.dumps(
                    {
                        "name": plan.name,
                        "path": str(path),
                        # Alias of "path" — sdd-planner/sdd-research read this
                        # key name into their PlannerOutput/ResearchOutput
                        # `worktree_path` field (spec §8); both keys always
                        # carry the same value.
                        "worktree_path": str(path),
                        "base_ref": plan.base_ref,
                        "created": created,
                    }
                )
            )
        else:
            print(str(path))

        return 0

    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
