#!/usr/bin/env python
"""Ruff runner that reports only the findings a branch introduced.

The dev-loop QA gate scopes lint to the files a feature changed — correct,
but too blunt: a five-line addition to a module carrying 28 pre-existing
``UP``-series findings turns that whole backlog into a blocker for the
feature, and the QA feedback then asks the worker to modernise a file it
barely touched. This filters ruff's output down to findings whose reported
source range intersects a line the branch actually added or modified.

Range intersection, not "the finding starts on a changed line", is the point:
``I001`` (un-sorted imports) is reported at the top of the import block with
an ``end_location`` spanning the whole block, so an import added five lines
below the reported row is still correctly attributed to this branch.

Usage:
    python -m scripts.sdd.lint_new [--base <ref>] <file> [<file> ...]

Exit status:
    0 — the branch introduced no new ruff finding (pre-existing ones are
        reported as an informational count only).
    1 — at least one new finding; each is printed in ``ruff --output-format
        concise`` shape.
    2 — ruff itself could not run.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

#: Refs tried, in order, to locate the branch point when --base is omitted.
#: Mirrors ``QANode._get_changed_files``' own candidate order.
_BASE_CANDIDATES = ("origin/dev", "origin/main")

_HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")


def _git(*args: str) -> str:
    """Run a git command, returning stdout — or ``""`` on any failure."""
    try:
        proc = subprocess.run(["git", *args], capture_output=True, text=True, check=False)
    except OSError:
        return ""
    return proc.stdout if proc.returncode == 0 else ""


def _merge_base(base: str | None) -> str:
    """The commit this branch forked from, or ``""`` when undeterminable."""
    for ref in ([base] if base else list(_BASE_CANDIDATES)):
        sha = _git("merge-base", ref, "HEAD").strip()
        if sha:
            return sha
    return ""


def _parse_hunks(diff_text: str, added: dict[str, set[int]]) -> None:
    """Fold ``git diff -U0`` output into a path -> added-line-numbers map."""
    current: str | None = None
    for line in diff_text.splitlines():
        if line.startswith("+++ b/"):
            current = line[len("+++ b/") :].strip()
            added.setdefault(current, set())
        elif line.startswith("@@") and current is not None:
            match = _HUNK_RE.match(line)
            if match:
                start = int(match.group(1))
                count = int(match.group(2) or 1)
                added[current].update(range(start, start + count))


def _added_lines(merge_base: str, paths: list[str]) -> dict[str, set[int]]:
    """Every line this branch added or modified, per path.

    One ``git diff`` against the merge base covers committed AND uncommitted
    work in a single call; untracked files are new in their entirety and are
    added separately.
    """
    added: dict[str, set[int]] = {}
    if merge_base:
        _parse_hunks(_git("diff", "-U0", merge_base, "--", *paths), added)

    untracked = _git("ls-files", "--others", "--exclude-standard", "--", *paths).split()
    for rel in untracked:
        try:
            total = len(Path(rel).read_text(encoding="utf-8", errors="replace").splitlines())
        except OSError:
            continue
        added.setdefault(rel, set()).update(range(1, total + 1))

    return added


def _ruff_findings(paths: list[str]) -> list[dict] | None:
    """Ruff's JSON findings for ``paths``, or ``None`` when ruff failed."""
    try:
        proc = subprocess.run(
            ["ruff", "check", "--output-format", "json", "--force-exclude", *paths],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    # ruff exits 1 when it finds violations — that is not a failure here.
    if proc.returncode not in (0, 1):
        sys.stderr.write(proc.stderr)
        return None
    try:
        return json.loads(proc.stdout or "[]")
    except json.JSONDecodeError:
        sys.stderr.write(proc.stdout)
        return None


def _is_new(finding: dict, added: dict[str, set[int]], repo_root: str) -> bool:
    """Whether a finding's source range intersects a line this branch changed."""
    filename = finding.get("filename") or ""
    try:
        rel = os.path.relpath(filename, repo_root)
    except ValueError:
        return True  # cannot attribute it — fail closed, report it
    lines = added.get(rel)
    if not lines:
        return False
    start = int((finding.get("location") or {}).get("row") or 0)
    end = int((finding.get("end_location") or {}).get("row") or start)
    if start <= 0:
        return True
    return any(row in lines for row in range(start, max(end, start) + 1))


def _format(finding: dict, repo_root: str) -> str:
    """One finding in ``ruff --output-format concise`` shape."""
    filename = finding.get("filename") or ""
    try:
        rel = os.path.relpath(filename, repo_root)
    except ValueError:
        rel = filename
    location = finding.get("location") or {}
    return (
        f"{rel}:{location.get('row', 0)}:{location.get('column', 0)}: "
        f"{finding.get('code') or '?'} {finding.get('message') or ''}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Report only the ruff findings this branch introduced.")
    parser.add_argument("--base", default=None, help="Base ref (default: origin/dev, then origin/main)")
    parser.add_argument("paths", nargs="*", help="Files to check")
    args = parser.parse_args(argv)

    if not args.paths:
        print("lint_new: no files to check — nothing to do.")
        return 0

    repo_root = _git("rev-parse", "--show-toplevel").strip() or os.getcwd()

    findings = _ruff_findings(args.paths)
    if findings is None:
        print("lint_new: ruff could not be run.", file=sys.stderr)
        return 2

    added = _added_lines(_merge_base(args.base), args.paths)
    new = [f for f in findings if _is_new(f, added, repo_root)]
    pre_existing = len(findings) - len(new)

    for finding in new:
        print(_format(finding, repo_root))

    if new:
        print(
            f"lint_new: {len(new)} new finding(s) introduced by this branch "
            f"({pre_existing} pre-existing finding(s) ignored)."
        )
        return 1

    print(f"lint_new: no new findings ({pre_existing} pre-existing finding(s) " f"in the changed files were ignored).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
