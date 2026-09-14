"""``check_id_collisions.py`` — defense-in-depth SDD ID collision scanner.

Independent, read-only backstop for the git-native compare-and-swap
allocator (``reserve_ids.py``, FEAT-387): scans the repo's current SDD
state for any ``TASK-<NNN>`` number associated with more than one distinct
feature slug. This is the check that would have caught the six real
collisions found during FEAT-380's closeout
(TASK-1939/1940/1941/1942/1944/1946, each shared with an unrelated
feature) had it existed at the time.

``TASK-<NNN>`` collisions across different slugs are treated as failures.
``FEAT-<NNN>`` reuse across specs is an accepted, pre-existing pattern
(``migrate_index.py``'s own documented convention — see FEAT-380 itself,
split across three specs sharing one FEAT-ID by design) and is reported
informationally only, never as a failure.

Usage:
    python -m scripts.sdd.check_id_collisions [--index-dir DIR] [--active-dir DIR]
        [--completed-dir DIR] [--specs-dir DIR]
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

from pydantic import BaseModel

#: Matches a TASK filename prefix, e.g. "TASK-1939-repl-dedicated-executor.md".
_TASK_FILENAME_RE = re.compile(r"^(TASK-\d+)-")

#: Matches the spec header line, e.g. "**Feature ID**: FEAT-387".
_FEATURE_ID_HEADER_RE = re.compile(r"\*\*Feature ID\*\*:\s*FEAT-(\d+)")


class CollisionReport(BaseModel):
    """One colliding numeric ID and every distinct slug/source using it."""

    id: str  # e.g. "TASK-1939"
    kind: str  # "task" | "feature"
    slugs: list[str]  # every distinct feature slug found using this id
    sources: list[str]  # file paths where each was found


def _id_sort_key(task_id: str) -> tuple[int, int, str]:
    """Order ids for reporting, tolerating shapes other than ``TASK-<NNN>``.

    Not every id in a per-spec index is ``TASK-<NNN>``: hotfix indexes use
    ``HOTFIX-<slug>-<n>`` (e.g. ``HOTFIX-chromemanager-async-migration-1``)
    and some legacy features use ``TASK-<NNN>-<NNN>``. The previous key,
    ``int(t.split("-")[1])``, raised ValueError on the first hotfix id and
    aborted the run — so this checker, whose entire job is to catch TASK-ID
    collisions, was detecting nothing at all.

    Ordering is presentational only; it never affects detection.

    Args:
        task_id: The identifier to order.

    Returns:
        A sort key placing numeric ids first (in numeric order), then any
        other shape, deterministically by string.
    """
    parts = task_id.split("-")
    if len(parts) > 1 and parts[1].isdigit():
        return (0, int(parts[1]), task_id)
    return (1, 0, task_id)


def _slug_from_filename(path: Path) -> str | None:
    """Best-effort slug extraction from a `TASK-<NNN>-<slug>.md` filename."""
    match = _TASK_FILENAME_RE.match(path.name)
    if not match:
        return None
    return path.stem[len(match.group(1)) + 1 :]


def find_collisions(
    index_dir: Path = Path("sdd/tasks/index"),
    active_dir: Path = Path("sdd/tasks/active"),
    completed_dir: Path = Path("sdd/tasks/completed"),
    specs_dir: Path = Path("sdd/specs"),
) -> list[CollisionReport]:
    """Scan SDD state for TASK-ID collisions and FEAT-ID reuse.

    Scans ``index_dir/*.json`` (task ``id`` fields + their owning
    ``feature``/``feature_id``), ``active_dir/*.md``, and
    ``completed_dir/*.md`` (filenames, matched by ``TASK-<NNN>-`` prefix)
    for any ``TASK-<NNN>`` number used by more than one distinct feature
    slug. Separately tallies ``FEAT-<NNN>`` -> spec slug from
    ``specs_dir/*.md`` headers for an informational (never-failing) report.

    Args:
        index_dir: Directory containing per-spec index JSON files.
        active_dir: Directory containing in-progress task markdown files.
        completed_dir: Directory containing completed task markdown files.
        specs_dir: Directory containing spec markdown files.

    Returns:
        One ``CollisionReport`` per colliding ``TASK-<NNN>`` number (kind
        ``"task"``) PLUS one per shared ``FEAT-<NNN>`` number (kind
        ``"feature"``, informational only — callers must not treat these
        as failures). Empty list overall = a fully clean tree.
    """
    # Two INDEPENDENT namespaces, tracked separately and never
    # cross-compared directly against each other — a per-task index entry's
    # "feature" slug (e.g. "sandbox-hardening") and a task file's own
    # descriptive slug (e.g. "repl-dedicated-executor", the `<slug>` in
    # `TASK-<NNN>-<slug>.md`) are never expected to be equal even for a
    # single, correctly-owned task, so mixing them into one dict produces
    # a false "collision" for nearly every healthy task in the repo.
    feature_owners: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    file_slugs: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))

    # 1. Walk sdd/tasks/index/*.json (including _orphans.json — it carries
    #    real task entries with real IDs, even though it has no meaningful
    #    feature_id header), read each task's own "id"/"feature" fields
    #    (authoritative — defends against a task somehow being appended to
    #    the wrong index file). This alone already detects a real,
    #    different-feature TASK-ID collision (confirmed empirically: the
    #    six known FEAT-380-era collisions are each present in TWO
    #    different per-spec index files under the same TASK-<NNN>).
    if index_dir.is_dir():
        for index_file in sorted(index_dir.glob("*.json")):
            try:
                doc = json.loads(index_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            for task in doc.get("tasks", []) or []:
                task_id = task.get("id")
                slug = task.get("feature")
                if not isinstance(task_id, str) or not slug:
                    continue
                feature_owners[task_id][slug].append(str(index_file))

    # 2. Independently scan sdd/tasks/active/*.md + completed/*.md
    #    filenames: if the SAME TASK-<NNN> prefix is used by files with
    #    genuinely DIFFERENT descriptive slugs, that is direct evidence of
    #    two distinct task instances sharing one number — regardless of
    #    whether the index disambiguates them by feature. This is a
    #    defense-in-depth fallback for the case where the index itself is
    #    missing or incomplete (e.g. a task file exists but was never
    #    recorded in any per-spec index).
    for task_dir in (active_dir, completed_dir):
        if not task_dir.is_dir():
            continue
        for task_file in sorted(task_dir.glob("TASK-*.md")):
            match = _TASK_FILENAME_RE.match(task_file.name)
            if not match:
                continue
            task_id = match.group(1)
            slug = _slug_from_filename(task_file)
            if not slug:
                continue
            file_slugs[task_id][slug].append(str(task_file))

    all_task_ids = set(feature_owners) | set(file_slugs)
    reports: list[CollisionReport] = []
    for task_id in sorted(all_task_ids, key=_id_sort_key):
        owners = feature_owners.get(task_id, {})
        if len(owners) > 1:
            # Authoritative: two different features' indexes both claim
            # this TASK-ID.
            reports.append(
                CollisionReport(
                    id=task_id,
                    kind="task",
                    slugs=sorted(owners),
                    sources=[src for slug in sorted(owners) for src in owners[slug]],
                )
            )
            continue

        slugs = file_slugs.get(task_id, {})
        if len(slugs) > 1:
            # Fallback: the index doesn't disambiguate (missing/incomplete),
            # but the filenames themselves reveal two distinct task
            # instances sharing this number.
            reports.append(
                CollisionReport(
                    id=task_id,
                    kind="task",
                    slugs=sorted(slugs),
                    sources=[src for slug in sorted(slugs) for src in slugs[slug]],
                )
            )

    # 3. Separately tally FEAT-<NNN> -> spec slug from sdd/specs/*.md
    #    headers for the informational (non-failing) report. Deliberately a
    #    DIFFERENT dict from `feature_owners` above (which tracks TASK-ID
    #    ownership) — reusing the same name for this unrelated FEAT-ID
    #    namespace would be a readability/maintenance hazard.
    feat_id_owners: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    if specs_dir.is_dir():
        for spec_file in sorted(specs_dir.glob("*.md")):
            try:
                text = spec_file.read_text(encoding="utf-8")
            except OSError:
                continue
            match = _FEATURE_ID_HEADER_RE.search(text)
            if not match:
                continue
            feature_id = f"FEAT-{match.group(1)}"
            slug = spec_file.stem.removesuffix(".spec")
            feat_id_owners[feature_id][slug].append(str(spec_file))

    for feature_id in sorted(feat_id_owners, key=lambda f: int(f.split("-")[1])):
        owners = feat_id_owners[feature_id]
        if len(owners) > 1:
            reports.append(
                CollisionReport(
                    id=feature_id,
                    kind="feature",
                    slugs=sorted(owners),
                    sources=[src for slug in sorted(owners) for src in owners[slug]],
                )
            )

    return reports


def _load_baseline(path: Path | None) -> set[str]:
    """Load a baseline allowlist of pre-existing, non-fatal TASK-ID collisions.

    Supports the one-time baseline exception (spec §5 Acceptance Criteria,
    last bullet): the six FEAT-380-era collisions — and, empirically, many
    more pre-``per-spec-index`` historical ``TASK-<NNN>`` reuses predating
    the global counter (FEAT-145) — must NOT retroactively fail CI. IDs
    listed here are still printed in the report (never hidden), just
    excluded from the failing set.

    Args:
        path: Path to a JSON file containing a list of ``TASK-<NNN>``
            strings (e.g. ``["TASK-1939", "TASK-1940"]``), or ``None``.

    Returns:
        The set of baselined IDs; empty if ``path`` is ``None`` or the
        file is missing/unreadable.
    """
    if path is None:
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    if isinstance(data, list):
        return {str(item) for item in data}
    return set()


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Exits 1 and prints a human-readable report naming every offending
    file/slug if any NEW (non-baselined) ``TASK-<NNN>`` collision is
    found; exits 0 otherwise. ``FEAT-<NNN>`` reuse is always printed
    informationally and never affects the exit code. Baselined
    collisions (``--baseline``) are still printed, but marked as
    non-fatal and excluded from the exit-code decision.
    """
    parser = argparse.ArgumentParser(
        description="Scan sdd/ for TASK-<NNN> collisions across features (FEAT-387).",
    )
    parser.add_argument("--index-dir", type=Path, default=Path("sdd/tasks/index"))
    parser.add_argument("--active-dir", type=Path, default=Path("sdd/tasks/active"))
    parser.add_argument("--completed-dir", type=Path, default=Path("sdd/tasks/completed"))
    parser.add_argument("--specs-dir", type=Path, default=Path("sdd/specs"))
    parser.add_argument(
        "--baseline",
        type=Path,
        default=None,
        help=(
            "Path to a JSON allowlist of pre-existing TASK-<NNN> collisions "
            "to report but NOT fail on (one-time baseline exception)."
        ),
    )
    args = parser.parse_args(argv)

    collisions = find_collisions(
        index_dir=args.index_dir,
        active_dir=args.active_dir,
        completed_dir=args.completed_dir,
        specs_dir=args.specs_dir,
    )
    baseline = _load_baseline(args.baseline)

    task_collisions = [c for c in collisions if c.kind == "task"]
    feature_reuse = [c for c in collisions if c.kind == "feature"]
    new_task_collisions = [c for c in task_collisions if c.id not in baseline]
    baselined_task_collisions = [c for c in task_collisions if c.id in baseline]

    if task_collisions:
        print("TASK-<NNN> collisions found:")
        for report in task_collisions:
            baseline_note = " (baselined — non-fatal)" if report.id in baseline else ""
            print(f"  {report.id}{baseline_note}: slugs={report.slugs}")
            for source in report.sources:
                print(f"    - {source}")

    if feature_reuse:
        print("FEAT-<NNN> reuse across specs (informational, not a failure):")
        for report in feature_reuse:
            print(f"  {report.id}: slugs={report.slugs}")
            for source in report.sources:
                print(f"    - {source}")

    if new_task_collisions:
        print(
            f"\nFAIL: {len(new_task_collisions)} new TASK-<NNN> collision(s) found "
            f"({len(baselined_task_collisions)} pre-existing, baselined "
            "collision(s) ignored)."
        )
        return 1

    print(
        f"OK: no new TASK-<NNN> collisions "
        f"({len(baselined_task_collisions)} pre-existing, baselined collision(s) "
        f"ignored, {len(feature_reuse)} informational FEAT-<NNN> reuse note(s))."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
