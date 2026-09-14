"""One-shot migration: monolithic ``sdd/tasks/.index.json`` → per-spec index files.

The script reads the legacy monolith, groups tasks by their ``feature`` slug
(NOT ``feature_id`` — multiple specs share a numeric ID in the wild, so the
slug is the disambiguator), and writes one JSON file per feature into
``sdd/tasks/index/<feature>.json``. Tasks without a resolvable feature go to
``_orphans.json`` with a stderr warning per orphan.

The script is **idempotent** — re-running on the same input produces
byte-equivalent output. It NEVER modifies or deletes the source monolith;
removing the source is a separate, explicit step performed by a human after
verifying the migration.

Usage:
    python -m scripts.sdd.migrate_index [--source PATH] [--dest DIR] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


def _completed_at_of_group(tasks: list[dict[str, Any]]) -> str | None:
    """Return the maximum ``completed_at`` across a group, or ``None``.

    A group is considered complete only when *every* task in it has
    ``status == "done"``. Reading the timestamp from the task entries
    themselves (instead of ``datetime.now()``) keeps the script
    byte-stable across reruns.
    """
    if not tasks or not all(t.get("status") == "done" for t in tasks):
        return None
    stamps = [t.get("completed_at") for t in tasks if t.get("completed_at")]
    return max(stamps) if stamps else None


def _build_meta_registry(raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Build a slug → header-metadata map from the monolith.

    Combines the current top-level feature with the ``previous_features``
    registry so every historic feature has a metadata source for its
    per-spec index header.
    """
    meta: dict[str, dict[str, Any]] = {}

    current_slug = raw.get("feature")
    if current_slug:
        meta[current_slug] = {
            "feature": current_slug,
            "feature_id": raw.get("feature_id"),
            "spec": raw.get("spec"),
            "created_at": raw.get("created_at"),
        }

    for prev in raw.get("previous_features", []) or []:
        slug = prev.get("feature")
        if slug and slug not in meta:
            meta[slug] = {
                "feature": slug,
                "feature_id": prev.get("feature_id"),
                "spec": prev.get("spec"),
                "created_at": prev.get("created_at"),
            }

    return meta


def migrate(source: Path, dest: Path, dry_run: bool = False) -> int:
    """Split the monolithic ``source`` index into per-spec files under ``dest``.

    Args:
        source: Path to the legacy ``sdd/tasks/.index.json``.
        dest: Directory where per-spec ``<feature>.json`` files are written
            (created if missing).
        dry_run: When ``True``, compute the plan and emit warnings but do
            not write any files.

    Returns:
        Process exit code: ``0`` on success.
    """
    raw = json.loads(source.read_text(encoding="utf-8"))
    feat_meta = _build_meta_registry(raw)

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    orphans: list[dict[str, Any]] = []
    for task in raw.get("tasks", []) or []:
        slug = task.get("feature")
        if not slug:
            orphans.append(task)
            continue
        grouped[slug].append(task)

    if not dry_run:
        dest.mkdir(parents=True, exist_ok=True)

    for slug in sorted(grouped):
        tasks = grouped[slug]
        meta = feat_meta.get(slug) or {
            "feature": slug,
            "feature_id": tasks[0].get("feature_id"),
            "spec": tasks[0].get("spec"),
            "created_at": None,
        }
        index_doc = {
            "feature": meta["feature"],
            "feature_id": meta.get("feature_id"),
            "spec": meta.get("spec"),
            "type": "feature",
            "base_branch": "dev",
            "created_at": meta.get("created_at"),
            "completed_at": _completed_at_of_group(tasks),
            "tasks": tasks,
        }
        if not dry_run:
            (dest / f"{slug}.json").write_text(
                json.dumps(index_doc, indent=2, sort_keys=False) + "\n",
                encoding="utf-8",
            )

    if orphans:
        for o in orphans:
            print(
                f"WARN: TASK-{o.get('id', '?')} has no feature; routed to _orphans.json",
                file=sys.stderr,
            )
        orph_doc = {
            "feature": "_orphans",
            "feature_id": None,
            "spec": None,
            "type": "feature",
            "base_branch": "dev",
            "created_at": None,
            "completed_at": None,
            "tasks": orphans,
        }
        if not dry_run:
            (dest / "_orphans.json").write_text(
                json.dumps(orph_doc, indent=2, sort_keys=False) + "\n",
                encoding="utf-8",
            )

    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for the migration script."""
    parser = argparse.ArgumentParser(
        description="Migrate monolithic SDD index to per-spec index files.",
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=Path("sdd/tasks/.index.json"),
        help="Path to the legacy monolithic index (default: sdd/tasks/.index.json)",
    )
    parser.add_argument(
        "--dest",
        type=Path,
        default=Path("sdd/tasks/index"),
        help="Destination directory for per-spec index files (default: sdd/tasks/index)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan only — do not write files (orphan warnings still emitted).",
    )
    args = parser.parse_args(argv)
    return migrate(args.source, args.dest, args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
