"""``IdLedger`` model + bootstrap for the git-native TASK/FEAT ID allocator.

This module defines the tiny, git-tracked "next ID" ledger
(``sdd/tasks/.id_ledger.json``) used by ``scripts/sdd/reserve_ids.py`` as a
compare-and-swap counter for globally-unique ``TASK-<NNN>`` (and
best-effort ``FEAT-<NNN>``) numbers (FEAT-387).

``IdLedger`` is deliberately kept a pure data container — no
reserve/increment logic lives here. That belongs to ``reserve_ids.py``
(Module 2), keeping this module and its allocator independently testable
per the spec's Module Breakdown.

Usage:
    python -m scripts.sdd.id_ledger bootstrap [--index-dir DIR] [--specs-dir DIR] [--out PATH]
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field

#: Default path to the git-tracked ledger file.
LEDGER_PATH = Path("sdd/tasks/.id_ledger.json")

#: Matches the spec header line, e.g. "**Feature ID**: FEAT-387".
_FEATURE_ID_HEADER_RE = re.compile(r"\*\*Feature ID\*\*:\s*FEAT-(\d+)")


class IdLedger(BaseModel):
    """The full contents of ``sdd/tasks/.id_ledger.json``.

    A single, tiny, git-tracked file acting as a compare-and-swap counter
    for globally-unique ``TASK-<NNN>`` numbers (and, best-effort,
    ``FEAT-<NNN>`` numbers). Every allocator reads this file, computes a
    reservation, and races to push an update — the push itself is the
    compare-and-swap: a non-fast-forward rejection means someone else
    already advanced the counter, so the allocator must re-read and retry.
    """

    next_task_id: int = Field(..., ge=1, description="Next unassigned TASK number.")
    next_feature_id: int = Field(..., ge=1, description="Next unassigned FEAT number.")
    updated_at: str = Field(..., description="ISO-8601 UTC timestamp of the last reservation.")
    updated_by: str = Field(
        ...,
        description=(
            "Free-text origin of the last reservation (feature slug or "
            "session id) — diagnostic only, not used for correctness."
        ),
    )


def load_ledger(path: Path) -> IdLedger:
    """Load and validate an ``IdLedger`` from ``path``.

    Args:
        path: Path to the ledger JSON file.

    Returns:
        The parsed and validated ``IdLedger``.
    """
    return IdLedger.model_validate_json(path.read_text(encoding="utf-8"))


def save_ledger(path: Path, ledger: IdLedger) -> None:
    """Write ``ledger`` to ``path`` as byte-stable JSON.

    Matches ``migrate_index.py``'s own output convention
    (``json.dumps(..., indent=2, sort_keys=False)`` plus a trailing
    newline) so diffs stay small and reviewable.

    Args:
        path: Destination path for the ledger JSON file.
        ledger: The ``IdLedger`` to serialize.
    """
    path.write_text(
        json.dumps(ledger.model_dump(), indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def _max_task_id(index_dir: Path) -> int:
    """Return the highest ``TASK-<NNN>`` number found across ``index_dir``.

    Scans every ``*.json`` file in ``index_dir`` (including
    ``_orphans.json`` — it carries real task entries with real IDs, even
    though it has no ``feature_id`` header worth scanning for the FEAT
    counter) and returns ``0`` if none are found.
    """
    max_id = 0
    for index_file in index_dir.glob("*.json"):
        try:
            doc = json.loads(index_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for task in doc.get("tasks", []) or []:
            task_id = task.get("id")
            if not isinstance(task_id, str):
                continue
            match = re.match(r"TASK-(\d+)", task_id)
            if match:
                max_id = max(max_id, int(match.group(1)))
    return max_id


def _max_feature_id(index_dir: Path, specs_dir: Path) -> int:
    """Return the highest ``FEAT-<NNN>`` number across index headers + specs.

    Scans both ``index_dir/*.json`` (top-level ``feature_id`` header,
    skipping ``_orphans.json`` since it has no meaningful header) AND
    ``specs_dir/*.md`` (the ``**Feature ID**: FEAT-<NNN>`` line), taking
    the max across both so the ledger never seeds behind an ID already in
    use by a spec with no tasks generated yet.
    """
    max_id = 0

    for index_file in index_dir.glob("*.json"):
        if index_file.name == "_orphans.json":
            continue
        try:
            doc = json.loads(index_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        feature_id = doc.get("feature_id")
        if isinstance(feature_id, str):
            match = re.match(r"FEAT-(\d+)", feature_id)
            if match:
                max_id = max(max_id, int(match.group(1)))

    for spec_file in specs_dir.glob("*.md"):
        try:
            text = spec_file.read_text(encoding="utf-8")
        except OSError:
            continue
        for match in _FEATURE_ID_HEADER_RE.finditer(text):
            max_id = max(max_id, int(match.group(1)))

    return max_id


def bootstrap_ledger(
    index_dir: Path = Path("sdd/tasks/index"),
    specs_dir: Path = Path("sdd/specs"),
) -> IdLedger:
    """Seed a fresh ``IdLedger`` strictly ahead of every ID currently in use.

    Scans ``sdd/tasks/index/*.json`` (all ``id``/``feature_id`` fields) AND
    ``sdd/specs/*.md`` (the ``**Feature ID**: FEAT-<NNN>`` header line) for
    the current maximum ``TASK-<NNN>`` and ``FEAT-<NNN>`` respectively, and
    returns an ``IdLedger`` with ``next_task_id``/``next_feature_id`` set to
    ``max + 1``.

    Args:
        index_dir: Directory containing per-spec index JSON files.
        specs_dir: Directory containing spec markdown files.

    Returns:
        A freshly bootstrapped ``IdLedger``.
    """
    max_task = _max_task_id(index_dir)
    max_feature = _max_feature_id(index_dir, specs_dir)

    return IdLedger(
        next_task_id=max_task + 1,
        next_feature_id=max_feature + 1,
        updated_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        updated_by="bootstrap",
    )


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: ``python -m scripts.sdd.id_ledger bootstrap``."""
    parser = argparse.ArgumentParser(
        description="IdLedger bootstrap for the SDD TASK/FEAT ID allocator (FEAT-387).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap_parser = subparsers.add_parser(
        "bootstrap",
        help="Seed sdd/tasks/.id_ledger.json strictly ahead of every ID currently in use.",
    )
    bootstrap_parser.add_argument(
        "--index-dir",
        type=Path,
        default=Path("sdd/tasks/index"),
        help="Directory containing per-spec index JSON files (default: sdd/tasks/index)",
    )
    bootstrap_parser.add_argument(
        "--specs-dir",
        type=Path,
        default=Path("sdd/specs"),
        help="Directory containing spec markdown files (default: sdd/specs)",
    )
    bootstrap_parser.add_argument(
        "--out",
        type=Path,
        default=LEDGER_PATH,
        help=f"Destination path for the bootstrapped ledger (default: {LEDGER_PATH})",
    )

    args = parser.parse_args(argv)

    if args.command == "bootstrap":
        ledger = bootstrap_ledger(index_dir=args.index_dir, specs_dir=args.specs_dir)
        save_ledger(args.out, ledger)
        print(
            f"Bootstrapped {args.out}: next_task_id={ledger.next_task_id}, "
            f"next_feature_id={ledger.next_feature_id}"
        )
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
