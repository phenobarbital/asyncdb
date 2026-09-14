#!/usr/bin/env bash
#
# heal_orphans.sh — Self-healing sweep for stalled SDD task files.
#
# Detects any file in sdd/tasks/active/ whose task is marked "done" in a
# per-spec index, and removes it (its canonical copy already lives in
# sdd/tasks/completed/). This is the safety net behind /sdd-done: even if a
# task-completion step copied instead of moved (leaving an active orphan), the
# orphan is reaped here before the feature is closed.
#
# A task is only reaped when ALL of these hold (defence in depth):
#   1. some per-spec index marks <TASK-ID> as status="done"
#   2. a completed/ twin file for the same TASK-ID exists
# Otherwise the active file is left untouched (it may be genuinely in flight).
#
# Usage:
#   scripts/sdd/heal_orphans.sh [feature-slug]   # one feature, or all if omitted
#   scripts/sdd/heal_orphans.sh --dry-run [feature-slug]
#
# Exit codes:
#   0  swept (with or without removals)
#   1  usage / environment error
set -euo pipefail

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
  shift
fi
FEATURE_SLUG="${1:-}"

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

ACTIVE_DIR="sdd/tasks/active"
COMPLETED_DIR="sdd/tasks/completed"
INDEX_DIR="sdd/tasks/index"

if [[ -n "$FEATURE_SLUG" ]]; then
  indices=("$INDEX_DIR/${FEATURE_SLUG}.json")
else
  shopt -s nullglob
  indices=("$INDEX_DIR"/*.json)
  shopt -u nullglob
fi

# Collect the set of TASK-IDs that are "done" across the selected indices.
done_ids="$(
  for idx in "${indices[@]}"; do
    [[ -f "$idx" ]] || continue
    jq -r '.tasks[]? | select(.status == "done") | .id' "$idx" 2>/dev/null || true
  done | sort -u
)"

reaped=0
kept=0
while IFS= read -r tid; do
  [[ -z "$tid" ]] && continue
  shopt -s nullglob
  active_matches=("$ACTIVE_DIR/${tid}-"*.md)
  completed_matches=("$COMPLETED_DIR/${tid}-"*.md)
  shopt -u nullglob
  [[ ${#active_matches[@]} -eq 0 ]] && continue
  if [[ ${#completed_matches[@]} -eq 0 ]]; then
    echo "⚠️  ${tid}: done but NO completed/ twin — leaving active file (manual review)."
    kept=$((kept + 1))
    continue
  fi
  for f in "${active_matches[@]}"; do
    if [[ "$DRY_RUN" -eq 1 ]]; then
      echo "would reap: $f"
    else
      # git rm stages the deletion when tracked; --ignore-unmatch exits 0 for
      # untracked files WITHOUT touching the working tree, so rm -f afterwards
      # unconditionally to guarantee the file is gone either way.
      git rm -q --ignore-unmatch "$f" >/dev/null 2>&1 || true
      rm -f "$f"
      echo "reaped orphan: $f"
    fi
    reaped=$((reaped + 1))
  done
done <<< "$done_ids"

echo "── heal_orphans: ${reaped} orphan(s) $([[ $DRY_RUN -eq 1 ]] && echo 'would be reaped' || echo 'reaped'), ${kept} kept for review."
exit 0
