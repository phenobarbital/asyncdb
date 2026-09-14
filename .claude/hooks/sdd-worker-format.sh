#!/usr/bin/env bash
# SubagentStop / Stop hook — run black + pylint after an sdd-worker finishes.
#
# Registered in .claude/settings.json for both events:
#   * SubagentStop  fires when sdd-worker runs as a delegated subagent;
#   * Stop          fires when it was launched standalone
#                   (`claude --agent sdd-worker`), where it IS the main agent.
# Both payloads carry `agent_type`, so one script covers both and no-ops for
# every other agent.
#
# Per-edit formatting is deliberately NOT used: sdd-worker commits after each
# task, and reformatting mid-task would interleave style churn with the task's
# own diff. Running once at the end produces a single, reviewable style commit.
#
# Never blocks: always exits 0, so a formatting problem cannot trap the worker.
# Skip with PARROT_SKIP_SDD_FORMAT=1.
set -uo pipefail

INPUT=$(cat)
[[ "${PARROT_SKIP_SDD_FORMAT:-}" == "1" ]] && exit 0
command -v jq >/dev/null 2>&1 || exit 0

AGENT_TYPE=$(echo "$INPUT" | jq -r '.agent_type // empty')
CWD=$(echo "$INPUT" | jq -r '.cwd // empty')

# Only sdd-worker. Any other agent (or a plain session) is none of our business.
[[ "$AGENT_TYPE" == "sdd-worker" ]] || exit 0
[[ -n "$CWD" && -d "$CWD" ]] || exit 0
cd "$CWD" || exit 0
git rev-parse --git-dir >/dev/null 2>&1 || exit 0

note() { echo "[sdd-worker-format] $*" >&2; }

# --- Never format on top of an unfinished git operation --------------------
GIT_DIR=$(git rev-parse --git-dir)
if [[ -d "$GIT_DIR/rebase-merge" || -d "$GIT_DIR/rebase-apply" \
      || -f "$GIT_DIR/MERGE_HEAD" || -f "$GIT_DIR/CHERRY_PICK_HEAD" ]]; then
    note "merge/rebase in progress — skipping."
    exit 0
fi

BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
case "$BRANCH" in
    main|master|dev|staging)
        note "on protected branch '$BRANCH' — skipping (workers run on feature branches)."
        exit 0
        ;;
esac

# --- Collect the Python files this worker actually touched -----------------
# Branch diff against the base plus anything still uncommitted. `dev` is the
# base for feature flows; fall back to the merge-base with origin/dev, and to
# HEAD~1 for a branch with no shared ancestor recorded.
BASE=$(git merge-base HEAD origin/dev 2>/dev/null \
       || git merge-base HEAD dev 2>/dev/null || echo "")
if [[ -n "$BASE" ]]; then
    CHANGED=$(git diff --name-only --diff-filter=ACMR "$BASE"...HEAD 2>/dev/null)
else
    CHANGED=$(git diff --name-only --diff-filter=ACMR HEAD~1 2>/dev/null)
fi
UNCOMMITTED=$(git status --porcelain | awk '{print $NF}')

mapfile -t FILES < <(printf '%s\n%s\n' "$CHANGED" "$UNCOMMITTED" \
    | grep -E '\.py$' \
    | grep -vE '(^|/)(\.venv|build|dist|node_modules|__pycache__)/' \
    | sort -u \
    | while read -r f; do [[ -n "$f" && -f "$f" ]] && echo "$f"; done)

if (( ${#FILES[@]} == 0 )); then
    note "no changed Python files — nothing to format."
    exit 0
fi

# --- Resolve the venv tools (the worktree has no .venv of its own) ---------
VENV_BIN=""
for candidate in "$CWD/.venv/bin" "${CLAUDE_PROJECT_DIR:-}/.venv/bin" \
                 "$(git rev-parse --git-common-dir 2>/dev/null)/../.venv/bin"; do
    if [[ -n "$candidate" && -x "$candidate/black" ]]; then
        VENV_BIN=$(cd "$candidate" && pwd)
        break
    fi
done
BLACK="${VENV_BIN:+$VENV_BIN/}black"
PYLINT="${VENV_BIN:+$VENV_BIN/}pylint"
command -v "$BLACK" >/dev/null 2>&1 || { note "black not found — skipping."; exit 0; }

# --- black (line-length comes from the root pyproject: 120) ----------------
BLACK_OUT=$("$BLACK" --quiet "${FILES[@]}" 2>&1)
mapfile -t REFORMATTED < <(git diff --name-only -- "${FILES[@]}" 2>/dev/null)

SUMMARY=""
if (( ${#REFORMATTED[@]} > 0 )); then
    note "black reformatted ${#REFORMATTED[@]} file(s):"
    printf '  %s\n' "${REFORMATTED[@]}" >&2

    # Stage ONLY the files black rewrote. Anything else in the tree belongs to
    # the worker or to a concurrent process and must not be swept into this
    # commit.
    git add -- "${REFORMATTED[@]}"
    if git diff --cached --quiet; then
        note "nothing staged after all — no commit."
    else
        MSG="style: apply black formatting (post sdd-worker)

Reformatted ${#REFORMATTED[@]} file(s) touched by the sdd-worker run on
${BRANCH}. Style only — no behavioral change.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
        if git commit -q -m "$MSG"; then
            SHA=$(git rev-parse --short HEAD)
            note "committed $SHA"
            SUMMARY="black reformatted ${#REFORMATTED[@]} file(s), committed as ${SHA} on ${BRANCH} (NOT pushed)."
        else
            note "commit failed — files left staged."
            SUMMARY="black reformatted ${#REFORMATTED[@]} file(s); the commit failed, they are left staged."
        fi
    fi
else
    note "black: ${#FILES[@]} file(s) already formatted."
    SUMMARY="black: ${#FILES[@]} file(s) already clean."
fi
[[ -n "$BLACK_OUT" ]] && note "black: $BLACK_OUT"

# --- pylint (informational — never blocks, never edits) --------------------
PYLINT_SUMMARY=""
if command -v "$PYLINT" >/dev/null 2>&1; then
    # Cap the file count: pylint imports what it lints, and a very wide feature
    # branch would otherwise stall the Stop event for minutes.
    LINT_FILES=("${FILES[@]:0:40}")
    (( ${#FILES[@]} > 40 )) && note "pylint: limiting to the first 40 of ${#FILES[@]} files."

    RC_ARG=()
    [[ -f "$CWD/.pylintrc" ]] && RC_ARG=(--rcfile "$CWD/.pylintrc")
    PYLINT_OUT=$(timeout 150 "$PYLINT" "${RC_ARG[@]}" --jobs=4 --score=y \
        --output-format=text "${LINT_FILES[@]}" 2>&1)
    PYLINT_RC=$?

    if (( PYLINT_RC == 124 )); then
        PYLINT_SUMMARY="pylint timed out after 150s — run it manually."
        note "$PYLINT_SUMMARY"
    else
        SCORE=$(echo "$PYLINT_OUT" | grep -oE 'rated at [-0-9.]+' | tail -1 | awk '{print $3}')
        ERRORS=$(echo "$PYLINT_OUT" | grep -cE '^[^ ]+:[0-9]+:[0-9]+: E[0-9]{4}')
        WARNS=$(echo "$PYLINT_OUT" | grep -cE '^[^ ]+:[0-9]+:[0-9]+: [WC][0-9]{4}')
        PYLINT_SUMMARY="pylint ${SCORE:-n/a}/10 — ${ERRORS} error(s), ${WARNS} warning(s) across ${#LINT_FILES[@]} file(s)."
        note "$PYLINT_SUMMARY"
        if (( ERRORS > 0 )); then
            note "pylint errors (E-class) — these are real defects, not style:"
            echo "$PYLINT_OUT" | grep -E '^[^ ]+:[0-9]+:[0-9]+: E[0-9]{4}' | head -20 >&2
        fi
    fi
else
    note "pylint not found — skipped."
fi

# Surface the outcome to the agent/user rather than burying it in the log.
jq -cn --arg msg "sdd-worker post-run format: ${SUMMARY} ${PYLINT_SUMMARY}" \
    '{systemMessage: $msg}' 2>/dev/null || true

exit 0
