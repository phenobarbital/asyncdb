#!/usr/bin/env bash
# PreToolUse hook — block destructive actions and name the safe alternative.
#
# Registered in .claude/settings.json for the Bash|Edit|Write matcher.
# Exit 0 = allow, exit 2 = block (stderr is fed back to Claude verbatim, which
# is why every block below spells out what to run instead — a bare refusal
# just makes the model retry a variant).
#
# Escape hatch: PARROT_ALLOW_DANGEROUS=1 in the environment disables every
# block. Intended for a deliberate recovery session, not for daily use.
#
# Deliberately NOT `set -e`: a non-zero from any probe would exit non-2, and
# only exit 2 blocks — a hook that dies mid-check fails open. Every check must
# be reachable.
set -uo pipefail

INPUT=$(cat)

if [[ "${PARROT_ALLOW_DANGEROUS:-}" == "1" ]]; then
    exit 0
fi

if ! command -v jq >/dev/null 2>&1; then
    # No jq: cannot parse the payload. Say so rather than silently allowing.
    echo '{"systemMessage": "dangerous-actions-blocker: jq not found, checks skipped."}'
    exit 0
fi

TOOL_NAME=$(echo "$INPUT" | jq -r '.tool_name // empty')

block() {
    # $1 = what was blocked, $2..$n = the safe alternative, line by line
    local what="$1"; shift
    echo "BLOCKED: $what" >&2
    if (( $# )); then
        echo "" >&2
        echo "Safe alternative:" >&2
        printf '  %s\n' "$@" >&2
    fi
    echo "" >&2
    echo "(If this is a deliberate recovery, ask the user to re-run with" >&2
    echo " PARROT_ALLOW_DANGEROUS=1 — do not work around this hook.)" >&2
    exit 2
}


# Blank out quoted spans so that text *about* a command is not mistaken for
# the command itself. `echo "<destructive command>"`, a commit message, or a
# note written to the wiki must not trip a command-position rule.
#
# Exception: when the command line hands a quoted string to a shell that will
# execute it (bash -c, sh -c, eval, ssh), the quoted content IS the command,
# so the raw string is scanned instead.
strip_quoted() {
    local raw="$1"
    if echo "$raw" | grep -qE '(^|[;&|]|\s)(bash|sh|zsh|dash|eval|ssh)\s+([^;&|]*\s)?-c\b' \
       || echo "$raw" | grep -qE '(^|[;&|]|\s)eval\s'; then
        # Keep the payload but neutralize the quote characters, so the rules
        # below see `bash -c  git reset --h4rd ...` with the inner command at
        # a word boundary they recognize -- returning the raw string leaves
        # the command glued to a quote and every rule misses it.
        printf '%s' "${raw//[\"\']/ }"
        return
    fi
    python3 - "$raw" <<'PYEOF' 2>/dev/null || printf '%s' "$raw"
import re
import sys

text = sys.argv[1]
text = re.sub(r"'[^']*'", " ", text)
text = re.sub(r'"[^"]*"', " ", text)
sys.stdout.write(text)
PYEOF
}

# ---------------------------------------------------------------------------
# Bash
# ---------------------------------------------------------------------------
if [[ "$TOOL_NAME" == "Bash" ]]; then
    CMD=$(echo "$INPUT" | jq -r '.tool_input.command // empty')
    # SCAN drives the command-position rules; CMD drives the value rules.
    SCAN=$(strip_quoted "$CMD")

    # --- git: history- and work-destroying operations ----------------------

    # `git reset --hard` on a shared branch is THE observed incident in this
    # repo: a concurrent SDD id-reservation run did `git reset --hard
    # origin/dev` and discarded three committed local commits.
    if echo "$SCAN" | grep -qE '(^|[;&|]|\s)git\s+([^;&|]*\s)?reset\s+([^;&|]*\s)?--hard'; then
        block "\`git reset --hard\` discards committed local work that is not on origin." \
            "git fetch origin" \
            "git log --oneline origin/<branch>..HEAD   # commits only you have" \
            "# If that list is non-empty, PUSH them before resetting." \
            "# To just catch up with origin without destroying anything:" \
            "git merge --ff-only origin/<branch>" \
            "# Already lost commits? They are unreferenced, not gone:" \
            "git reflog  &&  git tag -f recover/<name> <sha>  &&  git cherry-pick <sha>"
    fi

    # `git clean -fdx` wipes untracked files: .env, local configs, scratch work.
    if echo "$SCAN" | grep -qE '(^|[;&|]|\s)git\s+([^;&|]*\s)?clean\s+[^;&|]*-[a-zA-Z]*[dx]'; then
        block "\`git clean -fd/-fdx\` deletes untracked files (.env, local configs, scratch work)." \
            "git clean -nd          # dry run — list what WOULD be deleted, first" \
            "git stash -u           # keep untracked work recoverable instead"
    fi

    # Force-push to a long-lived branch. Feature branches are fine.
    # ERE has no negative lookahead, so --force-with-lease is excluded by a
    # separate test rather than inline in the pattern.
    if echo "$SCAN" | grep -qE 'git\s+push\b' \
       && echo "$SCAN" | grep -qE '(\s|^)(-f|--force)(\s|$|=)' \
       && ! echo "$SCAN" | grep -q -- '--force-with-lease'; then
        if echo "$SCAN" | grep -qE '\b(main|dev|staging|master)\b'; then
            block "Force-push to a protected branch (main/dev/staging) rewrites history other worktrees and CI depend on." \
                "git push origin <feature-branch>        # push your own branch" \
                "gh pr create --base dev                 # land it through a PR" \
                "# Genuinely need to overwrite your OWN feature branch?" \
                "git push --force-with-lease origin <feature-branch>"
        fi
    fi

    # Discarding the whole working tree.
    if echo "$SCAN" | grep -qE '(^|[;&|]|\s)git\s+(checkout|restore)\s+(--\s+)?\.\s*($|[;&|])' \
       || echo "$SCAN" | grep -qE '(^|[;&|]|\s)git\s+checkout\s+--force\b'; then
        block "\`git checkout .\` / \`git restore .\` discards every uncommitted change in the tree — including another agent's in-flight edits." \
            "git status                     # see what would be lost" \
            "git stash -u                   # reversible" \
            "git restore -- <specific-file> # scope it to the file you mean"
    fi

    # Deleting a protected branch.
    if echo "$SCAN" | grep -qE 'git\s+branch\s+[^;&|]*-[dD]\b[^;&|]*\b(main|dev|staging|master)\b'; then
        block "Deleting a protected branch (main/dev/staging)." \
            "# There is no safe variant. Delete a feature branch instead:" \
            "git branch -d <feature-branch>"
    fi

    # Force-removing a worktree bypasses the live-worker check.
    if echo "$SCAN" | grep -qE 'git\s+worktree\s+remove\s+[^;&|]*--force'; then
        block "\`git worktree remove --force\` skips the dirty/unpushed checks and may be killing a live sdd-worker's checkout." \
            "python scripts/remove_worktree.py list                    # see LIVE / dirty / unpushed" \
            "python scripts/remove_worktree.py remove <target> --dry-run" \
            "# or use the /remove-worktree command, which gates on all three."
    fi

    # --- filesystem --------------------------------------------------------

    # Recursive delete aimed at a root target: /, /*, ~, $HOME.
    # Anchored so that legitimate deletes under /tmp or the project are allowed.
    RM_ROOT='(^|[;&|(]|[[:space:]])[[:space:]]*(sudo[[:space:]]+)?rm([[:space:]]+(-[a-zA-Z]+|--[a-zA-Z-]+))*[[:space:]]+(-[a-zA-Z]*[rR][a-zA-Z]*|--recursive)([[:space:]]+(-[a-zA-Z]+|--[a-zA-Z-]+))*[[:space:]]+['"'"'"]?(/|~|\$\{?HOME\}?)[*/]*['"'"'"]?[[:space:]]*($|[;&|])'
    if echo "$SCAN" | grep -qE "$RM_ROOT"; then
        block "Recursive delete of a root target (/, /*, ~, \$HOME)." \
            "# Scope the delete to an explicit subdirectory and list it first:" \
            "ls <dir> && rm -rf <dir>"
    fi

    # Repo-critical paths that must never be blown away wholesale.
    if echo "$SCAN" | grep -qE 'rm\s+(-[a-zA-Z]+\s+)*-?[a-zA-Z]*[rR][a-zA-Z]*\s+[^;&|]*(\.venv|packages/?\s*$|\.git(/|\s|$)|\.claude/worktrees)'; then
        block "Recursive delete of a repo-critical path (.venv, packages/, .git, .claude/worktrees)." \
            "# .venv:              make distclean   (then: uv sync)" \
            "# a worktree:         python scripts/remove_worktree.py remove <target>" \
            "# build artefacts:    make clean"
    fi

    # Classic destroyers.
    for pattern in "mkfs" "dd if=" ":(){:|:&};:" "> /dev/sd" "--no-preserve-root" "chmod -R 777 /"; do
        if [[ "$SCAN" == *"$pattern"* ]]; then
            block "Destructive system command: '$pattern'"
        fi
    done

    # --- data --------------------------------------------------------------
    # Requires BOTH the destructive statement and a client that would execute
    # it — otherwise `echo "DROP TABLE ..."` and grepping a docstring block.
    if echo "$CMD" | grep -qiE '\b(DROP\s+(DATABASE|TABLE|SCHEMA)|TRUNCATE\s+TABLE)\b' \
       && echo "$CMD" | grep -qiE '\b(psql|mysql|mariadb|sqlite3|clickhouse-client|mongosh|influx|bq\s+query|duckdb)\b'; then
        block "Destructive SQL (DROP/TRUNCATE) against a live database." \
            "# Run it against a disposable test database, or write a migration" \
            "# the team can review and roll back."
    fi

    # --- publishing (irreversible, outward-facing) -------------------------
    if echo "$SCAN" | grep -qE '(twine\s+upload|uv\s+publish|python\s+-m\s+twine|poetry\s+publish|npm\s+publish)'; then
        block "Direct package publication. PyPI never allows re-uploading a version, so a mistake here is permanent." \
            "# This repo publishes via GitHub Releases -> .github/workflows/release.yml:" \
            "/release <patch|minor|major>" \
            "python scripts/release.py status     # see current versions first"
    fi

    # --- environment discipline (CLAUDE.md: uv only, venv always) ----------
    if echo "$SCAN" | grep -qE '(^|[;&|]\s*)(sudo\s+)?pip\s+install' \
       && ! echo "$SCAN" | grep -qE '(uv\s+pip|cibuildwheel|python\s+-m\s+pip\s+install\s+--upgrade\s+pip)'; then
        block "Bare \`pip install\` — this workspace is uv-managed; pip writes state uv will not track." \
            "source .venv/bin/activate && uv pip install <package>" \
            "# For a permanent dependency, add it to the package's pyproject.toml:" \
            "uv add <package>"
    fi

    # --- credential leakage ------------------------------------------------
    # Only a literal-looking secret VALUE blocks; `--token=$GH_TOKEN` and
    # `grep -r api_key` are legitimate and must stay allowed.
    #
    # Documentation placeholders match the same shapes and are NOT secrets --
    # AWS's own example key appears in this repo's test fixtures, and blocking
    # it stops legitimate work on those tests. A real leaked credential does
    # not carry the word EXAMPLE.
    PLACEHOLDER='EXAMPLE|PLACEHOLDER|REDACTED|YOUR[_-]|<your|xxxxxxxx|0{8,}|DUMMY|FAKE_'
    if echo "$CMD" | grep -qE '(sk-ant-[A-Za-z0-9_-]{20,}|sk-[A-Za-z0-9]{32,}|ghp_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{50,}|AKIA[A-Z0-9]{16}|xox[bpsa]-[A-Za-z0-9-]{20,})' \
       && ! echo "$CMD" | grep -qiE "$PLACEHOLDER"; then
        block "A literal API key or token appears in this command — it would land in shell history and the transcript." \
            "# Read it from the environment instead:" \
            "export MY_TOKEN=...     # in your own shell, not through the agent" \
            "<command> --token \"\$MY_TOKEN\""
    fi

    # --- warn (non-blocking) ----------------------------------------------
    if echo "$SCAN" | grep -qE 'git\s+(merge|pull)\b' && [[ -d .claude/worktrees ]]; then
        if pgrep -f 'sdd-worker' >/dev/null 2>&1; then
            echo '{"systemMessage": "A sdd-worker process is live. A merge/pull on a shared branch can check out files over its uncommitted edits — verify with `git status` and `ps -eo pid,args | grep sdd-worker` before proceeding."}'
        fi
    fi
fi

# ---------------------------------------------------------------------------
# Edit / Write
# ---------------------------------------------------------------------------
if [[ "$TOOL_NAME" == "Edit" || "$TOOL_NAME" == "Write" || "$TOOL_NAME" == "MultiEdit" ]]; then
    FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
    FILENAME=$(basename "$FILE_PATH")

    case "$FILENAME" in
        # Templates carry variable NAMES only — they are meant to be edited.
        .env.example|.env.sample|.env.template|.env.dist|.env.test)
            ;;
        .env|.env.*|credentials.json|serviceAccountKey.json|id_rsa|id_ed25519|id_ecdsa|.pypirc|.npmrc|secrets.yml|secrets.yaml)
            block "Writing to the secrets file '$FILENAME'." \
                "# Secrets are set outside the repo. Document the variable instead:" \
                "# add its NAME (never its value) to .env.example or docs/"
            ;;
    esac

    case "$FILE_PATH" in
        *.pem|*.key|*/.ssh/*)
            block "Writing to a credential file: $FILE_PATH"
            ;;
        */.venv/*)
            block "Writing inside .venv/ — it is generated and will be overwritten." \
                "# Change the dependency instead:" \
                "uv add <package>       # then: uv sync"
            ;;
        */node_modules/*|*/.git/objects/*)
            block "Writing inside a generated/internal directory: $FILE_PATH"
            ;;
    esac
fi

exit 0
