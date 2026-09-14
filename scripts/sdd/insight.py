#!/usr/bin/env python3
"""
SDD Insight — AI-Parrot internal AI-fluency + SDD-process analyzer.

    python3 scripts/sdd/insight.py

Adapted from the open-source "Claude Insight v2" engine
(https://github.com/Feloguarin/claude-insight, MIT — see
reference/sdd-insight/UPSTREAM-LICENSE). The upstream personal AI-fluency
analysis is preserved verbatim (Layer 1); AI-Parrot adds a second, SDD-aware
layer (Layer 2) that scores how well this repo follows its own Spec-Driven
Development workflow (brainstorm -> proposal -> spec -> task -> done -> review),
computed deterministically from the `sdd/` artifact tree and surfaced as a
sixth "Process Discipline" panel on top of the personal report.

Reads your local Claude Code transcripts (~/.claude/projects/**/*.jsonl),
estimates how skillfully you drive an AI coding agent, and writes a single
self-contained HTML report (./ai_fluency_report.html) that opens in your browser.

Design principles (see README "Methodology"):
  * It measures SKILL, not activity. Every score input is a per-prompt or
    per-opportunity RATE pushed through a saturating curve, so using the agent
    MORE can never raise your score — only using it BETTER can.
  * It only looks at YOUR real typed prompts and Claude's real tool actions.
    Tool-results, subagent turns, slash-command stubs, injected system text and
    pasted walls of text are filtered out before anything is scored.
  * Every number is auditable: baselines are recomputed from your corpus at
    runtime, formulas are documented, and thin signals are flagged "low data"
    and pulled toward a neutral 50 instead of faking confidence.

Pure Python standard library — no pip, no Ollama, no API key. One command runs the
whole pass: de-contaminate and scrub your transcripts, score them, and (as
`/ai-fluency` in Claude Code) write a Sonnet+Opus skill map grounded in the AI
Fluency framework on top. The only thing it writes is
the HTML report and a local copy of your transcripts in an archive
(~/.claude/insight-archive) so history survives Claude Code's 30-day cleanup —
pass --no-archive to skip that and read your transcripts without copying them.
"""

import argparse
import glob
import hashlib
import html
import json
import math
import os
import re
import shutil
import statistics
import sys
import webbrowser
from collections import Counter, defaultdict
from datetime import datetime

# --------------------------------------------------------------------------- #
# Constants & tunables (documented; shown in the report's methodology appendix)
# --------------------------------------------------------------------------- #

DEFAULT_DIRS = ["~/.claude/projects", "~/.claude/sessions"]

# Claude Code deletes transcripts older than its `cleanupPeriodDays` setting (default 30),
# so by default only ~30 days of history is ever on disk. We mirror each run's transcripts
# into this persistent archive so history accumulates indefinitely and survives the cleanup.
# Keep this on a PRIVATE, per-person path. A single archive folder shared between different
# people or computers (e.g. a synced team Dropbox) merges everyone's transcripts into one
# analysis — so each person must point --archive at their own location, not a shared one.
DEFAULT_ARCHIVE_DIR = "~/.claude/insight-archive"

GAP_CAP_SECONDS = 300          # idle gaps longer than this are NOT counted as active time
MAX_HUMAN_PROMPT_CHARS = 6000  # anything longer is treated as a paste/injection, not a typed prompt
PROVISIONAL_MIN_PROMPTS = 30   # below this the headline score is shown as a hedged range

EDIT_TOOLS = {"edit", "write", "multiedit", "notebookedit"}
READ_TOOLS = {"read", "grep", "glob"}

# Text that marks a "user"-role record as injected/system rather than typed by the human.
INJECTION_MARKERS = (
    "<task-notification>", "<command-name>", "<command-message>", "<command-args>",
    "<local-command-caveat>", "<local-command-stdout>", "<system-reminder>",
    "<bash-input>", "<bash-stdout>", "caveat: the messages below",
    "[request interrupted", "base directory for this skill", "<user-prompt-submit-hook>",
    "<user-memory-input>", "this session is being continued",
)

# Subagent system prompts get stored as plain user-role text with no other marker.
# They almost always open with "You are <role>…". This catches the back-door inflation.
_INJECTED_HEAD = re.compile(
    r"^\s*(you are\b|<[a-z][\w-]*>|base directory for this skill)", re.I
)

# A Claude Code slash command is recorded as a user message wrapping
# <command-name>/sdd-spec</command-name>. We extract the command name as workflow
# telemetry even though the stub itself is filtered out of the scored prompt corpus.
_CMD_NAME_RE = re.compile(r"<command-name>\s*(/?[\w-]+)\s*</command-name>", re.I)

# Broad, project-extensible verification matcher (matched against real Bash commands).
VERIFY_RE = re.compile(
    r"\b("
    r"pytest|unittest|jest|vitest|mocha|go test|cargo (test|build|check)|"
    r"npm (run )?(test|build|lint)|yarn (test|build|lint)|pnpm (test|build|lint)|"
    r"ruff|eslint|flake8|mypy|tsc\b|make (test|lint|build|check)|playwright|"
    r"python\d? -m \w|\.venv/bin/python|lsof -ti|curl .*(localhost|127\.0\.0\.1)|"
    r"docker compose|docker-compose|pre-commit"
    r")",
    re.I,
)
# Clean-teardown of a live system (small bonus, folded into Verification).
TEARDOWN_RE = re.compile(r"(lsof -ti.*kill|pkill|kill -9|docker compose down|docker-compose down)", re.I)

# Direction (prompt-quality) cues.
ARTIFACT_RE = re.compile(
    r"([\w./\-]+\.(py|js|ts|tsx|jsx|html|css|md|json|sh|ya?ml|toml|rs|go|java|cpp|c|rb|sql))"
    r"|((?:/[\w.\-]+){2,})"        # multi-segment paths (not bare /word or </tag>)
    r"|(`[^`]+`)"                  # inline code / quoted token
    r"|(\b\w+\(\))",               # function() reference
    re.I,
)
CONSTRAINT_CUE = re.compile(
    r"\b(only|must|should|shouldn't|don't|do not|never|always|keep|ensure|instead of|"
    r"at most|at least|exactly|without|except|make sure|no more than|leave .* as is)\b", re.I
)
INTENT_CUE = re.compile(
    r"\b(so that|because|the goal is|in order to|for the demo|for my|for the|so i can|so we can|"
    r"so it|i need|i want .* so)\b", re.I
)
ACTION_VERB = re.compile(
    r"\b(add|create|build|make|implement|write|fix|change|update|refactor|remove|delete|run|"
    r"generate|set up|setup|install|deploy|edit|rename|move|clean|stitch|speed up|merge|split)\b", re.I
)

# Iteration cues.
CORRECTION_CUE = re.compile(
    r"\b(no|nope|wrong|not quite|that's not|thats not|actually|instead|revert|undo|redo|try again|"
    r"too (aggressive|agressive|much|many|slow|fast|big|small)|still (broken|failing|wrong|not)|"
    r"doesn't work|does not work|not working|unteligible|unteliggeble)\b", re.I
)
PRAISE_CUE = re.compile(r"\b(great|perfect|love it|nice|awesome|excellent|beautiful|exactly)\b", re.I)
CORRECTION_RATE_CEILING = 0.35   # a "high" correction rate; lower is better

# Delegation / planning tool signals.
DELEGATION_TOOLS = {"agent", "task", "workflow", "exitplanmode", "enterplanmode"}

# Dimension weights (sum to 1.0).
WEIGHTS = {
    "Direction": 0.24,
    "Verification": 0.22,
    "Context": 0.22,
    "Iteration": 0.18,
    "Toolcraft": 0.14,
}
# Opportunity-count targets for per-dimension confidence shrinkage.
TARGET_N = {"Direction": 60, "Verification": 15, "Context": 25, "Iteration": 12, "Toolcraft": 40}

# User-facing labels. "Direction" is shown as "Briefing" so it never collides with the
# "Director" archetype (the dimension measures how well you brief; the archetype, that
# you delegate — different things).
DISPLAY_NAMES = {"Direction": "Briefing", "Verification": "Verification",
                 "Context": "Context-setting", "Iteration": "Iteration", "Toolcraft": "Toolcraft"}

def disp(name):
    return DISPLAY_NAMES.get(name, name)

# Teacher content for each skill (kind, plain-English, with before/after examples and a
# weekly practice). Used to make the report explain what to improve and exactly how.
SKILL_TEACH = {
    "Direction": {
        "what_it_is": "Telling the agent what you want and giving it something to aim at: a goal plus a file, a constraint, or a way to know it worked.",
        "why_it_matters": "When your goal and your limits are clear up front, the agent gets it right the first time instead of guessing and pulling you into rounds of fixes.",
        "how_to_improve": "Before you hit enter, add one anchor to your goal: the file to touch, a rule it must not break, or a 'done when…' line. One line is plenty.",
        "examples": [
            {"before": "fix the login bug", "after": "Users stay logged out after a correct password on Safari. The check lives in src/auth/session.ts. Fix it so a valid login sets the session cookie, and keep the current tests green."},
            {"before": "add caching to the API", "after": "Cache GET /products responses in api/products.py for 60s to ease DB load on repeat reads. Don't cache authed requests, and add a test that a second call within 60s skips the DB."},
        ],
        "practice": "Before sending a prompt, add one anchor to your goal: a file path, a constraint, or a 'done when…' line.",
        "good_looks_like": "Every request says what you want plus where to work or how success is judged, so the agent acts instead of guessing.",
    },
    "Verification": {
        "what_it_is": "Having the agent prove its own work — run the tests, build, lint, or launch the app — before it tells you it's done.",
        "why_it_matters": "Code that looks right but was never run is where most AI bugs hide; checking it turns “probably works” into “I watched it work.”",
        "how_to_improve": "In the same prompt that asks for the change, name the exact command that proves it (a test, build, lint, or curl) and tell the agent to run it and show you the output before stopping.",
        "examples": [
            {"before": "Fix the off-by-one in the pagination helper.", "after": "Fix the off-by-one in the pagination helper, then run `pytest tests/test_pagination.py -x` and paste the output. Don't call it fixed until that test passes."},
            {"before": "Add a /health endpoint to the FastAPI server.", "after": "Add a /health endpoint to the FastAPI server. Start it on port 8000, curl `localhost:8000/health`, and show me the response. Run `ruff check` too and confirm it's clean before you finish."},
        ],
        "practice": "Before you accept any change, ask: “How did you verify this? Run it and show me the output.”",
        "good_looks_like": "Every change ends with proof — a passing test, a green build, a real response — pasted back to you, not just a claim.",
    },
    "Context": {
        "what_it_is": "Pointing the agent at the real code — a file, a function, a line area — and having it read that before it changes anything.",
        "why_it_matters": "When the agent sees the actual current code first, its edits fit what's really there instead of a guess, so they apply cleanly the first time.",
        "how_to_improve": "Before any edit, name the exact file (and the function or area if you can) and tell the agent to read it first. Let it look before it leaps.",
        "examples": [
            {"before": "Add retry logic to the API client.", "after": "Read src/api/client.ts first, then add retry-with-backoff to the request() method. Show me the change before you apply it."},
            {"before": "Fix the timezone bug in the date formatter.", "after": "Open src/utils/date.ts and find formatDate(). Read how it handles timezones now, then fix the off-by-one so UTC inputs render in the user's local zone."},
        ],
        "practice": "Start your next edit request with “Read <file> first, then…” so the agent grounds itself before touching anything.",
        "good_looks_like": "Every edit lands on code the agent just read, so diffs apply cleanly with nothing broken around them.",
    },
    "Iteration": {
        "what_it_is": "When the agent goes the wrong way, steering it back with a precise correction — naming what broke and the rule to follow — instead of just “no” or “try again.”",
        "why_it_matters": "A precise correction lands the fix in one round; a vague “no” makes the agent guess again, and you burn turns while the code drifts further off.",
        "how_to_improve": "When a result is wrong, say three things in one message: the symptom you saw, the rule it broke, and what to do instead. Then let it run.",
        "examples": [
            {"before": "no that's not right, try again", "after": "The retry loop catches the exception but never re-raises after the last attempt, so failures look like successes. Re-raise the original error once retries run out, and keep the existing backoff."},
            {"before": "this is wrong, fix the test", "after": "The test passes because you mocked the function under test instead of the network call. Don't mock get_user — mock requests.get inside it, and assert it was called with the real URL."},
        ],
        "practice": "Before sending a correction, check it names both the symptom and the rule. If it only says “no,” add the missing half.",
        "good_looks_like": "One sharp correction — symptom, rule, and the fix — and the agent lands it on the next try.",
    },
    "Toolcraft": {
        "what_it_is": "Letting the agent use the right tool for each step — searching the code, running commands, starting the app, working in the background — instead of forcing everything through chat.",
        "why_it_matters": "The agent works faster and more reliably when it searches and runs things for real, rather than reasoning about the code from memory.",
        "how_to_improve": "Tell the agent which action to take first — search the codebase, run the suite, start the server — so it gathers facts and checks its work with the tool built for each step.",
        "examples": [
            {"before": "How does login work in this app?", "after": "Search the codebase for the login flow (grep for auth, session, login), read the files you find, then explain how a request goes from form submit to a logged-in session."},
            {"before": "Add a retry to the API client, and make sure the tests still pass.", "after": "Add retry-with-backoff to the API client. Then run the suite in the background; if anything fails, read the failure, fix it, and report back when it's green."},
        ],
        "practice": "Add one line to your next task telling the agent which action to take first: “search for…”, “run the tests”, or “start the server and check.”",
        "good_looks_like": "You hand off a whole job and the agent searches, edits, runs, and verifies on its own — each step using the tool made for it.",
    },
}

BANDS = [
    ("Operator", 0, 39, "You use the agent as fast hands. Prompts are short and underspecified, "
     "edits often happen without reading the file first, and changes are rarely verified. The "
     "fastest gains live right here: state a goal plus one constraint, and let the agent read "
     "before it edits."),
    ("Developing", 40, 54, "Real back-and-forth is emerging and one or two habits are solid. Some "
     "prompts carry a file path or a constraint; verification happens occasionally. The gap to the "
     "next level is consistency — doing the right thing by default, not just sometimes."),
    ("Proficient", 55, 69, "You drive the agent deliberately. Most prompts are specific, edits "
     "usually follow a read of the same file, and you verify more often than not. Solid, reliable "
     "AI-assisted engineering. Remaining gains are about altitude (saying why) and orchestration."),
    ("Advanced", 70, 84, "You orchestrate rather than operate. Prompts encode goals, constraints "
     "and acceptance criteria; reading precedes editing as a habit; verification is near-automatic; "
     "you use planning and delegation fluently. You brief the agent like a senior teammate."),
    ("Expert", 85, 100, "You treat the agent as a managed engineering system: consistently "
     "high-context prompts with explicit success criteria, disciplined read→edit→verify loops, "
     "deliberate delegation, and almost no wasted correction cycles."),
]

# Archetype axes and prototypes.
# The archetype describes YOUR DRIVING STYLE, so it is built only from signals you
# control and DISCOUNTS the habits Claude does on its own. Verification and Context
# (read-before-edit, running tests) are largely the agent's defaults, so they carry
# low "agency" weight; how you brief (Direction), correct (Iteration), reach for tools
# (Toolcraft) and hand off work (Delegation) carry full weight.
ARCHETYPE_AXES = ["Direction", "Verification", "Context", "Iteration", "Toolcraft", "Delegation"]
AGENCY = {"Direction": 1.0, "Verification": 0.35, "Context": 0.15,
          "Iteration": 1.0, "Toolcraft": 0.8, "Delegation": 1.0}

# Prototype vectors over ARCHETYPE_AXES (0-100). Delegation is the axis that separates
# a hands-off delegator from a hands-on builder. These are the five explicit, recognizable
# builder archetypes; the classifier picks the nearest one from your AGENCY-WEIGHTED vector.
PROTOTYPES = {
    "Autonomous Agent": {"emoji": "🤖", "vec": [58, 65, 62, 62, 85, 96],
        "blurb": "You delegate whole, end-to-end jobs and trust the agent to run them — you set the outcome and let Claude pick the steps."},
    "Architect":        {"emoji": "🏗️", "vec": [80, 66, 88, 65, 60, 48],
        "blurb": "You plan and explore before you build — you read and design first, so changes land on a clear structure."},
    "Debugger":         {"emoji": "🐛", "vec": [62, 88, 82, 85, 60, 28],
        "blurb": "You hunt problems methodically — read to diagnose, change, verify, and repeat until it's truly fixed."},
    "Collaborator":     {"emoji": "🤝", "vec": [66, 62, 66, 80, 55, 38],
        "blurb": "You work with the agent like a teammate — ask for options, give feedback, and steer toward alignment."},
    "Sprinter":         {"emoji": "⚡", "vec": [45, 38, 52, 46, 62, 30],
        "blurb": "You move fast and direct — terse prompts, quick turns, low ceremony. Great velocity; briefing and verification are the growth edges."},
}
ARCHETYPE_MARGIN = 0.06   # cosine-similarity margin below which we emit a blended label


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #

def _parse_ts(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _text_of(content):
    """Concatenate the text blocks of a message content (str or list)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(
            b.get("text", "") for b in content
            if isinstance(b, dict) and b.get("type") == "text"
        )
    return ""


def _is_tool_result(content):
    return isinstance(content, list) and any(
        isinstance(b, dict) and b.get("type") == "tool_result" for b in content
    )


def _looks_injected(text):
    head = text[:200].lstrip()
    if len(text) > MAX_HUMAN_PROMPT_CHARS:
        return True
    if _INJECTED_HEAD.match(head):
        return True
    low = text.lower()
    return any(m in low for m in INJECTION_MARKERS)


def _denamespace_tool(name):
    """mcp__<hash>__slack_read_thread -> slack_read_thread; keep core names as-is."""
    if name.startswith("mcp__"):
        parts = name.split("__")
        return parts[-1] if parts else name
    return name


# Redact machine-identifying home paths from free text before it is shown in the report or
# written to the evidence bundle. Applied only at PRESENTATION, never to the scored corpus,
# so scores stay byte-identical.
_HOME_PATH_RE = re.compile(r"(?:/Users/|/home/)[^/\s]+")
_WIN_HOME_RE = re.compile(r"[A-Za-z]:\\Users\\[^\\\s]+")


def _scrub_paths(text):
    """/Users/<name>/x -> ~/x ; bare /Users/<name> -> ~ ; same for /home/<name> and Windows."""
    if not isinstance(text, str):
        return text
    text = _HOME_PATH_RE.sub("~", text)
    text = _WIN_HOME_RE.sub("~", text)
    return text


class Corpus:
    """Everything we measured from the transcripts, cleanly separated from scoring."""

    def __init__(self):
        self.files = 0
        self.projects = set()
        self.total_bytes = 0
        self.user_records = 0
        self.filtered = Counter()       # why user records were not counted as prompts
        self.real_prompts = []          # list of dicts: text, project, session, idx
        self.tool_usage = Counter()     # de-namespaced tool name -> count
        self.total_tool_calls = 0
        self.delegation_events = 0
        self.all_commands = Counter()   # slash-command invocations seen (any /command)
        self.sdd_commands = Counter()   # subset: /sdd-* invocations (process telemetry)
        self.first_ts = None
        self.last_ts = None
        self.active_seconds = 0.0
        # Per-session ordered timelines of {"kind": "prompt"|"tool", ...}
        self.sessions = {}              # session_id -> {"project","timeline":[...]}


# Agent-to-agent transcripts (Claude Code subagents, Workflow runs) live under a
# ".../subagents/..." path. They are NOT the user's own prompts — counting them would
# contaminate the assessment and inflate counts every time a workflow is run — so they
# are excluded from discovery (an explicitly named single file is still honored).
_SUBAGENT_RE = re.compile(r"[/\\]subagents[/\\]")


def _filter_transcripts(paths):
    return [p for p in paths if not _SUBAGENT_RE.search(p)]


def discover_files(explicit):
    if explicit:
        p = os.path.expanduser(explicit)
        if os.path.isfile(p) and p.endswith(".jsonl"):
            return [p]
        if os.path.isdir(p):
            return _filter_transcripts(sorted(glob.glob(os.path.join(p, "**", "*.jsonl"), recursive=True)))
        return []
    env = os.environ.get("CLAUDE_PROJECTS_DIR")
    roots = [env] if env else DEFAULT_DIRS
    files = []
    for r in roots:
        rp = os.path.expanduser(r)
        if os.path.isdir(rp):
            files.extend(glob.glob(os.path.join(rp, "**", "*.jsonl"), recursive=True))
    return _filter_transcripts(sorted(set(files)))


def _dedupe_sessions(files):
    """When the same session shows up in more than one root (the live ~/.claude/projects dir
    AND the persistent archive — possibly under a since-renamed project folder, a different-case
    path, or a synced copy from another machine), keep a single copy of it: the largest one,
    since transcripts only ever grow, so the biggest file is the most complete. Claude Code
    session filenames are globally-unique IDs, so the filename alone identifies the session —
    keying on it (not the parent folder) is what makes the dedupe robust to all of the above."""
    best = {}
    for path in files:
        key = os.path.basename(path)
        try:
            size = os.path.getsize(path)
        except OSError:
            size = -1
        cur = best.get(key)
        if cur is None or size > cur[0]:
            best[key] = (size, path)
    return sorted(p for _, p in best.values())


def archive_transcripts(live_files, archive_dir):
    """Copy live transcripts into a persistent archive so they survive Claude Code's
    `cleanupPeriodDays` deletion. Each file is mirrored to
    <archive>/<project folder>/<session>.jsonl. We copy only when the archived copy is
    missing or strictly smaller than the live one (transcripts only grow, so a >= archive copy
    is the more complete one and must never be overwritten with a smaller/equal one). We write
    via a temp file + atomic replace, re-checking the archive size just before the swap so a
    concurrent run can't clobber a larger copy, and always clean up the temp file.
    Returns (n_new, n_updated); a stderr note is printed if any file could not be archived."""
    arch_root = os.path.expanduser(archive_dir)
    new = updated = failed = 0
    for path in live_files:
        project = os.path.basename(os.path.dirname(path)) or "default"
        dest_dir = os.path.join(arch_root, project)
        dest = os.path.join(dest_dir, os.path.basename(path))
        try:
            live_size = os.path.getsize(path)
        except OSError:
            continue
        arch_size = os.path.getsize(dest) if os.path.exists(dest) else -1
        if arch_size >= live_size:
            continue  # already archived an equal-or-more-complete copy
        tmp = dest + ".tmp"
        try:
            os.makedirs(dest_dir, exist_ok=True)
            shutil.copyfile(path, tmp)
            # Another run may have grown the archive while we were copying — don't shrink it.
            current = os.path.getsize(dest) if os.path.exists(dest) else -1
            if current >= live_size:
                continue
            os.replace(tmp, dest)  # atomic; never leaves a half-written archive copy
        except OSError:
            failed += 1
            continue
        finally:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass
        if arch_size < 0:
            new += 1
        else:
            updated += 1
    if failed:
        print(f"  Note: {failed} transcript(s) could not be archived to {archive_dir} "
              f"(check permissions / disk space). They were still analyzed from disk.",
              file=sys.stderr)
    return new, updated


def parse(files):
    c = Corpus()
    c.files = len(files)
    for path in files:
        project = os.path.basename(os.path.dirname(path)) or "default"
        c.projects.add(project)
        try:
            c.total_bytes += os.path.getsize(path)
        except OSError:
            pass
        session_id = os.path.splitext(os.path.basename(path))[0]
        timeline = []
        ts_in_file = []
        prompt_idx = 0
        try:
            fh = open(path, encoding="utf-8")
        except OSError:
            continue
        with fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = _parse_ts(e.get("timestamp"))
                if ts:
                    ts_in_file.append(ts)
                    c.first_ts = ts if c.first_ts is None or ts < c.first_ts else c.first_ts
                    c.last_ts = ts if c.last_ts is None or ts > c.last_ts else c.last_ts
                msg = e.get("message") if isinstance(e.get("message"), dict) else {}
                role = e.get("role") or msg.get("role") or e.get("type")
                content = msg.get("content", e.get("content"))

                if role == "assistant":
                    if isinstance(content, list):
                        for b in content:
                            if isinstance(b, dict) and b.get("type") == "tool_use":
                                raw = b.get("name", "unknown")
                                name = _denamespace_tool(raw)
                                c.tool_usage[name] += 1
                                c.total_tool_calls += 1
                                inp = b.get("input", {}) if isinstance(b.get("input"), dict) else {}
                                if name.lower() in DELEGATION_TOOLS:
                                    c.delegation_events += 1
                                if name.lower() == "bash" and inp.get("run_in_background"):
                                    c.delegation_events += 1
                                fpath = inp.get("file_path") or inp.get("path") or inp.get("notebook_path")
                                cmd = inp.get("command") if name.lower() == "bash" else None
                                timeline.append({
                                    "kind": "tool", "name": name.lower(),
                                    "file": fpath, "cmd": cmd,
                                })
                    continue

                if role != "user":
                    continue
                c.user_records += 1
                # Slash-command telemetry — scan BEFORE the noise filters below, because command
                # stubs are recorded as isMeta/injected user records and would otherwise be dropped
                # before we ever see the command name. This counts invocations, not scored prompts.
                for _m in _CMD_NAME_RE.finditer(_text_of(content)):
                    _cmd = _m.group(1).strip().lstrip("/").lower()
                    c.all_commands[_cmd] += 1
                    if _cmd.startswith("sdd-") or _cmd.startswith("sdd_"):
                        c.sdd_commands[_cmd] += 1
                if _is_tool_result(content):
                    c.filtered["tool results"] += 1
                    continue
                if e.get("isSidechain") is True:
                    c.filtered["subagent turns"] += 1
                    continue
                if e.get("isMeta") is True:
                    c.filtered["meta-injected"] += 1
                    continue
                text = _text_of(content).strip()
                if not text:
                    c.filtered["empty"] += 1
                    continue
                if _looks_injected(text):
                    c.filtered["injected / pasted"] += 1
                    continue
                # A genuine, human-typed prompt.
                prompt_idx += 1
                rec = {"text": text, "project": project, "session": session_id, "idx": prompt_idx}
                c.real_prompts.append(rec)
                timeline.append({"kind": "prompt", "text": text, "rec": rec})

        if len(ts_in_file) >= 2:
            ts_in_file.sort()
            c.active_seconds += sum(
                min((ts_in_file[i + 1] - ts_in_file[i]).total_seconds(), GAP_CAP_SECONDS)
                for i in range(len(ts_in_file) - 1)
            )
        if timeline:
            c.sessions[session_id] = {"project": project, "timeline": timeline}
    return c


# --------------------------------------------------------------------------- #
# Scoring helpers
# --------------------------------------------------------------------------- #

def squash(x, target):
    """Saturating curve: hitting `target` maxes the signal; exceeding adds nothing."""
    if target <= 0:
        return 0.0
    return max(0.0, min(1.0, x / target))


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def _run_fingerprint(corpus):
    """A stable hash of THIS run's de-contaminated prompt set. It binds an AI analysis
    (the Opus-stage skill map) to the exact data it was written from, so a stale or
    foreign ``analysis.json`` — e.g. left over from a previous run or another person on a
    machine that reuses the fixed ``~/.claude/insight/`` paths — carries a different
    fingerprint and is refused at merge time. This is what stops one person's written
    verdict from ever rendering inside someone else's report."""
    h = hashlib.sha256()
    for p in sorted(corpus.real_prompts, key=lambda r: (r["session"], r["idx"])):
        h.update(f"{p['session']}\x1f{p['idx']}\x1f{p['text']}\x1e".encode("utf-8"))
    h.update(f"|n={len(corpus.real_prompts)}".encode("utf-8"))
    return h.hexdigest()[:16]


def _is_action_prompt(text):
    return bool(ACTION_VERB.search(text))


# --------------------------------------------------------------------------- #
# The five dimensions — each returns (score_0_100, detail_dict, evidence_list)
# --------------------------------------------------------------------------- #

def score_direction(corpus):
    prompts = corpus.real_prompts
    n = len(prompts)
    if n == 0:
        return 0.0, {"n": 0}, []
    constraint = artifact = intent = 0
    weak_examples = []
    for p in prompts:
        t = p["text"]
        has_artifact = bool(ARTIFACT_RE.search(t))
        has_constraint = bool(CONSTRAINT_CUE.search(t) and ACTION_VERB.search(t))
        has_intent = bool(INTENT_CUE.search(t))
        artifact += 1 if has_artifact else 0
        constraint += 1 if has_constraint else 0
        intent += 1 if has_intent else 0
        if _is_action_prompt(t) and not (has_artifact or has_constraint or has_intent) and len(t) < 120:
            weak_examples.append(p)
    constraint_rate = constraint / n
    artifact_rate = artifact / n
    intent_rate = intent / n
    # front-loading: penalize rules first revealed via a high-info correction
    corr = _find_corrections(corpus)
    new_rule_corrections = sum(1 for x in corr if x["high_info"])
    action_prompts = max(1, sum(1 for p in prompts if _is_action_prompt(p["text"])))
    front_loading = 1 - clamp(new_rule_corrections / action_prompts, 0, 1)
    score = 100 * (
        0.30 * squash(constraint_rate, 0.45)
        + 0.20 * squash(artifact_rate, 0.45)
        + 0.25 * squash(intent_rate, 0.30)
        + 0.25 * front_loading
    )
    detail = {
        "n": n, "constraint_rate": constraint_rate, "artifact_rate": artifact_rate,
        "intent_rate": intent_rate, "front_loading": front_loading,
    }
    return score, detail, weak_examples[:6]


def _iter_sessions(corpus):
    for sid, s in corpus.sessions.items():
        yield sid, s["project"], s["timeline"]


def _find_corrections(corpus):
    """Correction turns: short rejections that follow an assistant action, praise-guarded."""
    out = []
    for sid, project, timeline in _iter_sessions(corpus):
        saw_tool = False
        for ev in timeline:
            if ev["kind"] == "tool":
                saw_tool = True
                continue
            t = ev["text"]
            head = t[:160]
            if CORRECTION_CUE.search(head) and not PRAISE_CUE.search(head) and saw_tool:
                high_info = bool(
                    re.search(r"\d", t) or ARTIFACT_RE.search(t) or len(t.split()) >= 8
                    or INTENT_CUE.search(t)
                )
                out.append({"session": sid, "project": project, "text": t, "high_info": high_info})
            saw_tool = False  # reset: correction must directly follow an action turn
    return out


def score_iteration(corpus):
    prompts = corpus.real_prompts
    n = len(prompts)
    corr = _find_corrections(corpus)
    k = len(corr)
    if n == 0:
        return 50.0, {"n": 0, "corrections": 0}, []
    rate = k / n
    specificity = (sum(1 for x in corr if x["high_info"]) / k) if k else 1.0
    score = 100 * (0.6 * (1 - clamp(rate / CORRECTION_RATE_CEILING, 0, 1)) + 0.4 * specificity)
    low_info = [x for x in corr if not x["high_info"]]
    # Confidence is keyed on prompt count n (the opportunity count), NOT correction count k:
    # a user with many clean prompts and zero corrections has STRONG evidence of good iteration,
    # so it must not be shrunk toward 50 as if it were "no data".
    detail = {"n": n, "corrections": k, "correction_rate": rate, "specificity": specificity}
    return score, detail, low_info[:4]


def score_context(corpus):
    total_edits = 0
    grounded = 0
    blind_examples = []
    for sid, project, timeline in _iter_sessions(corpus):
        read_paths = set()
        edited_paths = set()
        written_paths = set()   # files the agent authored this session (grounded to edit)
        for ev in timeline:
            if ev["kind"] != "tool":
                continue
            name, fpath = ev["name"], ev.get("file")
            if name in READ_TOOLS and fpath:
                read_paths.add(fpath)
            elif name in EDIT_TOOLS:
                total_edits += 1
                if not fpath:
                    grounded += 1  # can't attribute; don't penalize
                    continue
                is_new_write = (name == "write" and fpath not in read_paths and fpath not in edited_paths)
                # grounded if it was read, OR authored earlier this session, OR is being created now
                if fpath in read_paths or fpath in written_paths or is_new_write:
                    grounded += 1
                else:
                    blind_examples.append({"session": sid, "project": project, "file": fpath})
                if name == "write":
                    written_paths.add(fpath)
                edited_paths.add(fpath)
    if total_edits == 0:
        return 50.0, {"n": 0, "grounded": 0, "total_edits": 0, "rate": None}, []
    rate = grounded / total_edits
    score = 100 * squash(rate, 0.85)
    return score, {"n": total_edits, "grounded": grounded, "total_edits": total_edits, "rate": rate}, blind_examples[:4]


def score_verification(corpus):
    episodes = 0
    verified = 0
    teardown_bonus = 0
    unverified_examples = []
    for sid, project, timeline in _iter_sessions(corpus):
        open_ep = False
        ep_files = []
        for ev in timeline:
            if ev["kind"] == "prompt":
                # a "run it / does it work / confirm" prompt verifies an open episode
                if open_ep and re.search(r"\b(run it|does it work|confirm|check (it|that)|verify|did it work)\b",
                                         ev["text"], re.I):
                    verified += 1
                    open_ep = False
                continue
            name = ev["name"]
            cmd = ev.get("cmd") or ""
            if name in EDIT_TOOLS:
                if not open_ep:
                    open_ep = True
                    episodes += 1
                    ep_files = []
                if ev.get("file"):
                    ep_files.append(os.path.basename(ev["file"]))
            elif name == "bash":
                if TEARDOWN_RE.search(cmd):
                    teardown_bonus = 5
                if open_ep and VERIFY_RE.search(cmd):
                    verified += 1
                    open_ep = False
            elif name in READ_TOOLS and open_ep and ev.get("file") and os.path.basename(ev["file"]) in ep_files:
                # re-reading the just-edited file is a (weak) check
                verified += 1
                open_ep = False
        if open_ep:
            unverified_examples.append({"session": sid, "project": project,
                                        "files": ", ".join(sorted(set(ep_files))[:3]) or "files"})
    if episodes == 0:
        return 50.0, {"n": 0, "episodes": 0, "verified": 0, "rate": None}, []
    rate = verified / episodes
    score = min(100, 100 * squash(rate, 0.60) + teardown_bonus)
    return score, {"n": episodes, "episodes": episodes, "verified": verified, "rate": rate,
                   "teardown_bonus": teardown_bonus}, unverified_examples[:4]


def score_toolcraft(corpus):
    total = corpus.total_tool_calls
    if total == 0:
        return 0.0, {"n": 0, "distinct": 0, "evenness": 0.0, "delegation_events": 0}, []
    # Collapse case-variant duplicates (e.g. "Bash" vs "bash") for an honest distinct count.
    merged = Counter()
    for name, cnt in corpus.tool_usage.items():
        merged[name.lower()] += cnt
    distinct = len(merged)
    breadth = squash(distinct / 20, 1.0)
    # Shannon evenness of the usage distribution.
    counts = list(merged.values())
    H = -sum((x / total) * math.log(x / total) for x in counts if x > 0)
    evenness = (H / math.log(distinct)) if distinct > 1 else 0.0
    active_hours = max(corpus.active_seconds / 3600, 0.5)
    delegation = squash(corpus.delegation_events / active_hours, 2.0)
    score = 100 * (0.45 * breadth + 0.30 * evenness + 0.25 * delegation)
    detail = {"n": total, "distinct": distinct, "evenness": evenness,
              "delegation_events": corpus.delegation_events}
    return score, detail, []


# --------------------------------------------------------------------------- #
# Aggregate: confidence shrinkage, overall score, band, archetype
# --------------------------------------------------------------------------- #

def shrink(score, n, target_n):
    c = min(1.0, n / target_n) if target_n else 1.0
    return 50 + (score - 50) * c, c


def band_for(score):
    for name, lo, hi, meaning in BANDS:
        if lo <= score <= hi:
            return name, meaning
    return BANDS[-1][0], BANDS[-1][3]


def _cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


def classify_archetype(dim_scores, delegation_score):
    """Nearest-prototype over your DRIVING-STYLE vector, with a margin guard.

    The vector adds a Delegation axis and is AGENCY-WEIGHTED: axes you control
    (Direction, Iteration, Toolcraft, Delegation) count fully, while axes the agent
    mostly drives on its own (Verification, Context) are heavily discounted — so the
    archetype reflects how *you* drive, not Claude's built-in habits.
    """
    scores = dict(dim_scores)
    scores["Delegation"] = delegation_score
    V = [scores[ax] for ax in ARCHETYPE_AXES]
    names = list(PROTOTYPES.keys())
    mat = [PROTOTYPES[n]["vec"] for n in names]
    # z-score each axis across prototypes + the user vector, then apply agency weights
    cols = list(zip(*(mat + [V])))
    means = [statistics.mean(col) for col in cols]
    stds = [statistics.pstdev(col) or 1.0 for col in cols]
    w = [AGENCY[ax] for ax in ARCHETYPE_AXES]

    def zw(vec):
        return [w[i] * (v - means[i]) / stds[i] for i, v in enumerate(vec)]

    vz = zw(V)
    sims = sorted(((round(_cosine(vz, zw(PROTOTYPES[n]["vec"])), 3), n) for n in names), reverse=True)
    top_sim, top = sims[0]
    second_sim, second = sims[1]
    blended = (top_sim - second_sim) < ARCHETYPE_MARGIN
    second_short = second.replace("The ", "")
    article = "an" if second_short[:1] in "AEIOU" else "a"
    return {
        "primary": top, "primary_sim": top_sim, "secondary": second, "secondary_sim": second_sim,
        "blended": blended, "all": sims, "delegation_score": round(delegation_score),
        "label": f"{PROTOTYPES[top]['emoji']} {top}" + (f", with {article} {second_short} streak" if blended else ""),
        "blurb": PROTOTYPES[top]["blurb"],
    }


# --------------------------------------------------------------------------- #
# Analysis orchestration
# --------------------------------------------------------------------------- #

def analyze(corpus):
    raw, detail, evidence = {}, {}, {}
    for name, fn in (("Direction", score_direction), ("Verification", score_verification),
                     ("Context", score_context), ("Iteration", score_iteration),
                     ("Toolcraft", score_toolcraft)):
        s, d, ev = fn(corpus)
        raw[name], detail[name], evidence[name] = s, d, ev

    shrunk, conf = {}, {}
    for name in raw:
        shrunk[name], conf[name] = shrink(raw[name], detail[name].get("n", 0), TARGET_N[name])

    overall_raw = round(sum(WEIGHTS[n] * raw[n] for n in WEIGHTS))
    overall = round(sum(WEIGHTS[n] * shrunk[n] for n in WEIGHTS))
    band, band_meaning = band_for(overall)
    # Delegation is a user-driven archetype axis (handoffs per active hour).
    active_hours = max(corpus.active_seconds / 3600, 0.5)
    delegation_score = 100 * squash(corpus.delegation_events / active_hours, 2.0)
    archetype = classify_archetype(shrunk, delegation_score)

    # length distribution of real prompts (context only)
    lens = [len(p["text"]) for p in corpus.real_prompts]
    words = [len(p["text"].split()) for p in corpus.real_prompts]
    dist = {}
    if lens:
        dist = {
            "median_chars": int(statistics.median(lens)),
            "mean_chars": int(statistics.mean(lens)),
            "median_words": int(statistics.median(words)),
            "under_80_pct": round(100 * sum(1 for L in lens if L < 80) / len(lens)),
        }

    return {
        "raw": raw, "shrunk": shrunk, "conf": conf, "detail": detail, "evidence": evidence,
        "overall_raw": overall_raw, "overall": overall, "band": band, "band_meaning": band_meaning,
        "archetype": archetype, "dist": dist, "fingerprint": _run_fingerprint(corpus),
    }


def build_action_plan(corpus, result):
    """Growth cards ranked by impact = (target - score) * weight. The teaching copy
    comes from SKILL_TEACH; user-specific evidence comes from result['evidence']."""
    TARGET = 85
    cards = []
    for name in WEIGHTS:
        score = result["shrunk"][name]
        impact = (TARGET - score) * WEIGHTS[name]
        cards.append({"dim": name, "score": round(score), "impact": impact,
                      "weak": result["evidence"].get(name, []),
                      "detail": result["detail"][name]})
    cards.sort(key=lambda c: c["impact"], reverse=True)
    # strength callout = highest shrunk score
    strength = max(WEIGHTS, key=lambda n: result["shrunk"][n])
    return cards, strength


def _shortest_action_prompt(corpus):
    cands = [p["text"] for p in corpus.real_prompts if _is_action_prompt(p["text"]) and len(p["text"]) < 40]
    return min(cands, key=len) if cands else None


def build_evidence(corpus, result, cards, archive_info=None, sdd_result=None):
    """Serialize a de-contaminated EVIDENCE bundle for the two-model analysis pipeline
    (Sonnet 4.6 explores it; Opus 4.8 analyzes it against the bundled AI-fluency
    framework). It contains your real prompts/behavior with home paths scrubbed, and is
    git-ignored. Deterministic (no randomness) so runs are reproducible."""
    prompts = corpus.real_prompts
    sample, seen = [], set()

    def add(p):
        k = (p["session"], p["idx"])
        if k in seen:
            return
        seen.add(k)
        sample.append({"text": _scrub_paths(p["text"][:600]), "project": _project_label(p["project"]),
                       "chars": len(p["text"])})

    by_len = sorted(prompts, key=lambda p: len(p["text"]))
    for p in by_len[:6]:                 # the terse nudges
        add(p)
    for p in by_len[-14:]:               # the rich, intent-carrying prompts
        add(p)
    stride = max(1, len(prompts) // 20)  # an even spread through the timeline
    for p in prompts[::stride]:
        if len(sample) >= 50:
            break
        add(p)

    def clean_ex(items):
        out = []
        for e in items or []:
            if not isinstance(e, dict):
                continue
            c = {}
            if e.get("text"):
                c["text"] = _scrub_paths(str(e["text"])[:300])
            if e.get("file"):
                c["file"] = os.path.basename(str(e["file"]))
            if e.get("files"):
                c["files"] = str(e["files"])
            if e.get("project"):
                c["project"] = _project_label(e["project"])
            if c:
                out.append(c)
        return out

    span_days = (corpus.last_ts - corpus.first_ts).days if corpus.first_ts and corpus.last_ts else 0
    a = result["archetype"]
    return {
        "schema": "claude-insight-evidence/1",
        "meta": {
            "sessions": corpus.files, "projects": len(corpus.projects),
            "real_prompts": len(prompts), "user_records": corpus.user_records,
            "filtered_noise": dict(corpus.filtered),
            "span_days": span_days,
            "active_hours": round(corpus.active_seconds / 3600, 1),
            "archive": archive_info,
            "prompt_distribution": result["dist"],
            # Binds any analysis produced from this bundle back to this exact run; the
            # analysis stage must echo it so a stale/foreign analysis can be refused.
            "run_fingerprint": result.get("fingerprint"),
        },
        "scores": {
            "overall": result["overall"], "overall_raw": result["overall_raw"],
            "band": result["band"], "weights": WEIGHTS,
            "dimensions_raw": {k: round(v) for k, v in result["raw"].items()},
            "dimensions_adjusted": {k: round(v) for k, v in result["shrunk"].items()},
            "confidence": {k: round(v, 2) for k, v in result["conf"].items()},
            "dimension_names": DISPLAY_NAMES,
        },
        "dimension_detail": result["detail"],
        "archetype": {"primary": a["primary"], "secondary": a["secondary"],
                      "blended": a.get("blended")},
        "behavior": {
            "sample_prompts": sample,
            "weak_examples": {c["dim"]: clean_ex(c["weak"]) for c in cards},
            "tool_usage": dict(corpus.tool_usage),
            "delegation_events": corpus.delegation_events,
            "sdd_commands": dict(corpus.sdd_commands),
        },
        # Layer 2: repo-level Spec-Driven-Development adherence (None when no sdd/ tree).
        # The AI workflow uses this to write the Process-Discipline read; it is project
        # data, not this person's, so the analysis must frame it as "the team/repo", not "you".
        "sdd": ({
            "overall": sdd_result["overall"], "band": sdd_result["band"],
            "weights": sdd_result["weights"], "sub_scores": sdd_result["sub"],
            "metrics": sdd_result["raw"],
        } if sdd_result else None),
    }


def _analysis_section_html(analysis):
    """Render the AI-authored skill map (produced by the Opus analysis stage,
    grounded in reference/ai-fluency-framework.md). Falls back to nothing if absent."""
    if not analysis or not isinstance(analysis, dict):
        return ""
    parts = ['<section><h3>Skill map — analyzed against the AI Fluency framework</h3>']
    read = analysis.get("overall_read") or analysis.get("summary")
    if read:
        parts.append(f'<p class="assess">{_esc(read)}</p>')
    for s in analysis.get("skill_map") or []:
        if not isinstance(s, dict):
            continue
        comp = _esc(s.get("competency", "?"))
        lvl = s.get("level", "?")
        label = _esc(s.get("level_label", ""))
        summ = _esc(s.get("summary", ""))
        nxt = _esc(s.get("next_move", ""))
        ev = "".join(f"<li>“{_esc(str(x)[:200])}”</li>" for x in (s.get("evidence") or [])[:3])
        parts.append(
            f'<div class="dim"><div class="dim-h"><b>{comp}</b>'
            f'<span class="pill">Level {_esc(lvl)}/5 · {label}</span></div>'
            f'<p>{summ}</p>'
            + (f'<ul class="ev">{ev}</ul>' if ev else "")
            + (f'<p class="next"><b>Your next move:</b> {nxt}</p>' if nxt else "")
            + '</div>')
    strengths = analysis.get("strengths") or []
    if strengths:
        items = "".join(f"<li>{_esc(s)}</li>" for s in strengths[:5])
        parts.append(f'<p style="margin-top:14px"><b>What you already do well:</b></p><ul class="facts">{items}</ul>')
    parts.append('<p style="color:var(--mut);font-size:13px;margin-top:10px">'
                 'This section is written by Claude Opus 4.8 from your de-contaminated evidence '
                 '(explored by Claude Sonnet 4.6), grounded in the bundled AI Fluency framework. '
                 'The numbers above are computed deterministically and independently.</p>')
    parts.append('</section>')
    return "".join(parts)


def _growth_cards_html(analysis):
    """The 'how to grow' cards, written FOR THIS PERSON by the Opus analysis stage:
    each item names the habit, why it matters, how to grow it, and a before/after where
    the 'before' is one of THEIR real prompts and the 'after' is Opus's tailored rewrite.
    Returns '' when there is no analysis (the caller then falls back to the generic
    teaching examples), so the report only ever shows canned examples when no AI ran."""
    if not analysis or not isinstance(analysis, dict):
        return ""
    items = [g for g in (analysis.get("top_growth") or []) if isinstance(g, dict)]
    if not items:
        return ""
    out = []
    for i, g in enumerate(items[:3]):
        title = _esc(g.get("title", "Your next growth move"))
        why = _esc(g.get("why", ""))
        how = _esc(g.get("how", ""))
        before = g.get("example_before")
        after = g.get("example_after")
        ba = ""
        if before and after:
            ba = (f'<div class="ba"><div class="before"><span>A prompt you wrote</span>'
                  f'“{_esc(str(before)[:400])}”</div>'
                  f'<div class="after"><span>Tailored rewrite for you</span>'
                  f'“{_esc(str(after)[:600])}”</div></div>')
        out.append(
            f'<div class="card prio"><div class="ph">Priority {i + 1} · written for you</div>'
            f'<h4>{title}</h4>'
            + (f'<p class="why"><b>Why it matters.</b> {why}</p>' if why else "")
            + (f'<div class="wwh"><span class="lab">How to grow it</span>'
               + (f'<p class="how">{how}</p>' if how else "") + f'{ba}</div>'
               if (how or ba) else "")
            + '</div>')
    return "".join(out)


# --------------------------------------------------------------------------- #
# Reports
# --------------------------------------------------------------------------- #

def _project_label(name):
    """Claude encodes an absolute path with '-' for '/', so we can't perfectly
    recover hyphenated names. Drop the home/boilerplate prefix and show the rest.
    '-Users-me-Dropbox-AI-platzi-executive-assistant' -> 'AI platzi executive assistant'."""
    s = re.sub(r"^-?(?:Users|home)-[^-]+(?:-|$)", "", name)  # strip -Users-/-home-<user>- (mac & linux)
    s = re.sub(r"^Dropbox-", "", s)                          # strip a common cloud-folder prefix
    s = s.replace("-", " ").strip()
    # Nothing left -> the session ran in $HOME itself; never echo the raw name (it holds the username).
    if not s:
        return "home" if re.match(r"^-?(?:Users|home)-", name) else name
    return s


def terminal_summary(corpus, result):
    a = result["archetype"]
    lines = [
        "",
        f"  AI Fluency Score: {result['overall']}/100  ({result['band']})",
        f"  Archetype: {a['label']}",
        f"  Based on {len(corpus.real_prompts)} real prompts across {len(corpus.projects)} projects, "
        f"{corpus.files} sessions ({corpus.total_bytes/1e6:.1f} MB).",
        "",
    ]
    return "\n".join(lines)


def _esc(s):
    return html.escape(str(s))


# Each archetype's encouraging "next gain" — frames the top growth lever as a natural
# progression for that style rather than a deficit.
ARCH_PATHS = {
    "Autonomous Agent": "You already hand off whole jobs well — add one sharp sentence of intent per hand-off and far more will land right the first time, with less back-and-forth.",
    "Architect": "Your planning is a real strength — pair it with a quick check after each change so your designs ship proven, not just drawn.",
    "Debugger": "Your diagnostic discipline is excellent — capture each fix as a small reusable rule so the same bug never costs you twice.",
    "Collaborator": "Your back-and-forth keeps things aligned — front-loading a constraint or two will get you there in fewer rounds.",
    "Sprinter": "Your speed is real — a one-line brief plus a quick test keeps that speed from turning into rework.",
}

_SIG_DESC = {
    "Delegation": "how much you hand off — you give Claude whole jobs and trust it to run them end-to-end",
    "Toolcraft": "the range of tools you bring to bear — you reach past the shell for the right instrument",
    "Iteration": "how cleanly you change course — your corrections tend to name the fix, not just reject",
    "Briefing": "how concretely you frame requests when it matters",
}

# The specific, evidence-grounded line that explains each dimension as a growth edge.
_GROWTH_LINE = {
    "Direction": "{s}s win on how sharply they frame the work they hand off — and right now yours are often one-liners like “{ex}”, so Claude fills gaps you could have decided.",
    "Verification": "Right now changes often move on without a test, build or run to confirm them — the cheapest reliability you can buy back.",
    "Context": "Right now some edits land before the file has been read that session — an easy blind-edit risk to remove.",
    "Iteration": "Right now corrections lean toward brief rejections; naming the symptom and the exact rule resolves loops in fewer turns.",
    "Toolcraft": "Right now most work funnels through one tool — reaching for search, planning and delegation widens what you can take on.",
}


def build_assessment(corpus, result, cards):
    """A coherent, professional written read — synthesizes the numbers into one story
    and explicitly resolves the archetype-vs-weakest-dimension tension."""
    a = result["archetype"]
    arch = a["primary"]
    short = arch.replace("The ", "")
    art = "an" if short[:1] in "AEIOU" else "a"
    deleg = a["delegation_score"]
    n_deleg = corpus.delegation_events
    median = result["dist"].get("median_chars", "?")

    # signature strength = your strongest USER-driven signal (not Claude's defaults)
    user_signals = {
        "Briefing": result["shrunk"]["Direction"], "Iteration": result["shrunk"]["Iteration"],
        "Toolcraft": result["shrunk"]["Toolcraft"], "Delegation": float(deleg),
    }
    sig = max(user_signals, key=user_signals.get)

    growth = cards[0]["dim"]
    growth_disp = disp(growth)
    example = _shortest_action_prompt(corpus) or "run it"
    path_why = ARCH_PATHS.get(arch, "Keep building the habits below and your next run will show the gain.")

    p1 = (f"You drive Claude like <b>{_esc(a['label'])}</b>. {_esc(a['blurb'])} "
          f"The clearest signal is your delegation rate — <b>{deleg}/100</b>, from {n_deleg} hand-offs to "
          f"subagents, background jobs and planning — paired with fast, terse prompts (median "
          f"{median} characters).")

    # Only credit the read→edit→verify loop when the data actually shows it; otherwise this
    # clause was claiming a discipline some users' own report contradicts (0% verified / grounded).
    loop_ok = result["shrunk"]["Context"] >= 55 and result["shrunk"]["Verification"] >= 55
    p2_mid = ("That, plus the disciplined read→edit→verify loop your sessions show, is"
              if loop_ok else "That is")
    p2 = (f"Your strongest <i>self-driven</i> habit is {_esc(_SIG_DESC.get(sig, sig.lower()))}. "
          f"{p2_mid} why your overall score lands at <b>{result['overall']}/100 ({_esc(result['band'])})</b>.")

    gline = _GROWTH_LINE.get(growth, "").format(s=_esc(short), ex=_esc(example))
    p3 = (f"And the apparent tension, resolved: your lowest dimension is <b>{_esc(growth_disp)}</b> — but for "
          f"{art} {_esc(short)} that isn't a contradiction, it's the <i>defining</i> growth edge. {gline} "
          f"{_esc(path_why)}")

    return (f'<p class="assess">{p1}</p><p class="assess">{p2}</p><p class="assess">{p3}</p>')


def build_html(corpus, result, cards, strength, archive_info=None, analysis=None,
               analysis_note=None, sdd_result=None):
    a = result["archetype"]
    d = result["dist"]
    analysis_section = _analysis_section_html(analysis)
    sdd_section = _sdd_section_html(sdd_result, corpus, analysis)
    # When an AI analysis was expected but couldn't be used (no-op'd, empty, or from a
    # different run), say so plainly instead of letting the template-only report pass as
    # the full thing. Silent on a plain deterministic run (no --analysis was attempted).
    analysis_status_html = ""
    if not analysis_section and analysis_note:
        analysis_status_html = (
            '<section><div class="prov">ℹ️ <b>Deterministic report only.</b> '
            f'{_esc(analysis_note)} — the Sonnet&nbsp;+&nbsp;Opus skill map was not added on top. '
            'The scores, archetype and dimensions below are still fully computed from your data; '
            'to add the AI-written skill map, re-run <code>/ai-fluency</code> inside Claude Code.'
            '</div></section>')
    days = (corpus.last_ts - corpus.first_ts).days if corpus.first_ts and corpus.last_ts else 0
    active_h = corpus.active_seconds / 3600
    filtered_total = sum(corpus.filtered.values())
    provisional = len(corpus.real_prompts) < PROVISIONAL_MIN_PROMPTS

    DIM_BLURB = {
        "Direction": "How clearly you tell the agent what you want before it acts.",
        "Verification": "Whether changes get checked (tests / build / app) before moving on.",
        "Context": "Reading a file before editing it — grounded, not blind, changes.",
        "Iteration": "Correcting precisely instead of thrashing with vague rejections.",
        "Toolcraft": "Using a healthy range of tools — not forcing everything through one.",
    }

    def dim_rate_line(name):
        det = result["detail"][name]
        if name == "Verification" and det.get("rate") is not None:
            return f"{det['verified']} of {det['episodes']} edit-bursts verified ({det['rate']*100:.0f}%)"
        if name == "Context" and det.get("rate") is not None:
            return f"{det['grounded']} of {det['total_edits']} edits were grounded in a prior read ({det['rate']*100:.0f}%)"
        if name == "Direction":
            return (f"{det['constraint_rate']*100:.0f}% carry a constraint · "
                    f"{det['artifact_rate']*100:.0f}% name a file/error · {det['intent_rate']*100:.0f}% state a why")
        if name == "Iteration":
            return f"{det['corrections']} correction turns ({det['correction_rate']*100:.0f}% of prompts); {det['specificity']*100:.0f}% were specific"
        if name == "Toolcraft":
            return f"{det.get('distinct', 0)} distinct tools, evenness {det.get('evenness', 0.0):.2f}, {det.get('delegation_events', 0)} delegations"
        return ""

    # dimension bars
    dim_html = ""
    order = sorted(WEIGHTS, key=lambda n: result["shrunk"][n], reverse=True)
    for name in order:
        sc = round(result["shrunk"][name])
        raw_sc = round(result["raw"][name])
        c = result["conf"][name]
        lowdata = c < 0.75
        tag = ""
        if name == strength:
            tag = '<span class="tag s">Strength</span>'
        elif name == cards[0]["dim"]:
            tag = '<span class="tag w">Top growth lever</span>'
        ld = '<span class="tag ld">low data</span>' if lowdata else ""
        dim_html += f"""
      <div class="dim">
        <div class="top"><span class="name">{_esc(disp(name))} {tag}{ld}</span><span class="sval">{sc}<span class="hint">/100</span></span></div>
        <div class="bar"><i style="width:{sc}%"></i></div>
        <p class="def">{_esc(DIM_BLURB[name])}</p>
        <p class="rate">{_esc(dim_rate_line(name))}<span class="wt"> · weight {int(WEIGHTS[name]*100)}%</span></p>
      </div>"""

    # archetype affinity
    aff = ""
    for sim, nm in a["all"]:
        pct = max(0, round((sim + 1) / 2 * 100))
        aff += f"""<div class="bar-item"><div class="bl">{PROTOTYPES[nm]['emoji']} {_esc(nm)}</div>
          <div class="bt"><i style="width:{pct}%"></i></div><div class="bv">{sim:+.2f}</div></div>"""

    # data-ingested filter breakdown
    filt = "".join(
        f"<li><b>{v:,}</b> {_esc(k)}</li>" for k, v in corpus.filtered.most_common()
    )

    # Archive stat tile + the "why ~30 days / how to see more" callout.
    archive_tile = retention_note = ""
    arch_dir_disp = _esc(archive_info["dir"]) if archive_info else _esc(DEFAULT_ARCHIVE_DIR)
    if archive_info:
        archive_tile = (f'<div class="ing"><div class="n">{archive_info["archived_sessions"]:,}</div>'
                        f'<div class="l">sessions in your archive</div></div>')
    # Show the explainer whenever the visible history is short — that's the 30-day cleanup biting.
    if days <= 32:
        grew = ""
        if archive_info and archive_info.get("enabled"):
            grew = (f' This run preserved <b>{archive_info["new"]:,}</b> new session(s) to your '
                    f'archive (<code>{arch_dir_disp}</code>), so from here your history keeps growing '
                    f'past the 30-day wall. Keep this archive private to you — sharing one folder '
                    f'between people would mix everyone\'s transcripts into a single report.')
        retention_note = (
            '<div class="honesty" style="margin-top:14px">'
            f'<b>Why only ~{days} days?</b> Claude Code deletes transcripts older than your '
            '<code>cleanupPeriodDays</code> setting (default <b>30</b>), so that is all that was '
            'left on disk to read — not a limit of this tool. To analyze more history: '
            '<b>(1)</b> raise <code>cleanupPeriodDays</code> in <code>~/.claude/settings.json</code> '
            '(e.g. <code>"cleanupPeriodDays": 365</code>) to stop the deletion; '
            f'<b>(2)</b> keep running Claude Insight.{grew}'
            '</div>')

    # action cards (what/where/how)
    def evidence_html(card):
        name = card["dim"]
        ev = card["weak"]
        if not ev:
            return '<p class="ev-none">No clear examples in your transcripts — this is already a habit. ✓</p>'
        items = ""
        # small-sample guard per project
        proj_counts = Counter(p["project"] for p in corpus.real_prompts)
        for e in ev[:3]:
            if name == "Direction" or name == "Iteration":
                proj = e["project"]; txt = _scrub_paths(e["text"])
                small = " <em>(illustrative, small sample)</em>" if proj_counts.get(proj, 0) < 10 else ""
                items += f'<li>“{_esc(txt[:140])}” <span class="loc">— {_esc(_project_label(proj))}{small}</span></li>'
            elif name == "Context":
                small = " <em>(illustrative)</em>" if proj_counts.get(e["project"], 0) < 10 else ""
                items += f'<li>Edited <code>{_esc(os.path.basename(e["file"]))}</code> without reading it first <span class="loc">— {_esc(_project_label(e["project"]))}{small}</span></li>'
            elif name == "Verification":
                small = " <em>(illustrative)</em>" if proj_counts.get(e["project"], 0) < 10 else ""
                items += f'<li>A burst of edits to <code>{_esc(e["files"])}</code> with nothing run afterwards <span class="loc">— {_esc(_project_label(e["project"]))}{small}</span></li>'
        return f"<ul class='ev'>{items}</ul>"

    cards_html = ""
    for i, card in enumerate(cards[:2]):
        name = card["dim"]
        t = SKILL_TEACH[name]
        # These before/after pairs are a fixed teaching library, identical for every user
        # with this weak dimension — they are NOT drawn from this person's transcripts.
        # Label them as such so they can never be mistaken for a personalized rewrite (the
        # personalized signal is the "Where this shows up in your sessions" block above).
        ex_html = "".join(
            f'<div class="ba"><div class="before"><span>Instead of</span>“{_esc(e["before"])}”</div>'
            f'<div class="after"><span>Stronger</span>“{_esc(e["after"])}”</div></div>'
            for e in t["examples"]
        )
        cards_html += f"""
      <div class="card prio">
        <div class="ph">Priority {i+1} · {_esc(disp(name))} <span class="pscore">now {card['score']}/100</span></div>
        <h4>{_esc(t['what_it_is'])}</h4>
        <p class="why"><b>Why it matters.</b> {_esc(t['why_it_matters'])}</p>
        <div class="wwh"><span class="lab">Where this shows up in your sessions</span>{evidence_html(card)}</div>
        <div class="wwh"><span class="lab">How to grow it</span><p class="how">{_esc(t['how_to_improve'])}</p>
          <p class="exgen">Generic illustrations of the habit — <b>not</b> from your sessions:</p>
          {ex_html}
        </div>
        <p class="tgt">🎯 Try this next session: {_esc(t['practice'])}</p>
      </div>"""

    # strength callout — lead with the user's signature (self-driven) strength.
    # Floor the praise: if even the best dimension is weak, frame it as "relatively
    # strongest" rather than asserting a mastered habit the rate-line would contradict.
    s_det = dim_rate_line(strength)
    strong_score = round(result["shrunk"][strength])
    if strong_score >= 55:
        strength_head = "Keep doing this"
        strength_body = (f"{_esc(SKILL_TEACH[strength]['good_looks_like'])} The evidence in your "
                         f"sessions: {_esc(s_det)}. This is your foundation — build on it.")
    else:
        strength_head = "Your relatively strongest area"
        strength_body = (f"Even your strongest dimension has room to grow ({strong_score}/100), but this "
                         f"is the most natural place to build from. The evidence in your sessions: "
                         f"{_esc(s_det)}.")
    strength_html = f"""
      <div class="card keep">
        <div class="ph">{strength_head} · {_esc(disp(strength))} <span class="pscore">{strong_score}/100</span></div>
        <p>{strength_body}</p>
      </div>"""

    # skill map (levels)
    skill_levels = _skill_levels(result)
    skill_html = ""
    for sk in skill_levels:
        dots = "".join(
            f'<span class="dot {"on" if i < sk["level"] else ""}"></span>' for i in range(5)
        )
        skill_html += f"""<div class="skill">
          <div class="sk-top"><span class="sk-name">{_esc(sk['name'])} <span class="lvl">Level {sk['level']}/5</span></span><span class="sk-dots">{dots}</span></div>
          <p class="sk-what">{_esc(sk['what'])}</p>
          <p class="sk-now"><b>You're here:</b> {_esc(sk['now'])}</p>
          <p class="sk-next"><b>Next move:</b> {_esc(sk['next'])}</p></div>"""

    prov_banner = ""
    if provisional:
        prov_banner = (f'<div class="prov">⚠️ Provisional: only {len(corpus.real_prompts)} real prompts found — '
                       f'treat the score as a rough range (±10). It sharpens as you use Claude Code more.</div>')
    # On thin data the archetype is the least stable signal (a near-neutral vector lands on
    # whichever prototype is closest by a hair), so hedge it explicitly rather than asserting it.
    arch_hedge = ""
    if provisional:
        arch_hedge = ('<p style="margin-top:8px;font-size:12.5px;color:var(--warn)">Provisional — based on only '
                      f'{len(corpus.real_prompts)} prompt(s); the archetype can shift as more history accumulates.</p>')

    # fun facts
    facts = [
        f"{len(corpus.real_prompts)} prompts you actually typed (out of {corpus.user_records:,} user records — the rest were tool output, subagent turns or system text)",
        f"median prompt is {d.get('median_chars','?')} characters ({d.get('median_words','?')} words); {d.get('under_80_pct','?')}% are under 80 chars",
        f"{active_h:.0f} hours of hands-on time (idle gaps over 5 min excluded)",
        f"{result['detail']['Toolcraft']['distinct']} distinct tools used; {corpus.total_tool_calls:,} tool calls in total",
        f"most-used tool: {corpus.tool_usage.most_common(1)[0][0] if corpus.tool_usage else 'n/a'}",
        f"{corpus.delegation_events} delegations (subagents / background jobs / planning)",
    ]
    facts_html = "".join(f"<li>{_esc(f)}</li>" for f in facts)
    assessment_html = build_assessment(corpus, result, cards)

    # "What to improve": prefer the Opus analysis's tailored growth cards (grounded in this
    # person's real prompts). Only fall back to the generic teaching examples when no AI
    # analysis ran — so a finished report is personalized, not a library of stock examples.
    growth_cards = _growth_cards_html(analysis)
    if growth_cards:
        improve_cards = growth_cards
        improve_intro = ('<p class="exgen" style="margin-bottom:14px">Written for you by Claude '
                         'Opus&nbsp;4.8 from your real prompts — your highest-leverage moves, each '
                         'with one of your prompts rewritten.</p>')
    else:
        improve_cards = cards_html
        improve_intro = ""

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Your AI Fluency Report</title>
<style>
:root{{--bg:#0c0d18;--p:#15172a;--p2:#1d2040;--ink:#eef0ff;--mut:#a4a8cc;--line:#2a2d52;
--ac:#7c5cff;--ac2:#3ad6c9;--good:#3ad68a;--warn:#ffb454;--bad:#ff6b8b;}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:radial-gradient(1100px 640px at 72% -12%,#262a55 0%,var(--bg) 55%);color:var(--ink);
font:16px/1.65 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;padding-bottom:80px}}
.wrap{{max-width:880px;margin:0 auto;padding:0 22px}}
header{{text-align:center;padding:60px 0 12px}}
.kick{{letter-spacing:.22em;text-transform:uppercase;font-size:12px;color:var(--mut)}}
h1{{font-size:34px;margin:10px 0 4px}}
.sub{{color:var(--mut);max-width:620px;margin:6px auto 0;font-size:15px}}
.hero{{margin:30px auto 0;display:flex;gap:22px;align-items:stretch;flex-wrap:wrap;justify-content:center}}
.score-card{{background:linear-gradient(135deg,var(--p2),var(--p));border:1px solid var(--line);border-radius:22px;
padding:26px 30px;text-align:center;min-width:240px;box-shadow:0 18px 50px rgba(0,0,0,.4)}}
.ring{{position:relative;width:170px;height:170px;margin:0 auto}}
.ring .n{{position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center}}
.ring .n b{{font-size:50px;line-height:1}}
.ring .n s{{text-decoration:none;color:var(--mut);font-size:13px}}
.band{{margin-top:12px;font-size:19px;font-weight:700;color:var(--ac2)}}
.rawnote{{color:var(--mut);font-size:12px;margin-top:4px}}
.arch{{flex:1;min-width:260px;background:var(--p);border:1px solid var(--line);border-radius:22px;padding:24px 26px;text-align:left}}
.arch .emoji{{font-size:40px}}
.arch h2{{font-size:23px;margin:6px 0}}
.arch p{{color:var(--mut);font-size:15px}}
.prov{{background:rgba(255,180,84,.1);border:1px solid rgba(255,180,84,.35);color:#ffe6c2;border-radius:12px;padding:12px 16px;margin:22px 0 0;font-size:14px}}
section{{margin:42px 0}}
h3{{font-size:13px;letter-spacing:.16em;text-transform:uppercase;color:var(--mut);border-bottom:1px solid var(--line);padding-bottom:10px;margin-bottom:18px}}
.band-meaning{{background:var(--p);border:1px solid var(--line);border-left:4px solid var(--ac);border-radius:12px;padding:16px 20px;color:#dfe2ff}}
.assess{{background:var(--p);border:1px solid var(--line);border-radius:14px;padding:16px 20px;margin-bottom:12px;font-size:15.5px;line-height:1.7;color:#e8eaff}}
.assess b{{color:#fff}}
.ingest{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}}
.ing{{background:var(--p);border:1px solid var(--line);border-radius:14px;padding:14px 16px}}
.ing .n{{font-size:24px;font-weight:700;color:var(--ac2)}}
.ing .l{{color:var(--mut);font-size:13px;margin-top:2px}}
.honesty{{margin-top:16px;background:var(--p);border:1px solid var(--line);border-radius:14px;padding:16px 20px}}
.honesty b{{color:var(--ink)}}
.honesty ul{{list-style:none;display:flex;flex-wrap:wrap;gap:8px 22px;margin-top:8px}}
.honesty li{{color:var(--mut);font-size:14px}}
.dim{{background:var(--p);border:1px solid var(--line);border-radius:14px;padding:16px 20px;margin-bottom:12px}}
.dim .top{{display:flex;justify-content:space-between;align-items:baseline}}
.dim .name{{font-weight:700;font-size:17px}}
.dim .sval{{font-size:22px;font-weight:800}} .dim .hint{{color:var(--mut);font-size:12px;font-weight:400}}
.dim-h{{display:flex;justify-content:space-between;align-items:baseline;gap:12px;margin-bottom:6px}}
.dim-h b{{font-size:17px}}
.pill{{font-size:12px;font-weight:700;color:var(--ink);background:var(--p2);border:1px solid var(--line);border-radius:99px;padding:3px 11px;white-space:nowrap}}
.ev{{margin:8px 0 0 0;padding-left:18px}} .ev li{{color:var(--mut);font-size:14px;margin:3px 0}}
.next{{margin-top:8px;font-size:14.5px}} .next b{{color:#fff}}
.bar{{height:9px;background:#23264a;border-radius:99px;overflow:hidden;margin:11px 0 9px}}
.bar>i{{display:block;height:100%;border-radius:99px;background:linear-gradient(90deg,var(--ac),var(--ac2))}}
.def{{color:var(--ink);font-size:14.5px}} .rate{{color:var(--mut);font-size:13px;margin-top:3px}} .wt{{opacity:.7}}
.tag{{font-size:10.5px;padding:2px 8px;border-radius:99px;font-weight:700;margin-left:6px;vertical-align:middle}}
.tag.s{{background:rgba(58,214,138,.16);color:var(--good)}} .tag.w{{background:rgba(255,107,139,.16);color:var(--bad)}}
.tag.ld{{background:rgba(164,168,204,.16);color:var(--mut)}}
.bar-item{{display:flex;align-items:center;gap:12px;margin:7px 0}}
.bl{{min-width:160px;font-size:14px}} .bt{{flex:1;height:7px;background:#23264a;border-radius:99px;overflow:hidden}}
.bt>i{{display:block;height:100%;background:linear-gradient(90deg,var(--ac),var(--ac2))}} .bv{{min-width:46px;text-align:right;color:var(--mut);font-size:13px}}
.card{{background:var(--p);border:1px solid var(--line);border-radius:16px;padding:18px 22px;margin-bottom:14px}}
.prio{{border-left:4px solid var(--warn)}} .keep{{border-left:4px solid var(--good)}}
.ph{{font-size:12px;text-transform:uppercase;letter-spacing:.1em;color:var(--mut)}}
.pscore{{float:right;color:var(--ac2);letter-spacing:0}}
.card h4{{font-size:18px;margin:8px 0 12px}}
.wwh{{margin:12px 0}} .wwh .lab{{display:block;font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:var(--mut);margin-bottom:6px}}
ul.ev{{list-style:none}} ul.ev li{{background:var(--p2);border-radius:9px;padding:9px 12px;margin-bottom:7px;font-size:14px}}
.loc{{color:var(--mut);font-size:12.5px}} .ev-none{{color:var(--good);font-size:14px}}
.ba{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:8px}}
.why{{color:var(--mut);font-size:14px;margin:2px 0 4px}} .why b{{color:var(--ink)}}
.how{{font-size:14.5px;margin:0 0 4px}}
.exgen{{font-size:12px;color:var(--mut);margin:8px 0 2px;font-style:italic}}
.sk-what{{color:var(--ink);font-size:13.5px;margin-top:5px}}
.lvl{{font-size:11px;color:var(--ac2);font-weight:600;margin-left:6px}}
.before,.after{{border-radius:10px;padding:10px 13px;font-size:14px}}
.before{{background:rgba(255,107,139,.08);color:#ffd0da}} .after{{background:rgba(58,214,138,.08);color:#cfeede}}
.before span,.after span{{display:block;font-size:11px;text-transform:uppercase;letter-spacing:.08em;opacity:.7;margin-bottom:3px}}
.tgt{{margin-top:10px;color:var(--ac2);font-size:14px}}
.skill{{background:var(--p);border:1px solid var(--line);border-radius:14px;padding:14px 18px;margin-bottom:10px}}
.sk-top{{display:flex;justify-content:space-between;align-items:center}} .sk-name{{font-weight:700}}
.dot{{display:inline-block;width:11px;height:11px;border-radius:50%;background:#2a2d52;margin-left:4px}}
.dot.on{{background:linear-gradient(135deg,var(--ac),var(--ac2))}}
.sk-now{{color:var(--mut);font-size:13.5px;margin-top:6px}} .sk-next{{font-size:13.5px;margin-top:3px}}
.facts{{list-style:none}} .facts li{{background:var(--p);border:1px solid var(--line);border-radius:10px;padding:11px 15px;margin-bottom:8px;font-size:14.5px}}
.facts li::before{{content:"›";color:var(--ac2);font-weight:800;margin-right:9px}}
details{{background:var(--p);border:1px solid var(--line);border-radius:12px;padding:14px 18px;margin-top:14px}}
summary{{cursor:pointer;color:var(--mut);font-size:14px}} details p,details li{{color:var(--mut);font-size:13px;margin-top:8px}}
footer{{text-align:center;color:var(--mut);font-size:13px;margin-top:46px}}
code{{background:#23264a;padding:1px 6px;border-radius:5px;font-size:13px}}
@media(max-width:640px){{.ba{{grid-template-columns:1fr}}.bl{{min-width:120px}}}}
</style></head><body><div class="wrap">

<header>
  <div class="kick">Claude Insight · AI Fluency Report</div>
  <h1>How skillfully you build with AI</h1>
  <p class="sub">A read of how you actually drive Claude Code — measured from your real prompts and Claude's real actions, analyzed entirely on your machine.</p>
</header>

{prov_banner}

<div class="hero">
  <div class="score-card">
    <div class="ring">
      <svg width="170" height="170" style="transform:rotate(-90deg)">
        <circle cx="85" cy="85" r="74" fill="none" stroke="#23264a" stroke-width="12"/>
        <circle cx="85" cy="85" r="74" fill="none" stroke="url(#g)" stroke-width="12" stroke-linecap="round"
          stroke-dasharray="{2*math.pi*74*result['overall']/100:.0f} 999"/>
        <defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="#7c5cff"/><stop offset="1" stop-color="#3ad6c9"/></linearGradient></defs>
      </svg>
      <div class="n"><b>{result['overall']}</b><s>/ 100</s></div>
    </div>
    <div class="band">{_esc(result['band'])}</div>
    <div class="rawnote">raw {result['overall_raw']} · confidence-adjusted {result['overall']}</div>
  </div>
  <div class="arch">
    <div class="emoji">{PROTOTYPES[a['primary']]['emoji']}</div>
    <h2>{_esc(a['label'])}</h2>
    <p>{_esc(a['blurb'])}</p>
    <p style="margin-top:10px;font-size:13px">Closest match {a['primary_sim']:+.2f}, next is {_esc(a['secondary'].replace('The ',''))} {a['secondary_sim']:+.2f}{' — close, so this is a blend' if a['blended'] else ''}. Built from how <b>you</b> drive — your briefs, corrections, tool choices and how much you hand off ({a['delegation_score']}/100 delegation) — and it deliberately discounts the read-before-edit and run-the-tests habits Claude does on its own, so it reflects you, not the agent.</p>
    <p style="margin-top:8px;font-size:12.5px;color:var(--mut)">Your <b>score</b> measures the quality of the collaboration (you + Claude); your <b>archetype</b> measures your driving style alone — so they can differ on purpose.</p>
    {arch_hedge}
  </div>
</div>

<section>
  <h3>Professional assessment</h3>
  {assessment_html}
</section>

<section>
  <h3>What your score means</h3>
  <div class="band-meaning"><b>{_esc(result['band'])} ({result['overall']}/100).</b> {_esc(result['band_meaning'])}</div>
</section>

<section>
  <h3>How much data this is based on</h3>
  <div class="ingest">
    <div class="ing"><div class="n">{corpus.files}</div><div class="l">sessions scanned</div></div>
    <div class="ing"><div class="n">{len(corpus.projects)}</div><div class="l">projects</div></div>
    <div class="ing"><div class="n">{corpus.total_bytes/1e6:.1f} MB</div><div class="l">transcript data parsed</div></div>
    <div class="ing"><div class="n">{days} days</div><div class="l">span of activity</div></div>
    <div class="ing"><div class="n">{len(corpus.real_prompts)}</div><div class="l">real prompts you typed</div></div>
    <div class="ing"><div class="n">{active_h:.0f} h</div><div class="l">hands-on active time</div></div>
    {archive_tile}
  </div>
  {retention_note}
  <div class="honesty">
    <b>The honest part:</b> we found {corpus.user_records:,} “user” records but only <b>{len(corpus.real_prompts)}</b> are prompts <b>you</b> typed. We filtered out {filtered_total:,} that the old tool wrongly counted:
    <ul>{filt}</ul>
    <p style="color:var(--mut);font-size:13px;margin-top:10px">Your real prompts: median {d.get('median_chars','?')} chars · {d.get('under_80_pct','?')}% under 80 chars · {active_h:.0f} h hands-on active time (idle gaps over 5 min are excluded — not raw wall-clock).</p>
  </div>
</section>

{analysis_section}
{analysis_status_html}
{sdd_section}

<section>
  <h3>The five dimensions</h3>
  {dim_html}
</section>

<section>
  <h3>What to improve — and exactly how</h3>
  {improve_intro}
  {improve_cards}
  {strength_html}
</section>

<section>
  <h3>Your skill map</h3>
  {skill_html}
</section>

<section>
  <h3>Archetype affinity</h3>
  {aff}
</section>

<section>
  <h3>Honest numbers at a glance</h3>
  <ul class="facts">{facts_html}</ul>
</section>

<section>
  <h3>Methodology &amp; honesty</h3>
  <details><summary>How every number was computed (click to expand)</summary>
    <p><b>Only real prompts are scored.</b> A “user” record counts as a prompt only if it is not a tool-result, not a subagent (sidechain) turn, not meta/injected, not a slash-command stub, and not a paste/system-prompt over {MAX_HUMAN_PROMPT_CHARS:,} chars or opening with “You are …”. This removes the contamination that made the old tool report a {d.get('mean_chars','?')}-vs-real average.</p>
    <p><b>Everything is a rate, then squashed.</b> Each dimension is a per-prompt or per-opportunity rate run through min(1, rate/target), so doing more work never raises the score — only doing it better does. Weights: Briefing 24%, Verification 22%, Context-setting 22%, Iteration 18%, Toolcraft 14%.</p>
    <p><b>Thin signals are hedged, not faked.</b> Each dimension is pulled toward a neutral 50 in proportion to how many opportunities it had (e.g. Iteration had only {result['detail']['Iteration']['corrections']} corrections, so it is flagged “low data”). Both raw and confidence-adjusted scores are shown.</p>
    <p><b>Archetype</b> describes your <b>driving style</b>, not the collaboration's quality, so it is built on a separate <b>agency-weighted</b> vector: Briefing, Iteration, Toolcraft and Delegation (handoffs to subagents/background jobs/planning) count fully, while Verification and Context — habits Claude largely does on its own — are discounted ({int(AGENCY['Verification']*100)}% and {int(AGENCY['Context']*100)}% weight). It is the nearest prototype by cosine on z-scored values; if the top two are within {ARCHETYPE_MARGIN} we show a blend. <b>Active time</b> caps idle gaps at {GAP_CAP_SECONDS//60} min. <b>Fixes vs v1:</b> prompt mis-count, length inflation, idle-time over-count, random archetype, uncapped tool-diversity, and keyword “error” false-positives.</p>
    <p><b>Limits:</b> this measures observable behavior, not intent; detectors are heuristic and English-biased; it's a single snapshot, not a trend. Terse prompts that carry intent from the prior turn can under-score Direction.</p>
  </details>
</section>

<footer>Generated locally by Claude Insight v2 · your transcripts never left this machine.</footer>
</div></body></html>"""


def _skill_levels(result):
    """Map dimension scores to L1-L5 skill levels with now/next text."""
    def lvl(score):
        return max(1, min(5, int(score // 20) + 1))
    s = result["shrunk"]
    defs = [
        ("Briefing & specificity", "Direction",
         "name a goal + one anchor (path, constraint, or acceptance test) in most action prompts",
         {1: "Mostly short nudges with little context.", 2: "Occasional context; one constraint sometimes.",
          3: "Most prompts carry a goal + one anchor.", 4: "Goal + constraint + criterion are common.",
          5: "Consistently high-context with front-loaded rules."}),
        ("Verification discipline", "Verification",
         "end edit-bursts by running the tests / the app before moving on",
         {1: "Edits accepted blind, almost no checks.", 2: "Verifies occasionally.",
          3: "Verifies most bursts of edits.", 4: "Verifies nearly every change.",
          5: "Verification is a reflex — stated up front and layered."}),
        ("Context grounding (read→edit)", "Context",
         "have the agent read the target file before changing it",
         {1: "Often edits files it never read.", 2: "Reads before editing about half the time.",
          3: "Usually points the agent at the right place first.", 4: "Routinely reads target + deps before changing.",
          5: "Deliberate exploration before non-trivial changes."}),
        ("Iteration & recovery", "Iteration",
         "make corrections name a symptom + the exact rule, in one line",
         {1: "Low-info rejections, long loops.", 2: "Corrects but vaguely.",
          3: "Mixes precise and bare corrections.", 4: "Low correction rate, mostly specific.",
          5: "Surgical feedback; turns misses into reusable rules."}),
        ("Toolcraft & orchestration", "Toolcraft",
         "reach past the shell — search, planning, delegation for the right jobs",
         {1: "Effectively one tool.", 2: "The core trio (Bash/Read/Edit).",
          3: "Adds search/web and some planning.", 4: "Comfortable with MCP + balanced spread.",
          5: "20+ tools used appropriately, low concentration."}),
    ]
    out = []
    for name, dim, nxt, rub in defs:
        L = lvl(s[dim])
        out.append({"name": name, "dim": dim, "level": L, "now": rub[L],
                    "what": SKILL_TEACH[dim]["what_it_is"],
                    "next": nxt if L < 5 else "maintain this — it's a real strength."})
    return out


# --------------------------------------------------------------------------- #
# SDD layer (AI-Parrot internal) — process-discipline metrics from sdd/ artifacts
# --------------------------------------------------------------------------- #
#
# Layer 1 (everything above) measures one PERSON's AI fluency from transcripts.
# Layer 2 (here) measures how well THIS REPO follows its own Spec-Driven
# Development workflow, read deterministically from the sdd/ artifact tree. The
# two are orthogonal — the report shows both — so a strong solo prompter on a
# team that skips reviews still sees the review gap, and vice-versa.

# A spec is considered to carry acceptance criteria if it has such a heading.
_SDD_AC_RE = re.compile(r"^#{1,4}\s*.*acceptance\s+criteria", re.I | re.M)
_SDD_STATUS_RE = re.compile(r"^\*\*Status\*\*:\s*(.+?)\s*$", re.I | re.M)

# SDD process sub-dimensions and weights (sum to 1.0). Mirrors the upstream
# dimension model so the report renders them with the same machinery.
SDD_WEIGHTS = {
    "Pipeline": 0.22,        # research-first: specs backed by tasks; proposals before specs
    "Decomposition": 0.22,   # task granularity, declared deps / parallelism / effort
    "Acceptance": 0.20,      # specs state testable acceptance criteria
    "Closure": 0.18,         # features and their tasks actually reach "done"
    "Review": 0.18,          # completed features get a recorded review
}
SDD_DISPLAY = {
    "Pipeline": "Pipeline adherence", "Decomposition": "Task decomposition",
    "Acceptance": "Acceptance criteria", "Closure": "Cycle closure", "Review": "Review coverage",
}
SDD_BLURB = {
    "Pipeline": "Specs are backed by a task index and preceded by research (proposal/brainstorm).",
    "Decomposition": "Features are split into right-sized tasks with declared dependencies, parallelism and effort.",
    "Acceptance": "Specs state explicit, testable acceptance criteria before work starts.",
    "Closure": "Features and their tasks are driven all the way to done — not left half-open.",
    "Review": "Completed features get a recorded code review, closing the SDD feedback loop.",
}
SDD_BANDS = [
    ("Ad-hoc", 0, 39, "The workflow exists but artifacts skip steps: specs without tasks, tasks without "
     "acceptance criteria, or features that never close. The biggest wins are mechanical — back every "
     "spec with a task index and an acceptance section."),
    ("Forming", 40, 54, "The core loop runs but unevenly. Some features go research -> spec -> task -> done "
     "cleanly; others skip review or decomposition. The gap to the next level is consistency."),
    ("Disciplined", 55, 69, "Most features follow the full loop: researched, specced, decomposed, closed. "
     "Remaining gains are in review coverage and acceptance-criteria rigor."),
    ("Rigorous", 70, 84, "The SDD loop is the default path: research-first specs, well-decomposed tasks with "
     "dependencies, criteria stated up front, features closed and reviewed."),
    ("Exemplary", 85, 100, "Spec-Driven Development is institutional: near-complete pipeline coverage, "
     "consistently right-sized tasks, acceptance criteria everywhere, and reviews closing the loop."),
]


def _sdd_band(score):
    for name, lo, hi, meaning in SDD_BANDS:
        if lo <= score <= hi:
            return name, meaning
    return SDD_BANDS[-1][0], SDD_BANDS[-1][3]


def parse_sdd(sdd_dir):
    """Read the sdd/ tree (specs, proposals, task indexes, reviews, state) and return a flat
    dict of raw counts + derived rates. Pure stdlib, strictly read-only — never mutates a thing."""
    root = os.path.expanduser(sdd_dir)
    out = {"present": os.path.isdir(root), "dir": sdd_dir}
    if not out["present"]:
        return out

    def _norm(p):
        return p.replace("\\", "/")

    specs = glob.glob(os.path.join(root, "specs", "*.md"))
    specs = sorted(p for p in specs if "/archived/" not in _norm(p))
    proposals = sorted(glob.glob(os.path.join(root, "proposals", "*.md")))
    indexes = sorted(glob.glob(os.path.join(root, "tasks", "index", "*.json")))
    reviews = [p for p in glob.glob(os.path.join(root, "reviews", "**", "*"), recursive=True)
               if os.path.isfile(p)]
    state_dirs = [d for d in glob.glob(os.path.join(root, "state", "*")) if os.path.isdir(d)]

    # --- Specs: acceptance-criteria coverage + status mix ---
    spec_with_ac = 0
    status_mix = Counter()
    for p in specs:
        try:
            txt = open(p, encoding="utf-8", errors="ignore").read()
        except OSError:
            continue
        if _SDD_AC_RE.search(txt):
            spec_with_ac += 1
        m = _SDD_STATUS_RE.search(txt)
        status_mix[m.group(1).strip().lower() if m else "unknown"] += 1

    # --- Task indexes: decomposition + cycle closure + lead time ---
    feats = feats_closed = 0
    tasks_total = tasks_done = 0
    tasks_with_dep = tasks_parallel = tasks_with_effort = 0
    tasks_per_feat = []
    lead_minutes = []
    indexed_specs = set()
    for ip in indexes:
        try:
            data = json.load(open(ip, encoding="utf-8"))
        except (OSError, json.JSONDecodeError, ValueError):
            continue
        feats += 1
        if data.get("completed_at"):
            feats_closed += 1
        if data.get("spec"):
            indexed_specs.add(os.path.basename(data["spec"]))
        tk = data.get("tasks") or []
        tasks_per_feat.append(len(tk))
        for t in tk:
            if not isinstance(t, dict):
                continue
            tasks_total += 1
            if t.get("status") == "done":
                tasks_done += 1
            if t.get("depends_on"):
                tasks_with_dep += 1
            if t.get("parallel"):
                tasks_parallel += 1
            if t.get("effort"):
                tasks_with_effort += 1
            st, ct = _parse_ts(t.get("started_at")), _parse_ts(t.get("completed_at"))
            if st and ct:
                # Index timestamps mix tz-aware and naive forms; normalize to naive to compare.
                st = st.replace(tzinfo=None)
                ct = ct.replace(tzinfo=None)
                if ct >= st:
                    lead_minutes.append((ct - st).total_seconds() / 60.0)

    spec_basenames = {os.path.basename(p) for p in specs}
    specs_with_tasks = len(spec_basenames & indexed_specs)

    def rate(num, den):
        return (num / den) if den else None

    out.update({
        "counts": {
            "specs": len(specs), "proposals": len(proposals), "indexes": len(indexes),
            "reviews": len(reviews), "state_dirs": len(state_dirs),
            "features": feats, "features_closed": feats_closed,
            "tasks_total": tasks_total, "tasks_done": tasks_done,
            "specs_with_tasks": specs_with_tasks, "specs_with_ac": spec_with_ac,
        },
        "status_mix": dict(status_mix),
        "spec_ac_rate": rate(spec_with_ac, len(specs)),
        "spec_coverage": rate(specs_with_tasks, len(specs)),
        "research_depth": rate(len(proposals) + len(state_dirs), feats),
        "review_coverage": rate(len(reviews), feats),
        "feat_closure_rate": rate(feats_closed, feats),
        "task_done_rate": rate(tasks_done, tasks_total),
        "decomp": {
            "median_tasks_per_feature": statistics.median(tasks_per_feat) if tasks_per_feat else 0,
            "dep_rate": rate(tasks_with_dep, tasks_total),
            "parallel_rate": rate(tasks_parallel, tasks_total),
            "effort_rate": rate(tasks_with_effort, tasks_total),
        },
        "median_lead_minutes": statistics.median(lead_minutes) if lead_minutes else None,
    })
    return out


def score_sdd(sdd):
    """Turn raw sdd/ metrics into 0-100 sub-scores + an overall SDD-adherence score.
    Returns None when there is no sdd/ tree or no features to judge."""
    if not sdd.get("present") or not sdd.get("counts", {}).get("features"):
        return None

    def pct(x):
        return 0.0 if x is None else clamp(x, 0.0, 1.0)

    dc = sdd["decomp"]
    mt = dc["median_tasks_per_feature"] or 0
    # Granularity peaks at a 3-8 task/feature sweet spot; thin or giant decompositions taper off.
    if 3 <= mt <= 8:
        gran = 1.0
    elif mt < 3:
        gran = clamp(mt / 3.0, 0.0, 1.0)
    else:
        gran = clamp(8.0 / mt, 0.0, 1.0)
    decomposition = (0.35 * gran + 0.25 * pct(dc["dep_rate"])
                     + 0.20 * pct(dc["parallel_rate"]) + 0.20 * pct(dc["effort_rate"]))

    sub = {
        "Pipeline": 100 * (0.6 * pct(sdd["spec_coverage"]) + 0.4 * squash(sdd["research_depth"] or 0, 0.7)),
        "Decomposition": 100 * decomposition,
        "Acceptance": 100 * pct(sdd["spec_ac_rate"]),
        "Closure": 100 * (0.5 * pct(sdd["feat_closure_rate"]) + 0.5 * pct(sdd["task_done_rate"])),
        "Review": 100 * squash(sdd["review_coverage"] or 0, 0.5),
    }
    overall = round(sum(SDD_WEIGHTS[k] * sub[k] for k in SDD_WEIGHTS))
    band, meaning = _sdd_band(overall)
    return {
        "sub": {k: round(v) for k, v in sub.items()},
        "overall": overall, "band": band, "band_meaning": meaning,
        "weights": SDD_WEIGHTS, "raw": sdd,
    }


def _sdd_section_html(sdd_result, corpus, analysis=None):
    """Render the SDD process-discipline panel. Returns '' when no SDD data is available."""
    if not sdd_result:
        return ""
    # Layer-2 AI narrative (Opus sdd_read), rendered above the deterministic bars when present.
    sdd_read = ""
    if isinstance(analysis, dict) and isinstance(analysis.get("sdd_read"), str) and analysis["sdd_read"].strip():
        sdd_read = (f'<p class="assess" style="margin-top:14px">{_esc(analysis["sdd_read"].strip())}</p>')
    c = sdd_result["raw"]["counts"]
    sub = sdd_result["sub"]
    bars = ""
    for name in sorted(SDD_WEIGHTS, key=lambda n: sub[n], reverse=True):
        sc = sub[name]
        bars += f"""
      <div class="dim">
        <div class="top"><span class="name">{_esc(SDD_DISPLAY[name])}</span><span class="sval">{sc}<span class="hint">/100</span></span></div>
        <div class="bar"><i style="width:{sc}%"></i></div>
        <p class="def">{_esc(SDD_BLURB[name])}</p>
        <p class="rate"><span class="wt">weight {int(SDD_WEIGHTS[name]*100)}%</span></p>
      </div>"""

    dc = sdd_result["raw"]["decomp"]
    lead = sdd_result["raw"].get("median_lead_minutes")

    def _p(x):
        return "n/a" if x is None else f"{x*100:.0f}%"

    facts = [
        f"{c['features']} features tracked · {c['features_closed']} closed "
        f"({_p(sdd_result['raw']['feat_closure_rate'])})",
        f"{c['specs']} specs · {c['specs_with_tasks']} backed by a task index "
        f"({_p(sdd_result['raw']['spec_coverage'])}) · {c['specs_with_ac']} state acceptance criteria "
        f"({_p(sdd_result['raw']['spec_ac_rate'])})",
        f"{c['tasks_total']:,} tasks · {c['tasks_done']:,} done ({_p(sdd_result['raw']['task_done_rate'])}) · "
        f"median {dc['median_tasks_per_feature']:.0f} tasks/feature",
        f"{c['proposals']} proposals + {c['state_dirs']} research bundles before specs · "
        f"{c['reviews']} recorded reviews ({_p(sdd_result['raw']['review_coverage'])})",
    ]
    if lead is not None:
        facts.append(f"median task lead time {lead/60:.1f} h (started -> completed, where timestamped)")
    cmd_line = ""
    if corpus and corpus.sdd_commands:
        top = ", ".join(f"/{k} ({v})" for k, v in corpus.sdd_commands.most_common(6))
        cmd_line = (f'<p class="rate" style="margin-top:10px">SDD slash-commands seen in your transcripts: '
                    f'{_esc(top)}</p>')
    facts_html = "".join(f"<li>{_esc(f)}</li>" for f in facts)

    return f"""
<section>
  <h3>SDD process discipline — how this repo runs Spec-Driven Development</h3>
  <div class="band-meaning"><b>{_esc(sdd_result['band'])} ({sdd_result['overall']}/100).</b> {_esc(sdd_result['band_meaning'])}</div>
  {sdd_read}
  <div style="margin-top:18px">{bars}</div>
  <ul class="facts" style="margin-top:14px">{facts_html}</ul>
  {cmd_line}
  <p style="color:var(--mut);font-size:13px;margin-top:10px">Computed deterministically from the repo's
  <code>{_esc(sdd_result['raw']['dir'])}/</code> tree (specs, proposals, task indexes, reviews, research
  state). This panel measures the <b>project's</b> workflow adherence, independent of the personal
  AI-fluency scores above.</p>
</section>"""


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def main(argv=None):
    ap = argparse.ArgumentParser(description="Claude Insight v2 — AI fluency analyzer (one command, zero install).")
    ap.add_argument("path", nargs="?", help="transcript dir or .jsonl file (default: ~/.claude/projects)")
    ap.add_argument("-o", "--out", default="ai_fluency_report.html", help="HTML output path")
    ap.add_argument("--json", action="store_true", help="print raw metrics as JSON and exit")
    ap.add_argument("--no-open", action="store_true", help="don't auto-open the report in a browser")
    ap.add_argument("--archive", default=os.environ.get("CLAUDE_INSIGHT_ARCHIVE", DEFAULT_ARCHIVE_DIR),
                    metavar="DIR",
                    help="persistent archive that preserves transcripts beyond Claude Code's "
                         "30-day cleanup so history accumulates (default ~/.claude/insight-archive; "
                         "keep it private to you — a folder shared between people mixes their data)")
    ap.add_argument("--no-archive", action="store_true",
                    help="don't copy this run's transcripts into the archive (still reads an existing one)")
    ap.add_argument("--evidence", metavar="PATH",
                    help="write the de-contaminated evidence bundle (JSON) for the two-model "
                         "analysis pipeline to PATH ('-' for stdout), then continue")
    ap.add_argument("--analysis", metavar="PATH",
                    help="merge an AI analysis (JSON from the Opus stage) into the report's skill map")
    ap.add_argument("--analysis-evidence", metavar="PATH", dest="analysis_evidence",
                    help="the evidence bundle the --analysis was produced from; its run_fingerprint "
                         "is checked against this run so a stale/foreign analysis can't be merged")
    ap.add_argument("--quiet", action="store_true",
                    help="suppress the terminal summary (the skill's internal measure pass uses this "
                         "so the score isn't surfaced before the full AI report is ready)")
    ap.add_argument("--sdd-dir", default=os.environ.get("SDD_DIR", "sdd"), metavar="DIR", dest="sdd_dir",
                    help="path to the repo's SDD artifact tree for the Layer-2 process-discipline panel "
                         "(default ./sdd; skipped automatically if it doesn't exist)")
    ap.add_argument("--no-sdd", action="store_true", dest="no_sdd",
                    help="skip the Layer-2 SDD process-discipline analysis (personal fluency only)")
    args = ap.parse_args(argv)

    files = discover_files(args.path)

    # Default mode: maintain + read the persistent archive so we can analyze more than the
    # ~30 days Claude Code keeps on disk. Skipped when an explicit path is given.
    archive_info = None
    if not args.path:
        archive_dir = os.path.expanduser(args.archive)
        new = updated = 0
        if not args.no_archive:
            new, updated = archive_transcripts(files, archive_dir)
        arch_files = _filter_transcripts(glob.glob(os.path.join(archive_dir, "**", "*.jsonl"), recursive=True))
        merged = _dedupe_sessions(files + arch_files)
        archive_info = {
            "dir": args.archive, "enabled": not args.no_archive,
            "live_sessions": len(files), "archived_sessions": len(arch_files),
            "merged_sessions": len(merged), "new": new, "updated": updated,
        }
        files = merged
        # If most of what we're analyzing comes only from the archive (not this machine's
        # live transcripts), a shared/synced archive could be feeding in someone else's data.
        archive_only = archive_info["merged_sessions"] - archive_info["live_sessions"]
        if archive_only > max(25, 2 * archive_info["live_sessions"]):
            print(f"  Note: {archive_only} of {archive_info['merged_sessions']} analyzed sessions exist "
                  f"only in the archive ({args.archive}), not in your live transcripts. If that archive "
                  f"is shared or synced across people/machines, this report may mix in data that isn't "
                  f"yours — point --archive at a private, per-person path.", file=sys.stderr)

    if not files:
        where = args.path or "~/.claude/projects"
        print(f"No Claude Code transcripts found in {where}.\n"
              f"Point at your transcripts with:  python3 insight.py /path/to/dir", file=sys.stderr)
        return 1

    corpus = parse(files)
    if not corpus.real_prompts:
        print("Found transcripts but no real human-typed prompts to analyze.", file=sys.stderr)
        return 1

    result = analyze(corpus)
    cards, strength = build_action_plan(corpus, result)

    # Layer 2: repo-level SDD process discipline (None when disabled or no sdd/ tree).
    sdd_result = None if args.no_sdd else score_sdd(parse_sdd(args.sdd_dir))

    if args.evidence:
        bundle = build_evidence(corpus, result, cards, archive_info, sdd_result)
        text = json.dumps(bundle, indent=2)
        if args.evidence == "-":
            print(text)
        else:
            ep = os.path.abspath(args.evidence)
            os.makedirs(os.path.dirname(ep) or ".", exist_ok=True)
            with open(ep, "w", encoding="utf-8") as f:
                f.write(text)
            if not args.quiet:
                print(f"  Evidence: {ep}", file=sys.stderr)

    analysis = None
    analysis_note = None
    if args.analysis:
        try:
            with open(os.path.expanduser(args.analysis), encoding="utf-8") as f:
                analysis = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            print(f"Could not read --analysis {args.analysis}: {e}", file=sys.stderr)
            return 1
        # Don't blindly trust the analysis file: it lives at a fixed, reused path, so it
        # may be empty (the AI stage no-op'd) or left over from a different run/person.
        # Validate shape + provenance; on any failure render the deterministic report
        # only, and say so, rather than pasting someone else's verdict into this report.
        current_fp = result.get("fingerprint")
        if not isinstance(analysis, dict) or not analysis.get("skill_map"):
            print("  Note: --analysis had no usable skill map (the AI stage may not have run); "
                  "rendering the deterministic report only.", file=sys.stderr)
            analysis_note = "the AI skill-map stage returned no usable output"
            analysis = None
        elif args.analysis_evidence:
            # Deterministic provenance gate: the analysis is valid for this run only if the
            # evidence it was built from fingerprints to THIS run's data. insight.py wrote
            # that fingerprint, so this check never depends on the model copying anything.
            evidence_fp = None
            try:
                with open(os.path.expanduser(args.analysis_evidence), encoding="utf-8") as f:
                    evidence_fp = (json.load(f).get("meta") or {}).get("run_fingerprint")
            except (OSError, json.JSONDecodeError):
                evidence_fp = None
            if evidence_fp != current_fp:
                print(f"  Note: the --analysis does not match this run (its evidence fingerprint "
                      f"{evidence_fp} != {current_fp}). Ignoring it so it can't leak into this "
                      f"report; rendering the deterministic report only.", file=sys.stderr)
                analysis_note = ("the saved AI analysis was produced from a different run / "
                                 "dataset, so it was not used")
                analysis = None
        else:
            # Manual --analysis with no evidence binding: if the file itself happens to carry a
            # run_fingerprint, honor it; otherwise merge (back-compat with hand-written analyses).
            supplied_fp = analysis.get("run_fingerprint")
            if supplied_fp and current_fp and supplied_fp != current_fp:
                print(f"  Note: the supplied --analysis was produced from a DIFFERENT run "
                      f"(fingerprint {supplied_fp} != {current_fp}). Ignoring it so it can't "
                      f"leak into this report; rendering the deterministic report only.",
                      file=sys.stderr)
                analysis_note = ("the saved AI analysis was produced from a different run / "
                                 "dataset, so it was not used")
                analysis = None

    if args.json:
        payload = {
            "overall": result["overall"], "overall_raw": result["overall_raw"],
            "band": result["band"], "archetype": result["archetype"]["label"],
            "dimensions_raw": result["raw"], "dimensions_adjusted": result["shrunk"],
            "confidence": result["conf"], "detail": result["detail"],
            "data_ingested": {
                "files": corpus.files, "projects": len(corpus.projects),
                "bytes": corpus.total_bytes, "user_records": corpus.user_records,
                "real_prompts": len(corpus.real_prompts), "filtered": dict(corpus.filtered),
                "active_hours": round(corpus.active_seconds / 3600, 1),
                "prompt_distribution": result["dist"],
                "archive": archive_info,
            },
            "sdd_discipline": ({"overall": sdd_result["overall"], "band": sdd_result["band"],
                                "sub_scores": sdd_result["sub"], "metrics": sdd_result["raw"]}
                               if sdd_result else None),
        }
        print(json.dumps(payload, indent=2))
        return 0

    # Render fully before touching the file, so a render error can't leave a 0-byte report.
    html_doc = build_html(corpus, result, cards, strength, archive_info, analysis,
                          analysis_note, sdd_result)
    out_path = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_doc)

    if not args.quiet:
        print(terminal_summary(corpus, result))
        if sdd_result:
            print(f"  SDD Process Discipline: {sdd_result['overall']}/100  ({sdd_result['band']}) "
                  f"— {sdd_result['raw']['counts']['features']} features, "
                  f"{sdd_result['raw']['counts']['tasks_total']:,} tasks.")
        if archive_info and archive_info["enabled"]:
            print(f"  Archive: {archive_info['merged_sessions']} sessions preserved at "
                  f"{archive_info['dir']} ({archive_info['new']} new, {archive_info['updated']} updated this run).")
        print(f"  Report: {out_path}\n")
    if not args.no_open:
        try:
            webbrowser.open(f"file://{out_path}")
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
