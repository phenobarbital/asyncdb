# /sdd-spec — Scaffold a Feature Specification

Scaffold a new Feature Specification using the SDD methodology.


## Usage
```
/sdd-spec <feature-name> [--type feature|hotfix] [--base-branch <branch>] [-- free-form description and notes]
```

`--type` / `--base-branch` are explicit overrides for the flow resolved in
§2d — they win over any brainstorm/proposal frontmatter and over the
`WorkKind` mapping (FEAT-466). Omit both to let §2d resolve the flow from
the exploration doc (or default to `feature`/`dev` when none exists).

## Guardrails
- Always use the official template at `sdd/templates/spec.md`.
- Do NOT write implementation bodies in the spec — but every §3 module MUST
  carry an **Interface Skeleton** (signatures, docstrings, `verified:` anchors;
  see §4 item 6). Bodies belong to task Implementation Blueprints (`/sdd-task`).
- **Feature IDs are unique by construction**: new `FEAT-<NNN>` numbers are
  reserved via `scripts/sdd/reserve_ids.py` (FEAT-387) — a git-native
  compare-and-swap ledger, not a manual "check existing specs" scan — so
  two `/sdd-spec` runs racing each other cannot silently collide on the
  same number. See §5 below. The one documented exception is intentional
  `FEAT-<NNN>` reuse across a deliberate multi-spec split of one
  initiative (e.g. FEAT-380 across `sandbox-hardening`,
  `shelltool-hardening`, `tool-result-compression`) — declared explicitly
  via a `reuse_feature_id` frontmatter field, never a silent fallback.
- If a `.brainstorm.md` exists for this feature in `sdd/proposals/`, use it as input.
- **NEVER re-ask a question that the brainstorm already answered.** Resolved
  answers must be carried forward verbatim, not re-opened. See §2 for the
  resolved-question convention and §3 for what you MAY ask.
- **Always commit the spec file to the current branch** so worktrees can see it.

## Steps

### 1. Parse Input
- **feature-name**: slug-friendly kebab-case. If not provided, ask.
- **free-form notes**: anything after `--`, used as Problem Statement seed.

### 2. Check for Prior Exploration (and carry it forward)

Look for prior exploration documents in `sdd/proposals/`:
- `<feature-name>.brainstorm.md` → structured options analysis with a Recommended Option.
- `<feature-name>.proposal.md` → discussion output.

If neither exists, proceed to §3.

**If a brainstorm exists, you MUST treat it as the authoritative input.**
Do the following in order before writing anything or asking the user anything:

#### 2a. Map the brainstorm into the spec

Carry each brainstorm section into the spec per this mapping. This is not
optional — every non-empty brainstorm section below has a target in the spec:

| Brainstorm section | Spec target |
|---|---|
| Problem Statement | §1 Motivation — Problem Statement (verbatim, condensed only if needed) |
| Constraints & Requirements | §1 Goals + §5 Acceptance Criteria (every hard constraint becomes a checkable criterion) |
| Recommendation + Recommended Option body | §2 Architectural Design — Overview |
| Feature Description → User-Facing Behavior | §2 Overview |
| Feature Description → Internal Behavior | §2 Component Diagram + Integration Points |
| Feature Description → Edge Cases & Error Handling | §7 Known Risks / Gotchas |
| Capabilities (New + Modified) | §3 Module Breakdown (one module per capability as a starting point) |
| Impact & Integration table | §2 Integration Points |
| Code Context (entire section) | §6 Codebase Contract (re-verify every reference — code may have shifted) |
| Libraries / Tools table | §7 External Dependencies |
| Parallelism Assessment | Worktree Strategy section |
| Open Questions (see 2b) | §8 Open Questions (with resolved/unresolved state preserved) |

Rejected options from the brainstorm are NOT carried into the spec body.
They may be referenced in one line inside §1 Non-Goals if the rejection
excludes something a reader might expect (e.g. *"Runtime fallback-on-failure
was rejected in brainstorm — see proposals/<name>.brainstorm.md Option A."*).

#### 2b. Parse the Open Questions section — resolved vs. unresolved

The brainstorm's Open Questions use this convention:

```
- [ ] Unresolved question — *Owner: name*
- [x] Resolved question — *Owner: name*: <answer text>
```

- A **`[x]`** checkbox means the user has already answered the question.
  The answer is the text after the final `:` on the same line (or the
  indented lines immediately below, if any).
- A **`[ ]`** checkbox means the question is still open.

**Rules for resolved (`[x]`) questions — this is the heart of the fix:**

1. **Do NOT re-ask the user.** Never include a resolved brainstorm question
   in the clarifying-question batch in §3.
2. **Route the answer into the spec body where the decision actually
   applies** — not just into §8. For example:
   - "Default backend when unset → sqlite" → state this in §2 Overview and
     add an acceptance criterion in §5. Do not leave it as an open question.
   - "Binary overflow path declared in `project/conf.py`" → add the config
     key to §6 Configuration References and mention the path in §7
     Patterns to Follow. Do not describe the design as "mingled" or any
     alternative that contradicts the resolved answer.
   - "No TTL in SQL backends" → reflect this in the schema DDL (no
     `expires_at` predicate) and in §7 Known Risks.
3. **Also echo the resolution in §8** as a resolved item so readers can
   audit the decision trail:
   ```
   - [x] <Question restated> — *Resolved in brainstorm*: <answer verbatim>
   ```
   This keeps §8 honest about what was decided and by whom.
4. **If a resolved answer conflicts with your own instinct for the spec**,
   the brainstorm wins. Do not silently override it. If you believe the
   answer is wrong, surface the conflict to the user as a *new* question
   in §3 — do not rewrite the answer.

**Rules for unresolved (`[ ]`) questions:**

- Carry them forward into §8 of the spec as `[ ]` items.
- They are fair game for §3 clarifying questions, but only if they
  genuinely block the spec (ones that can be decided during
  implementation should stay as `[ ]` in §8 and not be asked now).

#### 2c. Show the user the carry-forward summary before asking anything

Before the clarifying-question round in §3, print:

```
Loaded brainstorm: sdd/proposals/<feature-name>.brainstorm.md
  Recommended Option: <X — name>
  Resolved questions carried forward (N): <one-line list>
  Unresolved questions remaining (M): <one-line list>
  Clarifying questions I still need to ask (K): <one-line list or "none">
```

If K is zero, proceed directly to §3b without asking anything.

#### 2d. Sync the Base Branch (FEAT-145, resolver added FEAT-466)

Resolve `(TYPE, BASE_BRANCH)` deterministically via
`scripts.sdd.sdd_meta.resolve_flow()` — explicit `--type`/`--base-branch`
flags win over the brainstorm/proposal frontmatter, which wins over the
default (`feature`/`dev`). Unlike a bare `parse()` call, `resolve_flow()`
does NOT raise `FileNotFoundError` when no exploration doc exists — that is
exactly the dev-loop bug path's normal situation (no brainstorm, no
proposal), and it is why this step no longer calls `parse()` directly:

```bash
META=$(python -c "
from pathlib import Path
from scripts.sdd.sdd_meta import resolve_flow
m = resolve_flow(
    doc_path=Path('<brainstorm-or-proposal-path>') if '<exploration-doc-exists>' else None,
    type_override='<--type value, or empty string if not passed>' or None,
    base_branch_override='<--base-branch value, or empty string if not passed>' or None,
)
print(m.type, m.base_branch)")
TYPE=$(echo "$META" | awk '{print $1}')
BASE_BRANCH=$(echo "$META" | awk '{print $2}')
```

**Validation (hotfix/main):** `resolve_flow()` enforces `type='hotfix' ⇒
base_branch='main'` internally (it returns a `FlowMeta`, whose own
cross-field validator raises `ValueError` — this is NOT re-derived as a
separate bash check). The `python -c` call above therefore exits non-zero
and prints a traceback when a `--type hotfix --base-branch dev`-style
combination (or hotfix frontmatter with a non-`main` base) is resolved. On
that failure, abort with the same message as before:
```
⚠️  type='hotfix' requires base_branch='main' (got base_branch='<value>').
   Fix the brainstorm/proposal frontmatter (or --base-branch flag) and
   re-run /sdd-spec.
```

**Validation (feature/not-main):** `FlowMeta` does NOT reject
`type='feature', base_branch='main'` at the schema layer (that refusal is
intentionally a command-layer guard, FEAT-187) — so this check stays a
manual bash condition after the resolver returns successfully. If
`TYPE == "feature"` and `BASE_BRANCH == "main"`, abort:
```
⚠️  type='feature' cannot base on 'main'. Features land on dev (default)
   or staging (during a release freeze). For changes that must base on
   main, set type='hotfix' in the document frontmatter.
```

Note: `staging` is a valid `base_branch` for `type: feature` during a release freeze
(FEAT-187). Use `base_branch: staging` when applying stabilization fixes to a
frozen release candidate.

**Sync:** before scaffolding, switch to the base branch and pull:
```bash
git checkout "$BASE_BRANCH"
git pull --ff-only origin "$BASE_BRANCH"
```

If the working tree is dirty, abort with:
```
⚠️  Cannot sync <BASE_BRANCH>: working tree has uncommitted changes.
   Stash or commit first, then re-run /sdd-spec.
```

If `--ff-only` fails, abort with:
```
⚠️  Cannot fast-forward <BASE_BRANCH>. Reconcile manually
   (git pull --rebase or merge), then re-run /sdd-spec.
```

Carry `TYPE` and `BASE_BRANCH` forward into the spec's frontmatter at §5.

### 3b. Collaborative Design Research (codex seat — optional, NEVER blocking)

An independent design opinion over the **accepted exploration document**, taken
*before* you draft §2/§6 so it cannot become a ratification of your own design
(FEAT-545). This step is optional: every failure below is recorded as a skip
reason for spec §9 and the command continues. **This step must never abort
`/sdd-spec`** — `sdd-planner` runs this command unattended.

**Preconditions (any false ⇒ skip):**
- §2 found `<exploration-doc>` and its status is `accepted` (brainstorm
  `**Status**: accepted`, or proposal frontmatter `status: accepted`).
- `command -v codex` succeeds.

**Rules (identical to the Adversarial Cross-Check in `.claude/agents/code-reviewer.md`):**
- **Never feed the reviewer your reasoning, draft, or preferred conclusion.**
  The brief carries ONLY the exploration document and verified code anchors.
- **Run it in the background** — a call takes 30 s to 10 min. Do not call it
  per edit.
- **Treat the output as advisory.** Every suggestion gets a disposition:
  `CONFIRM` (fold into §2/§3/§7), `REJECT` (record why), `ESCALATE` (becomes
  a `[ ]` item in §8).
- **Never silently concede and never silently drop** a suggestion.
- **Verify the reviewer's evidence.** Every `affected_paths` entry is checked
  for repository containment, then with `test -e`; a path resolving outside
  the repo ⇒ `REJECT` "path outside repository"; an unverifiable path ⇒
  `REJECT` "path not found".

> **`agy` (Google Gemini / Antigravity) MUST NOT be used for this seat** — same
> ban and same reason as for code review (`CLAUDE.md`, "Adversarial Second
> Opinion"). With no `codex`, skip; do not substitute another external CLI.

#### 3b.1 Detect and probe
```bash
REPO_ROOT="$(pwd)"                                   # /sdd-spec always runs from the repo root (§2d)
MODEL="${SDD_DESIGN_RESEARCH_MODEL:-gpt-5.6-luna}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
DR="sdd/state/.design_research/<feature-name>-${RUN_ID}"   # id-independent, run-scoped staging: FEAT-ID is reserved only in §5
mkdir -p "$DR"; SKIP_REASON=""
if ! command -v codex >/dev/null 2>&1; then SKIP_REASON="codex CLI not installed"; fi
if [ -z "$SKIP_REASON" ]; then
  # stdin MUST be redirected: without a TTY `codex exec` prints "Reading additional
  # input from stdin..." and blocks until the timeout (rc=124) even though the
  # prompt is passed as an argument (observed 2026-09-10, codex-cli 0.153/0.154).
  timeout 120 codex exec --ephemeral --sandbox read-only -m "$MODEL" \
    -c model_reasoning_effort=high --ignore-user-config \
    -o "$DR/probe.txt" "Reply with exactly the single word OK." < /dev/null >/dev/null 2>&1 \
    || SKIP_REASON="model probe failed for $MODEL (rc=$?)"
fi
if [ -z "$SKIP_REASON" ]; then
  PROBE_TEXT="$(cat "$DR/probe.txt" 2>/dev/null | tr -d '[:space:]')"
  [ "$PROBE_TEXT" = "OK" ] || SKIP_REASON="model probe returned unexpected output for $MODEL"
fi
if command -v codex >/dev/null 2>&1; then
  CODEX_VERSION="$(codex --version 2>/dev/null | awk '{print $2}')"
  PROBE_OUTPUT="$(cat "$DR/probe.txt" 2>/dev/null || echo "")"
  python -c "
import json, sys
json.dump({
    'model': sys.argv[1],
    'codex_cli_version': sys.argv[2],
    'reasoning_effort': 'high',
    'timeout_s': 600,
    'probe_output': sys.argv[3],
}, open(sys.argv[4], 'w'), indent=2)
" "$MODEL" "$CODEX_VERSION" "$PROBE_OUTPUT" "$DR/run.json"
fi
```

#### 3b.2 Render the neutral brief (skipped when `SKIP_REASON` is already set)
Write each extracted value below to its own file under `$DR` — `problem_statement.txt`,
`constraints_and_goals.txt`, `recommended_option_or_scope.txt`, `code_context_paths.txt`,
`open_questions.txt`, `question.txt` (plain UTF-8 text, no code fences) — **before** running
the renderer, sourced from:
- `problem_statement.txt` ← brainstorm "## Problem Statement" | proposal "## 1. Synthesis Summary" + §0 Origin quote
- `constraints_and_goals.txt` ← brainstorm "## Constraints & Requirements" | proposal "### 2.2 Constraints Discovered"
- `recommended_option_or_scope.txt` ← brainstorm "## Recommendation" + Recommended Option body | proposal "## 3. Probable Scope" (or "## 3. Hypothesis")
- `code_context_paths.txt` ← the **paths only** (one per line) from brainstorm "## Code Context" | proposal "### 2.1 Localization"
- `open_questions.txt` ← the `[ ]` items of the exploration doc (or "none")
- `question.txt` ← "Given this accepted design intent and these verified code anchors, how would you build it? What is missing, risky, or better done another way?"

```bash
if [ -z "$SKIP_REASON" ]; then
  python - "$DR" <<'PY' || SKIP_REASON="brief rendering failed"
import sys
from pathlib import Path

dr = Path(sys.argv[1])
template = Path("sdd/templates/design_research.prompt.md").read_text(encoding="utf-8")
names = [
    "problem_statement", "constraints_and_goals", "recommended_option_or_scope",
    "code_context_paths", "open_questions", "question",
]
for name in names:
    value = (dr / f"{name}.txt").read_text(encoding="utf-8").strip()
    template = template.replace("{{" + name + "}}", value)
assert "{{" not in template, "unfilled placeholder remains"
(dr / "brief.md").write_text(template, encoding="utf-8")
PY
fi
```
FORBIDDEN in the brief: anything you have written for this spec, your
reasoning, this command's text, or a preferred answer. If in doubt, leave it out.
Note: the template's own header comment intentionally spells placeholder names WITHOUT
`{{ }}` braces, precisely so this whole-document `str.replace()` cannot also rewrite the
comment (verified by TASK-3099's dry run, which caught this exact corruption before the fix).

#### 3b.3 Run codex (capped, synchronous — NOT a background job)
```bash
if [ -z "$SKIP_REASON" ]; then
  STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"
  timeout 600 codex exec --ephemeral --sandbox read-only --cd "$REPO_ROOT" \
    -m "$MODEL" -c model_reasoning_effort=high --ignore-user-config \
    --output-schema sdd/templates/design_research.schema.json \
    -o "$DR/suggestions.json" - < "$DR/brief.md" > "$DR/codex.log" 2>&1
  rc=$?
  [ "$rc" -eq 124 ] && SKIP_REASON="codex timed out after 600s"
  [ "$rc" -ne 0 ] && [ -z "$SKIP_REASON" ] && SKIP_REASON="codex exited $rc (see $DR/codex.log)"
  ENDED_AT="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"
  if [ -f "$DR/run.json" ]; then
    python -c "
import json, sys
d = json.load(open(sys.argv[1]))
d['started_at'] = sys.argv[2]
d['ended_at'] = sys.argv[3]
d['exit_code'] = int(sys.argv[4])
json.dump(d, open(sys.argv[1], 'w'), indent=2)
" "$DR/run.json" "$STARTED_AT" "$ENDED_AT" "$rc"
  fi
fi
```
This call blocks for up to 600s (`timeout 600`, foreground). There is no background/job-control
mechanism here — if you want to do other useful work (e.g. start §4 codebase research) while
waiting, run this step as a separate shell invocation and poll/join it yourself; do not assume
concurrency is provided for you.

#### 3b.4 Validate and triage
```bash
if [ -z "$SKIP_REASON" ]; then
  python -c "
import json, sys, jsonschema
s = json.load(open('sdd/templates/design_research.schema.json'))
d = json.load(open('$DR/suggestions.json'))
jsonschema.Draft202012Validator(s).validate(d)
print(len(d['suggestions']), 'suggestions')" || SKIP_REASON="suggestions.json failed schema validation"
fi
```
For each suggestion (when not skipped), for every `affected_paths` entry:
1. **Containment check first**: resolve the path against `$REPO_ROOT` and confirm it
   stays inside it —
   ```bash
   python -c "
import os, sys
p = os.path.realpath(sys.argv[1])
root = os.path.realpath('$REPO_ROOT')
sys.exit(0 if p == root or p.startswith(root + os.sep) else 1)" "<path>" \
     || REASON="REJECT — path outside repository: <path>"
   ```
   A path that fails containment is `REJECT — path outside repository: <path>` and is
   NOT passed to `test -e` at all.
2. **Existence check** (only for paths that passed containment): `test -e <path>` —
   unverifiable ⇒ `REJECT — path not found: <path>`.
3. Read the cited spots for paths that pass both checks; decide **CONFIRM / REJECT /
   ESCALATE** with a one-sentence reason; write `$DR/triage.md` using the §9 table
   shape from `sdd/templates/spec.md`.

#### 3b.5 Fold and record
- `CONFIRM` → apply while drafting §2 Overview / §3 modules / §7 notes; the
  §9 row's "Landed in" names the section.
- `ESCALATE` → add a `[ ]` question to §8 (owner: the user); "Landed in" = `§8 Q<N>`.
- `REJECT` → row only.
- Fill spec **§9 Design Research Cross-Check** from `$DR/triage.md`, with
  `Model: <MODEL>` and `Status: completed` — or, when skipped, a single
  line `Status: skipped (<SKIP_REASON>)` and an empty table.
- §6 moves `$DR` to `sdd/state/<FEAT-ID>/design_research/` and commits it
  with the spec.

### 3. Ask Clarifying Questions (only what is genuinely missing)

After §2c, you may ask the user **only** for gaps the brainstorm/proposal did
not cover. Typical legitimate gaps:

- Spec-level fields that don't exist in a brainstorm (Target version, Author
  attribution if unclear, Status lifecycle preference).
- New `[ ]` open questions that genuinely block the design (not ones that
  can be deferred to implementation).
- Ambiguities discovered during codebase research in §4 (e.g., two plausible
  integration points — which one to use).

**Forbidden in this step:**
- Re-asking anything that already appeared in the brainstorm's Open Questions
  as `[x]` resolved.
- Re-asking Problem Statement / Constraints / Recommended Option — the
  brainstorm is authoritative on those.
- Asking the user to restate the feature goals in their own words when the
  brainstorm already states them.

Ask in a single batch so the user answers once and you proceed. If there is
nothing to ask, skip this step silently.

### 4. Research the Codebase & Build Codebase Contract
Before writing the spec:
- Read existing specs in `sdd/specs/` directory.
- Identify related existing components (AbstractClient, AgentCrew, BaseLoader, etc.).
- Note what can be reused vs. what must be created.

**CRITICAL — Codebase Contract Construction:**
This step prevents AI hallucinations during implementation. You MUST:

1. **If a brainstorm exists**: carry forward its entire `## Code Context` section
   into the spec's `## 6. Codebase Contract` section. Re-verify each reference
   is still accurate (code may have changed since brainstorm).
2. **For every class/module referenced in the spec**: `read` the actual source file
   and record exact class signatures, method signatures (with parameter types and
   return types), and key attributes — with file paths and line numbers.
3. **Verify all imports**: confirm that all referenced imports resolve by
   checking `__init__.py` exports and module structure. Do not assume.
4. **Record what does NOT exist**: if you searched for a plausible module, class,
   or method and it does not exist, add it to the "Does NOT Exist" subsection.
   This is the most effective anti-hallucination measure — it explicitly tells
   implementing agents what NOT to reference.
5. **Include user-provided code**: if the user or brainstorm provided code snippets,
   preserve them as verified references in the contract.
6. **Interface Skeletons (FEAT-545)**: for every §3 module write the public
   signatures and docstrings of what the module adds or changes — no bodies —
   each line that touches existing code carrying `# verified: path:NN`. These
   skeletons are what `/sdd-task` turns into per-task Implementation Blueprints,
   so a name fixed here is not renegotiable later.

#### Identify delegation-eligible modules

While writing §3 Module Breakdown, fill the "Delegation-eligible modules"
sub-table: for each module state whether its design is complete enough that
implementing it is mechanical, and record the decided patterns and exact
contracts (signatures, error codes, file layout). Architecture decisions
stay with the thinking model — eligibility never delegates a design choice.

### 5. Scaffold the Spec
1. Read the template at `sdd/templates/spec.md`. The template already contains
   a YAML frontmatter block at the top (FEAT-145).
2. Create `sdd/specs/<feature-name>.spec.md` filled in with:
   - **Frontmatter** at the very top: set `type` and `base_branch` to the
     values resolved in §2d. Do NOT strip the frontmatter. The block must
     match this shape exactly:
     ```yaml
     ---
     type: feature        # or: hotfix
     base_branch: dev     # or: main (mandatory for hotfix)
     # reuse_feature_id: FEAT-<NNN>   # OPTIONAL — only for an intentional
     #   multi-spec split of one initiative (see Guardrails); when present,
     #   skip the reserve_ids.py call below and use this ID verbatim.
     ---
     ```
   - **Feature ID — SKIP ENTIRELY when `TYPE == "hotfix"` (FEAT-466).** A
     bugfix is not a feature and reserves no `FEAT-<NNN>`: ledger ids exist
     for features and the brainstorm → spec → task SDD flow, and a hotfix is
     identified by its **Jira issue key** instead. Do NOT call
     `reserve_ids.py --kind feature` on the hotfix path — write no
     `FEAT-<NNN>` anywhere in the spec, and use the Jira key as the
     document's identity line (`**Jira**: <KEY>` in place of `**Feature
     ID**: FEAT-<NNN>`). This is not an oversight to fix later: it is the
     whole point — the allocator's "current branch must equal
     `--base-branch`" precondition (`reserve_ids.py`, `_assert_safe_to_reserve`)
     would otherwise be
     unreachable for a hotfix that has to reserve on `main` while another
     worktree already has `main` checked out. Skipping reservation removes
     the problem rather than solving it.

     For `TYPE == "feature"`, **first check whether this slug already owns
     an ID**. A slug has ONE feature ID for its lifetime: re-running
     `/sdd-spec` over an existing spec must REUSE that number, never
     reserve a second one. A second reservation forks the feature's
     identity — the spec and the task index move to the new number while
     the task files, the branch and the worktree keep the old one, and
     `DevelopmentNode._find_feature_slug` (which matches strictly on the
     index header's `feature_id` **inside the worktree**) then finds no
     index at all and silently degrades a whole multi-agent pool to a
     single agent. This is not hypothetical: it happened on 2026-09-01 to
     `formfield-content-type` (FEAT-488 → FEAT-489), and cost a full run.

     ```bash
     EXISTING=$(python -c "
     from pathlib import Path
     from scripts.sdd.reserve_ids import existing_feature_id
     found = existing_feature_id(Path('.'), '<feature-name>')
     print(found[0] if found else '')")
     ```

     If `$EXISTING` is non-empty, **use it verbatim as this spec's Feature
     ID and skip the reservation entirely** — say so in the §7 output
     ("reusing FEAT-<NNN>, already owned by this slug"). Only when it is
     empty do you reserve:
     ```bash
     FEAT_ID=$(python -m scripts.sdd.reserve_ids --kind feature --count 1 \
       --base-branch "$BASE_BRANCH" --label <feature-name>)
     ```
     `reserve_ids.py` enforces the same rule itself and exits non-zero
     naming the owned ID, so forgetting this check is a hard failure, not
     a silently burned number.
     On success this prints exactly one `FEAT-<NNN>` line; use it verbatim
     as this spec's Feature ID. `reserve_ids.py` pushes its own ledger-only
     commit to `origin/$BASE_BRANCH` as part of this call (retrying
     internally on a non-fast-forward rejection) and refuses to run if
     tracked files have uncommitted changes besides the ledger file. It
     builds that commit on `origin/$BASE_BRANCH` with git plumbing, so it
     never publishes or discards local-only commits on your branch. If the command exits non-zero, **STOP** and report the error —
     do NOT fall back to hand-computing a number.

     **Escape hatch — intentional FEAT-ID reuse**: if this spec is a
     deliberate split of an existing initiative across multiple specs (the
     FEAT-380-style pattern — `sandbox-hardening.spec.md`,
     `shelltool-hardening.spec.md`, and `tool-result-compression.spec.md`
     all intentionally share `FEAT-380`), do NOT call `reserve_ids.py`.
     Instead, set `reuse_feature_id: FEAT-<NNN>` in the frontmatter above,
     state the reused ID explicitly, and skip the reservation call
     entirely — this documents the intentional reuse for auditability
     rather than silently reusing a stale number.
   - Today's date.
   - Answers from user (or prior exploration documents).
   - Architectural patterns from your codebase research.
3. When populating §8 Open Questions, apply the resolved/unresolved
   partition from §2b. Resolved items use `[x]` and the carried-forward
   answer; unresolved items use `[ ]`.
4. Before finishing, sanity-check the spec against the brainstorm:
   for every `[x]` resolved question in the brainstorm, search the spec
   body for a passage that reflects the resolution. If you cannot find
   one, you have failed to carry the decision forward — fix the spec
   before committing.

**Worktree hint (new section in spec):**
Include a `## Worktree Strategy` section in the spec with:
- Default isolation unit: `per-spec` or `per-task`.
- If `per-spec`: all tasks run sequentially in one worktree.
- If mixed: list which tasks are parallelizable and why.
- Cross-feature dependencies: list any specs that must be merged first.

### 6. Commit the Spec

> **CRITICAL — Worktrees branch from the current state of the repo.**
> If the spec is not committed, any worktree created later will NOT see it,
> and the `sdd-worker` agent will fail with "no spec found".

> **CRITICAL — Only commit the spec file. NEVER commit unrelated changes.**
> Other files may be modified or unstaged in the working directory — do NOT
> touch them. Follow the exact sequence below.

```bash
# 1. Unstage everything first to ensure a clean staging area
git reset HEAD

# 2. Stage ONLY the spec file (+ the design-research transcript when §3b ran) — NEVER "git add ." / "-A"
git add sdd/specs/<feature-name>.spec.md
if [ -d "$DR" ]; then
  STAGE_TMP="sdd/state/.design_research/.promote-<FEAT-ID>-${RUN_ID}"
  PROMOTED="sdd/state/<FEAT-ID>/design_research"
  if [ -e "$PROMOTED" ]; then
    echo "⚠️  $PROMOTED already exists — leaving $DR in place for manual review (run-id ${RUN_ID}); not overwriting existing design research."
  else
    mkdir -p "sdd/state/<FEAT-ID>" "$STAGE_TMP" && cp -a "$DR"/. "$STAGE_TMP"/ \
      && mv "$STAGE_TMP" "$PROMOTED" \
      && rm -rf "$DR" \
      && git add "$PROMOTED/" \
      || { echo "⚠️  Promotion of $DR failed — left in place for inspection (run-id ${RUN_ID})." ; rm -rf "$STAGE_TMP"; }
  fi
fi

# 3. Verify ONLY those paths are staged
git diff --cached --name-only
# Expected: sdd/specs/<feature-name>.spec.md [+ sdd/state/<FEAT-ID>/design_research/*]
# If ANY other files appear, run "git reset HEAD" and start over

# 4. Commit
git commit -m "sdd: add spec for FEAT-<ID> — <feature-name>"
```

### 7. Output

**`TYPE == "feature"`:**
```
✅ Spec created and committed: sdd/specs/<feature-name>.spec.md

   Feature ID: FEAT-<ID>
   Isolation: per-spec (sequential tasks) | mixed (some parallel tasks)
   Design research: <N> suggestions — <C> confirmed / <R> rejected / <E> escalated   (model <MODEL>)
   # or:  Design research: skipped (<SKIP_REASON>)

   To create a worktree for this feature after task decomposition:
     git worktree add -b feat-<FEAT-ID>-<feature-name> \
       .claude/worktrees/feat-<FEAT-ID>-<feature-name> HEAD

Next:
  1. Review the spec — check Acceptance Criteria and Architectural Design.
  2. Mark status: approved when ready.
  3. Run /sdd-task sdd/specs/<feature-name>.spec.md
```

**`TYPE == "hotfix"` (FEAT-466 — no id reserved):**
```
✅ Spec created and committed: sdd/specs/<feature-name>.spec.md

   Identity: Jira <KEY> (no FEAT-<NNN> reserved — a bugfix is not a feature)
   Base branch: main

   To create the worktree (origin/main, never HEAD/dev):
     git worktree add -b hotfix-<KEY>-<feature-name> \
       .claude/worktrees/hotfix-<KEY>-<feature-name> origin/main

Next:
  1. Review the spec — check Acceptance Criteria and Architectural Design.
  2. Mark status: approved when ready.
  3. Normally: skip /sdd-task and dispatch straight to development (the
     single-agent path handles a one-or-two-commit hotfix directly from
     the spec). Only run /sdd-task if this hotfix genuinely needs
     decomposition — it will not reserve TASK-<NNN> ids either.
```

## Reference
- Template: `sdd/templates/spec.md`
- Existing specs: `sdd/specs/`
- SDD methodology: `sdd/WORKFLOW.md`
- Worktree policy: `CLAUDE.md` (section "Worktree Policy")
- Design-research brief: `sdd/templates/design_research.prompt.md` (FEAT-545)
- Design-research schema: `sdd/templates/design_research.schema.json` (FEAT-545)

## Anti-Hallucination Policy

The `## 6. Codebase Contract` section in the spec is **mandatory** for any spec
that references existing codebase components. A spec without a codebase contract
will produce tasks that hallucinate imports and attributes.

**Quality bar**: Every entry in the contract must include a file path and line number.
Entries without verification evidence must be marked as `(unverified — check before use)`.