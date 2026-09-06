---
description: Research-first proposal generator. Takes a source (Jira ticket, inline text, or file), investigates the codebase, synthesizes findings with confidence-graded reasoning, and produces a grounded proposal document that feeds into /sdd-spec or /sdd-brainstorm.
---

# /sdd-proposal — Research-First Feature Proposal

Take a thin source — typically a sparse Jira ticket like *"Nextstop module no genera el PDF"* —
and produce a proposal document that is **grounded in the codebase before any human Q&A**.

The pipeline inverts `/sdd-brainstorm`: instead of asking the user to fill the gaps,
the agent investigates the repo first, builds a confidence-graded synthesis, and only
then asks targeted questions about genuine unknowns.

```
/sdd-proposal <source>
  → Phase 0: source resolution
  → Phase 1: research plan generation (gate)
  → Phase 2: agentic codebase research (budgeted)
  → Phase 3: synthesis (chain-of-thought, evidence-grounded)
  → Phase 4: review gate (human validates synthesis)
  → Phase 5: targeted Q&A (only for material unknowns)
  → Phase 6: enriched proposal rendering
  → Phase 7: commit + next-command recommendation
```

## Usage

```
/sdd-proposal NAV-8421                          # Jira key
/sdd-proposal "Nextstop no genera el PDF"       # inline text
/sdd-proposal path/to/notes.md                  # file path
/sdd-proposal NAV-8421 --mode=investigation     # force mode
/sdd-proposal NAV-8421 --mode=enrichment
/sdd-proposal NAV-8421 --no-gate                # skip user gates (autopilot mode)
/sdd-proposal --resume FEAT-156                 # resume interrupted run
/sdd-proposal NAV-8421 --budget=loose           # tight | default | loose
```

## Guardrails

- **Wiki-first, then grep, then ask.** The codebase knowledge graph
  (``wikitoolkit``) is the fastest orientation source. Query it before
  grep/glob/read. The codebase itself is the primary evidence source.
  Never ask the user something the repo or wiki can answer.
- **No fabricated paths or symbols.** Every code reference in the proposal must
  trace to a finding ID. Validated by the synthesis linter (Step 3.5).
- **Honest confidence.** If research is thin, the proposal must say so — not
  inflate certainty. `overall_confidence` is bounded by the budget consumed.
- **Tri-mode source resolution** mirrors `/sdd-fromjira` (Jira / inline / file).
- **Always commit the proposal file** so worktrees can see it.
- **Do NOT generate implementation code.** Output is a proposal, not a spec.
- **State persists** under `sdd/state/<FEAT-ID>/` for resumability and audit.

## Steps

### 0. Save Repo Path
```bash
REPO_ROOT=$(pwd)
```
You must be on `dev`, `staging`, or the integration branch when starting. The
`base_branch` for the proposal defaults to `dev` for `type: feature` flows and to
`main` for `type: hotfix` flows. `staging` is valid as a `base_branch` for
`type: feature` during a release freeze (FEAT-187).

**Validation:** if `TYPE == "feature"` and `BASE_BRANCH == "main"`, abort:
```
⚠️  type='feature' cannot base on 'main'. Features land on dev (default)
   or staging (during a release freeze). For changes that must base on
   main, set type='hotfix' in the document frontmatter.
```

### 1. Parse Input & Allocate FEAT-ID

Extract from the user's invocation:
- **source**: Jira key (regex `^[A-Z]+-\d+$`), inline string, or path to file.
- **flags**: `--mode`, `--no-gate`, `--resume`, `--budget`.

If `--resume <FEAT-ID>` is set, jump to **Step R** at the bottom.

Otherwise, allocate a new FEAT-ID:
- Read existing IDs in `sdd/specs/`, `sdd/proposals/`, `sdd/state/`.
- Take `max(existing) + 1`. Start at `FEAT-001` if none exist.

Create state directory:
```bash
FEAT_ID="FEAT-XXX"   # allocated above
STATE_DIR="sdd/state/${FEAT_ID}"
mkdir -p "${STATE_DIR}/findings"
```

Initialize `${STATE_DIR}/state.json` per the schema in
`sdd/templates/state.schema.json`. Set:
- `phase: "source_resolved"` (will update at end of step)
- `started_at`: now
- `mode`: from `--mode` flag, or `"auto"` if not specified
- `budget`: per `--budget` profile (see table below)

| `--budget`  | files_read | grep_calls | git_calls | depth | wall_seconds |
|-------------|-----------:|-----------:|----------:|------:|-------------:|
| `tight`     |         15 |         10 |         5 |     1 |          120 |
| `default`   |         40 |         25 |        10 |     2 |          300 |
| `loose`     |        100 |         60 |        20 |     3 |          900 |

### 2. Phase 0 — Resolve Source

#### 2a. Jira source

Use the same access strategy as `/sdd-fromjira`:
- **MCP path**: `jira_get_issue(issue_key=<KEY>)` if available.
- **curl fallback**: load `JIRA_INSTANCE`, `JIRA_USERNAME`, `JIRA_API_TOKEN`
  via navconfig (see `/sdd-fromjira` for the eval snippet).

Capture: summary, description, comments, components, labels, status, priority,
acceptance criteria field, linked issues.

#### 2b. Inline source

Take the entire string as the body. No fetch needed.

#### 2c. File source

Read the file. If it has frontmatter, extract metadata; otherwise treat the
whole file as body text.

#### 2d. Persist source

Write the raw source to `${STATE_DIR}/source.md` with a small frontmatter:
```yaml
---
kind: jira | inline | file
jira_key: NAV-XXXX | null
fetched_at: <ISO timestamp>
summary_oneline: <≤120 chars one-line summary>
---
```

Update `state.json`:
- `source.*` fields populated
- `phase: "source_resolved"`

### 3. Phase 1 — Generate Research Plan

#### 3a. Check wiki availability

Before generating the plan, probe whether the knowledge graph is built:
```bash
wikitoolkit status 2>&1
```
If the output contains "Wiki not built", set `wiki_available: false` in
`state.json` and inform the planner so it omits `wiki_*` query types.
Otherwise set `wiki_available: true`.

#### 3b. Run the planner prompt

Invoke the prompt at `sdd/templates/research_plan.prompt.md`, passing:
- contents of `${STATE_DIR}/source.md`
- the budget block
- `wiki_available` flag (so the planner knows whether to emit wiki queries)
- repository top-level structure (`ls -la` of repo root, plus `find . -type d -maxdepth 3`)

The planner produces a JSON document conforming to
`sdd/templates/research_plan.schema.json` — a list of `queries` with
intent, type (`grep | glob | read | git_log | tree`), parameters, and
priority.

Persist to `${STATE_DIR}/research_plan.json`.

#### 3b. Present plan to user (gate)

Unless `--no-gate` is set, print:

```
🔍 Research Plan for <FEAT-ID> — <one-line summary>

Mode (detected): investigation | enrichment

Queries (N=<count>):
  Q001  [grep]      "nextstop"                    → locate module
  Q002  [grep]      "generate_pdf|render_pdf"     → find PDF entrypoints
  Q003  [glob]      "**/nextstop/**"              → list module files
  Q004  [git_log]   app/modules/nextstop/         → recent activity
  Q005  [read]      tests/nextstop/test_pdf.py    → existing test coverage
  ...

Budget: <profile> (max 40 files, 25 greps, 10 git, depth 2, 300s)

Proceed? [y / edit / abort]
```

- `y`: continue to Phase 2.
- `edit`: open the plan in `$EDITOR`, re-validate against schema, then continue.
- `abort`: mark `state.json` as `phase: "failed"` with `errors[].message: "user abort"`.

Update `state.json`:
- `phases.plan.approved_by_user: true`
- `phase: "plan_approved"`

### 4. Phase 2 — Execute Research

#### 4a. Iteration loop

For each query in the approved plan, in priority order:

1. **Budget check.** Compare `consumed` vs `budget`. If any limit will be
   exceeded by this query, skip it and record `queries_skipped++`.
   **Wiki queries (`wiki_query`, `wiki_page`, `wiki_related`) are free —
   always execute them regardless of budget.**
2. **Execute** the query using the appropriate tool:
   - `wiki_query` → `wikitoolkit query "<question>"` — returns ranked
     page stubs with IDs, scores, and summaries. Record page IDs for
     follow-up `wiki_page`/`wiki_related` queries.
   - `wiki_page` → `wikitoolkit page <id>` — returns full page content
     (file summary, API outline, content). Use the page ID returned by
     a prior `wiki_query`.
   - `wiki_related` → `wikitoolkit related <id>` — returns typed edges
     (`contains`, `references`) to neighbouring files/modules. Use to
     discover integration points and dependencies.
   - `grep` / `glob` → use the agent's search tools
   - `read` → read the target file (or specified line range)
   - `git_log` → `git log --follow --since=<window> -- <path>`
   - `tree` → `find <path> -type f -maxdepth N`
3. **Persist a digest** at `${STATE_DIR}/findings/F<NNN>-<slug>.md` using the
   format in `sdd/templates/finding.md`. The digest is **compact** — never
   inline full file content; cite line ranges and excerpts only. For wiki
   findings, cite the wiki page ID and score alongside file paths.
4. **Decide on recursion.** If a finding surfaces a new file/symbol that
   strongly warrants exploration AND `depth_reached < max_depth`, add a
   follow-up query to the plan with `parent_id: F<NNN>` and re-enter the loop.
5. **Update `state.json`** after each query: `consumed.*`, `phases.research.*`.

#### 4b. Stop conditions

- All planned queries executed.
- Budget exhausted (set `truncated: true` and `truncation_reason`).
- Hard timeout reached (`max_wall_seconds`).

Update `state.json`:
- `phase: "research_complete"`
- `phases.research.completed_at`, `duration_seconds`, `files_read`, etc.

### 5. Phase 3 — Synthesis

#### 5a. Build the synthesis input

Concatenate, in order:
- `${STATE_DIR}/source.md` content
- `${STATE_DIR}/research_plan.json` content
- All `${STATE_DIR}/findings/*.md` digests, wrapped in `<finding id="F00N">` tags
- A `<budget_status>` block with `consumed`, `truncated`, `truncation_reason`
- A `<mode>` block with the resolved mode (forced or auto-detected)

#### 5b. Run the synthesis prompt

Invoke `sdd/templates/synthesis.prompt.md`. The agent produces a JSON object
conforming to the synthesis output schema embedded in that prompt.

Persist the **JSON only** (not the `<thinking>` block) to
`${STATE_DIR}/synthesis.json`. If you want auditability, also persist the
thinking to `${STATE_DIR}/synthesis.thinking.log` — this is optional but
recommended.

#### 5c. Lint the synthesis

Validate `synthesis.json` against the rules:

1. Every `path` and `symbol` mentioned in `localization` and `constraints`
   appears in at least one finding's `citations`.
2. Every `evidence` array contains finding IDs that exist.
3. `overall_confidence` ≤ minimum of (median claim confidence, localization
   confidence, hypothesis-1 / scope confidence).
4. If `truncated: true`, `overall_confidence` ≤ `medium` and
   `recommended_next_command` ≠ `sdd-task`.
5. `unknowns.length ≤ 5`.

If any rule fails, **re-prompt the synthesis once** with the violations
appended as a corrective addendum. If it still fails, surface the error to
the user and stop with `phase: "failed"`.

Update `state.json`:
- `phase: "synthesis_complete"`
- `phases.synthesis.*` populated from the JSON

### 6. Phase 4 — Review Gate

Print a human-readable summary of the synthesis:

```
🧠 Synthesis Summary — <FEAT-ID>

Mode: investigation | enrichment
Overall confidence: high | medium | low

Localization (N=<count>):
  app/modules/nextstop/pdf.py::generate_pdf  (lines 47-89) — entrypoint  [F001, F003]
  app/modules/nextstop/pdf.py::render_chunk  (lines 142-160) — chunk renderer  [F003]
  ...

Top hypothesis:
  → "<hypothesis 1 statement>"
    Confidence: medium
    Supporting: F002, F005
    Reasoning: <one sentence>

Confidence map:
  ✓ <claim> [high]   ← directly cited
  ✓ <claim> [high]
  ◐ <claim> [medium] ← inferred
  ◐ <claim> [medium]
  ◌ <claim> [low]    ← weakly supported

Unknowns the codebase couldn't answer (<count>):
  U1: <question>
  U2: <question>

Budget: <consumed>/<budget> (<truncated|complete>)

Validate? [y / refine-research / refine-synthesis / abort]
```

- `y`: continue.
- `refine-research`: jump back to Phase 1 with prior plan as seed; user can
  add queries.
- `refine-synthesis`: re-run synthesis prompt with user's free-form notes
  appended as `<user_correction>...</user_correction>`.
- `abort`: mark failed.

Update `state.json`: `phase: "review_gate"` (and `qa_pending` after).

### 7. Phase 5 — Targeted Q&A (Conditional)

Only run if `synthesis.unknowns.length > 0` AND `--no-gate` is not set.

For each unknown, ask **one** question:

```
❓ U<N>: <question text>

Context: this would resolve claim(s) <Cx, Cy>.

Plausible answers (pick one or write your own):
  a) <answer-1>
  b) <answer-2>
  c) <answer-3>
  d) (other — type your answer)
```

Wait for user response. Persist answers into `synthesis.json` under
`unknowns[i].user_answer`. Promote resolved unknowns into the proposal's
"Resolved Questions" section in Phase 6.

Update `state.json`:
- `phases.qa.questions_total`, `questions_answered`
- `phase: "qa_complete"`

If `--no-gate` is set OR there are zero unknowns, set `phases.qa.status: "skipped"` with reason.

### 8. Phase 6 — Render the Enriched Proposal

#### 8a. Generate slug

Build a kebab-case slug from the source summary. For Jira sources, prepend
the key: `<jira-key>-<slug>`. Examples:
- `nav-8421-nextstop-pdf-generation-bug`
- `redis-token-cache-invalidation`

#### 8b. Render via template

Read `sdd/templates/proposal.md` and fill it from:
- `state.json` → frontmatter (id, title, type, mode, status, source, base_branch, confidence)
- `source.md` → §0 Origin
- `synthesis.json` → §1 Synthesis Summary, §2 Codebase Findings, §3 Hypothesis/Scope, §4 Confidence Map
- `synthesis.json.unknowns` (with user answers) → §5 Open Questions
- `synthesis.json.recommended_next_command` → §6 Recommended Next Step
- `state.json` paths → §7 Research Audit

Write to `sdd/proposals/<slug>.proposal.md`.

Set frontmatter:
- `status: discussion` if any unknowns remain unresolved
- `status: review` if all unknowns resolved but user hasn't accepted
- `status: accepted` only if the user explicitly says "accept" at the final summary

Update `state.json`:
- `phases.output.proposal_path: "sdd/proposals/<slug>.proposal.md"`
- `phase: "proposal_drafted"`

### 9. Phase 7 — Commit & Recommend Next

#### 9a. Commit the proposal

```bash
# Unstage everything first to ensure a clean staging area
git reset HEAD
# Stage ONLY proposal + state files — NEVER use "git add ." or "git add -A"
git add sdd/proposals/<slug>.proposal.md sdd/state/<FEAT-ID>/
# Verify staging
git diff --cached --name-only
# If ANY unrelated files appear, run "git reset HEAD" and start over
git commit -m "sdd: research-grounded proposal for <FEAT-ID> — <one-line>"
```

Update `state.json`:
- `phases.output.commit_sha`: from `git rev-parse HEAD`
- `phase: "committed"`

#### 9b. Print the next-step recommendation

Use `synthesis.recommended_next_command` to render the suggestion:

```
✅ Proposal saved and committed: sdd/proposals/<slug>.proposal.md

   FEAT-ID:    FEAT-XXX
   Mode:       investigation | enrichment
   Confidence: medium
   Audit:      sdd/state/FEAT-XXX/

Recommended next step:
  → /sdd-spec FEAT-XXX
    Rationale: <synthesis.recommended_next_command.rationale>

Alternatives:
  → /sdd-brainstorm FEAT-XXX  (if you want to explore alternative architectures)
  → /sdd-task FEAT-XXX        (if the fix is trivial — single file)
  → manual review             (research was truncated; review state.json)
```

### Step R — Resume an Interrupted Run

If `--resume <FEAT-ID>` is set:

1. Read `sdd/state/<FEAT-ID>/state.json`.
2. Inspect `phase` and `resume_hint.next_phase`.
3. Re-enter the corresponding step:
   - `source_resolved` → Step 3 (plan generation)
   - `plan_approved` → Step 4 (research execution)
   - `research_complete` → Step 5 (synthesis)
   - `synthesis_complete` → Step 6 (review gate)
   - `qa_complete` → Step 8 (render)
   - `proposal_drafted` → Step 9 (commit)
4. If a phase is in `running` status (orphaned from a crash), restart **that
   phase** from scratch (don't try to recover partial state for it).

Print:
```
↻ Resuming FEAT-XXX from phase: <phase>
  Last activity: <updated_at>
  Next action: <resume_hint.next_action>
```

## Output

```
✅ Proposal saved and committed: sdd/proposals/<slug>.proposal.md

   FEAT-ID:           FEAT-XXX
   Mode:              investigation
   Overall confidence: medium
   Findings:           11 files read, 9 grep, 3 git
   Synthesis:          4 high-confidence claims, 3 medium, 1 low
   Unknowns resolved:  2 / 2

Next steps:
  → /sdd-spec FEAT-XXX     (recommended — high-confidence localization)
  → /sdd-brainstorm FEAT-XXX  (alternative — explore architectural options)
```

## How `/sdd-spec` and `/sdd-brainstorm` Consume This Document

The enriched proposal is designed as a drop-in replacement for either a
brainstorm or a free-form proposal in the existing pipeline. Mappings:

| Proposal section              | → `/sdd-spec` consumes as              |
|-------------------------------|----------------------------------------|
| §0 Origin                     | Spec Metadata (Jira key, source)       |
| §1 Synthesis Summary          | Spec §1 Motivation                     |
| §2.1 Localization             | Spec §6 Codebase Contract (verified)   |
| §2.2 Constraints              | Spec §5 Acceptance Criteria + §7 Patterns |
| §2.3 Recent History           | Spec §6 Codebase Contract (Recent Activity) |
| §3 Hypothesis / Scope         | Spec §2 Architectural Design           |
| §4 Confidence Map             | Spec §8 Open Questions (low-confidence items) |
| §5 Open Questions (resolved)  | Spec §8 Open Questions (`[x]`)         |
| §5 Open Questions (unresolved)| Spec §8 Open Questions (`[ ]`)         |

`/sdd-spec` reads the proposal's frontmatter `research_state` to access the
full state for re-verification of the codebase contract.

## Differences from `/sdd-brainstorm` and `/sdd-fromjira`

| Aspect                  | `/sdd-brainstorm`        | `/sdd-fromjira`            | `/sdd-proposal` (this)         |
|-------------------------|--------------------------|----------------------------|--------------------------------|
| Order                   | Q&A → research           | Jira fetch → Q&A → research| Research → synthesis → Q&A     |
| When to use             | Greenfield features      | Jira-seeded brainstorm     | Sparse tickets / bugs          |
| Output                  | Brainstorm w/ 3+ options | Brainstorm w/ Jira context | Single-hypothesis proposal     |
| Hallucination defense   | Code Context section     | Code Context section       | Lint + finding ID grounding    |
| Confidence reporting    | Implicit                 | Implicit                   | Explicit confidence map        |
| Resumability            | None                     | None                       | Full state.json checkpoints    |
| Budget control          | None                     | None                       | Hard limits per profile        |
| Routes to               | `/sdd-spec`              | `/sdd-spec`                | `/sdd-spec` OR `/sdd-brainstorm` (decided by confidence) |

**When to pick which**:
- **`/sdd-proposal`** — the source is a thin Jira ticket, a bug report, or any
  request where the codebase has more context than the requester provided.
  This is the new default entry point for tickets.
- **`/sdd-brainstorm`** — truly greenfield features with no existing code to
  investigate (e.g., "add a billing module from scratch").
- **`/sdd-fromjira`** — kept for backward compatibility; superseded by
  `/sdd-proposal NAV-XXXX` for most uses.

## Reference

- Proposal template:        `sdd/templates/proposal.md`
- State schema:             `sdd/templates/state.schema.json`
- Synthesis prompt:         `sdd/templates/synthesis.prompt.md`
- Research-plan prompt:     `sdd/templates/research_plan.prompt.md`
- Research-plan schema:     `sdd/templates/research_plan.schema.json`
- Finding digest template:  `sdd/templates/finding.md`
- SDD methodology:          `sdd/WORKFLOW.md`
- Worktree policy:          `CLAUDE.md` (section "Worktree Policy")
