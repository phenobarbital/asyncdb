---
id: FEAT-XXX
title: <one-line title — replace with synthesis.summary_oneline>
slug: <kebab-case-slug>
type: feature | hotfix | bug-investigation
mode: investigation | enrichment
status: discussion | review | accepted
source:
  kind: jira | inline | file
  jira_key: NAV-XXXX | null
  jira_url: https://trocglobal.atlassian.net/browse/NAV-XXXX | null
  fetched_at: YYYY-MM-DD
  summary_oneline: <≤120 chars>
overall_confidence: high | medium | low
base_branch: dev
research_state: sdd/state/FEAT-XXX/
created: YYYY-MM-DD
updated: YYYY-MM-DD
---

# FEAT-XXX — <title>

> **Mode**: investigation | enrichment
> **Confidence**: high | medium | low
> **Source**: [NAV-XXXX](url) — *or* `inline` — *or* `file: path/to/notes.md`
> **Audit**: [`sdd/state/FEAT-XXX/`](../state/FEAT-XXX/)

---

## 0. Origin

The original request, preserved verbatim. The full source (with Jira metadata,
comments, etc.) is at `sdd/state/FEAT-XXX/source.md`.

> <verbatim quote of source — for Jira, use the description; for inline, the
> input string; for file, an excerpt with elision marks if long>

**Initial signals** (extracted, not interpreted):
- Verbs: <e.g. "no genera" → suggests bug>
- Named entities: <e.g. "Nextstop", "PDF">
- Components / labels: <from Jira metadata>
- Acceptance criteria provided: <yes / no / count>

---

## 1. Synthesis Summary

<One paragraph, ≤6 sentences. States the request in our own words, names the
likely affected area, and previews the recommendation. Every named path or
symbol here MUST also appear in §2.1 (with finding citations).>

---

## 2. Codebase Findings

> All entries in this section are grounded in the research findings persisted
> at `sdd/state/FEAT-XXX/findings/`. Each cites the finding ID(s) that justify
> its inclusion. **No fabricated paths or symbols.**

### 2.1 Localization

The code areas relevant to this request:

| # | Path | Symbol | Lines | Role | Evidence |
|---|------|--------|-------|------|----------|
| 1 | `app/modules/nextstop/pdf.py` | `generate_pdf` | 47-89 | entrypoint orchestrating PDF rendering | F001, F003 |
| 2 | `app/modules/nextstop/pdf.py` | `render_chunk` | 142-160 | per-section render helper | F003 |
| 3 | `libs/pdf_renderer.py`        | `render`       | 12-58 | shared sync renderer (used elsewhere) | F004 |

> If localization is empty or thin, that itself is a finding — record it
> in §4 Confidence Map as a low-confidence claim.

### 2.2 Constraints Discovered

Conventions, contracts, or downstream callers any solution must respect.

- **Shared sync renderer.** `libs/pdf_renderer.render` is called from at least
  3 other modules (legacy reports, invoicing, audit) and is synchronous.
  *Implication*: any async refactor in nextstop must not break sync callers.
  *Evidence*: F004

- **Test isolation.** PDF tests are skipped under `pytest -m "not pdf"` in CI
  by default; they only run on tagged builds.
  *Implication*: regressions can land silently in `dev`.
  *Evidence*: F006

- **Async-first convention.** AI-Parrot conventions require async public
  methods; this module recently migrated.
  *Implication*: rolling back to sync is not an option.
  *Evidence*: F002

### 2.3 Recent History (Relevant)

Commits on the affected paths in the last 30 days, ordered newest first.

| Commit | When | Author | Message | Touched files |
|--------|------|--------|---------|---------------|
| `a3f4b2` | 2 days ago | @dev1 | refactor: switch to async PDF render | `app/modules/nextstop/pdf.py`, `tests/nextstop/test_pdf.py` |
| `e891cc` | 8 days ago | @dev2 | fix: PDF margins for landscape mode | `libs/pdf_renderer.py` |

> If history is empty, state so explicitly. Absence of recent activity is itself
> useful evidence (rules out recent-change regression).

---

## 3. Hypothesis  *(if mode = investigation)*

> Replace this entire section with §3-Scope below if mode = enrichment.

### Hypothesis 1 — <one-line statement>  · Confidence: medium

**Supporting evidence**: F002, F005
**Contradicting evidence**: —
**Reasoning**: The async refactor in `a3f4b2` added `await` to a function
(`libs.pdf_renderer.render`) that is still synchronous; it now returns a
coroutine that the caller never awaits. The test in F005 was skipped at the
same commit, which would have caught the regression.

**Suggested next probe**:
```bash
pytest tests/nextstop/test_pdf.py::test_generate_pdf -s
# look for "coroutine 'render' was never awaited" warning
```

### Hypothesis 2 — <one-line statement>  · Confidence: low

<Repeat structure. Keep at most 3 hypotheses, ranked.>

---

## 3. Probable Scope  *(if mode = enrichment)*

> Replace this entire section with §3-Hypothesis above if mode = investigation.

### What's New

- **<New module / function / endpoint>** — <one-line role>

### What Changes

- **`<path>`::<symbol>** — <change summary>  *Evidence*: F00X

### What's Untouched (Non-Goals)

Explicitly out of scope, to prevent later scope creep:
- <thing 1>
- <thing 2>

### Patterns to Follow

- <pattern from existing code>  *Evidence*: F00X

### Integration Risks

- <risk>: <impact + mitigation>  *Evidence*: F00X

---

## 4. Confidence Map

Every atomic claim in this proposal, with its evidence and confidence level.
Readers can audit the proposal by walking this table.

| ID | Claim | Evidence | Confidence | Reasoning |
|----|-------|----------|------------|-----------|
| C1 | PDF entrypoint lives at `app/modules/nextstop/pdf.py:generate_pdf` | F001 | high | direct grep + read confirmation |
| C2 | The bug was introduced in commit `a3f4b2` | F002 | medium | timing aligns; no log/stack trace confirms causality |
| C3 | The shared `render` helper is sync | F004 | high | direct read of function signature |
| C4 | Test was skipped intentionally | — | low | inferred from absence of skip-reason comment |

Distribution: **<H>** high, **<M>** medium, **<L>** low.

> If `low` claims are critical to the conclusion, the overall confidence
> should reflect that — not be averaged up.

---

## 5. Open Questions

### Resolved (during proposal phase)

- [x] **<question text>** — *Resolved*: <user's answer verbatim>
  *Resolves claims*: C2

### Unresolved (defer to spec / implementation)

- [ ] **<question text>** — *Owner*: <name | "tbd">
  *Blocks claims*: C4
  *Plausible answers*: a) intentional skip · b) accidental skip from refactor

> If §5 has more than 5 unresolved entries, the research budget was likely
> too tight or the source too vague for a proposal — consider escalating to
> `/sdd-brainstorm` instead.

---

## 6. Recommended Next Step

**`/sdd-spec FEAT-XXX`** — *Rationale*: localization is high-confidence
(C1, C3) and the fix scope is well-bounded; no architectural fork to explore.

### Alternatives

- **`/sdd-brainstorm FEAT-XXX`** — if you want to explore alternative
  architectural approaches (e.g., make `render` async vs. wrap with
  `asyncio.to_thread()`).
- **`/sdd-task FEAT-XXX`** — if you accept the recommended fix as-is and
  want a single task in the queue (suitable only for trivial localized fixes).
- **Manual review** — if research was truncated (`overall_confidence: low`)
  or contradictions surfaced in findings.

---

## 7. Research Audit

Full state of the research session, for reproducibility and review.

| Artifact | Path |
|----------|------|
| State checkpoints | `sdd/state/FEAT-XXX/state.json` |
| Source (raw) | `sdd/state/FEAT-XXX/source.md` |
| Research plan | `sdd/state/FEAT-XXX/research_plan.json` |
| Findings (digests) | `sdd/state/FEAT-XXX/findings/F001-*.md`, ... |
| Synthesis (JSON) | `sdd/state/FEAT-XXX/synthesis.json` |
| Synthesis reasoning | `sdd/state/FEAT-XXX/synthesis.thinking.log` (if persisted) |

**Budget consumed**:
- Files read: 11 / 40
- Grep calls: 9 / 25
- Git calls: 3 / 10
- Wall time: 215.6s / 300s
- Truncated: **no**

**Mode determination**: `auto` → resolved to `investigation` (negation in
source: "no genera").

---

## 8. Provenance

| Field | Value |
|-------|-------|
| Generated by | `/sdd-proposal v1.0` |
| Synthesis prompt | `sdd/templates/synthesis.prompt.md v1.0` |
| Plan prompt | `sdd/templates/research_plan.prompt.md v1.0` |
| Schema versions | state=1.0, synthesis=1.0, research_plan=1.0 |
| Operator | <user / agent name> |
