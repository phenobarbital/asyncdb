---
name: sdd-insight
description: Analyze how you collaborate with Claude Code AND how this repo follows its Spec-Driven Development workflow. Produces a two-layer HTML report — a personal AI-fluency skill map (score, archetype, the four competencies, five dimensions, tailored growth levers) plus a repo-level SDD Process Discipline panel (pipeline, decomposition, acceptance criteria, cycle closure, review coverage), computed from your transcripts and the sdd/ artifact tree. Use when asked to analyze AI fluency, prompting style, builder profile, or SDD process health, or when run as /sdd-insight.
argument-hint: "[TRANSCRIPT_PATH | --no-open]"
allowed-tools: Bash(python3 *), Bash(source *), Read, Write, Workflow
---

# SDD Insight — one command, two layers

You produce a two-layer report for this developer and this repository, in one run:

1. **Measure (deterministic).** `scripts/sdd/insight.py` parses Claude Code transcripts,
   de-contaminates and scrubs them, computes the personal AI-fluency numbers (Layer 1),
   AND scores the repo's SDD workflow adherence from the `sdd/` artifact tree (Layer 2).
2. **Explore (Sonnet 4.6).** Parallel explorers read the evidence — one per AI-fluency
   competency, plus one SDD-process auditor.
3. **Analyze (Opus 4.8).** A senior assessor writes the personal skill map AND a grounded
   read of the repo's SDD process, then verifies both against the evidence.

Working files live under `~/.claude/sdd-insight/`. The engine, framework and workflow are
internal to this repo (`scripts/sdd/`, `reference/sdd-insight/`, `.claude/workflows/`).

> **Always activate the venv first** (project rule): `source .venv/bin/activate`.
> The engine is pure stdlib, but follow the rule for consistency.

## Step 1 — Measure + emit evidence

Working files live at fixed, reused paths, so first delete leftovers from a previous run so a
stale `analysis.json` can never be merged as if it belonged to this run:

```bash
source .venv/bin/activate
rm -f ~/.claude/sdd-insight/evidence.json ~/.claude/sdd-insight/analysis.json
mkdir -p ~/.claude/sdd-insight
```

Then measure (use `--quiet` so no score is surfaced before the final report). Run from the
repo root so `--sdd-dir sdd` resolves to this repo's artifact tree:

```bash
python3 scripts/sdd/insight.py \
  --evidence ~/.claude/sdd-insight/evidence.json \
  --sdd-dir sdd --no-open --quiet \
  -o ~/.claude/sdd-insight/sdd_insight_report.html $ARGUMENTS
```

This writes the evidence bundle (carrying both the personal signals and the repo `sdd` block)
and a deterministic fallback report. **Do not report any score yet** — continue to Steps 2–3.
If it reports no transcripts, tell the user to pass their transcript directory as `$ARGUMENTS`
(default `~/.claude/projects`).

## Step 2 — Run the two-layer analysis workflow

Print the absolute paths the workflow needs (it reads them with its own Read tool):

```bash
python3 -c "import os; print(os.path.expanduser('~/.claude/sdd-insight/evidence.json')); print(os.path.abspath('reference/sdd-insight/ai-fluency-framework.md'))"
```

Then call the **Workflow** tool with:
- `name`: `sdd-insight`
- `args`: `{ "evidence": "<first line above>", "framework": "<second line above>" }`

The workflow returns the analysis JSON (overall_read, skill_map of the four competencies,
top_growth, strengths, and `sdd_read` — the repo SDD process narrative). Sonnet 4.6 explores,
Opus 4.8 analyzes + verifies — model selection is baked into the workflow.

## Step 3 — Render the final report

Only if Step 2 returned an analysis. Write the returned JSON to
`~/.claude/sdd-insight/analysis.json`, then merge it — passing the evidence bundle it was built
from so the engine confirms the analysis belongs to this exact run:

```bash
python3 scripts/sdd/insight.py \
  --analysis ~/.claude/sdd-insight/analysis.json \
  --analysis-evidence ~/.claude/sdd-insight/evidence.json \
  --sdd-dir sdd \
  -o ~/.claude/sdd-insight/sdd_insight_report.html $ARGUMENTS
```

The engine fingerprints this run and compares it to the evidence bundle's `run_fingerprint`;
on mismatch it prints a note and renders the deterministic report instead. On success the
report carries Opus's tailored skill map, growth levers, AND the SDD process read on top of the
deterministic numbers and the SDD Process Discipline panel. Point the user to
`~/.claude/sdd-insight/sdd_insight_report.html`.

## Step 4 — Narrate (don't re-derive)

Only after the final report exists, give a short read in chat: the **personal score + band +
archetype** in one sentence, the **single highest-leverage personal growth move**, and the
**repo SDD Process Discipline score + band** with its weakest dimension (e.g. review coverage).
Keep it to a paragraph or two; the report has the depth.

## Fallbacks

- **No Workflow capability?** The Step-1 deterministic report is complete on its own (it
  already includes the SDD panel) — skip Steps 2–3; read the numbers from
  `~/.claude/sdd-insight/evidence.json` to narrate.
- **Personal fluency only?** Pass `--no-sdd` in Steps 1 and 3 to skip the Layer-2 SDD analysis.
- **Different SDD tree?** Pass `--sdd-dir <path>`.

## Notes

- Original transcripts are never modified; the `sdd/` tree is read strictly read-only.
- Scores measure observable behavior, not intent; thin signals are flagged "low data" and hedged.
- The SDD panel measures the **project's** workflow adherence; it is independent of the personal
  AI-fluency scores and is the same for any developer running it against this repo.
- Adapted from the open-source Claude Insight engine (MIT) — see
  `reference/sdd-insight/UPSTREAM-LICENSE`.
