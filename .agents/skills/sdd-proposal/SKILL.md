---
name: sdd-proposal
description: Convert a thin Jira issue, inline request, or notes file into a research-grounded SDD proposal with persisted findings and confidence-graded synthesis.
---

# SDD Proposal

Use this skill when the user asks to run `sdd-proposal`, turn a ticket into a
proposal, research a bug report before spec writing, or resume proposal
research.

Codex invocation: `$sdd-proposal <source> [--mode=investigation|enrichment] [--budget=tight|default|loose] [--no-gate]`.

## Purpose

Produce a proposal document grounded in repository evidence before broad human
Q&A.

Pipeline:

```text
source -> research plan -> budgeted research -> synthesis -> review gate -> targeted Q&A -> proposal
```

## Guardrails

- Wiki first, then `rg`, then source reads, then questions.
- Do not invent paths, symbols, classes, packages, or imports.
- Every code claim must cite a persisted finding ID.
- Keep confidence honest; do not inflate certainty when research is thin.
- Persist state under `sdd/state/<FEAT-ID>/`.
- Do not write implementation code.
- Commit only proposal and state files.

## Inputs

Accept:

- Jira key: `^[A-Z]+-\d+$`
- inline text
- file path
- `--resume FEAT-NNN`
- `--mode=investigation|enrichment`
- `--budget=tight|default|loose`
- `--no-gate`

Budget profiles:

| Profile | Files | Greps | Git | Depth | Seconds |
|---|---:|---:|---:|---:|---:|
| tight | 15 | 10 | 5 | 1 | 120 |
| default | 40 | 25 | 10 | 2 | 300 |
| loose | 100 | 60 | 20 | 3 | 900 |

## Workflow

1. Resolve or resume:
   - For `--resume`, read `sdd/state/<FEAT-ID>/state.json` and continue from
     its phase.
   - Otherwise allocate the next `FEAT-NNN` from existing specs, proposals, and
     state directories for proposal state identity only. Formal feature IDs for
     specs are still reserved later by `$sdd-spec`.
2. Create:
   - `sdd/state/<FEAT-ID>/state.json`
   - `sdd/state/<FEAT-ID>/source.md`
   - `sdd/state/<FEAT-ID>/findings/`
3. Resolve source:
   - Jira: prefer available Jira MCP tools; fallback to `JIRA_INSTANCE`,
     `JIRA_USERNAME`, and `JIRA_API_TOKEN` only when configured.
   - Inline: preserve the exact input.
   - File: read and preserve the source file body and frontmatter.
4. Build a research plan:
   - Probe `wikitoolkit status`.
   - Use `sdd/templates/research_plan.prompt.md` and
     `sdd/templates/research_plan.schema.json` when present.
   - Include wiki, grep, read, git log, and tree queries as appropriate.
   - Present the plan unless `--no-gate` was supplied.
5. Execute research:
   - Wiki queries are free against the budget.
   - Use `rg` for grep-style searches.
   - Read files before citing symbols.
   - Persist compact findings as
     `sdd/state/<FEAT-ID>/findings/FNNN-<slug>.md` using
     `sdd/templates/finding.md`.
6. Synthesize:
   - Use source, plan, findings, and budget status.
   - Produce `sdd/state/<FEAT-ID>/synthesis.json`.
   - Lint the synthesis:
     - every path/symbol is backed by a finding
     - every evidence ID exists
     - confidence is bounded by research quality
     - truncated research cannot recommend jumping directly to tasks
     - unknowns are limited to five
7. Review gate:
   - Show localization, top hypothesis or scope, confidence map, unknowns, and
     budget use.
   - Allow user choices: proceed, refine research, refine synthesis, abort.
8. Targeted Q&A:
   - Ask only material unknowns that the repo could not answer.
   - Persist answers into `synthesis.json`.
9. Render:
   - Read `sdd/templates/proposal.md`.
   - Write `sdd/proposals/<slug>.proposal.md`.
   - Set status to `discussion`, `review`, or `accepted` based on unresolved
     unknowns and explicit user acceptance.
10. Commit:
   - clear staging with `git reset HEAD`
   - stage only `sdd/proposals/<slug>.proposal.md` and `sdd/state/<FEAT-ID>/`
   - verify cached names
   - commit `sdd: research-grounded proposal for <FEAT-ID> - <summary>`

## Output

Report:

```text
Proposal saved and committed: sdd/proposals/<slug>.proposal.md
FEAT-ID: FEAT-NNN
Mode: investigation|enrichment
Confidence: high|medium|low
Audit: sdd/state/FEAT-NNN/
Next: $sdd-spec <slug> or $sdd-brainstorm <slug>
```

## References

- `sdd/templates/proposal.md`
- `sdd/templates/state.schema.json`
- `sdd/templates/research_plan.prompt.md`
- `sdd/templates/research_plan.schema.json`
- `sdd/templates/synthesis.prompt.md`
- `sdd/templates/finding.md`
- `sdd/WORKFLOW.md`

