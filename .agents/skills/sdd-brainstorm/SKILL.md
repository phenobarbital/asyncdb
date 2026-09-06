---
name: sdd-brainstorm
description: Explore a feature idea through structured Q&A, codebase research, options, tradeoffs, and a committed SDD brainstorm document.
---

# SDD Brainstorm

Use this skill when the user asks to run `sdd-brainstorm`, create a brainstorm,
or explore a feature idea before writing a formal spec.

Codex invocation: `$sdd-brainstorm <feature-slug> -- <notes>`.

## Purpose

Produce `sdd/proposals/<feature-slug>.brainstorm.md` from an idea. This is an
exploration artifact, not implementation code and not a spec.

Pipeline:

```text
$sdd-brainstorm -> review -> $sdd-spec -> $sdd-task -> $sdd-start
```

## Guardrails

- Do not write implementation code.
- Use `sdd/templates/brainstorm.md`.
- Preserve the YAML frontmatter from the template.
- Always resolve and record `type` and `base_branch`.
- Always include concrete existing-code references with file paths and line
  numbers.
- Verify packages in project dependency files before recommending them.
- Commit only the brainstorm file unless the user explicitly asks otherwise.

## Flow Type

Ask these first unless the user already supplied explicit values:

1. Is this `feature` or `hotfix`?
2. For `feature`, which base branch? Default: `dev`. Use `staging` during a
   release freeze or a parent feature branch for sub-features. For `hotfix`,
   base is always `main`.

Defaults when unanswered: `type: feature`, `base_branch: dev`.

Validation:

- `type: hotfix` requires `base_branch: main`.
- `type: feature` with `base_branch: main` is invalid. Stop and explain that
  features land on `dev`, `staging`, or a non-main parent feature branch.

## Workflow

1. Parse input:
   - feature slug in kebab-case
   - notes after `--`
   - optional `--type feature|hotfix`
   - optional `--base-branch <branch>`
2. Run at least two Q&A rounds:
   - Round 1: intent, users, expected behavior, integration points, success
     criteria.
   - Round 2: gaps, edge cases, failure modes, tradeoffs, assumptions.
3. Research the codebase:
   - Run `wikitoolkit query "<focused question>"` before source scans.
   - Inspect at least one relevant wiki page with `wikitoolkit page <id>` or
     related entries when available.
   - Use `rg` for source search.
   - Read the source files before citing classes, functions, methods, imports,
     or paths.
4. Build the Code Context section:
   - User-provided snippets, preserved verbatim.
   - Verified imports and signatures with paths and line numbers.
   - Things searched for that do not exist.
5. Generate at least three solution options:
   - name
   - what it does
   - pros and cons
   - effort: Low, Medium, or High
   - libraries/tools and existing code to reuse
   - at least one less obvious approach
6. Recommend one option and explain the tradeoff.
7. Fill the remaining template sections:
   - Problem Statement
   - Constraints & Requirements
   - Feature Description
   - Capabilities
   - Impact & Integration
   - Open Questions
   - Parallelism Assessment
8. Save `sdd/proposals/<feature-slug>.brainstorm.md`.
9. Commit only that brainstorm file:
   - clear staging with `git reset HEAD`
   - stage only the brainstorm
   - verify `git diff --cached --name-only`
   - commit `sdd: add brainstorm for <feature-slug>`

## Output

Report:

```text
Brainstorm saved and committed: sdd/proposals/<feature-slug>.brainstorm.md
Recommended: Option <letter> - <name>
Effort: <Low|Medium|High>
Worktree isolation: <per-spec|mixed>
Open questions: <count>
Next: $sdd-spec <feature-slug>
```

## References

- `sdd/templates/brainstorm.md`
- `sdd/templates/spec.md`
- `sdd/WORKFLOW.md`

