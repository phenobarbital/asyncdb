---
name: sdd-spec
description: Convert a brainstorm, proposal, or direct feature request into a formal SDD specification with verified codebase contracts and flow metadata.
---

# SDD Spec

Use this skill when the user asks to run `sdd-spec`, create a formal SDD
specification, or convert a brainstorm/proposal into a spec.

Codex invocation: `$sdd-spec <feature-slug> [--type feature|hotfix] [--base-branch <branch>] [-- <notes>]`.

## Purpose

Create `sdd/specs/<feature-slug>.spec.md`, the single source of truth for a
feature or hotfix.

## Guardrails

- Do not implement code.
- Use `sdd/templates/spec.md`.
- Preserve YAML frontmatter.
- If prior exploration exists, consume it before asking the user questions.
- Never re-ask brainstorm questions marked `[x]`; carry answers forward
  verbatim and route them into the correct spec sections.
- Build a verified Codebase Contract with file paths and line numbers.
- Commit only the spec file.

## Workflow

1. Parse:
   - feature slug
   - free-form notes after `--`
   - optional `--type`
   - optional `--base-branch`
2. Locate prior exploration:
   - `sdd/proposals/<feature-slug>.brainstorm.md`
   - `sdd/proposals/<feature-slug>.proposal.md`
3. If a brainstorm exists, treat it as authoritative:
   - Problem Statement -> Motivation
   - Constraints -> Goals and Acceptance Criteria
   - Recommendation -> Architectural Overview
   - Feature Description -> Overview, Integration Points, Risks
   - Capabilities -> Module Breakdown
   - Impact table -> Integration Points
   - Code Context -> Codebase Contract, after re-verification
   - Libraries -> External Dependencies
   - Parallelism Assessment -> Worktree Strategy
   - Open Questions -> preserve `[x]` and `[ ]`
4. Resolve flow metadata with `scripts.sdd.sdd_meta.resolve_flow()`:
   - explicit flags win
   - exploration frontmatter is next
   - default is `type: feature`, `base_branch: dev`
5. Validate:
   - hotfix requires `main`
   - feature must not use `main`
6. Sync base branch:
   - refuse if inside `.claude/worktrees/`
   - refuse dirty worktree
   - `git checkout <base_branch>`
   - `git pull --ff-only origin <base_branch>`
7. Ask clarifying questions only for real blockers:
   - missing target version/status/author preferences
   - unresolved questions that block design
   - contradictions discovered during codebase research
8. Research codebase:
   - run `wikitoolkit query "<focused question>"` before source scans
   - inspect at least one wiki result when available
   - use `rg`
   - read every file before citing imports, methods, classes, signatures, or
     paths
   - record plausible things that do not exist
9. Reserve identity:
   - For `type: feature`, call:
     `python -m scripts.sdd.reserve_ids --kind feature --count 1 --base-branch <base_branch> --label <feature-slug>`.
   - Use the returned `FEAT-NNN` verbatim.
   - Do not fall back to hand-computed IDs.
   - For `type: hotfix`, reserve no `FEAT-NNN`; use the Jira key as identity
     when available.
   - If frontmatter has `reuse_feature_id`, use it only for an intentional
     multi-spec split and document that reuse.
10. Write the spec:
   - frontmatter `type` and `base_branch`
   - ID or Jira identity
   - date
   - architecture and module breakdown
   - tests and acceptance criteria
   - mandatory Codebase Contract
   - Worktree Strategy
   - Open Questions with resolved/unresolved state preserved
11. Commit:
   - clear staging with `git reset HEAD`
   - stage only `sdd/specs/<feature-slug>.spec.md`
   - verify cached names
   - commit `sdd: add spec for FEAT-NNN - <feature-slug>` for features, or a
     hotfix equivalent for hotfixes

## Output

For features:

```text
Spec created and committed: sdd/specs/<feature-slug>.spec.md
Feature ID: FEAT-NNN
Isolation: per-spec|mixed
Next: review, mark approved, run $sdd-task sdd/specs/<feature-slug>.spec.md
```

For hotfixes:

```text
Spec created and committed: sdd/specs/<feature-slug>.spec.md
Identity: Jira <KEY> or hotfix slug
Base branch: main
Next: normally skip task decomposition and dispatch direct development; run
$sdd-task only for unusually large hotfixes.
```

## References

- `sdd/templates/spec.md`
- `sdd/WORKFLOW.md`
- `scripts/sdd/sdd_meta.py`
- `scripts/sdd/reserve_ids.py`

