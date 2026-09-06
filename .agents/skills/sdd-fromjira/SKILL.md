---
name: sdd-fromjira
description: Bootstrap an SDD Brainstorm from a Jira ticket, conducting Q&A and codebase research.
---

# SDD From Jira

Use this skill when the user asks to bootstrap an SDD brainstorm from a Jira ticket, run `sdd-fromjira`, or convert a Jira issue into a feature proposal.

Invocation: `sdd-fromjira <JIRA_KEY> [--complexity=fix|simple|standard|complex] [--skip-qa]`.

## Purpose

Fetch requirements from a Jira ticket, structure them, conduct targeted Q&A, research the codebase for reusable components, and produce a worker-ready brainstorm document at `sdd/proposals/<issue-key>-<slug>.brainstorm.md`.

## Guardrails

- Read-only on Jira: do not modify the Jira issue in this skill.
- Always use the official template at `sdd/templates/brainstorm.md`.
- Always commit the brainstorm file to git upon generation.
- Never write implementation code in the brainstorm document.
- Set flow type in frontmatter: `type: feature, base_branch: dev` (or `hotfix`/`main` for bug tickets).

## Workflow

1. Fetch Jira ticket:
   - Use Jira MCP tool (`jira_get_issue`) if available, or curl fallback via environment variables (`JIRA_INSTANCE`, `JIRA_API_TOKEN`).
   - Extract summary, description, acceptance criteria, components, labels, subtasks.
2. Parse content:
   - Convert description/ADF to plain text.
   - Extract acceptance criteria and constraints.
3. Classify complexity:
   - `fix` (1 round Q&A), `simple` (2 rounds), `standard` (2-3 rounds), `complex` (3+ rounds).
4. Present ticket context:
   - Show summary, components, AC, and complexity assessment to user.
5. Interactive Q&A:
   - Target genuine gaps left open by Jira (not generic questions).
6. Codebase research:
   - Run `wikitoolkit query` before source scans.
   - Read source files to build verified Code Context (signatures, imports, and what does NOT exist).
7. Generate 3+ options:
   - Map each option against Jira AC.
   - Recommend one option with explicit tradeoff rationale.
8. Save and commit:
   - Save to `sdd/proposals/<issue-key>-<slug>.brainstorm.md`.
   - Stage only the brainstorm file and commit: `sdd: add brainstorm from Jira <issue-key> — <slug>`.

## References

- `sdd/templates/brainstorm.md`
- `sdd/WORKFLOW.md`
