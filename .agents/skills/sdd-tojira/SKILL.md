---
name: sdd-tojira
description: Export an SDD Specification to a Jira Story (and optionally subtasks).
---

# SDD To Jira

Use this skill when the user asks to export an SDD spec to Jira, sync tasks as subtasks, or run `sdd-tojira`.

Invocation: `sdd-tojira <spec-path-or-FEAT-ID> [--ticket KEY] [--with-subtasks] [--project KEY]`.

## Purpose

Export an approved specification (`sdd/specs/*.spec.md`) to Jira as a Story, optionally creating Jira subtasks for each decomposed task in `sdd/tasks/index/<feature>.json`, linking the Jira key in the spec, and committing the change.

## Guardrails

- Spec should be `status: approved` (confirm if draft).
- Check for existing ticket linkage to run in idempotent UPDATE mode.
- Map acceptance criteria into Jira custom fields (`customfield_10021`, etc.).
- Commit only the modified spec and task index files.

## Workflow

1. Resolve specification:
   - Accept spec path or `FEAT-NNN`.
   - Read spec frontmatter, motivation, architecture, acceptance criteria, and test spec.
2. Check Jira connection:
   - Prefer Jira MCP tools (`jira_create_issue`, `jira_update_issue`).
   - Fallback to curl with `JIRA_INSTANCE`, `JIRA_USERNAME`, `JIRA_API_TOKEN`.
3. Create or update Story:
   - Set summary, description, component, and AC.
4. Create Subtasks (if `--with-subtasks`):
   - For each task in `sdd/tasks/index/<feature>.json`, create Jira subtask with original estimate.
   - Record created Jira keys in the task index.
5. Update spec with Jira link:
   - Add `jira: <KEY>` to frontmatter and link in markdown body.
6. Commit changes:
   - Stage spec and index file only.
   - Commit: `sdd: export FEAT-<ID> to Jira <JIRA_KEY>`.

## References

- `sdd/specs/*.spec.md`
- `sdd/tasks/index/<feature>.json`
- `sdd/WORKFLOW.md`
