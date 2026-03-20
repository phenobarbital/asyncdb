---
model: haiku
---

# /sdd-status — SDD Task Board

Read `sdd/tasks/.index.json` and print a human-friendly status report of all SDD tasks.

## Usage
```
/sdd-status
/sdd-status <feature-name>
```

## Guardrails
- If `sdd/tasks/.index.json` does not exist, inform the user and suggest running `/sdd-task` first.
- Read-only — do not modify any files.

## Steps

### 1. Read the Index
Read `sdd/tasks/.index.json`. If a `<feature-name>` filter is provided, show only tasks for that feature.

### 2. Group and Display
Group tasks by `status` (in-progress → pending → done) and by feature. Print:

```
📊 SDD Task Board
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Feature: <feature>
Spec: sdd/specs/<feature>.spec.md

  🔄 In-Progress
     TASK-<NNN> — <title>  [<priority>/<effort>]  assigned: <who>

  ⏳ Pending
     TASK-<NNN> — <title>  [<priority>/<effort>]  blocked-by: <deps or —>

  ✅ Done
     TASK-<NNN> — <title>

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Summary: <N> done / <N> in-progress / <N> pending / <N> total
```

### 3. Highlight Blockers
If any pending tasks are blocked (deps not done), add a blockers section:
```
⚠ Blockers:
  TASK-<NNN> waiting on TASK-<X> (<status>)
```

## Reference
- Index file: `sdd/tasks/.index.json`
- SDD methodology: `sdd/WORKFLOW.md`
