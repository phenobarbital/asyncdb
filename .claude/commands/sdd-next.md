---
model: haiku
---

# /sdd-next — Suggest Next Unblocked SDD Tasks

Read `sdd/tasks/.index.json`, identify unblocked tasks, and suggest assignments.
Shows worktree context to help the user decide where to run each task.

## Guardrails
- Only suggest tasks with status `"pending"` and all dependencies `"done"`.
- If `sdd/tasks/.index.json` does not exist, inform the user and suggest running `/sdd-task` first.
- Sort by priority (high → medium → low), then by effort (S → M → L → XL).

## Steps

### 1. Read the Index
Read `sdd/tasks/.index.json`.

### 2. Detect Active Worktrees
Run `git worktree list` to identify which feature worktrees are currently active.
Map each active worktree to its feature ID by matching the worktree name pattern
`feat-<FEAT-ID>-<slug>` or `task-<TASK-ID>-<slug>`.

### 3. Compute Unblocked Tasks
For each task with `status: "pending"`:
- Check that every task in `depends_on` has `status: "done"`.
- If all deps are done (or `depends_on` is empty) → task is **unblocked**.

### 4. Group and Annotate
Group unblocked tasks by feature. For each task, determine:
- **Has active worktree**: the feature already has a worktree running → suggest
  `/sdd-start TASK-<NNN>` inside that worktree session.
- **Needs new worktree**: no active worktree for this feature → show the
  `git worktree add` command.
- **Parallel task**: marked `parallel: true` → can use its own worktree.

### 5. Sort and Present
Sort unblocked tasks by priority, then effort. Output:

```
📋 Next unblocked SDD tasks:

FEAT-007 — Ontological RAG
  🟢 Active worktree: feat-007-ontology-rag
  1. TASK-003 — GraphStore           [high / M]
     Depends-on: TASK-001 ✅, TASK-002 ✅
     → /sdd-start TASK-003  (run inside existing worktree)

FEAT-008 — MCP Security Layer
  🔵 No worktree — create one:
     git worktree add -b feat-008-mcp-security .claude/worktrees/feat-008-mcp-security HEAD
     cd .claude/worktrees/feat-008-mcp-security
  2. TASK-010 — SecurityLayer base   [high / M]
     Depends-on: none
     → /sdd-start TASK-010

FEAT-009 — Security Toolkits
  ⚡ Parallel tasks (can run in separate worktrees):
  3. TASK-042 — Prowler Toolkit      [medium / S]
     git worktree add -b task-042-prowler .claude/worktrees/task-042-prowler HEAD
     → cd .claude/worktrees/task-042-prowler && /sdd-start TASK-042
  4. TASK-043 — Trivy Toolkit        [medium / S]
     git worktree add -b task-043-trivy .claude/worktrees/task-043-trivy HEAD
     → cd .claude/worktrees/task-043-trivy && /sdd-start TASK-043
```

If no tasks are unblocked:
```
⚠ No unblocked tasks found.
  All pending tasks are waiting on: <list of blocking task IDs>
  Run /sdd-status for the full board.
```

### 6. Show In-Progress Summary
After the unblocked list, show a brief summary of what's currently running:

```
🔄 In progress:
  TASK-002 — OntologyParser [in feat-007-ontology-rag]
  TASK-021 — Trivy Toolkit  [in task-021-trivy-toolkit]
```

## Reference
- Index file: `sdd/tasks/.index.json`
- Active worktrees: `git worktree list`
- Worktree policy: `CLAUDE.md` (section "Worktree Policy")
- SDD methodology: `sdd/WORKFLOW.md`