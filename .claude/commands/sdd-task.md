# /sdd-task — Decompose a Spec into SDD Tasks

Decompose an approved Feature Specification into atomic, assignable implementation tasks.

## Usage
```
/sdd-task sdd/specs/<feature-name>.spec.md
```

## Guardrails
- Only decompose specs with `status: approved`.
- Each task must be independently implementable and testable.
- Check `sdd/tasks/.index.json` for existing tasks to avoid duplication.
- Do NOT write implementation code — tasks are plans, not code.
- Mark tasks that can run in parallel worktrees with `parallel: true`.
- **Must run on `dev` branch** (or the integration branch). Not inside a worktree.
- **Always commit task files and index to `dev`** before creating the worktree.

## Steps

### 1. Verify Branch
Confirm you are on the integration branch (`dev`), NOT inside a worktree:
```bash
git branch --show-current  # should be "dev"
```
If not on `dev`, warn:
```
⚠️  /sdd-task should run on the dev branch so all worktrees can see the tasks.
   Current branch: <branch>
   Switch to dev first: git checkout dev && git pull origin dev
```

### 2. Read the Spec
Read the spec file provided by the user (e.g., `sdd/specs/<feature>.spec.md`).
- If spec is not `status: approved`, warn and ask to confirm.
- Extract: Feature ID, title, module breakdown, acceptance criteria, dependencies.

### 3. Plan Task Decomposition
Analyze the spec and identify atomic tasks:
- One task per module, class, or distinct deliverable.
- Order tasks to respect implementation dependencies.
- Aim for tasks completable in 1–4 hours each.

**Parallelism analysis:**
- Identify tasks within the spec that share NO files or imports with other tasks.
- Mark those tasks as `parallel: true` — they CAN run in separate worktrees.
- Tasks that import/extend code from a prior task in the same spec are `parallel: false` (default).
- Document the rationale in the `parallelism_notes` field.

### 4. Generate Tasks
1. Ensure `sdd/tasks/active/` directory exists (create if needed).
2. Read the task template at `sdd/templates/task.md`.
3. For each task, create `sdd/tasks/active/TASK-<NNN>-<slug>.md` using the template.

Create or update `sdd/tasks/.index.json` with the schema:
```json
{
  "tasks": [
    {
      "id": "TASK-<NNN>",
      "slug": "<slug>",
      "title": "<title>",
      "feature_id": "FEAT-<NNN>",
      "feature": "<feature-slug>",
      "spec": "sdd/specs/<feature>.spec.md",
      "status": "pending",
      "priority": "<high|medium|low>",
      "effort": "<S|M|L|XL>",
      "depends_on": [],
      "parallel": false,
      "parallelism_notes": "<rationale>",
      "assigned_to": null,
      "started_at": null,
      "file": "sdd/tasks/active/TASK-<NNN>-<slug>.md"
    }
  ]
}
```

**Field clarification:**
- `feature_id`: Formal Feature ID from the spec (e.g., `"FEAT-014"`).
- `feature`: Kebab-case slug (e.g., `"videoreel-visual-changes"`).

### 5. Commit Tasks and Index to `dev`

```bash
git add sdd/tasks/.index.json
git add sdd/tasks/active/TASK-*
git commit -m "sdd: add <N> tasks for FEAT-<ID> — <feature-name>"
```

### 6. Create the Feature Worktree

After committing to `dev`, create the worktree so it inherits the tasks:

```bash
git worktree add -b feat-<FEAT-ID>-<slug> \
  .claude/worktrees/feat-<FEAT-ID>-<slug> HEAD
```

### 7. Output
```
✅ Generated and committed <N> tasks for FEAT-<ID> — <feature-name>

Tasks created:
  TASK-<NNN> — <title> [<priority>/<effort>]
  ...

Feature worktree created:
  .claude/worktrees/feat-<FEAT-ID>-<slug>

Next:
  cd .claude/worktrees/feat-<FEAT-ID>-<slug>
  /sdd-start TASK-<NNN>   # begin first task
```

## Reference
- Task template: `sdd/templates/task.md`
- Index schema: `sdd/WORKFLOW.md` (section "Task Index Schema")
- Completed tasks go to: `sdd/tasks/completed/`