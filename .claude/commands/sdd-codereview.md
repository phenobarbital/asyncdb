# /sdd-codereview — Code Review a Completed SDD Task

Reads the task file from `sdd/tasks/completed/`, loads every referenced file, and applies the
`code-reviewer` rule to produce a structured review report.

## Usage
```
/sdd-codereview sdd/tasks/completed/TASK-001-music-generation-model.md
/sdd-codereview TASK-001
/sdd-codereview music-generation-model
```

If nothing is provided, list the files in `sdd/tasks/completed/` and ask the user to pick one.

## Steps

### 1. Resolve the Task File
1. If the user passes a full path, use it directly.
2. Otherwise, scan `sdd/tasks/completed/` for a filename matching `TASK-<NNN>*` or `*<slug>*`.
3. If still ambiguous, list matches and ask.

### 2. Load Context
Read the task file and extract:
- **Spec file** path → read it.
- **Files created/modified** (from "Scope" or "Files" section) → read each one.
- **Acceptance criteria** → used to validate correctness.
- **Completion Note** → understand what was actually done.

### 3. Apply Code Review Criteria

Evaluate the implementation across these dimensions:

#### Correctness & Logic
- Does the code satisfy the task's acceptance criteria?
- Are there edge cases or error paths not handled?
- Verify the chain of thought: are assumptions documented and verifiable?

#### Code Quality
- **DRY**: Is there duplicated logic that should be extracted?
- **SOLID**: Does the code respect single responsibility, open/closed, etc.?
- **Abstraction level**: Are abstractions appropriate, or over/under-engineered?

#### Performance
- Any N+1 query patterns or unnecessary loops?
- Blocking I/O in async contexts?
- Obvious algorithmic inefficiencies?

#### Security
- Input validation and sanitisation present?
- SQL/NoSQL injection risks?
- XSS/CSRF exposure (if applicable)?
- Hardcoded secrets or credentials?

#### Documentation
- Public APIs and classes have docstrings?
- Complex logic has inline comments?
- Type hints applied consistently?

#### Testing
- Do the tests cover the acceptance criteria?
- Are edge cases and failure modes tested?
- Test quality: meaningful assertions vs. trivial checks?

### 4. Produce the Review Report
Output a structured markdown report:

```markdown
# Code Review: TASK-<NNN> — <title>

**Spec**: sdd/specs/<feature>.spec.md
**Reviewed files**: <list>
**Overall verdict**: ✅ Approved | ⚠ Approved with notes | ❌ Needs changes

---

## Summary
<2–3 sentence overall assessment>

## Findings

### 🔴 Critical (must fix before merge)
- **[file:line]** <description of issue>

### 🟡 Major (should fix)
- **[file:line]** <description>

### 🟢 Minor / Suggestions
- **[file:line]** <description>

## Acceptance Criteria Check
| Criterion | Status | Notes |
|-----------|--------|-------|
| <criterion> | ✅ / ❌ | <notes> |

## Positive Highlights
- <what was done well>
```

### 5. Save the Report (Optional)
If the user confirms, save the report to:
`sdd/reviews/TASK-<NNN>-review.md`

## Reference
- Completed tasks: `sdd/tasks/completed/`
- Task index: `sdd/tasks/.index.json`
- SDD methodology: `sdd/WORKFLOW.md`
