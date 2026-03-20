# /sdd-spec — Scaffold a Feature Specification

Scaffold a new Feature Specification for AI-Parrot using the SDD methodology.

## Usage
```
/sdd-spec <feature-name> [-- free-form description and notes]
```

## Guardrails
- Always use the official template at `sdd/templates/spec.md`.
- Do NOT write implementation code in the spec — specs are design documents.
- Feature IDs must be unique. Check existing specs before assigning.
- If a `.brainstorm.md` exists for this feature in `sdd/proposals/`, use it as input.
- **Always commit the spec file to the current branch** so worktrees can see it.

## Steps

### 1. Parse Input
- **feature-name**: slug-friendly kebab-case. If not provided, ask.
- **free-form notes**: anything after `--`, used as Problem Statement seed.

### 2. Check for Prior Exploration
Look for prior exploration documents in `sdd/proposals/`:
- `.brainstorm.md` → structured options analysis, use Recommended Option.
- `.proposal.md` → discussion output, use Motivation + Scope sections.

If found, pre-fill the spec from that document. Minimise questions to the user.

### 3. Research the Codebase
Before writing the spec:
- Read existing specs in `sdd/specs/` directory.
- Identify related existing components (AbstractClient, AgentCrew, BaseLoader, etc.).
- Note what can be reused vs. what must be created.

### 4. Scaffold the Spec
1. Read the template at `sdd/templates/spec.md`.
2. Create `sdd/specs/<feature-name>.spec.md` filled in with:
   - Feature ID (check existing; increment last; start at FEAT-001 if none).
   - Today's date.
   - Answers from user (or prior exploration documents).
   - Architectural patterns from your codebase research.

**Worktree hint (new section in spec):**
Include a `## Worktree Strategy` section in the spec with:
- Default isolation unit: `per-spec` or `per-task`.
- If `per-spec`: all tasks run sequentially in one worktree.
- If mixed: list which tasks are parallelizable and why.
- Cross-feature dependencies: list any specs that must be merged first.

### 5. Commit the Spec

> **CRITICAL — Worktrees branch from the current state of the repo.**
> If the spec is not committed, any worktree created later will NOT see it,
> and the `sdd-worker` agent will fail with "no spec found".

```bash
git add sdd/specs/<feature-name>.spec.md
git commit -m "sdd: add spec for FEAT-<ID> — <feature-name>"
```

### 6. Output
```
✅ Spec created and committed: sdd/specs/<feature-name>.spec.md

   Feature ID: FEAT-<ID>
   Isolation: per-spec (sequential tasks) | mixed (some parallel tasks)

   To create a worktree for this feature after task decomposition:
     git worktree add -b feat-<FEAT-ID>-<feature-name> \
       .claude/worktrees/feat-<FEAT-ID>-<feature-name> HEAD

Next:
  1. Review the spec — check Acceptance Criteria and Architectural Design.
  2. Mark status: approved when ready.
  3. Run /sdd-task sdd/specs/<feature-name>.spec.md
```

## Reference
- Template: `sdd/templates/spec.md`
- Existing specs: `sdd/specs/`
- SDD methodology: `sdd/WORKFLOW.md`
- Worktree policy: `CLAUDE.md` (section "Worktree Policy")