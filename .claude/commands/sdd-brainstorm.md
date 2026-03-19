# /sdd-brainstorm — Structured Idea Exploration

Explore a feature idea by generating multiple approaches with library and code references,
then produce a brainstorm document that feeds directly into `/sdd-spec`.

```
/sdd-brainstorm → (review) → /sdd-spec → /sdd-task → /sdd-start → implement
```

## Guardrails
- **No implementation code** — this is about ideas, references, and tradeoffs.
- Output is a brainstorm document, not a spec or task.
- Always use the template at `sdd/templates/brainstorm.md`.
- Include concrete library/package references with versions when possible.
- Reference existing codebase modules that would be reused or extended.
- **Always commit the brainstorm file** so other commands and worktrees can see it.

## Steps

### 1. Parse Input
Extract from the user's invocation:
- **topic / feature-name**: slug-friendly, kebab-case. If not provided, ask.
- **free-form notes**: anything after `--`, used as initial context.

### 2. Understand the Goal
Before generating options, establish:
- **Problem**: What specific problem are we solving?
- **User**: Who is affected? (end users, developers, ops)
- **Constraints**: Performance targets, compatibility, timeline, existing patterns.

If the user provided notes, extract these from context. Otherwise, ask briefly.

### 3. Interactive Discovery (Mandatory — minimum 2 rounds)

**DO NOT skip this step.** Before writing anything, conduct at least **two full rounds**
of questions-and-answers with the user to deeply understand the feature.

**Round 1 — Clarify intent and scope:**
Ask 3–5 targeted questions about:
- The core use case and expected behavior.
- Integration points with existing components.
- Non-obvious constraints (performance, backwards compatibility, security).
- What success looks like for this feature.

Wait for the user's answers before proceeding.

**Round 2 — Drill into gaps and tradeoffs:**
Based on the user's Round 1 answers, ask 3–5 follow-up questions about:
- Ambiguities or contradictions in the answers.
- Edge cases and failure scenarios.
- Priority between competing concerns (e.g., speed vs. flexibility).
- Any assumptions you're making that need validation.

Wait for the user's answers before proceeding.

**Additional rounds** are encouraged if:
- Core architectural questions remain unresolved.
- The user's answers reveal new dimensions not covered.
- Tradeoffs need explicit user decisions.

**Rule:** Only proceed to codebase research and option generation once you are confident
that all core questions have been answered. If a question is critical and unanswered,
ask — do not assume.

### 4. Research the Codebase
Scan the project for relevant existing components:
- Search for related modules, classes, and patterns.
- Identify reusable code that any solution should build on.
- Note existing dependencies that could be leveraged.

### 5. Generate Options
Produce **at least 3** distinct approaches. For each option:
- Descriptive name and explanation (WHAT, not HOW).
- Pros and cons (be honest about tradeoffs).
- Effort estimate (Low / Medium / High).
- **Libraries / Tools**: table of packages with purpose and notes.
- **Existing Code to Reuse**: specific paths and descriptions.

Include at least one unconventional or less obvious approach.

### 6. Recommend
Select one option and explain the reasoning:
- Reference specific tradeoffs from the options.
- Explain what you're trading off and why it's acceptable.

### 7. Describe the Feature
Write a detailed feature description based on the recommended option:
- **User-facing behavior**: what the user sees/experiences.
- **Internal behavior**: high-level flow and responsibilities (no code).
- **Edge cases & error handling**: boundary conditions, failure modes.

### 8. Map to SDD Structures
Fill in the remaining template sections:
- **Capabilities**: new and modified (kebab-case identifiers).
- **Impact & Integration**: affected components table.
- **Open Questions**: unresolved items with owners.

### 9. Parallelism Assessment
Evaluate the feature's decomposition potential for parallel development:

- **Internal parallelism**: Can this feature's tasks be split into independent
  worktrees? (e.g., separate toolkits, isolated loaders, unrelated endpoints)
- **Cross-feature independence**: Does this feature conflict with any in-flight
  specs? List shared files or modules.
- **Recommended isolation**: `per-spec` (all tasks sequential in one worktree)
  or `mixed` (some tasks can use individual worktrees).
- **Rationale**: Brief explanation of why the recommended isolation makes sense.

### 10. Save and Commit
1. Read the template at `sdd/templates/brainstorm.md`.
2. Create `sdd/proposals/<feature-name>.brainstorm.md` with today's date.
3. Set `Status: exploration`.
4. **Commit:**
   ```bash
   git add sdd/proposals/<feature-name>.brainstorm.md
   git commit -m "sdd: add brainstorm for <feature-name>"
   ```

### 11. Output
```
✅ Brainstorm saved and committed: sdd/proposals/<feature-name>.brainstorm.md

   Recommended: Option <X> — <n>
   Effort: <Low|Medium|High>
   Worktree isolation: <per-spec|mixed>
   Open questions: <count>

Next steps:
  - Review and refine the brainstorm
  - When ready: /sdd-spec <feature-name> (uses brainstorm as input)
```

## How sdd-spec Consumes This Document

When `/sdd-spec` is invoked with a feature name that has a `.brainstorm.md` in `sdd/proposals/`:
- **Problem Statement** → Spec Section 1 (Motivation & Business Requirements)
- **Constraints** → Spec Section 5 (Acceptance Criteria)
- **Recommended Option** → Spec Section 2 (Architectural Design)
- **Libraries / Tools** → Spec Section 6 (External Dependencies)
- **Feature Description** → Spec Section 2 (Overview + Integration Points)
- **Capabilities** → Spec Section 3 (Module Breakdown)
- **Impact & Integration** → Spec Section 2 (Integration Points)
- **Parallelism Assessment** → Spec Worktree Strategy section
- **Open Questions** → Spec Section 7

## Reference
- Brainstorm template: `sdd/templates/brainstorm.md`
- Proposal template: `sdd/templates/proposal.md`
- Spec template: `sdd/templates/spec.md`
- SDD methodology: `sdd/WORKFLOW.md`
- Worktree policy: `CLAUDE.md` (section "Worktree Policy")