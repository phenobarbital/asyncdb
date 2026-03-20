# /sdd-proposal — Feature Proposal & Discussion

Propose a feature idea in plain, non-technical language. The agent discusses the idea
with you, asks clarifying questions, and then generates a formal SDD specification
from the gathered information.

This is the recommended entry point for new features:
```
/sdd-proposal → discuss → /sdd-spec (auto-generated) → /sdd-task → /sdd-start → implement
```

## Guardrails
- Keep the discussion **non-technical** — focus on WHY and WHAT, not HOW.
- Do NOT generate implementation code.
- The output is a proposal document AND (optionally) a scaffolded spec.
- If the user is unsure about scope, help narrow it down before proceeding.
- **Always commit the proposal file** so other commands and worktrees can see it.

## Steps

### 1. Parse Input
Extract from the user's invocation:
- **feature-title**: short descriptive name. If not provided, ask.
- **free-form notes**: anything the user provides as initial context.

### 2. Scaffold the Proposal
1. Read the template at `sdd/templates/proposal.md`.
2. Create `sdd/proposals/<feature-title>.proposal.md` with today's date.
3. Pre-fill any sections using the user's free-form notes.

### 3. Discuss and Clarify
Walk through each section of the proposal with the user, asking:

**Motivation (Why)**:
- What problem does this solve?
- Who is affected? (end users, developers, ops)
- Why is this needed now?

**Scope (What Changes)**:
- What new capabilities does this introduce?
- What existing behavior changes?
- What is explicitly NOT changing?

**Impact**:
- Which existing components are affected?
- Are there breaking changes?
- Any new dependencies?

**Open Questions**:
- What are you unsure about?
- Are there alternative approaches you've considered?

**Parallelism Potential**:
- Are there independent sub-features that could be developed in parallel?
- Does this feature depend on any in-flight features (active worktrees)?

Update the proposal document with the user's answers as you go.

### 4. Summarize the Discussion
Once all sections are filled, present a summary:
```
📋 Proposal Summary: <feature-title>

Why:    <one-line motivation>
What:   <one-line scope>
Impact: <affected areas>
New capabilities: <list>
Modified capabilities: <list or "none">
Open questions: <count remaining>
Parallelism: <per-spec | mixed — brief rationale>
```

Ask the user: **"Ready to generate a formal spec from this proposal?"**

### 5. Generate the Spec (Optional)
If the user agrees:
1. Run the `/sdd-spec` workflow logic internally, using the proposal content to pre-fill:
   - Motivation → Section 1 (Motivation & Business Requirements)
   - Capabilities → Section 3 (Module Breakdown)
   - Impact → Section 2 (Integration Points)
   - Open Questions → Section 7
   - Parallelism notes → Worktree Strategy section
2. Link the proposal to the spec: add a `**Spec**: sdd/specs/<n>.spec.md` line to the proposal.
3. Mark the proposal `status: accepted`.
4. **Commit both the proposal and spec** (spec commit handled by `/sdd-spec` logic).

If the user is not ready, leave the proposal as `status: discussion` for further iteration.

### 6. Commit the Proposal

> **CRITICAL — Worktrees branch from the current state of the repo.**
> If the proposal is not committed, `/sdd-spec` in a worktree won't find it.

```bash
git add sdd/proposals/<feature-title>.proposal.md
git commit -m "sdd: add proposal for <feature-title>"
```

If the spec was also generated in Step 5, the spec commit happens via `/sdd-spec` logic
(separate commit).

### 7. Output
```
✅ Proposal saved and committed: sdd/proposals/<feature-title>.proposal.md

Next steps:
  - Continue discussing: edit the proposal directly
  - Generate spec: run /sdd-spec <feature-title> (or accept now)
  - Once spec is approved: run /sdd-task
  - Begin implementation: run /sdd-start <FEAT-ID>
```

## Reference
- Proposal template: `sdd/templates/proposal.md`
- Spec template: `sdd/templates/spec.md`
- SDD methodology: `sdd/WORKFLOW.md`
- Worktree policy: `CLAUDE.md` (section "Worktree Policy")