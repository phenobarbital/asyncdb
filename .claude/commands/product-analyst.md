---
description: Deep-dive a product or feature idea for AI-Parrot — define how it should be realized, its potential impact, feasibility, hidden assumptions, and opportunities. Interactive (clarifies hidden assumptions with you, like /sdd-brainstorm), produces a standalone analysis doc.
---

# /product-analyst — Strategic Idea Deep-Dive

Take a raw idea and pressure-test it BEFORE any spec work: what problem it really solves,
who for, how it should be realized inside AI-Parrot, its potential impact, feasibility,
the **hidden assumptions** it quietly depends on, and the **opportunities** it unlocks.

```
/product-analyst → (review analysis) → /sdd-brainstorm → /sdd-spec → /sdd-task → /sdd-start
        │
        └─ standalone: the analysis can also just inform a go/no-go decision and stop here.
```

This command runs **interactively** in the main loop, so it can clarify hidden assumptions
with you in real time. For heavy research it **delegates** to the `product-analyst`
subagent (autonomous deep-dive). The output is a **standalone** document — it does NOT
touch the SDD task index or any branch state.

## Guardrails
- **No implementation code, no spec, no tasks.** This is strategy and analysis only.
- Output is a single standalone doc under `docs/product-analysis/`.
- **Balanced**: pair every upside with the assumption that must hold for it to be real.
- Every claim about existing AI-Parrot code must be **verified** (`path:line`). Never
  invent classes, modules, or integrations.
- Do NOT commit unless the user asks (standalone artifact; not part of SDD auto-commit).

## Steps

### 1. Parse Input
Extract from the invocation:
- **idea / title** → derive a kebab-case `<idea-slug>`. If absent, ask for the idea.
- **free-form notes** after `--` → initial context.

### 2. Frame the Idea
Restate the idea in one sentence and name, provisionally: the **job** it does and the
**user** (developer? operator? end-user of an agent built with AI-Parrot?). Show this
back to the user so a wrong frame gets corrected before any analysis.

### 3. Interactive Discovery — Surface Hidden Assumptions (mandatory, ≥2 rounds)
**Do not skip.** This is the differentiator from a one-shot analysis. Conduct at least
two rounds of Q&A; the goal is to drag *unstated* assumptions into the open.

**Round 1 — intent, user, success:** 3–5 questions on the core use case, who exactly
benefits, what "success" looks like (a metric), and any hard constraints.

**Round 2 — assumptions & tradeoffs:** 3–5 follow-ups that name the assumptions you're
detecting and ask the user to confirm or break them. Probe: "this only matters if X is
true — is it?", contradictions in Round 1, edge/failure cases, and what would make this
*not* worth doing.

Continue with more rounds if core questions remain open. Only proceed once the idea's
intent, audience, and success criteria are clear. If something critical is unknown,
ask — do not assume silently.

### 4. Delegate Research (parallel, via the product-analyst subagent)
Launch **two research lanes concurrently** using the Agent tool (`subagent_type:
product-analyst`), passing the framed idea + discovery answers to each:

- **Lane A — Codebase fit**: what already exists in AI-Parrot that this builds on,
  competes with, or conflicts with (clients/bots/tools/toolkits/crew/skills/RAG/
  integrations/memory). Must return verified `path:line` references and an explicit
  "does NOT exist" list.
- **Lane B — External feasibility**: comparable tools/libraries, prior art, relevant
  standards (e.g. MCP, A2A, OpenAPI), and rough effort signals. Cite sources.

If the idea is small/internal, Lane B may be skipped — say so. Use returned findings;
do not re-derive them yourself.

### 5. Synthesize the Deep-Dive
Combine discovery answers + research into the analysis, applying:
- **Jobs-to-be-Done** — the job a developer hires this to do.
- **Value Proposition Canvas** — pains relieved / gains created.
- **RICE / ICE** — rough prioritization signal.
- **Pre-mortem** — "it shipped and failed — why?" → feeds Risks + Hidden Assumptions.

Frame "How It Should Be Realized" at the **strategy** level against AI-Parrot's surfaces
(no code): which abstractions it extends, which integration path (A2A / MCP / OpenAPI),
where it lives.

### 6. Write the Standalone Document
Create `docs/product-analysis/<idea-slug>.analysis.md` (make the dir if needed). Use
today's date from the environment (`date +%F` — do not guess). Structure:

```markdown
# Product Analysis — <Idea Title>

> Status: analysis · Date: <YYYY-MM-DD> · Verdict: <go | iterate | no-go>

## 1. Idea in One Line
## 2. Problem & Opportunity
## 3. Target Users & Jobs-to-be-Done
## 4. How It Should Be Realized        (strategy mapped to AI-Parrot surfaces; no code)
## 5. Potential Impact                 (value prop, differentiation, success metrics/KPIs)
## 6. Feasibility                      (approach, effort T-shirt, dependencies,
                                        codebase readiness w/ verified refs, RICE/ICE)
## 7. Hidden Assumptions               (table: assumption | why assumed | how to validate | risk if wrong)
## 8. Opportunities & Adjacencies
## 9. Risks & Mitigations
## 10. Open Questions
## 11. Recommendation & Next Steps     (verdict + cheapest de-risking experiment;
                                        if go → /sdd-brainstorm <idea-slug>)

## Appendix — Code Context (verified)
- Reusable / integration points (`path:line`)
- Does NOT exist (verified)
- External sources cited
```

### 7. Output
```
✅ Product analysis saved: docs/product-analysis/<idea-slug>.analysis.md

   Verdict: <go | iterate | no-go>
   Top hidden assumptions: <count>
   Cheapest next experiment: <one line>

Next steps:
  - Review the analysis (especially §7 Hidden Assumptions)
  - If go → /sdd-brainstorm <idea-slug>  (the analysis feeds problem/impact/constraints)
  - To commit the doc: ask me — it is a standalone artifact, not auto-committed.
```

## How /sdd-brainstorm Can Consume This
When the verdict is **go**, the analysis maps cleanly into a follow-up brainstorm:
- §2 Problem & Opportunity → brainstorm "Problem"
- §3 Users & JTBD → brainstorm "User"
- §5 Impact + §6 Feasibility constraints → brainstorm "Constraints"
- §4 How It Should Be Realized → seeds the brainstorm's option generation
- Appendix Code Context → carries verified references forward (anti-hallucination)

## Reference
- Subagent: `.claude/agents/product-analyst.md` (autonomous deep-dive / research lanes)
- Next phase: `/sdd-brainstorm` → `.claude/commands/sdd-brainstorm.md`
- Architecture context: `.agent/CONTEXT.md`
