---
description: Turn a terse idea or a thin Jira ticket into a grounded, well-defined user-story artifact (story, scope, behavior, acceptance & success criteria) by reading the codebase and forcing decisions through interactive Q&A. Optionally injects the enriched block back into a Jira ticket without touching the original description.
argument-hint: "[--jira-ticket <JIRA_KEY>] [--out <path>] <idea text | empty to use a ticket/context>"
allowed-tools: Read, Grep, Glob, Bash(rg:*), Bash(fd:*), Bash(git grep:*), Bash(ls:*), Bash(curl:*), Bash(jq:*), Bash(python:*), Bash(git add:*), Bash(git reset:*), Bash(git commit:*), Bash(git diff:*)
---

# /enrich-story — Idea/Ticket → Grounded User-Story Artifact

Transform a rough idea or a one-line Jira ticket into a **decision-closed, code-grounded user-story** that a senior engineer can act on and that feeds straight into the SDD pipeline.

```
terse idea / thin Jira ticket
        │
   /enrich-story   ← interactive Q&A + codebase grounding
        │
  grounded user-story artifact  ──►  /sdd-brainstorm → /sdd-spec
        └─ (optional) injected back into the Jira ticket (--jira-ticket)
```

This is **pre-SDD grooming**. It does not produce specs, tasks, or code. It produces a clear requirement with explicit scope, behavior, acceptance criteria, and a short list of genuinely-open questions.

## Usage
```
/enrich-story "tenants should be able to pause a running flow"
/enrich-story --jira-ticket NAV-8042                 # seed from the ticket AND inject back into it
/enrich-story --jira-ticket NAV-8042 "make it per-tenant, not global"   # ticket = seed + target, text augments
/enrich-story --out docs/stories/pause-flow.md "..." # also save the artifact to a file
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `<idea text>` | conditional | The rough idea/user story. Required unless `--jira-ticket` is given or a topic is clear from context. |
| `--jira-ticket <JIRA_KEY>` | no | Read this ticket's summary+description as the seed **and** make it the injection target. |
| `--out <path>` | no | Also write the final artifact to this path (markdown). |

If `--jira-ticket` is given and inline text is also present, the ticket is both seed and target; the inline text is treated as additional intent. If nothing is provided, use the conversation context; if there is no clear topic, **ask** — do not invent one.

---

# Core principles

This command is **always interactive**. Its job is not to write quickly — it is to **close decisions**. Two failure modes it must avoid:

- **Enriching by invention.** Filling gaps with plausible-but-unverified detail produces an artifact that looks solid and sends a developer down the wrong path. Every concrete suggestion must be grounded in the codebase or explicitly marked unverified.
- **Bypassing the human on real ambiguity.** When the seed leaves a genuine decision open, the command asks a trade-off question — it does not silently pick.

Hard rules:

- **Read before you ground.** Never reference an import path, class, registry, toolkit, or contract you have not located and read in this session. Cite with **grep anchors** (symbol + file path), never line numbers.
- **Ask on ambiguity.** If any mandatory decision dimension (below) is unresolved by the seed + codebase, ask. No artificial cap on the number of questions; no redundant questions.
- **Open Questions ≠ unasked questions.** The final artifact's `Open Questions` section contains only decisions that *remain* open after the Q&A round (e.g. needs product input, needs sandbox access, depends on an external party) — not things the command failed to ask.
- **No code, no specs, no tasks.** Output is a requirement artifact only.
- **Language:** conduct the Q&A in the user's language; write the final artifact in **English**.

---

# Codebase grounding (AI-Parrot)

Before proposing any default, search the workspace to align with real patterns. The codebase is the only authority — these are search starting points (uv-workspace monorepo):

- **`ai-parrot`** (core): `AbstractBot`, `AbstractClient`, `AbstractTool`, `AbstractToolkit`, `BotManager`, `AgentRegistry`, `ToolManager`, `AgentCrew`, `AgentsFlow` (DAG), `EventBus`, `HookManager`, memory/RAG/ontology layers.
- **`ai-parrot-tools`**: tools & toolkits. **`ai-parrot-loaders`**: loaders producing `List[Document]`. Satellite **`AI-Parrot-Integrations`**: channel integrations.
- Recurring patterns to align with: decorator-registered typed registries (`@register_node_type` / `NODE_TYPE_REGISTRY`, `ACTION_REGISTRY`, `ExtractionPlanRegistry`, etc.), `Abstract*` extension seams, mixins (`OntologyRAGMixin`, `MCPEnabledMixin`, `PersistenceMixin`), async-first, Pydantic v2 at I/O boundaries, `asyncpg` (not SQLAlchemy), per-backend storage (Postgres/PgVector, ArangoDB, Redis, S3).
- Invariants to honor when scoping behavior: loaders produce `List[Document]` and never embed agent/LLM logic; observers cannot stop execution (interceptors can); routing/targeting on security-sensitive paths is deterministic, not LLM-driven; toolkits over multi-purpose tools (separate named operations).

Grounded defaults must reference real endpoints, contracts, services, or registries and say briefly *why* they fit. Generic suggestions are not acceptable when code evidence exists.

---

# Steps

## 1. Ingest the seed

- If `--jira-ticket <KEY>`: fetch the ticket (see **Jira Access Strategy**) and read its summary + description as the seed. Echo what you read.
- Else: use the inline text (and/or conversation context) as the seed.
- Briefly restate, in the user's language: what is being asked, the problem it solves, and what is unclear.

## 2. Ground against the codebase

Search the workspace (Glob/Grep/`rg`) for the components, contracts, and patterns the seed touches. Note what you actually found (grep anchors). This evidence is what makes your suggested defaults trustworthy.

## 3. Interactive Q&A round (close the decisions)

Ask trade-off questions (A vs B), each resolving one concrete decision, each with a **grounded suggested default** where the code supports one. Iterate until decisions are closed or the user explicitly chooses to leave one open. Conversational tone, user's language.

### Mandatory decision dimensions

Your questions must collectively cover these. If any is unresolved, ask about it.

1. **Solution shape** — new vs extend (e.g. new `AbstractToolkit` vs extend an existing one; new registered node type vs mixin; new loader vs new tool).
2. **Affected components & contracts** — which ABCs/registries/managers are touched; the Pydantic I/O boundary (inputs/outputs and their shapes).
3. **Actor & usage context** — who triggers this and through which surface (channel, agent, MCP, flow); multi-tenant implications.
4. **Behavior** — normal flow, edge cases, and failure modes (async boundaries, retries, timeouts, fallbacks).
5. **Data & persistence** — which backend and why (Postgres/PgVector, ArangoDB, Redis, S3, etc.); state/lifecycle.
6. **Scope boundaries** — explicitly in scope vs out of scope (Non-Goals).
7. **Acceptance & success criteria** — observable, testable conditions for "done" (and how they'd be tested — e.g. integration test, `moto`+`syrupy` for AWS).
8. **Constraints** — performance/security limits when relevant (e.g. observability overhead ceilings, deterministic routing on security paths).

## 4. Confirm before drafting

When decisions are closed, ask (user's language): *"Decisiones cerradas. ¿Redacto el artifact?"* Do not draft until the user confirms.

## 5. Draft the artifact (English, after confirmation)

```markdown
# <Concise feature title>

> Source: <Jira KEY | inline idea | conversation context> · Enriched: <YYYY-MM-DD>

## Story
As a <actor>, I want <capability>, so that <outcome>.

## Objective
<1–2 paragraphs: the problem and the intended outcome.>

## Context & Codebase Grounding
<Where this lives and what it touches, with grep anchors: `Symbol` in `package/.../file.py`.
What existing pattern it follows and why.>

## Scope
### In scope
- ...
### Out of scope (Non-Goals)
- ...

## Closed Decisions
- <decision> — <rationale, grounded where possible>

## Expected Behavior
- Normal flow: ...
- Edge cases: ...
- Failure modes: ...

## Expected Output / Contract
- <inputs/outputs, Pydantic shapes, return contract — e.g. `List[Document]`>

## Acceptance Criteria
1. <observable, testable condition>
2. ...
   _(behavioral items may use Given / When / Then)_

## Success Criteria
- <how we know it's correctly implemented; test strategy>

## Open Questions
- <only decisions still genuinely open after Q&A — needs product input / sandbox / external party. Empty if none.>

---
_Next: feed to `/sdd-brainstorm` → `/sdd-spec`._
```

If `--out <path>` was given, also write the artifact to that path.

## 6. (Optional) Inject into the Jira ticket — `--jira-ticket`

Only if `--jira-ticket` was provided. **Never touch the original description.** Append the enriched block below the original, preceded by a clear machine-and-human marker so the boundary is unmistakable.

### Append-only, idempotent block

The injected block is delimited by markers the command owns:

```
──────────────────────────────────────────────
🤖 AI-Enriched Specification  ·  /enrich-story  ·  <YYYY-MM-DD>
(Everything above this line is the original report, untouched.
 Everything below was generated by AI from codebase grounding.)
enrich-story:begin id=<JIRA_KEY> rev=<YYYY-MM-DDThh:mm>
──────────────────────────────────────────────

<artifact body, sections 1–N>

enrich-story:end
```

- **First run:** read the current description, then set the new description = `<original description>` + the marker block + artifact. The original text is preserved verbatim above the first marker.
- **Re-run (idempotent):** if an `enrich-story:begin … enrich-story:end` block already exists, **replace only the content between the markers**; keep everything above the first marker (the original report and any human edits) intact. Never stack multiple enriched blocks.
- Do **not** modify the ticket summary, status, assignee, or any field other than the description.
- An explicit overwrite of the original (replacing rather than appending) is **out of scope** for this command — refuse and explain if asked.

### Jira Access Strategy (mirrors `/sdd-tojira`)

Use **mcp-atlassian** if available, falling back to **curl**.

- **Read:** `jira_get_issue(issue_key=...)` (MCP) or `GET $JIRA_INSTANCE/rest/api/3/issue/<KEY>` (curl). Parse `fields.summary` and `fields.description`.
- **Write:** `jira_update_issue(issue_key=..., description=...)` (MCP) or `PUT .../rest/api/3/issue/<KEY>` (curl).
- **curl env** (loaded via navconfig `env/.env`): `JIRA_INSTANCE`, `JIRA_USERNAME`, `JIRA_API_TOKEN`.
  ```bash
  eval "$(python -c "from navconfig import config; import os; [print(f'export {k}={v}') for k,v in os.environ.items() if k.startswith('JIRA_')]")"
  JIRA_INSTANCE="${JIRA_INSTANCE%/}"
  ```
- **ADF note:** Jira Cloud v3 stores descriptions as ADF. Preserve the original ADF document and append new nodes (a `rule` divider + `heading` marker + the body) rather than replacing the doc. mcp-atlassian handles markdown→ADF conversion internally; with curl, append to the existing `content` array.
- If mcp-atlassian is unavailable and env vars are missing, stop and print setup instructions — do not partially write.

## 7. Output summary

```
✅ Enriched story ready: <title>
   Source: NAV-8042  ·  Grounded against: <N> files
   Acceptance criteria: <N>   Open questions: <N>

   Jira: appended enriched block to NAV-8042 (original description untouched)  [if --jira-ticket]
   Saved: docs/stories/pause-flow.md                                          [if --out]

Next: /sdd-brainstorm  →  /sdd-spec
```

# Edge cases

- **Empty/one-line ticket:** the seed is thin by design — the Q&A round is where the content comes from. Do not draft from the seed alone.
- **Ticket description already has an enriched block:** re-run replaces between markers only (idempotent).
- **User declines to close a decision:** record it in `Open Questions` with the reason; do not invent a resolution.
- **No codebase evidence for a needed default:** ask rather than guess; if still unresolved, mark the assumption as `not verified` in the artifact.
- **mcp-atlassian + curl both unavailable:** produce the artifact (and `--out` file) anyway; report that Jira injection was skipped.

# Reference
- Jira export sibling: `/sdd-tojira`
- Downstream: `/sdd-brainstorm`, `/sdd-spec`
- Spec template / conventions: `sdd/templates/spec.md`, `sdd/WORKFLOW.md`

---

# Seed

$ARGUMENTS
