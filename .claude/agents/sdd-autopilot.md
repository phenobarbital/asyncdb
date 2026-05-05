# SDD Multi-Agent Orchestration — Design Document

## Overview

A two-phase orchestration pipeline for Claude Code that takes a Jira ticket
from inception to reviewed PR with minimal human intervention.

**Phase 1 — Planning (interactive):** `/sdd-jira` command
**Phase 2 — Execution (autonomous):** `sdd-autopilot` agent

The separation exists because planning REQUIRES human judgment (scope decisions,
architectural tradeoffs, priority calls) while execution can be pre-authorized
once the spec is approved.

---

## Architecture

### The Key Insight: Shell-Level AgentCrew

Claude Code agents run as separate CLI processes. Orchestration happens at the
shell level via `claude -p "prompt"` (non-interactive) and `claude --agent <name>`
invocations. This mirrors the `AgentCrew.run_flow()` DAG pattern but with:

- **Agents** = Claude Code agent definitions (`.claude/agents/*.md`)
- **Tasks** = CLI invocations with structured prompts
- **Dependencies** = sequential execution with exit code checks
- **Context passing** = files on disk (reports, diffs, task index)
- **Gates** = file-based checkpoints that the orchestrator reads before proceeding

### Process Model

```
┌─────────────────────────────────────────────────────────────────┐
│ Phase 1: /sdd-jira (INTERACTIVE — human in the loop)           │
│                                                                 │
│   Jira ticket ──► brainstorm Q&A ──► spec ──► tasks ──► approve │
│                   (2+ rounds)        (commit)  (commit)         │
└─────────────────────────────────────┬───────────────────────────┘
                                      │ human approves spec
                                      ▼
┌─────────────────────────────────────────────────────────────────┐
│ Phase 2: sdd-autopilot (AUTONOMOUS — pre-authorized)           │
│                                                                 │
│   ┌──────────┐   ┌───────────────┐   ┌──────────┐              │
│   │sdd-worker│──►│code-reviewer  │──►│qa-runner  │              │
│   │(worktree)│   │(same worktree)│   │(worktree) │              │
│   └──────────┘   └───────┬───────┘   └─────┬────┘              │
│                          │                  │                    │
│                   ┌──────▼──────┐    ┌──────▼──────┐            │
│                   │  Gate:      │    │  Gate:      │            │
│                   │  review OK? │    │  tests pass?│            │
│                   └──────┬──────┘    └──────┬──────┘            │
│                          │                  │                    │
│              ┌───────────▼──────────────────▼─────────┐         │
│              │ sdd-done (merge to dev) + create PR    │         │
│              └───────────────────┬────────────────────┘         │
│                                  │                              │
│              ┌───────────────────▼────────────────────┐         │
│              │ pr-review (verify AC compliance)       │         │
│              │  ├─ ✅ → add comment, label, notify    │         │
│              │  └─ ❌ → convert to draft, loop back   │         │
│              └───────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                          Human reviews PR, approves, merges
```

---

## Phase 1: `/sdd-jira` Command

### Purpose

Interactive planner that converts a Jira ticket into a fully specified,
task-decomposed, worktree-ready feature — with enough detail that `sdd-worker`
can execute without asking questions.

### Usage

```
/sdd-jira NAV-8036
/sdd-jira NAV-8036 --auto-approve    # skip manual spec approval (risky)
/sdd-jira NAV-8036 --complexity=fix  # hint: simple fix, minimal Q&A
```

### Flow

#### 1. Fetch Jira Context

```bash
# Fetch ticket via Jira REST API (same pattern as /pr-review)
curl -s -u "$JIRA_USERNAME:$JIRA_TOKEN" \
  "$JIRA_SERVER_URL/rest/api/3/issue/$JIRA_KEY?expand=renderedFields" \
  | jq '{
    key: .key,
    summary: .fields.summary,
    description: .renderedFields.description,
    acceptance_criteria: (.fields.customfield_10021 // .fields.customfield_10022 // .fields.customfield_10035),
    type: .fields.issuetype.name,
    priority: .fields.priority.name,
    components: [.fields.components[].name],
    labels: .fields.labels,
    subtasks: [.fields.subtasks[].key],
    links: [.fields.issuelinks[] | {type: .type.name, key: (.inwardIssue.key // .outwardIssue.key)}]
  }'
```

#### 2. Classification & Complexity Assessment

Before asking questions, classify the ticket:

| Signal | Complexity | Q&A Rounds | Task Count |
|--------|-----------|------------|------------|
| Bug fix, 1 component, clear AC | `fix` | 1 | 1-2 |
| Small feature, 2-3 components | `simple` | 2 | 2-4 |
| Cross-cutting feature, vague AC | `standard` | 2-3 | 3-8 |
| Architecture change, new abstractions | `complex` | 3+ | 5+ |

The `--complexity` flag overrides auto-detection.

#### 3. Interactive Q&A (adapted from sdd-brainstorm)

The Q&A adapts to complexity:

**For `fix` complexity:**
- 1 round, 2-3 questions: root cause hypothesis, affected files, regression risk
- Skip brainstorm doc — go straight to spec

**For `simple`/`standard`:**
- 2+ rounds as defined in sdd-brainstorm
- Focus on: scope boundaries, integration points, edge cases
- Generate brainstorm doc with approaches

**For `complex`:**
- 3+ rounds
- Include architectural tradeoff discussion
- May suggest breaking into multiple tickets

#### 4. Generate Spec with "Worker-Ready" Detail

This is the critical difference from regular `/sdd-spec`. The spec must include
enough detail that `sdd-worker` is pre-authorized to implement without asking:

##### a) Mandatory Pseudo-Code Sections

Every section in "Architectural Design" must include pseudo-code with
detailed comments, not just prose descriptions:

```markdown
## 4. Architectural Design

### 4.1 OAuth Callback Handler

```python
# File: parrot/integrations/jira/oauth.py
# Extends: aiohttp route handler

async def handle_oauth_callback(request: web.Request) -> web.Response:
    """
    Receives the OAuth 2.0 authorization code from Jira's redirect.
    
    Flow:
    1. Extract `code` and `state` from query params
    2. Validate `state` against Redis-stored CSRF token
    3. Exchange `code` for access_token via Jira's token endpoint
    4. Store token in Redis keyed by user_id (from state)
    5. Return success page / redirect to original context
    
    Error handling:
    - Missing code → 400 with "Authorization denied" message
    - Invalid state → 403 with "CSRF validation failed"
    - Token exchange failure → 502 with retry hint
    """
    code = request.query.get('code')
    state = request.query.get('state')
    
    # Validate CSRF
    stored_state = await redis.get(f"oauth:state:{state}")
    if not stored_state:
        raise web.HTTPForbidden(text="Invalid OAuth state")
    
    user_id = json.loads(stored_state)['user_id']
    
    # Exchange code for token
    token_response = await exchange_oauth_code(
        code=code,
        redirect_uri=OAUTH_REDIRECT_URI,  # from env
        client_id=JIRA_OAUTH_CLIENT_ID,
        client_secret=JIRA_OAUTH_CLIENT_SECRET,
    )
    
    # Store token
    await credential_store.save(
        user_id=user_id,
        provider='jira',
        credentials=token_response,
        ttl=token_response.get('expires_in', 3600)
    )
    
    return web.Response(text="Authorization successful. You can close this tab.")
`` `
```

##### b) Explicit "Does NOT Exist" Section (Anti-Hallucination)

```markdown
## 6. Codebase Contract

### Does NOT Exist (verified)
- `parrot.auth.OAuthManager` — does not exist, must be created
- `AbstractToolkit.set_credentials()` — no such method
- `parrot.tools.context.permission_context` — planned but not yet implemented
- `JiraToolkit.oauth_mode` — no such attribute; auth is set at __init__
```

##### c) Acceptance Criteria Mapping

Each AC from Jira gets mapped to a specific task:

```markdown
## 7. AC-to-Task Mapping

| Jira AC | Task | Verification Method |
|---------|------|---------------------|
| User can authenticate via OAuth | TASK-001 | Integration test: mock OAuth flow |
| Session expires after 30 min | TASK-002 | Unit test: TTL check |
| Error shown on invalid credentials | TASK-001 | Unit test: error handler |
```

#### 5. Generate Tasks (delegates to /sdd-task)

After spec approval, automatically runs task decomposition.

**Pre-authorization guarantee**: Each task file includes:
- Complete pseudo-code (not just "implement X")
- Verified codebase contract (file paths + line numbers)
- Input/output examples
- Test skeleton

#### 6. Create Worktree

```bash
git worktree add -b feat-<FEAT-ID>-<slug> \
  .claude/worktrees/feat-<FEAT-ID>-<slug> HEAD
```

#### 7. Output & Handoff

```
✅ /sdd-jira complete for NAV-8036

   Jira: NAV-8036 — "Add OAuth 2.0 support for JiraToolkit"
   Spec: sdd/specs/jira-oauth.spec.md (committed to dev)
   Tasks: 4 tasks generated (committed to dev)
   Worktree: .claude/worktrees/feat-071-jira-oauth

   AC Coverage: 3/3 criteria mapped to tasks

   To execute autonomously:
     cd .claude/worktrees/feat-071-jira-oauth
     claude --agent sdd-autopilot --verbose

   Or manually:
     cd .claude/worktrees/feat-071-jira-oauth
     claude --agent sdd-worker --verbose
```

---

## Phase 2: `sdd-autopilot` Agent

### Purpose

Autonomous orchestrator that chains: worker → reviewer → QA → done → pr-review.
Runs inside the feature worktree. Each sub-agent is invoked as a separate
Claude Code process via `claude -p` or `claude --agent`.

### Agent Definition

File: `.claude/agents/sdd-autopilot.md`

```markdown
---
name: sdd-autopilot
description: |
  End-to-end autonomous feature pipeline. Orchestrates sdd-worker,
  code-reviewer, qa-runner, sdd-done, and pr-review sequentially
  with quality gates between each stage.
model: sonnet
color: green
permissionMode: bypassPermissions
tools: Read, Write, Edit, Bash, Glob, Grep
---
```

### Orchestration Engine

The autopilot doesn't implement features itself — it orchestrates other agents
via bash and reads their outputs to make decisions. The core loop is:

```bash
#!/bin/bash
# Conceptual model of what sdd-autopilot does internally.
# The agent executes these steps using bash tool calls.

FEAT_ID="$1"
WORKTREE_DIR=$(pwd)
REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || echo "$WORKTREE_DIR")
MAX_REVIEW_ITERATIONS=2
REVIEW_ITERATION=0

# ──────────────────────────────────────────────
# Stage 1: IMPLEMENT (sdd-worker)
# ──────────────────────────────────────────────
claude --agent sdd-worker \
  --model sonnet \
  --verbose \
  --output-format json \
  -p "Implement all tasks for $FEAT_ID" \
  2>&1 | tee .autopilot/worker-output.log

WORKER_EXIT=$?
if [[ $WORKER_EXIT -ne 0 ]]; then
    echo "❌ sdd-worker failed. Check .autopilot/worker-output.log"
    exit 1
fi

# ──────────────────────────────────────────────
# Stage 2: CODE REVIEW (code-reviewer)
# ──────────────────────────────────────────────
while [[ $REVIEW_ITERATION -lt $MAX_REVIEW_ITERATIONS ]]; do
    claude --agent code-reviewer \
      --model sonnet \
      -p "Review all code in this worktree for $FEAT_ID. \
          Check: correctness, security, performance, tests. \
          Output a structured report to .autopilot/review-report.md \
          with verdict: APPROVED | NEEDS_CHANGES" \
      2>&1 | tee .autopilot/review-output.log

    VERDICT=$(grep -oP '(?<=verdict:\s)(APPROVED|NEEDS_CHANGES)' .autopilot/review-report.md)

    if [[ "$VERDICT" == "APPROVED" ]]; then
        echo "✅ Code review passed (iteration $((REVIEW_ITERATION + 1)))"
        break
    fi

    echo "⚠️ Code review: NEEDS_CHANGES (iteration $((REVIEW_ITERATION + 1)))"
    
    # Feed review findings back to worker for fixes
    claude --agent sdd-worker \
      --model sonnet \
      -p "Fix the issues found in .autopilot/review-report.md for $FEAT_ID. \
          Only fix the issues listed — no scope creep." \
      2>&1 | tee .autopilot/fix-output-$REVIEW_ITERATION.log

    REVIEW_ITERATION=$((REVIEW_ITERATION + 1))
done

if [[ "$VERDICT" != "APPROVED" ]]; then
    echo "❌ Code review not approved after $MAX_REVIEW_ITERATIONS iterations."
    echo "   Human review required. See .autopilot/review-report.md"
    exit 2
fi

# ──────────────────────────────────────────────
# Stage 3: QA / TESTING (qa-runner)
# ──────────────────────────────────────────────
claude --agent qa-runner \
  --model sonnet \
  -p "Run test suite for $FEAT_ID. Execute: \
      1. Unit tests for all new/modified files \
      2. Integration tests if test fixtures exist \
      3. Linting (ruff check + mypy) \
      Output report to .autopilot/qa-report.md \
      with verdict: PASS | FAIL" \
  2>&1 | tee .autopilot/qa-output.log

QA_VERDICT=$(grep -oP '(?<=verdict:\s)(PASS|FAIL)' .autopilot/qa-report.md)

if [[ "$QA_VERDICT" != "PASS" ]]; then
    echo "❌ QA failed. See .autopilot/qa-report.md"
    echo "   Attempting auto-fix..."
    
    claude --agent sdd-worker \
      --model sonnet \
      -p "Fix failing tests described in .autopilot/qa-report.md for $FEAT_ID." \
      2>&1 | tee .autopilot/qa-fix-output.log
    
    # Re-run QA once
    claude --agent qa-runner \
      --model sonnet \
      -p "Re-run test suite for $FEAT_ID after fixes." \
      2>&1 | tee .autopilot/qa-rerun-output.log

    QA_VERDICT=$(grep -oP '(?<=verdict:\s)(PASS|FAIL)' .autopilot/qa-report.md)
    if [[ "$QA_VERDICT" != "PASS" ]]; then
        echo "❌ QA still failing after fix attempt. Human intervention required."
        exit 3
    fi
fi

# ──────────────────────────────────────────────
# Stage 4: MERGE TO DEV + CREATE PR (sdd-done)
# ──────────────────────────────────────────────
# Push feature branch
git push origin HEAD

# Create PR against dev
PR_URL=$(gh pr create \
  --base dev \
  --head "$(git branch --show-current)" \
  --title "feat($FEAT_ID): $(cat sdd/specs/*.spec.md | head -1 | sed 's/# //')" \
  --body-file .autopilot/review-report.md \
  2>&1)

echo "PR created: $PR_URL"

# ──────────────────────────────────────────────
# Stage 5: PR REVIEW against Jira AC (pr-review)
# ──────────────────────────────────────────────
# Extract Jira key from spec metadata
JIRA_KEY=$(grep -oP '(?<=jira:\s)\S+' sdd/specs/*.spec.md || echo "")

if [[ -n "$JIRA_KEY" ]]; then
    claude -p "/pr-review $PR_URL $JIRA_KEY --auto-draft" \
      2>&1 | tee .autopilot/pr-review-output.log
fi

# ──────────────────────────────────────────────
# Stage 6: NOTIFY
# ──────────────────────────────────────────────
echo "
╔══════════════════════════════════════════════════╗
║  sdd-autopilot complete for $FEAT_ID             ║
╠══════════════════════════════════════════════════╣
║  Worker:      ✅ All tasks implemented            ║
║  Review:      ✅ Approved (iter: $REVIEW_ITERATION)║
║  QA:          ✅ $QA_VERDICT                       ║
║  PR:          $PR_URL                             ║
║  Jira review: $([ -n "$JIRA_KEY" ] && echo "✅" || echo "⏭ skipped") ║
╚══════════════════════════════════════════════════╝

Next: Review and merge the PR.
"
```

### Gate System

Each stage produces a checkpoint file in `.autopilot/`:

```
.autopilot/
├── state.json              # Current pipeline state
├── worker-output.log       # sdd-worker stdout
├── review-report.md        # Code review findings
├── review-output.log       # code-reviewer stdout
├── qa-report.md            # Test results
├── qa-output.log           # qa-runner stdout
├── pr-review-output.log    # PR review stdout
├── fix-output-0.log        # Fix iteration 0
└── fix-output-1.log        # Fix iteration 1
```

**`state.json` schema:**

```json
{
  "feature_id": "FEAT-071",
  "jira_key": "NAV-8036",
  "started_at": "2026-04-14T10:30:00Z",
  "stages": {
    "worker": {
      "status": "completed",
      "started_at": "...",
      "completed_at": "...",
      "tasks_completed": 4,
      "tasks_total": 4
    },
    "review": {
      "status": "completed",
      "iterations": 1,
      "verdict": "APPROVED",
      "critical_issues": 0,
      "major_issues": 2,
      "minor_issues": 5
    },
    "qa": {
      "status": "completed",
      "verdict": "PASS",
      "tests_run": 23,
      "tests_passed": 23,
      "tests_failed": 0,
      "coverage": "87%"
    },
    "pr": {
      "status": "completed",
      "url": "https://github.com/Trocdigital/.../pull/4029",
      "number": 4029
    },
    "pr_review": {
      "status": "completed",
      "ac_met": 3,
      "ac_total": 3,
      "verdict": "APPROVED"
    }
  },
  "current_stage": "completed",
  "exit_code": 0
}
```

### Resume Capability

If the autopilot is interrupted, it can resume from the last completed stage:

```bash
# sdd-autopilot reads state.json on startup
# If worker completed but review hasn't started → skip to review
# If review completed but QA hasn't → skip to QA
```

---

## New Agent: `qa-runner`

### Purpose

Runs the test suite, linting, and type checking for a feature's changed files.
Produces a structured report.

### Agent Definition

File: `.claude/agents/qa-runner.md`

```markdown
---
name: qa-runner
description: |
  QA agent that validates a feature implementation by running tests,
  linting, and type checking. Produces a structured qa-report.md.
model: sonnet
color: yellow
tools: Read, Bash, Glob, Grep
---

# QA Runner

You validate feature implementations. You do NOT fix code — you report issues.

## Process

1. Read the spec and task files to understand what was implemented.
2. Identify all new/modified files from git diff.
3. Run the test suite:
   a. `pytest` for the specific test files related to the feature
   b. `pytest --tb=short` for a quick full-suite sanity check
   c. `ruff check` on modified files
   d. `mypy --strict` on modified files (if mypy config exists)
4. Verify acceptance criteria can be tested:
   - Each AC should have at least one test
   - Flag AC without test coverage

## Output

Write `.autopilot/qa-report.md`:

```markdown
# QA Report: FEAT-<ID>

**verdict: PASS | FAIL**

## Test Results
- Unit tests: 15/15 passed
- Integration tests: 3/3 passed
- Linting: 0 errors, 2 warnings
- Type checking: 0 errors

## Coverage
- New code coverage: 87%
- Files without tests: [list]

## AC Test Coverage
| AC | Has Test | Test File | Status |
|----|----------|-----------|--------|
| ... | ✅/❌ | ... | PASS/FAIL |

## Issues Found
- [file:line] description (severity)
`` `
```

---

## Pre-Authorization Model

### The Problem

`sdd-worker` runs with `permissionMode: bypassPermissions` but still needs
to understand WHAT to build. If tasks are vague ("implement OAuth support"),
the worker either asks questions (blocking automation) or guesses (producing
wrong code).

### The Solution: Task Specification Depth

The `/sdd-jira` command ensures each task contains enough detail that the
worker never needs to ask:

#### Level 1 — Minimum (current sdd-task)
```markdown
## Scope
- Create `oauth.py` in `parrot/integrations/jira/`
- Implement OAuth callback handler
```

#### Level 2 — Worker-Ready (required for autopilot)
```markdown
## Scope
- **CREATE** `parrot/integrations/jira/oauth.py`
- **MODIFY** `parrot_tools/jiratoolkit.py` — add `_resolve_credentials()` method

## Pseudo-Code

### oauth.py
```python
# 1. Route handler: POST /oauth/jira/callback
# 2. Extract code + state from query params
# 3. Validate state against Redis (key: oauth:state:{state_value})
# 4. Exchange code → access_token via POST to:
#      https://auth.atlassian.com/oauth/token
#    Body: grant_type=authorization_code, code=..., redirect_uri=...
# 5. Store token in Redis (key: oauth:token:{user_id}, TTL from expires_in)
# 6. Return HTML success page
`` `

### jiratoolkit.py modifications
```python
# In JiraToolkit.__init__:
#   Add self.credential_resolver: Optional[CredentialResolver] = None
#   If credential_resolver is provided, skip basic_auth/token_auth setup
#
# New method _resolve_credentials(self, user_id: str) -> Dict:
#   If self.credential_resolver:
#     return await self.credential_resolver.resolve('jira', user_id)
#   Else:
#     return {'auth_type': self.auth_type, 'token': self.token, ...}
`` `

## Codebase Contract
### Verified Imports
- `from parrot_tools.toolkit import AbstractToolkit` — packages/ai-parrot-tools/src/parrot_tools/toolkit.py:15
- `from parrot.tools.manager import ToolManager` — packages/ai-parrot/src/parrot/tools/manager.py:1

### Does NOT Exist
- `parrot.auth.CredentialResolver` — must be created in this task
- `AbstractToolkit.credentials` — no such attribute
- `JiraToolkit.refresh_token()` — no such method

## Test Skeleton
```python
@pytest.mark.asyncio
async def test_oauth_callback_valid_code():
    """Mock Jira token endpoint, send valid code, verify token stored in Redis."""
    ...

@pytest.mark.asyncio
async def test_oauth_callback_invalid_state():
    """Send request with invalid state, expect 403."""
    ...
`` `
```

### Complexity-Based Detail Level

| Complexity | Pseudo-code | Test Skeleton | Contract | Estimated Worker Prompts |
|-----------|-------------|---------------|----------|--------------------------|
| `fix` | Diff-level (3-5 lines) | Optional | Minimal | 0-1 |
| `simple` | Method-level | Required | Imports only | 0-2 |
| `standard` | Full (Level 2) | Required | Full | 0 |
| `complex` | Full + sequence diagrams | Required | Full + diagrams | 0-1 |

---

## Execution Modes

### Full Autonomous (for fixes and simple features)

```bash
# Human does: review Jira ticket, approve spec
/sdd-jira NAV-8036

# Then walk away
cd .claude/worktrees/feat-071-jira-oauth
claude --agent sdd-autopilot --verbose
# ... coffee ...
# Come back to a ready-to-review PR
```

### Semi-Autonomous (for standard features)

```bash
# Planning phase (interactive)
/sdd-jira NAV-8036

# Run worker + reviewer only
cd .claude/worktrees/feat-071-jira-oauth
claude --agent sdd-worker --verbose
claude --agent code-reviewer -p "Review feat-071"

# Human reviews code, then:
/sdd-done FEAT-071
/pr-review <PR_URL> NAV-8036
```

### Background Execution (tmux)

```bash
cd .claude/worktrees/feat-071-jira-oauth
tmux new -s feat-071 \
  "claude --agent sdd-autopilot --verbose 2>&1 | tee .autopilot/full.log"
# Ctrl+B, D to detach
# tmux attach -t feat-071 to check progress
```

---

## Error Recovery & Safety

### Retry Budget

| Stage | Max Retries | On Exhaustion |
|-------|-------------|---------------|
| Worker | 0 (tasks have internal retries) | STOP, report |
| Review | 2 iterations (review → fix → review) | STOP, mark PR draft |
| QA | 1 (fix → rerun) | STOP, report |
| PR creation | 1 | STOP, manual PR |
| PR review | 0 | Comment posted regardless |

### Blast Radius Control

The autopilot NEVER:
- Merges PRs (human does this)
- Modifies code on `main` or `dev` directly
- Deletes branches or worktrees
- Pushes with `--force`
- Modifies files outside the feature scope

The autopilot CAN:
- Push the feature branch
- Create PRs (against `dev`)
- Add PR comments and labels
- Convert PRs to draft (with `--auto-draft`)
- Update Jira ticket status (optional, with `--update-jira`)

### Notification

On completion (success or failure), the autopilot can notify via:

```bash
# Option 1: Jira comment
curl -s -u "$JIRA_USERNAME:$JIRA_TOKEN" \
  -X POST "$JIRA_SERVER_URL/rest/api/3/issue/$JIRA_KEY/comment" \
  -H "Content-Type: application/json" \
  -d "{\"body\": {\"type\": \"doc\", \"version\": 1, \"content\": [...]}}"

# Option 2: GitHub PR comment (already done by pr-review)

# Option 3: Slack webhook (if configured)
curl -s -X POST "$SLACK_WEBHOOK_URL" \
  -d "{\"text\": \"sdd-autopilot completed $FEAT_ID: $PR_URL\"}"
```

---

## Files to Create

| File | Type | Purpose |
|------|------|---------|
| `.claude/commands/sdd-jira.md` | Command | Interactive planner |
| `.claude/agents/sdd-autopilot.md` | Agent | Autonomous orchestrator |
| `.claude/agents/qa-runner.md` | Agent | Test execution & reporting |
| `.claude/commands/pr-review.md` | Command | PR vs Jira AC review |

---

## Comparison with ai-parrot AgentCrew

| Aspect | AgentCrew (ai-parrot) | sdd-autopilot (Claude Code) |
|--------|----------------------|----------------------------|
| Runtime | Python asyncio | Bash + Claude CLI processes |
| Agent type | `BasicAgent` / `AbstractBot` | `.claude/agents/*.md` files |
| Context passing | In-memory `AgentContext` | Files on disk (`.autopilot/`) |
| Dependency graph | `task_flow()` + DAG resolver | Sequential with gates |
| Error handling | `on_error` transitions | Exit codes + retry loops |
| Parallelism | `asyncio.gather()` | Not needed (sequential pipeline) |
| Observability | `ExecutionMemory` + logs | `state.json` + log files |
| Resume | No | Yes (reads `state.json`) |
| LLM for synthesis | Optional synthesis step | PR review = synthesis |
