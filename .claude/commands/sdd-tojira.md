---
description: Export an SDD Specification to a Jira Story (and optionally subtasks). Creates or updates a ticket, updates the spec with the Jira key, and commits the change.
---

# /sdd-tojira — Export Specification to Jira

Export the content of a formal specification file (`sdd/specs/*.spec.md`) to a new
or existing Jira ticket. Optionally creates subtasks from decomposed SDD tasks.

```
/sdd-spec → /sdd-task → /sdd-tojira → Jira Story + Subtasks
```

## Usage
```
/sdd-tojira sdd/specs/jira-oauth.spec.md
/sdd-tojira sdd/specs/jira-oauth.spec.md --ticket NAV-8036   # link to existing ticket
/sdd-tojira sdd/specs/jira-oauth.spec.md --with-subtasks     # also create subtasks from tasks
/sdd-tojira sdd/specs/jira-oauth.spec.md --project=NAVAI     # override project key
/sdd-tojira FEAT-071                                         # resolve by Feature ID
/sdd-tojira FEAT-071 --ticket NAV-8036 --with-subtasks       # full combo
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `<spec_path \| FEAT-ID>` | yes | Path to `.spec.md` or Feature ID to resolve |
| `--ticket <JIRA_KEY>` | no | Existing Jira ticket to update instead of creating |
| `--with-subtasks` | no | Create Jira sub-tasks from `sdd/tasks/.index.json` |
| `--project <KEY>` | no | Override default project key (default: `NAV`) |

## Guardrails
- The input must be a valid path to an existing `.spec.md` file, or a Feature ID.
- Do NOT create duplicate tickets — resolve existing ones first.
- Default target: Project `NAV`, Component `Nav-AI`, Issue Type `Story`.
- **Always commit the spec update** (with Jira key) so worktrees can see it.
- Do NOT modify existing Jira tickets unless the user explicitly requests an update
  OR the ticket was resolved via `--ticket` / spec metadata.

## Jira Access Strategy

Use **mcp-atlassian** if available, falling back to **curl** if not.

### Detect available method
```bash
# Check if mcp-atlassian tools are available
# If jira_create_issue tool exists → use MCP
# Otherwise → use curl with env vars
```

### MCP path (preferred)
```
jira_create_issue(project_key="NAV", summary="...", ...)
jira_search(jql="...", ...)
```

### curl fallback
```bash
# Requires env vars loaded via navconfig (env/.env):
#   JIRA_INSTANCE  — e.g. https://trocglobal.atlassian.net/
#   JIRA_USERNAME  — email for Jira Cloud
#   JIRA_API_TOKEN — API token (Personal Access Token)
#
# Load them:
#   eval "$(python -c "from navconfig import config; import os; [print(f'export {k}={v}') for k,v in os.environ.items() if k.startswith('JIRA_')]")"

JIRA_INSTANCE="${JIRA_INSTANCE%/}"
curl -s -u "$JIRA_USERNAME:$JIRA_API_TOKEN" \
  -H "Content-Type: application/json" \
  -X POST "$JIRA_INSTANCE/rest/api/3/issue" \
  -d '{ ... }'
```

## Steps

### 1. Resolve Spec File and Determine Mode

#### 1a. Resolve the spec

If the user passes a Feature ID instead of a path:
```bash
grep -rl "FEAT-071" sdd/specs/ | head -1
```

Read the spec file and extract all fields (see table in Step 2).

#### 1b. Resolve the Jira ticket (tri-mode resolution)

Determine whether to CREATE a new ticket or UPDATE an existing one.
Evaluate these sources in priority order — **first match wins**:

```
Priority 1: --ticket argument
   User passed --ticket NAV-8036
   → MODE = UPDATE, JIRA_KEY = NAV-8036

Priority 2: Spec metadata
   Spec frontmatter contains `jira: NAV-8036`
   OR spec body contains `**Jira**: [NAV-8036](...)`
   → MODE = UPDATE, JIRA_KEY = NAV-8036

Priority 3: Search by Feature ID
   jira_search(jql="project = NAV AND summary ~ \"FEAT-071\"")
   → If found:
       ⚠️  Existing ticket found: NAV-8036 — "[FEAT-071] jira-oauth"
           Status: In Progress | Assignee: jleon

           Options:
           1. Update — sync description and AC from spec (recommended)
           2. Skip — do nothing, just link spec to this ticket
           3. Create new — create a separate ticket (not recommended)

       Wait for user choice. Default: update.
   → MODE = UPDATE | SKIP | CREATE based on choice

Priority 4: No match
   → MODE = CREATE
```

#### 1c. Announce mode

```
📋 /sdd-tojira: FEAT-071 — jira-oauth

   Spec: sdd/specs/jira-oauth.spec.md
   Mode: UPDATE existing NAV-8036  |  CREATE new ticket
   Project: NAV
```

### 2. Extract Spec Content

| Field | Source in Spec | Maps to Jira |
|-------|---------------|--------------|
| Feature ID | Metadata header | Summary prefix: `[FEAT-NNN]` |
| Feature Name | `# <title>` | Summary |
| Section 1 | Motivation & Business Requirements | Description |
| Section 5 | Acceptance Criteria | AC custom field |
| Components | Module Breakdown / Impact | Jira components |
| Effort | Worktree Strategy or task index | Original estimate |

**Description format:**
```markdown
## Motivation

<Section 1 content>

## Architectural Overview

<Section 2 summary — first 2-3 paragraphs only, not full design>

## Acceptance Criteria

<Section 5 content>

---
_Exported from SDD spec: sdd/specs/<feature-name>.spec.md_
_Feature ID: FEAT-<ID>_
```

**Acceptance Criteria format** for the AC custom field:
```
# User can authenticate via OAuth 2.0
# Tokens are stored securely in Redis
# Token refresh happens automatically
```

**Estimate** from task index (if `--with-subtasks`):
```bash
TOTAL_SECONDS=$(echo "$TASKS" | jq '[.tasks[] | select(.feature_id=="FEAT-071") |
  if .effort=="S" then 14400
  elif .effort=="M" then 28800
  elif .effort=="L" then 57600
  elif .effort=="XL" then 115200
  else 28800 end] | add')
```
Default: `28800` (8h = 1 day) if no tasks exist.

### 3. Execute: CREATE or UPDATE

#### If MODE = CREATE

**MCP path:**
```
jira_create_issue(
    project_key="NAV",
    summary="[FEAT-071] jira-oauth — OAuth 2.0 support for JiraToolkit",
    issue_type="Story",
    description="<formatted description>",
    components="Nav-AI",
    additional_fields='{"timeoriginalestimate": "<TOTAL_SECONDS>"}'
)
```

**curl fallback:**
```bash
curl -s -u "$JIRA_USERNAME:$JIRA_API_TOKEN" \
  -H "Content-Type: application/json" \
  -X POST "$JIRA_INSTANCE/rest/api/3/issue" \
  -d '{
    "fields": {
      "project": {"key": "NAV"},
      "summary": "[FEAT-071] jira-oauth — OAuth 2.0 support for JiraToolkit",
      "issuetype": {"name": "Story"},
      "description": {"type": "doc", "version": 1, "content": [...]},
      "components": [{"name": "Nav-AI"}],
      "timeoriginalestimate": 28800
    }
  }'
```

Extract the created ticket key: `JIRA_KEY=$(echo "$RESPONSE" | jq -r '.key')`

#### If MODE = UPDATE

Update description and estimate on the existing ticket.

**Important**: In UPDATE mode, do NOT overwrite the summary — the user may have
customized it in Jira. Only update description, AC, estimate, and components.

**MCP path:**
```
jira_update_issue(
    issue_key="NAV-8036",
    description="<formatted description>",
    additional_fields='{"timeoriginalestimate": "<TOTAL_SECONDS>"}'
)
```

**curl fallback:**
```bash
curl -s -u "$JIRA_USERNAME:$JIRA_API_TOKEN" \
  -H "Content-Type: application/json" \
  -X PUT "$JIRA_INSTANCE/rest/api/3/issue/$JIRA_KEY" \
  -d '{
    "fields": {
      "description": {"type": "doc", "version": 1, "content": [...]},
      "timeoriginalestimate": 28800
    }
  }'
```

### 4. Set Acceptance Criteria

After creating or updating the ticket, set the AC custom field:

**MCP path:**
```
jira_update_issue(issue_key="<JIRA_KEY>", additional_fields='{"customfield_10021": "<formatted AC>"}')
```

**curl fallback:**
```bash
curl -s -u "$JIRA_USERNAME:$JIRA_API_TOKEN" \
  -H "Content-Type: application/json" \
  -X PUT "$JIRA_INSTANCE/rest/api/3/issue/$JIRA_KEY" \
  -d '{"fields": {"customfield_10021": "<formatted AC>"}}'
```

Try fields in order: `customfield_10021`, `customfield_10022`, `customfield_10035`.
Log which field worked for future reference.

### 5. Create Subtasks (if --with-subtasks)

If tasks exist in `sdd/tasks/.index.json` for this feature:

**Pre-check in UPDATE mode**: Check if subtasks already exist on the ticket.
If they do, only create the missing ones:
```
⚠️  NAV-8036 already has 2 subtasks:
    NAV-8037 — [TASK-001] OAuth callback handler
    NAV-8038 — [TASK-002] CredentialResolver abstraction

    Missing from Jira (will create):
    TASK-003 — JiraToolkit OAuth integration
    TASK-004 — Redis token storage

    Proceed? (y/N)
```

For each task without a Jira subtask:

**MCP path:**
```
jira_create_issue(
    project_key="NAV",
    summary="[TASK-001] OAuth callback handler",
    issue_type="Sub-task",
    parent="<JIRA_KEY>",
    description="<task description + scope>",
    additional_fields='{"timeoriginalestimate": "<effort_seconds>"}'
)
```

**curl fallback:**
```bash
curl -s -u "$JIRA_USERNAME:$JIRA_API_TOKEN" \
  -H "Content-Type: application/json" \
  -X POST "$JIRA_INSTANCE/rest/api/3/issue" \
  -d '{
    "fields": {
      "project": {"key": "NAV"},
      "parent": {"key": "<JIRA_KEY>"},
      "summary": "[TASK-001] OAuth callback handler",
      "issuetype": {"name": "Sub-task"},
      "description": {"type": "doc", "version": 1, "content": [...]},
      "timeoriginalestimate": 14400
    }
  }'
```

Map effort to seconds: S=4h(14400), M=8h(28800), L=16h(57600), XL=32h(115200).

### 6. Update Spec with Jira Key

Ensure the spec file has the Jira key. Skip if already present:

```bash
grep -q "^jira:" sdd/specs/<feature>.spec.md && echo "Already linked"
grep -q "^\*\*Jira\*\*:" sdd/specs/<feature>.spec.md && echo "Already linked"
```

If not present, add after the title:
```markdown
# FEAT-071 — OAuth 2.0 support for JiraToolkit

**Jira**: [NAV-8036](https://trocglobal.atlassian.net/browse/NAV-8036)
**Status**: approved
```

Or in YAML frontmatter: add `jira: NAV-8036`.

### 7. Update Task Index (if --with-subtasks)

Add Jira keys to each task entry in `sdd/tasks/.index.json`:

```json
{
  "id": "TASK-001",
  "feature_id": "FEAT-071",
  "jira_key": "NAV-8037",
  "jira_parent": "NAV-8036"
}
```

### 8. Commit Changes

```bash
# CRITICAL: Unstage everything first — NEVER commit unrelated changes
git reset HEAD
# Stage ONLY the modified SDD files — NEVER use "git add ." or "git add -A"
git add sdd/specs/<feature-name>.spec.md
# If subtasks were created:
git add sdd/tasks/.index.json
# Verify ONLY the expected files are staged
git diff --cached --name-only
# If ANY unrelated files appear, run "git reset HEAD" and start over
git commit -m "sdd: export FEAT-<ID> to Jira <JIRA_KEY>"
```

Skip commit if nothing changed (spec already had jira key, no new subtasks).

### 9. Output

#### CREATE mode
```
✅ Spec exported to Jira: NAV-8036 (created)
   https://trocglobal.atlassian.net/browse/NAV-8036

   Project: NAV | Component: Nav-AI | Type: Story
   Estimate: 3d (24h across 4 tasks)
   AC: 3 criteria exported

   Subtasks created:
     NAV-8037 — [TASK-001] OAuth callback handler [S/4h]
     NAV-8038 — [TASK-002] CredentialResolver abstraction [M/8h]
     NAV-8039 — [TASK-003] JiraToolkit OAuth integration [M/8h]
     NAV-8040 — [TASK-004] Redis token storage [S/4h]

   Spec updated and committed.

Next steps:
  1. Review the ticket in Jira.
  2. Assign and prioritize in your sprint.
  3. To implement: /sdd-start or use sdd-autopilot.
```

#### UPDATE mode
```
✅ Spec synced to Jira: NAV-8036 (updated)
   https://trocglobal.atlassian.net/browse/NAV-8036

   Updated: description, AC, estimate
   Subtasks: 2 existing + 2 created

   Spec already linked — no commit needed.
```

## Reverse Linking

The `jira:` metadata in the spec enables:
- `/pr-review` to auto-detect the Jira key from the spec
- `sdd-autopilot` to post completion comments back to Jira
- `/sdd-done` to optionally transition the Jira ticket to "Done"
- `/sdd-tojira` itself to detect UPDATE mode on re-runs (idempotent)

## Edge Cases

- **Spec not approved**: Warn and ask for confirmation.
- **No AC in spec**: Create ticket without AC. Warn.
- **mcp-atlassian not configured**: Fall back to curl. If env vars missing, error with setup instructions.
- **--ticket points to wrong project**: Warn about mismatch. Proceed but note it.
- **Subtask type not available**: Fall back to linked Tasks:
  ```
  jira_create_issue(issue_type="Task", ...)
  jira_link_issues(inward="NAV-8036", outward="NAV-8037", link_type="is parent of")
  ```
- **ADF vs Markdown**: Jira Cloud v3 requires ADF. Use v2 with markdown, or construct ADF JSON.
  mcp-atlassian handles conversion internally.
- **Custom field IDs differ**: AC auto-detection tries 10021 → 10022 → 10035. If all fail, skip AC.
- **Idempotent re-runs**: Second run detects existing key (Priority 2) → UPDATE mode. No duplicates.

## Reference
- Jira tool (MCP): `mcp_mcp-atlassian_jira_create_issue`
- Jira tool (ai-parrot): `JiraToolkit.jira_create_issue()`
- Spec template: `sdd/templates/spec.md`
- Task index: `sdd/tasks/.index.json`
- SDD methodology: `sdd/WORKFLOW.md`
- Auto-commit rule: `CLAUDE.md` (section "SDD Auto-Commit Rule")
