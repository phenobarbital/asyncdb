---
description: Full security audit of the ai-parrot monorepo — dependencies, secrets, agent-framework attack surface, CI/release supply chain, plus the config check
argument-hint: "[--scope deps|secrets|code|ci|config|all] [--report <path>]"
allowed-tools: Bash, Read, Grep, Glob, Write
---

# /security-audit — Full Monorepo, 3–8 min

Senior application-security pass over the whole `ai-parrot` uv workspace:
11 published distributions, 3 Rust crates, a Go bridge, 5 GitHub Actions
workflows that publish to PyPI, and the Claude Code config.

Produce a **scored** report with a prioritized remediation plan. Save it to
`artifacts/logs/security-audit-<YYYY-MM-DD>.md` (or `--report <path>`).

Scope defaults to `all`; `--scope` runs a single phase.

---

## Phase 0 — Map the surface

```bash
ls packages/
find packages -name Cargo.toml -not -path '*/target/*'
ls services/ .github/workflows/
source .venv/bin/activate && python scripts/release.py status
```

Record which distributions are **published to PyPI** — a vulnerability in one
of those ships to every downstream installer, which raises its severity by one
level over the same issue in an unpublished path.

---

## Phase 1 — Config (delegates to `/security-check`)

Run every phase of `.claude/commands/security-check.md`: agent privileges,
hook exfiltration, command/skill injection, memory poisoning, MCP, permissions.
Carry its findings into the score.

**Score**: 0 CRITICAL and 0 HIGH → +20 · any HIGH → +10 · any CRITICAL → 0

---

## Phase 2 — Secrets across the workspace

```bash
# Provider key shapes — the high-confidence patterns
grep -rnE 'sk-ant-[A-Za-z0-9_-]{20,}|sk-[A-Za-z0-9]{32,}|ghp_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{50,}|AKIA[A-Z0-9]{16}|xox[bpsa]-[A-Za-z0-9-]{20,}|AIza[A-Za-z0-9_-]{35}' \
  --include='*.py' --include='*.toml' --include='*.yaml' --include='*.yml' \
  --include='*.json' --include='*.md' --include='*.sh' --include='*.rs' --include='*.go' \
  --exclude-dir={.venv,.git,node_modules,build,dist,target,__pycache__,.claude} . 2>/dev/null | head -30

# Assignment-shaped literals (noisier — triage each)
grep -rnE '(api[_-]?key|password|passwd|secret|token)\s*=\s*["'"'"'][^"'"'"'$\{][^"'"'"']{12,}["'"'"']' \
  --include='*.py' --include='*.yaml' --include='*.yml' \
  --exclude-dir={.venv,.git,build,dist,target,tests,__pycache__} packages/ 2>/dev/null | head -30

grep -rn 'BEGIN [A-Z ]*PRIVATE KEY' --exclude-dir={.venv,.git,target} . 2>/dev/null

# .env hygiene
git ls-files | grep -E '(^|/)\.env($|\.)' || echo "OK: no .env tracked"
find . -name '.env*' -not -path '*/.venv/*' -not -path '*/.git/*' -not -name '.env.example' -type f 2>/dev/null

# Anything a past commit still carries
git log --all --oneline -S'sk-ant-' -- . 2>/dev/null | head
```

Every hit needs triage: a test fixture, a docs placeholder and a live key look
identical to grep. Report only what you confirmed by reading the line. The
current baseline is AWS's own documentation placeholder key in
`packages/ai-parrot-tools/tests/pulumi/test_config.py` and
`packages/ai-parrot/tests/test_security_base_executor.py` — a fixture, not a
finding. (`dangerous-actions-blocker.sh` allowlists the same placeholder
shapes, so grepping for them does not trip the hook.)

**Score**: 0 live secrets → +20 · 1–3 → +10 · 4+ → 0 · private key committed → −10

**If a live key is found**: it is compromised the moment it is in git history.
Rotating it is the fix; deleting the line is not. Say so explicitly.

---

## Phase 3 — Dependency and supply chain

```bash
source .venv/bin/activate

# Python — pip-audit is not installed by default; uvx runs it without polluting the venv
uvx pip-audit --strict 2>&1 | tail -40 || echo "pip-audit unavailable"

# Rust (3 crates: navrules, yaml-rs, codec-rs)
for c in packages/navrules/rust packages/ai-parrot/src/parrot/yaml-rs packages/ai-parrot/src/parrot/codec-rs; do
  [ -f "$c/Cargo.toml" ] && (cd "$c" && cargo audit 2>&1 | tail -15)
done

# Go bridge
[ -d services/whatsapp-bridge ] && (cd services/whatsapp-bridge && go list -m all 2>/dev/null | head)

# Lockfile integrity
git status --porcelain uv.lock
```

Then the **workspace-specific** checks a generic audit misses:

```bash
# Unpinned / floating deps in published packages
grep -nE '^\s*"[a-zA-Z0-9_.-]+"\s*,' packages/*/pyproject.toml | head -30
# Direct-from-git or URL dependencies (bypass PyPI review entirely)
grep -rnE '@\s*git\+|\bgit\+https|http://' packages/*/pyproject.toml
# Core pins in lockstep with the core version
grep -rn 'ai-parrot>=' packages/*/pyproject.toml
source .venv/bin/activate && python scripts/release.py status
```

- [ ] A published package depending on a `git+` URL? → **HIGH** (unreviewable,
      mutable source shipped to PyPI users)
- [ ] Any `http://` dependency URL? → **CRITICAL**
- [ ] `ai-parrot>=` pins behind the current core version? → **MEDIUM**
      (`python scripts/release.py bump patch` re-syncs them)

**Score**: 0 vulns → +20 · only low/medium → +12 · any high → +5 · any critical → 0

---

## Phase 4 — Agent-framework attack surface

This is the phase a generic audit does not have. AI-Parrot *is* the untrusted
input boundary: it executes tools, loads skills from disk, consumes third-party
MCP servers and OpenAPI specs, and talks A2A. Model output reaching any of
these is an injection sink.

```bash
source .venv/bin/activate

# 1. Dynamic execution reachable from model-controlled input
grep -rnE '\b(eval|exec)\s*\(|__import__\s*\(|pickle\.loads|yaml\.load\s*\((?!.*Loader)|marshal\.loads' \
  --include='*.py' packages/*/src/ | grep -v test | head -30

# 2. Shell execution — check every call site for shell=True with an f-string
grep -rnE 'subprocess\.(run|Popen|call|check_output)|os\.system|shell\s*=\s*True' \
  --include='*.py' packages/*/src/ | grep -v test | head -30

# 3. The parrot.tools -> parrot_tools -> plugins.tools meta_path redirect:
#    an import-hijack surface. Confirm it cannot resolve outside those roots.
sed -n '1,120p' packages/ai-parrot/src/parrot/tools/__init__.py

# 4. Skills are loaded from disk into the prompt — where from?
grep -rn 'learned_dir\|skills_dir\|SkillsDirectoryLoader\|assets_dir' \
  --include='*.py' packages/*/src/parrot/skills/ | head -20

# 5. Path traversal in the sandboxed skill-asset reader
grep -rn 'read_skill_asset' -A 30 --include='*.py' packages/*/src/parrot/skills/ | head -50

# 6. Outbound HTTP: CLAUDE.md mandates aiohttp; requests/httpx signal an
#    unreviewed path (and a blocking call in an async context)
grep -rnE '^\s*import requests|^\s*import httpx|requests\.(get|post)' \
  --include='*.py' packages/*/src/ | grep -v test | head -20

# 7. SSRF: user/model-supplied URLs fetched without an allowlist
grep -rn 'OpenAPIToolkit\|from_openapi\|spec_url' --include='*.py' packages/*/src/ | head -20

# 8. Guardrails still wired in (they live under bots/guardrails/)
ls packages/ai-parrot/src/parrot/bots/guardrails/
grep -rn 'injection' --include='*.py' packages/*/src/ -l | head
```

Rate each by whether **model-controlled or remote data can reach it**:

- [ ] `eval` / `exec` / `pickle.loads` on anything derived from a tool result,
      an LLM response, or a loaded document → **CRITICAL**
- [ ] `shell=True` with an interpolated variable that traces back to model
      output → **CRITICAL**
- [ ] `yaml.load` without `SafeLoader` on external content → **HIGH**
- [ ] Skill/asset reader that accepts `..` or an absolute path → **HIGH**
- [ ] A remote OpenAPI spec or MCP server URL fetched with no allowlist →
      **HIGH** (SSRF + the returned spec becomes tool definitions)
- [ ] Credentials logged: `self.logger.*` interpolating a key/token → **HIGH**
- [ ] `requests`/`httpx` in an async path → **MEDIUM** (convention violation
      per CLAUDE.md, and a blocked event loop)

**Do not report a grep hit as a finding.** Read the call site and state the
concrete path from untrusted input to the sink, or drop it.

**Score**: no reachable sink → +25 · reachable but mitigated → +15 · one
reachable unmitigated → +5 · several → 0

---

## Phase 5 — CI and release supply chain

`release.yml` fires on `release: [created]` and publishes 12 build jobs to
PyPI with `id-token: write` (trusted publishing) — so anything that can create
a release, or inject a step into that workflow, can publish as this project.

```bash
ls .github/workflows/
# Actions pinned to a tag vs a SHA — a moved tag is remote code in your release
grep -rnE 'uses:\s*[^@]+@' .github/workflows/ | grep -vE '@[0-9a-f]{40}' | head -30
# Elevated permissions
grep -rn -B3 -A3 'permissions:' .github/workflows/
# Secrets referenced
grep -rnE 'secrets\.[A-Z_]+' .github/workflows/
# Triggers that run on untrusted PR content with write access
grep -rn 'pull_request_target\|workflow_run' .github/workflows/
# Any step that runs a script from the PR branch
grep -rnE 'run:.*(\./|bash |python )' .github/workflows/ | head -20
```

- [ ] `pull_request_target` combined with a checkout of the PR head? →
      **CRITICAL** (fork code runs with repo secrets)
- [ ] Third-party action pinned to a **tag** rather than a full SHA? → **MEDIUM**
      (`actions/*` and `pypa/*` are first-party; a random third-party action is
      **HIGH**)
- [ ] `permissions:` broader than the job needs? → **MEDIUM**
- [ ] Branch protection on `main`/`staging`? CLAUDE.md says it should require
      PRs, CI and signed commits but is **not configured declaratively** —
      verify and report:
      ```bash
      gh api repos/:owner/:repo/branches/main/protection 2>&1 | head -20
      ```
      Unprotected `main` on a repo that auto-publishes to PyPI → **HIGH**
- [ ] CodeQL (`codeql-analysis.yml`) still running and green?
      ```bash
      gh run list --workflow=codeql-analysis.yml --limit 3
      ```

**Score**: all clean → +15 · minor gaps → +8 · CRITICAL trigger or unprotected
publishing branch → 0

---

## Phase 6 — Score and report

Total out of 100 (20 config + 20 secrets + 20 deps + 25 framework + 15 CI):

| Score | Posture |
|---|---|
| 90–100 | Strong |
| 70–89 | Good, gaps to close |
| 50–69 | Needs work |
| < 50 | At risk — stop feature work and remediate |

Write to `artifacts/logs/security-audit-<date>.md`:

```markdown
# Security Audit — ai-parrot

Date: <ISO>   Branch: <branch>   Commit: <sha>
Score: <N>/100 — <posture>

## Findings by severity
### CRITICAL
- **<title>** — `<file>:<line>`
  - Path from untrusted input: <concrete chain>
  - Impact: <what an attacker gets>
  - Fix: <exact change>

### HIGH / MEDIUM / LOW
  (same shape)

## Phase scores
| Phase | Score | Notes |
|---|---|---|
| Config | /20 | |
| Secrets | /20 | |
| Dependencies | /20 | |
| Framework surface | /25 | |
| CI & release | /15 | |

## Verified clean
- <checks that passed — the coverage signal>

## Remediation plan
1. <highest severity × lowest effort first, with the command>

## Not covered
- <what this audit did not reach, and why>
```

---

## Rules

- **Never** paste a discovered secret value into the report — reference
  `file:line` and its shape (`sk-ant-…`, 108 chars).
- Every finding needs a **concrete failure path**, not a pattern match.
  "`eval` appears in `x.py:42`" is not a finding; "a tool result reaches
  `eval` at `x.py:42` via `y()`" is.
- Distinguish **reachable** from **present**. Say which you established.
- If a scanner is missing (`pip-audit`, `cargo audit`), report the phase as
  **not covered** rather than as passing.
- Read-only by default. `--fix` is not supported here — remediation in this
  scope needs human judgment.

## Related

| Command | Scope |
|---|---|
| `/security-audit` | Full monorepo (THIS) |
| `/security-check` | Config only, ~60s |
| `/security-review` | Built-in review of the current branch diff |
| `/code-review` | Correctness and quality, not security posture |
