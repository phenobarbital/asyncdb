---
# SDD flow type and base branch (FEAT-145).
# - type: feature  (default)  → base_branch: dev (or any non-main branch)
# - type: hotfix              → base_branch MUST be: main
type: feature
base_branch: dev
---

# Brainstorm: <Title>

**Date**: YYYY-MM-DD
**Author**: <name>
**Status**: exploration | accepted | rejected
**Recommended Option**: <Option Letter>

---

## Problem Statement

<!-- What problem are we solving? Who is affected?
     Why is this needed now? Be specific about the pain point or opportunity. -->

## Constraints & Requirements

<!-- Hard constraints that any solution must satisfy.
     e.g. performance targets, compatibility, budget, timeline, security. -->

- Constraint 1
- Constraint 2

---

## Options Explored

### Option A: <Name>

<!-- Describe this approach in detail. Focus on WHAT it does, not HOW to code it. -->

✅ **Pros:**
- Benefit 1
- Benefit 2

❌ **Cons:**
- Drawback 1

📊 **Effort:** Low | Medium | High

📦 **Libraries / Tools:**
| Package | Purpose | Notes |
|---|---|---|
| `package-name` | What it does | version, maturity, etc. |

🔗 **Existing Code to Reuse:**
- `path/to/module.py` — description of what to reuse

---

### Option B: <Name>

<!-- Describe this approach in detail. -->

✅ **Pros:**
- Benefit 1

❌ **Cons:**
- Drawback 1

📊 **Effort:** Low | Medium | High

📦 **Libraries / Tools:**
| Package | Purpose | Notes |
|---|---|---|
| `package-name` | What it does | version, maturity, etc. |

🔗 **Existing Code to Reuse:**
- `path/to/module.py` — description of what to reuse

---

### Option C: <Name>

<!-- Describe this approach in detail. -->

✅ **Pros:**
- Benefit 1

❌ **Cons:**
- Drawback 1

📊 **Effort:** Low | Medium | High

📦 **Libraries / Tools:**
| Package | Purpose | Notes |
|---|---|---|
| `package-name` | What it does | version, maturity, etc. |

🔗 **Existing Code to Reuse:**
- `path/to/module.py` — description of what to reuse

---

## Recommendation

**Option <X>** is recommended because:

<!-- Explain the reasoning. Reference tradeoffs from the options above.
     Be honest about what you're trading off. -->

---

## Feature Description

<!-- Detailed explanation of the feature as it would be built using the recommended option.
     This should be thorough enough to feed directly into a spec.
     Cover: user-facing behavior, internal behavior, edge cases, error handling. -->

### User-Facing Behavior
<!-- What does the end user see or experience? -->

### Internal Behavior
<!-- How does it work at a high level? No implementation code — describe flow and responsibilities. -->

### Edge Cases & Error Handling
<!-- What happens when things go wrong? Boundary conditions? -->

---

## Capabilities

### New Capabilities
<!-- Capabilities being introduced.
     Use kebab-case identifiers (e.g., user-auth, data-export).
     Each accepted capability maps to a spec file at docs/sdd/specs/<name>.spec.md -->
- `<name>`: <brief description>

### Modified Capabilities
<!-- Existing capabilities whose requirements change.
     Use existing spec names from docs/sdd/specs/. Leave empty if none. -->

---

## Impact & Integration

<!-- Which existing components are affected?
     Are there breaking changes? New dependencies? Deployment changes?
     Consider: APIs, data models, configuration, CI/CD. -->

| Affected Component | Impact Type | Notes |
|---|---|---|
| `component` | extends / modifies / depends on | ... |

---

## Code Context

<!-- CRITICAL: This section preserves verified code references so they survive
     the brainstorm → spec → task pipeline and prevent hallucinations during implementation.
     Include ONLY code that has been verified to exist in the current codebase. -->

### User-Provided Code
<!-- Paste any code snippets the user provided during brainstorming.
     Tag each with its source (file path if known, or "user-provided"). -->

```python
# Source: <file_path or "user-provided">
# <paste code here>
```

### Verified Codebase References
<!-- Actual signatures, imports, and attributes discovered during codebase research.
     Each entry MUST include the file path and line number where it was verified. -->

#### Classes & Signatures
```python
# From parrot/path/to/file.py:NN
class ExistingClass(BaseClass):
    attribute: Type  # line NN
    async def method(self, param: Type) -> ReturnType:  # line NN
        ...
```

#### Verified Imports
```python
# These imports have been confirmed to work:
from parrot.module import ClassName  # parrot/module/__init__.py:NN
```

#### Key Attributes & Constants
<!-- List attributes/constants that tasks will need to reference -->
- `ClassName.attribute_name` → `Type` (parrot/path/file.py:NN)

### Does NOT Exist (Anti-Hallucination)
<!-- List things that might seem like they should exist but DO NOT.
     This prevents implementing agents from assuming these are available. -->
- ~~`parrot.module.NonExistentThing`~~ — does not exist
- ~~`ClassName.phantom_attribute`~~ — not a real attribute

---

## Parallelism Assessment

<!-- Evaluate the feature's decomposition potential for parallel development.
     This informs sdd-spec's worktree strategy section. -->

- **Internal parallelism**: <!-- Can this feature's tasks be split into independent worktrees? -->
- **Cross-feature independence**: <!-- Does this feature conflict with any in-flight specs? List shared files or modules. -->
- **Recommended isolation**: <!-- per-spec | mixed -->
- **Rationale**: <!-- Brief explanation of why the recommended isolation makes sense. -->

---

## Open Questions

<!-- Anything unresolved. Each should have an owner if possible.
     Convention (important — consumed by /sdd-spec):
       [ ] unresolved question — *Owner: name*
       [x] resolved question — *Owner: name*: <answer text>
     When you resolve a question, flip the checkbox to [x] and append the
     answer after the final `:` on the owner line. /sdd-spec carries these
     forward into the spec body instead of re-asking them. -->
- [ ] Question 1 — *Owner: name*
- [ ] Question 2 — *Owner: name*
