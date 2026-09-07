---
# SDD flow type and base branch (FEAT-145).
# - type: feature  (default)  → base_branch: dev (or any non-main branch)
# - type: hotfix              → base_branch MUST be: main
type: feature
base_branch: dev
---

# Feature Specification: <Feature Name>

**Feature ID**: FEAT-<NNN>
**Date**: YYYY-MM-DD
**Author**: <name>
**Status**: draft | review | approved
**Target version**: x.y.z

---

## 1. Motivation & Business Requirements

> Why does this feature exist? What problem does it solve?

### Problem Statement
<!-- Describe the pain point or capability gap -->

### Goals
- Goal 1
- Goal 2

### Non-Goals (explicitly out of scope)
- Non-goal 1

---

## 2. Architectural Design

### Overview
<!-- High-level description of the solution approach -->

### Component Diagram
```
ComponentA ──→ ComponentB ──→ ComponentC
                   │
                   └──→ ComponentD
```

### Integration Points
<!-- How does this feature integrate with existing AI-Parrot components? -->

| Existing Component | Integration Type | Notes |
|---|---|---|
| `AbstractClient` | extends | ... |
| `AgentCrew` | uses | ... |

### Data Models
```python
# Key data structures / Pydantic models
class FeatureModel(BaseModel):
    field: type
```

### New Public Interfaces
```python
# New classes/functions exposed to users
class NewComponent:
    async def method(self, param: Type) -> ReturnType:
        ...
```

---

## 3. Module Breakdown

> Define the discrete modules that will be implemented.
> These directly map to Task Artifacts in Phase 2.

### Module 1: <Name>
- **Path**: `parrot/path/to/module.py`
- **Responsibility**: What this module does
- **Depends on**: existing module or Module N from this spec

### Module 2: <Name>
- **Path**: `parrot/path/to/module2.py`
- **Responsibility**: ...
- **Depends on**: Module 1

---

## 4. Test Specification

### Unit Tests
| Test | Module | Description |
|---|---|---|
| `test_component_init` | Module 1 | Validates initialization with valid config |
| `test_component_error` | Module 1 | Handles invalid input gracefully |

### Integration Tests
| Test | Description |
|---|---|
| `test_end_to_end_flow` | Full pipeline from input to output |

### Test Data / Fixtures
```python
# Key fixtures needed
@pytest.fixture
def sample_config():
    return {...}
```

---

## 5. Acceptance Criteria

> This feature is complete when ALL of the following are true:

- [ ] All unit tests pass (`pytest tests/unit/ -v`)
- [ ] All integration tests pass (`pytest tests/integration/ -v`)
- [ ] Documentation updated in `docs/`
- [ ] No breaking changes to existing public API
- [ ] Performance benchmark: <metric> within <threshold>
- [ ] Criterion N

---

## 6. Codebase Contract

> **CRITICAL — Anti-Hallucination Anchor**
> This section is the single source of truth for what exists in the codebase.
> Implementation agents MUST NOT reference imports, attributes, or methods
> not listed here without first verifying they exist via `grep` or `read`.

### Verified Imports
<!-- Exact import statements confirmed to work. Agents MUST use these verbatim. -->
```python
from parrot.module import ClassName  # verified: parrot/module/__init__.py:NN
```

### Existing Class Signatures
<!-- Exact signatures of classes/methods that tasks will extend or call.
     Include attribute types and method signatures with line numbers. -->
```python
# parrot/path/to/file.py
class ExistingClass(BaseClass):
    attribute: Type  # line NN
    async def method(self, param: Type) -> ReturnType:  # line NN
```

### Integration Points
<!-- How new code connects to existing code. Specify exact method calls. -->
| New Component | Connects To | Via | Verified At |
|---|---|---|---|
| `NewClass` | `ExistingClass.method()` | method call | `path/file.py:NN` |

### Does NOT Exist (Anti-Hallucination)
<!-- Things that look plausible but DO NOT exist in the codebase.
     Prevents agents from inventing imports or attributes. -->
- ~~`parrot.module.NonExistentThing`~~ — does not exist
- ~~`ClassName.phantom_method()`~~ — not a real method

---

## 7. Implementation Notes & Constraints


### Patterns to Follow
- Use `AbstractBase` pattern from `parrot/base/`
- Follow async-first design throughout
- Pydantic models for all structured data
- Comprehensive logging with `self.logger`

### Known Risks / Gotchas
- Risk 1 and mitigation
- Risk 2 and mitigation

### External Dependencies
| Package | Version | Reason |
|---|---|---|
| `package` | `>=x.y` | why needed |

---

## 8. Open Questions

> Questions that must be resolved before or during implementation.

- [ ] Question 1 — *Owner: name*
- [ ] Question 2 — *Owner: name*

---

## Revision History

| Version | Date | Author | Change |
|---|---|---|---|
| 0.1 | YYYY-MM-DD | name | Initial draft |
