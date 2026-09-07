# TASK-<NNN>: <Title>

**Feature**: FEAT-<NNN> — <Feature Title>
**Spec**: `sdd/specs/<feature-slug>.spec.md`
**Status**: pending
**Priority**: high | medium | low
**Estimated effort**: S (< 2h) | M (2-4h) | L (4-8h) | XL (> 8h)
**Depends-on**: TASK-<X>, TASK-<Y>   *(or "none")*
**Assigned-to**: unassigned

---

## Context

> Why this task exists. Its role in the broader feature.
> Reference the spec section it implements.

---

## Scope

> Precisely what this task must implement. Nothing more.
> Use imperative language: "Implement X", "Add Y", "Refactor Z".

- Implement ...
- Add ...
- Write tests for ...

**NOT in scope**: (list things that might seem related but belong to other tasks)

---

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `parrot/path/to/new_file.py` | CREATE | Main implementation |
| `tests/unit/test_new_file.py` | CREATE | Unit tests |
| `parrot/path/to/existing.py` | MODIFY | Add import / register component |

---

## Codebase Contract (Anti-Hallucination)

> **CRITICAL**: This section contains VERIFIED code references from the actual codebase.
> The implementing agent MUST use these exact imports, class names, and method signatures.
> **DO NOT** invent, guess, or assume any import, attribute, or method not listed here.
> If you need something not listed, VERIFY it exists first with `grep` or `read`.

### Verified Imports
<!-- Exact import statements. Use these VERBATIM — do not guess alternatives. -->
```python
from parrot.module import ClassName  # verified: parrot/module/__init__.py:NN
```

### Existing Signatures to Use
<!-- Classes/methods this task extends, calls, or integrates with.
     Include the file path and line number for each. -->
```python
# parrot/path/to/file.py:NN
class ExistingClass(BaseClass):
    attribute: Type  # line NN
    async def method(self, param: Type) -> ReturnType:  # line NN
```

### Does NOT Exist
<!-- Things the agent might assume exist but DO NOT. Prevents hallucination. -->
- ~~`parrot.module.NonExistentThing`~~ — does not exist
- ~~`ClassName.phantom_attribute`~~ — not a real attribute

---

## Implementation Notes

> Technical guidance for the executing agent.

### Pattern to Follow
```python
# Reference implementation pattern from existing code
# e.g. copy this structure from parrot/loaders/base.py
class ExistingPattern(AbstractBase):
    async def method(self) -> Result:
        ...
```

### Key Constraints
- Must be async throughout
- Use Pydantic for all data models
- Follow existing naming conventions in the module
- Add `self.logger` calls at key points

### References in Codebase
- `parrot/path/reference1.py` — pattern to follow
- `parrot/path/reference2.py` — integration point

---

## Acceptance Criteria

- [ ] Implementation complete per scope
- [ ] All tests pass: `pytest <test_path> -v`
- [ ] No linting errors: `ruff check parrot/<path>`
- [ ] Imports work: `from parrot.<module> import <Component>`
- [ ] Criterion N

---

## Test Specification

> Minimal test scaffold. The agent must make these pass.
> Add more tests as needed.

```python
# tests/unit/test_<module>.py
import pytest
from parrot.<module> import <Component>


@pytest.fixture
def component():
    return <Component>(config={...})


class Test<Component>:
    def test_initialization(self, component):
        """Component initializes with valid config."""
        assert component is not None

    def test_main_behavior(self, component):
        """Describe main expected behavior."""
        result = component.method(input)
        assert result == expected

    async def test_async_operation(self, component):
        """Test async operations."""
        result = await component.async_method(input)
        assert result.status == "success"

    def test_error_handling(self, component):
        """Component handles invalid input gracefully."""
        with pytest.raises(ValueError, match="expected message"):
            component.method(invalid_input)
```

---

## Agent Instructions

When you pick up this task:

1. **Read the spec** at the path listed above for full context
2. **Check dependencies** — verify `Depends-on` tasks are in `tasks/completed/`
3. **Verify the Codebase Contract** — before writing ANY code:
   - Confirm every import in "Verified Imports" still exists (`grep` or `read` the source)
   - Confirm every class/method in "Existing Signatures" still has the listed attributes
   - If anything has changed, update the contract FIRST, then implement
   - **NEVER** reference an import, attribute, or method not in the contract without verifying it exists
4. **Update status** in `tasks/.index.json` → `"in-progress"` with your session ID
5. **Implement** following the scope, codebase contract, and notes above
6. **Verify** all acceptance criteria are met
7. **Move this file** to `tasks/completed/TASK-<NNN>-<slug>.md`
8. **Update index** → `"done"`
9. **Fill in the Completion Note** below

---

## Completion Note

*(Agent fills this in when done)*

**Completed by**: <session or agent ID>
**Date**: YYYY-MM-DD
**Notes**: What was implemented, any deviations from scope, issues encountered.

**Deviations from spec**: none | describe if any
