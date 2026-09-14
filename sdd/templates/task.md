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

## Delegation Contract

> **OPTIONAL.** Include this section ONLY when the task is
> delegation-eligible: the design is COMPLETE (every target file listed,
> every implementation block containing the already-decided code, no open
> questions), hashes were computed at `/sdd-task` time, and they are
> re-verified at execution time. `design_complete: true` is a declaration
> the author signs — the validator checks structure, the thinking model
> verifies semantics.
>
> **Remove this section entirely if the task is not delegation-eligible.**
> A task without it takes the normal implementation route, which is the
> default and is never a failure.

```json
{
  "schema_version": 1,
  "task_id": "TASK-<NNN>",
  "spec_path": "sdd/specs/<feature>.spec.md",
  "design_complete": true,
  "targets": [
    {
      "path": "pkg/greeter.py",
      "action": "create",
      "expected_sha256": null,
      "planned_changes": "New module with greet()",
      "blocks": ["impl-greeter"]
    },
    {
      "path": "pkg/__init__.py",
      "action": "modify",
      "expected_sha256": "<sha256 — MUST BE REPLACED with the real digest>",
      "planned_changes": "Export greet",
      "blocks": ["impl-init"]
    }
  ],
  "references": [
    {
      "path": "pkg/__init__.py",
      "sha256": "<sha256 — MUST BE REPLACED with the real digest>",
      "start_line": 1,
      "end_line": 3,
      "purpose": "existing exports"
    }
  ],
  "implementation_blocks": ["impl-greeter", "impl-init"],
  "acceptance_criteria": ["pytest tests/test_greeter.py passes"],
  "validation_commands": [["pytest", "tests/test_greeter.py", "-q"]]
}
```

Compute every digest for real — the `<sha256 …>` placeholders above are
**rejected** by the validator as `invalid_packet`, which is the intended
safety net against shipping an unverified packet:

```bash
sha256sum pkg/__init__.py
# or, without coreutils:
python -c "import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" pkg/__init__.py
```

A `create` target MUST have a block tagged with `path=<target>`; that block
IS the new file's content. A `modify` target's blocks describe the decided
edit and MUST carry `expected_sha256`.

```python id=impl-greeter path=pkg/greeter.py
def greet(name: str) -> str:
    """Return a greeting."""
    return f"hello {name}"
```

```python id=impl-init
# Apply to pkg/__init__.py: add after the existing imports
from .greeter import greet

__all__ = [*__all__, "greet"]
```

Block ids match `^[a-z0-9][a-z0-9._-]{0,63}$` and must be unique in the
file. Blocks must contain no placeholders (`...` on its own line, `TODO`,
`FIXME`, `XXX`, `<angle placeholders>`, `raise NotImplementedError`) — the
validator rejects them as `placeholder_code`.


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

## Implementation Blueprint

> **CRITICAL — Executor-ready starting point.** Write each block below to its declared
> path nearly verbatim, then complete every `# FILL IN:` marker. Blocks were derived
> from the spec's Interface Skeletons and re-verified against the Codebase Contract
> above when this task was written. This is NOT the full implementation:
> business-logic branches, edge cases and test bodies are `FILL IN` stubs by design.
> Never change a signature, class name, or file path the blueprint fixes.

### Steps (in order)
1. <imperative step> — *why*: <one sentence>
2. <imperative step> — *why*: <one sentence>

### `parrot/path/to/new_file.py` (CREATE)
```python
"""<module docstring>."""
from __future__ import annotations

from parrot.module import ClassName  # verified: parrot/module/__init__.py:NN


class NewComponent(ClassName):
    """<one-line purpose>."""

    async def method(self, param: Type) -> ReturnType:
        """<what it returns and when it raises>."""
        self.logger.debug("method: %s", param)
        # FILL IN: <the exact decision left to you> — bounded by <constraint | AC-N>
        raise NotImplementedError
```
**Why this shape**: <2–4 sentences: which spec decision each block implements; what must NOT change>

### `parrot/path/to/existing.py` (MODIFY)
```python
# occurrences: <N> (verified: grep -c '<verbatim anchor line>' parrot/path/to/existing.py)
# AFTER — insert below `<verbatim anchor line>` (verified: parrot/path/to/existing.py:NN)
<new lines>
```
**Why**: <1–2 sentences>. If `<N>` > 1: replace the block above with
`# FILL IN: disambiguate — quote enough surrounding context (2–3 lines) to make the
anchor unique` instead of a bare one-line anchor.

### FILL IN checklist
- [ ] `new_file.py::NewComponent.method` — <decision>; bounded by <constraint | AC-N>

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
5. **Implement** — start from the Implementation Blueprint blocks, complete every
   `# FILL IN:` marker, and never change a signature or path the blueprint fixes
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
