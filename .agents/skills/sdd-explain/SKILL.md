---
name: sdd-explain
description: Explain how a subsystem, component, or symbol works in the current project, grounded in the real codebase with architecture maps or deep traces.
---

# SDD Explain

Use this skill when the user asks to explain how a subsystem, component, or symbol works in the project, onboarding architecture explanations, or deep code traces.

Invocation: `sdd-explain [--deep] <subsystem | component | symbol | question>`.

## Purpose

Provide code-grounded explanations of how the repository actually works right now. Produces Subsystem Maps (architecture/onboarding) by default, or Implementation Traces with `--deep`.

## Guardrails

- **Read before explaining**: Never describe an import path, class, method, registry, or decorator without locating and reading it first.
- **Cite with grep anchors**: Reference symbols by name and file path, not brittle line numbers.
- **State evidence**: Explicitly state which files and symbols were read.
- **No unsolicited fixes/code**: This skill explains; it does not refactor or modify code.

## Workflow

1. Parse arguments:
   - Check for `--deep` (or `-d`) flag for Implementation Trace mode.
   - Extract target subsystem, component, or symbol.
2. Locate code:
   - Use `find_by_name`, `grep_search`, or `rg` to find real symbols and files.
   - Read the source files.
3. Subsystem Map Mode (default):
   - Locate: package entry points and anchor symbols.
   - What it is: single responsibility and system role.
   - The cast: concrete ABCs, registries, mixins, managers.
   - Data & control flow: end-to-end request lifecycle and boundaries.
   - Conventions & invariants: architectural constraints and why they exist.
   - Where to look next: key files and adjacent subsystems.
4. Implementation Trace Mode (`--deep`):
   - Target resolution: exact symbols and files resolved.
   - Execution trace: call-by-call walk through async boundaries, decorators, dispatches.
   - Contracts & types: Pydantic models, protocol signatures, validation rules.
   - Edge cases & failure modes: error handling, fallbacks, timeouts.
   - Coupling map: dependencies and callers.
   - Gotchas: non-obvious behaviors and historical pitfalls.

## References

- `AGENTS.md`
- `.agent/CONTEXT.md`
- `sdd/WORKFLOW.md`
