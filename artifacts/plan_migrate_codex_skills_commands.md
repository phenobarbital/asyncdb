# Migrate Codex skills and commands

## Objective

Merge the Codex and agent skill/command material currently present in
`../ai-parrot` into this repository.

## Scope

- Merge `../ai-parrot/.agent/skills/` into `.agent/skills/`.
- Merge `../ai-parrot/.agent/workflows/` into `.agent/workflows/`.
- Copy native Codex skills from `../ai-parrot/.agents/skills/`.
- Merge `../ai-parrot/.claude/commands/` into `.claude/commands/`.
- Copy the source Codex agent definition and config from `.codex/`.

Destination-only skills are retained. Source caches, worktrees, Claude
settings, agents, hooks, and rules are outside this request and are excluded.
The destination `.agent/CONTEXT.md` remains asyncdb-specific.

## Risks and checks

- Some source commands and skills describe AI-Parrot paths; scan the migrated
  files for those references and report them for follow-up if adaptation would
  require repository-specific design decisions.
- Source workflows currently contain uncommitted changes; use the source
  working tree as supplied.
- Exclude Python caches and bytecode from the migration.
- Verify file inventories, preservation of destination-only files, and Git
  status after the merge.

## Result

- All source files in the scoped directories are present in the destination
  with identical contents.
- Destination-only legacy skills were preserved.
- Native Codex TOML parses successfully and the migrated native skills pass the
  available skill validator.
- `git diff --check` passes for tracked command and plan changes.
- The source contains AI-Parrot-specific references in some migrated skills and
  commands; these were retained verbatim because translating them to asyncdb
  would require product and architecture decisions beyond a file migration.
