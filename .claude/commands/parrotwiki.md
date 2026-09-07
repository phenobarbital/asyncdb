---
description: Query or maintain the repository LLM-wiki knowledge graph (wikitoolkit)
argument-hint: [query <question> | page <id> | related <id> | remember <fact> | note <id> <text> | link <a> <b> | memories | audit | status | build | --wiki [dir]]
allowed-tools: Bash(wikitoolkit:*)
---

# /parrotwiki — codebase knowledge graph

Arguments: `$ARGUMENTS`

This repository has an LLM-wiki knowledge base built from the source
tree (see the "Codebase Knowledge Graph" section of CLAUDE.md).
Interpret the arguments as one of the following actions and run the
matching `wikitoolkit` command with Bash:

- `query <question>` — run `wikitoolkit query "<question>"`. Read the
  most promising results with `wikitoolkit page <id>` and answer the
  question citing page ids. Prefer this over grepping raw files.
- `page <id>` — run `wikitoolkit page <id>` and summarise it.
- `related <id>` — run `wikitoolkit related <id>` and explain how the
  neighbours connect.
- `status` — run `wikitoolkit status` and report plane health.
- `build` — run `wikitoolkit build` and report what changed.
- `remember <fact>` — save durable knowledge: run
  `wikitoolkit remember "<fact>" --category <note|decision|lesson|concept>`
  (add `--title` for a short handle and `--link <page_id>` to connect
  it to the pages it is about). Report the saved page id.
- `note <id> <text>` — run `wikitoolkit note <id> "<text>"` to append
  an attributed note to an existing page.
- `link <a> <b>` — run `wikitoolkit link <a> <b> --rel <relation>`
  to connect two pages (default relation `references`).
- `memories` — run `wikitoolkit memories` and summarise what has
  been saved.
- `audit` — run `wikitoolkit audit` and summarise recent writes.
- `--wiki [dir]` — build a human-readable markdown wiki from the
  graph: run `wikitoolkit export -o <dir>` (default `docs/wiki`) and
  list what was written.
- no arguments — run `wikitoolkit status` and briefly explain the
  available actions above.

If `wikitoolkit` reports the wiki is not built yet, run
`wikitoolkit build` first, then retry the requested action.
