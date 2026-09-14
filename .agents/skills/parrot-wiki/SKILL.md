---
name: parrot-wiki
description: Query the repository LLM-wiki before raw source scans, and save durable knowledge into it.
---

# Parrot Wiki

Start codebase investigations with `wikitoolkit query "<focused question>"`
or the native MCP tools (`wiki_query`, `wiki_page`, `wiki_related`,
`wiki_symbol_lookup`, `wiki_code_outline`, `wiki_blast_radius`). Fall back to
raw search only after those paths are empty.

The wiki is also persistent memory. Save durable facts, decisions, and lessons
with `wikitoolkit remember "<fact>" --category <note|decision|lesson|concept>`
or the `wiki_remember` MCP tool. Use `wikitoolkit note`, `wikitoolkit link`,
`wikitoolkit memories`, and `wikitoolkit audit` to maintain and review that knowledge.
