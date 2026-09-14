---
id: F001
query_id: Q001
type: grep | glob | read | git_log | tree
intent: <copied from the query's intent>
executed_at: YYYY-MM-DDTHH:MM:SSZ
duration_ms: 234
parent_id: null   # set if this finding came from a recursive follow-up
depth: 0
---

# F001 — <one-line title derived from query intent>

## Summary

<2-4 sentences. What did this query reveal? Be concrete and information-dense.
The synthesis agent will read MANY findings; brevity matters.>

## Citations

> **Format rule**: every path/symbol the synthesis agent might reference must
> appear here. The lint phase rejects synthesis output that names paths absent
> from any finding's citations.

- path: `app/modules/nextstop/pdf.py`
  lines: 47-89
  symbol: `generate_pdf`
  excerpt: |
    async def generate_pdf(trip: Trip) -> bytes:
        """Generate PDF for a trip."""
        chunks = await self._build_chunks(trip)
        rendered = await render(chunks)   # <-- await on sync render
        return rendered

- path: `app/modules/nextstop/pdf.py`
  lines: 142-160
  symbol: `render_chunk`
  excerpt: |
    def render_chunk(chunk: Chunk) -> bytes:
        ...

## Notes

<Optional: anything not fitting into Citations. Cross-references to other
findings, edge cases observed, hypotheses spawned. Keep brief.>

---

<!-- =================== TEMPLATE GUIDE =================== -->
<!--
Type-specific content guidelines:

## type: grep
Summary: how many matches, where they cluster, what symbols dominate.
Citations: 5-15 representative match locations. Don't dump all matches.

## type: glob
Summary: how many files, breakdown by directory.
Citations: full file list (tree-style if helpful).

## type: read
Summary: 2-3 sentence description of what the file does.
Citations: line ranges for the most important blocks (function defs,
class definitions, key constants). Excerpts ≤10 lines each.

## type: git_log
Summary: how many commits, who authored, time range, dominant theme.
Citations: per-commit entry with SHA, date, author, message, files touched.
Optional: include diff hunks for the most relevant commits if --with_diff.

## type: tree
Summary: top-level structure observed.
Citations: directory tree, max 30 lines.

## Content rules

1. NEVER paste the full content of a read file. Excerpts only.
2. Excerpts ≤10 lines each. If more is needed, that's a sign you should split
   into multiple targeted reads.
3. Always preserve line numbers — they're how the synthesis agent grounds
   claims spatially.
4. If a query returned NOTHING (no matches, empty file, etc.), still produce
   a finding — set Summary to "no matches found for <pattern>". This is
   useful evidence (proves absence).
5. If a query FAILED (path doesn't exist, git error), record it under
   Summary with the error and skip Citations.

-->
