---
name: bookstore
description: Research the user's indexed book library with PageIndex and the bookstore MCP tools. Use for questions about what a book says, which books cover a topic, library comparisons, or cited passages; not for general source-code search or buying books.
---

# Research the indexed book library

Use the connected `bookstore` MCP server. Discover its tools by their
`bookstore_*` names; Codex may wrap them with a server namespace. The server
exposes seven read-only tools over project and global libraries.

## Choose books, search sections, read evidence

1. Start with `bookstore_catalog_search(query, top_k=8)` using topic keywords.
   This searches catalog cards through SQLite FTS5 without an LLM. Keep the
   returned `book_id` values. Use `bookstore_list_books()` for inventory or
   when a named book cannot be located by topic search.
2. Inspect `bookstore_get_toc(book_id)` for chapter titles, `node_id` values,
   and page ranges. Use `bookstore_get_card(book_id)` when you need the full
   summary, authors, or topics to compare candidate books.
3. Use `bookstore_search_book(book_id, query, top_k=8)` for a chosen book.
   For a question spanning books, use `bookstore_search(query, book_ids=[...],
   max_books=3)`; omit `book_ids` for automatic catalog shortlisting. Keep the
   book cap small because configured LLM search can make calls per book.
4. Read relevant matches with `bookstore_read_section(book_id, node_id)`.
   Keep each node paired with its book. Retrieve only the sections needed
   to answer, rather than loading whole books.

For “which book covers X?”, catalog cards may be sufficient. For claims about
what a book actually says, read section content; search summaries and scores
are navigation aids. If catalog search is empty, try fewer keywords or a title
lookup before concluding that the library has no relevant book.

## Cite the evidence

Cite *Book Title*, “Section Title”, pp. start–end using the titles and
`start_page` / `end_page` returned by the tools. When page numbers are absent
(common for Markdown), cite book and section only. Distinguish books when
comparing their advice. An empty `content` field is missing evidence; do not
invent a passage or present a catalog summary as a quotation.

## Degraded or disconnected search

- Without `PARROT_BOOKSTORE_LLM`, in-book/cross-book search is BM25-only and
  requires `bm25s`. If that dependency is missing, catalog search, cards,
  inventory, ToC, and section reads still work. Select sections from the ToC
  and disclose the narrower search when it affects the answer.
- If MCP is disconnected, run `parrot codex install --no-build` in the target repository,
  then restart Codex in that trusted project. CLI fallbacks are `bookstore search "topic" --catalog-only`,
  `bookstore list --json`, `bookstore show <book_id> --json`,
  `bookstore toc <book_id>`, and `bookstore search "question" --book <book_id>`.
  If the executable is missing, use the Python environment where ai-parrot is installed with
  `python -m parrot.knowledge.bookstore.cli` as the command prefix.
  The CLI has no section-read command: `show` returns a card, not book text.
  Report that limitation if source content cannot be retrieved.

## Library management

Indexing and removal are CLI-only. Perform them when requested, not as an
automatic side effect of a research question. `bookstore locations` shows
resolved paths; `bookstore add notes.md --no-llm` indexes deterministic
Markdown, and `bookstore add-folder ./books --dry-run` previews a batch.
PDF and other format ingestion may require an LLM and optional dependencies.

`PARROT_LIBRARY_DIR` overrides the project library. Otherwise it lives under
the active Git root at `.parrot/library`; the global library is under
`${PARROT_HOME:-~/.parrot}/library`. Project books win ID collisions. Optional
LLM configuration is `PARROT_BOOKSTORE_LLM="provider:model"` plus
`PARROT_BOOKSTORE_LLM_LIGHT` (a model ID from the same provider).
