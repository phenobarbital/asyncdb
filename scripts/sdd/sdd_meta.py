"""SDD flow-type frontmatter parser shared across SDD commands and agents.

This module is the single source of truth for reading and emitting the
YAML frontmatter block that brainstorm/proposal/spec documents carry to
declare their SDD flow type and base branch.

The contract is intentionally tiny: a Pydantic model with a cross-field
validator, a forgiving ``parse`` that returns sensible defaults when no
frontmatter is present (so legacy specs keep working), and a symmetric
``emit`` used by generation commands when scaffolding new documents.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, model_validator

logger = logging.getLogger(__name__)


#: Canonical long-lived branches in the Git Parrot Flow (FEAT-187).
#: Commands use this for a soft warning when ``base_branch`` falls
#: outside the set; ``FlowMeta`` itself accepts any string so
#: sub-feature branches keep working (see CLAUDE.md).
KNOWN_BRANCHES: frozenset[str] = frozenset({"main", "staging", "dev"})

#: WorkKind -> (flow type, base branch). A bugfix is a hotfix and lands on
#: main; everything else is a feature and lands on dev. This mapping used to
#: live only as prose in .claude/agents/sdd-research.md (FEAT-466).
WORK_KIND_FLOW: dict[str, tuple[str, str]] = {
    "bug": ("hotfix", "main"),
    "enhancement": ("feature", "dev"),
    "new_feature": ("feature", "dev"),
}


class FlowMeta(BaseModel):
    """SDD flow metadata derived from a doc's YAML frontmatter."""

    type: Literal["feature", "hotfix"]
    base_branch: str

    @model_validator(mode="after")
    def _hotfix_implies_main(self) -> "FlowMeta":
        if self.type == "hotfix" and self.base_branch != "main":
            raise ValueError("type='hotfix' requires base_branch='main' " f"(got base_branch={self.base_branch!r})")
        return self


def parse(doc_path: Path) -> FlowMeta:
    """Parse YAML frontmatter from a brainstorm/proposal/spec markdown file.

    The frontmatter block, when present, must be the first thing in the
    file: a line containing only ``---``, the YAML body, and a closing
    ``---`` line. Anything before the opening ``---`` (including a UTF-8
    BOM or leading whitespace) means the file is treated as having no
    frontmatter and the defaults are returned.

    Args:
        doc_path: Path to the markdown file to inspect.

    Returns:
        ``FlowMeta(type="feature", base_branch="dev")`` when no
        frontmatter is present (backwards-compat for in-flight specs);
        otherwise a fully-validated ``FlowMeta``.

    Raises:
        pydantic.ValidationError: When frontmatter is present but
            invalid (e.g. ``type: hotfix`` without ``base_branch: main``).
    """
    text = doc_path.read_text(encoding="utf-8")
    if not text.startswith("---"):
        return FlowMeta(type="feature", base_branch="dev")
    parts = text.split("---", 2)
    if len(parts) < 3:
        return FlowMeta(type="feature", base_branch="dev")
    block = yaml.safe_load(parts[1]) or {}
    if not isinstance(block, dict):
        return FlowMeta(type="feature", base_branch="dev")
    return FlowMeta(**block)


def emit(meta: FlowMeta) -> str:
    """Render a ``FlowMeta`` as a Jekyll-style frontmatter block.

    The returned string is suitable for prepending to a brainstorm,
    proposal, or spec markdown file. It always ends with a newline so
    the existing document body can be concatenated directly.

    Args:
        meta: The flow metadata to serialize.

    Returns:
        A string of the form ``"---\\n<yaml>\\n---\\n"``.
    """
    body = yaml.safe_dump(meta.model_dump(), sort_keys=False).rstrip()
    return f"---\n{body}\n---\n"


def resolve_flow(
    *,
    kind: str | None = None,
    doc_path: Path | None = None,
    type_override: str | None = None,
    base_branch_override: str | None = None,
) -> FlowMeta:
    """Resolve the SDD flow type and base branch for a run.

    Precedence, highest first:
      1. ``type_override`` / ``base_branch_override`` (explicit caller intent)
      2. ``doc_path`` frontmatter, when the path is given AND exists
      3. ``WORK_KIND_FLOW[kind]``
      4. ``("feature", "dev")``

    Levels 1 and 2/3 compose: an explicit ``base_branch_override`` with no
    ``type_override`` keeps the type resolved from the lower level, and vice
    versa. This is what lets the console override only the base branch.

    Args:
        kind: A ``WorkKind`` value (``"bug"``/``"enhancement"``/
            ``"new_feature"``). Unknown or ``None`` falls through to the
            default.
        doc_path: Optional brainstorm/proposal/spec whose frontmatter should
            be consulted. A path that does not exist is treated as "no
            document", NOT as an error.
        type_override: Explicit ``"feature"``/``"hotfix"``.
        base_branch_override: Explicit branch name.

    Returns:
        A validated ``FlowMeta``.

    Raises:
        ValueError: When the resolved combination is invalid — e.g.
            ``type="hotfix"`` with a base branch other than ``"main"``. Raised
            by ``FlowMeta``'s own validator, not re-implemented here.
    """
    resolved_type: str | None = None
    resolved_base: str | None = None

    if doc_path is not None and doc_path.exists():
        from_doc = parse(doc_path)
        resolved_type, resolved_base = from_doc.type, from_doc.base_branch
    elif kind in WORK_KIND_FLOW:
        resolved_type, resolved_base = WORK_KIND_FLOW[kind]

    final_type = type_override or resolved_type or "feature"
    final_base = base_branch_override or resolved_base or "dev"

    if final_base not in KNOWN_BRANCHES:
        logger.warning(
            "base_branch %r is not one of the canonical branches %s; " "assuming a sub-feature branch.",
            final_base,
            sorted(KNOWN_BRANCHES),
        )
    return FlowMeta(type=final_type, base_branch=final_base)
