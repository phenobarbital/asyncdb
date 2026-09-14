---
name: meta-prompting
description: Recursive prompt improvement with quality-driven iteration for Claude Code. Use when you need systematic, measurable improvement of LLM outputs through complexity routing, context extraction, and iterative refinement.
---

# Meta-Prompting Framework

Recursive prompt improvement through quality-driven iteration with automatic complexity routing.

## When to Use This Skill

- Task requires multiple refinements for quality
- Quality is critical (production code, important deliverables)
- Want systematic, measurable improvement
- First attempt was insufficient
- Building something complex that needs iterative approach
- Need to extract patterns and learnings from intermediate outputs

## Core Components

### 1. Complexity Analysis
Determine optimal strategy by analyzing task complexity (0.0-1.0):

| Level | Score | Characteristics | Strategy |
|-------|-------|-----------------|----------|
| SIMPLE | < 0.3 | Single-step, clear I/O | direct_execution |
| MEDIUM | 0.3-0.7 | Multi-step, design decisions | multi_approach_synthesis |
| COMPLEX | > 0.7 | Architectural, trade-offs | autonomous_evolution |

### 2. Context Extraction
Extract learnings from each iteration:
- Patterns identified
- Constraints discovered
- What worked well
- What needs improvement

### 3. Quality Assessment
Score outputs (0.0-1.0) on:
- Correctness
- Completeness
- Error handling
- Documentation
- Test coverage

### 4. Iteration Loop
```
Task → Analyze Complexity → Select Strategy → Generate
         ↑                                        ↓
         └── Extract Context ← Assess Quality ←──┘
                                   (quality < threshold?)
```

## Meta-Prompting Strategies

### Simple Tasks (direct_execution)
```
You are {skill}.

Task: {task}

Execute with clear, step-by-step reasoning:
1. Understand the requirements
2. Implement the solution
3. Verify correctness

Provide complete, working code.
```

### Medium Tasks (multi_approach_synthesis)
```
You are {skill} using meta-cognitive strategies.

Task: {task}

Approach:
1. Generate 2-3 different approaches
2. Evaluate strengths and weaknesses of each
3. Choose the optimal approach with justification
4. Implement the chosen solution
5. Include edge case handling and tests

{previous_context}
```

### Complex Tasks (autonomous_evolution)
```
You are {skill} performing autonomous problem evolution.

Task: {task}

Strategy:
1. Generate 3+ architectural hypotheses
2. For each hypothesis, identify:
   - Strengths and use cases
   - Weaknesses and failure modes
   - Key tradeoffs
3. Test hypotheses against constraints
4. Synthesize optimal solution from best elements
5. Document decision rationale

{previous_context}

Previous iteration learnings:
{extracted_patterns}
{improvements_needed}
```

## Usage

### Via Slash Command
```bash
/meta-prompt "Write function to validate email addresses"
/meta-prompt "Create priority queue" --threshold=0.95
/meta-prompt "Design rate limiter" --iterations=5 --skill=architect
```

### Via Python Engine
```python
from meta_prompting_engine.llm_clients.claude import ClaudeClient
from meta_prompting_engine.core import MetaPromptingEngine

llm = ClaudeClient(api_key="...")
engine = MetaPromptingEngine(llm)

result = engine.execute_with_meta_prompting(
    skill="python-programmer",
    task="Create a function to validate email addresses",
    max_iterations=3,
    quality_threshold=0.90
)

print(f"Quality: {result.quality_score}")
print(f"Iterations: {result.iterations}")
print(result.output)
```

## Configuration

Default settings:
```yaml
meta_prompting:
  max_iterations: 3
  quality_threshold: 0.90
  auto_stop: true
  complexity_thresholds:
    simple: 0.3
    medium: 0.7
```

## Real Test Results

**Palindrome Checker**: 2 iterations, 4,316 tokens, +21% quality improvement
**Find Maximum**: 2 iterations, 3,998 tokens, +20% quality improvement

## When NOT to Use

- Simple one-off tasks (just ask directly)
- Exploratory brainstorming
- Time-critical work (adds latency)
- Task is ambiguous (clarify first with /speckit.clarify)

## Sub-Skills

Located in `~/.claude/skills/meta-prompting/`:
- `analyze-complexity/` - Determine optimal strategy
- `assess-quality/` - Score output quality
- `extract-context/` - Extract patterns from outputs
- `meta-prompt-iterate/` - Full recursive workflow

## Source

Framework: `~/.local/meta-prompting-framework`
Engine: `~/.local/meta-prompting-framework/meta_prompting_engine/core.py`
